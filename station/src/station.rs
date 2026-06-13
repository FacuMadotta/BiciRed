use crate::connection::ConnectionActor;
use actix::prelude::*;
use common::*;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

const PENDING_RENTS_PREFIX: &str = "pending_rents_";
const PENDING_CHARGES_PREFIX: &str = "pending_charges_";
const OFFLINE_PREFIX: &str = "inventario_estacion_";
const TIMEOUT_SECS: u64 = 30; // Tiempo máximo para esperar un commit antes de abortar la transacción
const PRE_AUTH_AMOUNT_CENTS: u32 = 100; // Monto filo de Pre Autorización
const AMOUNT_PER_MINUTE_CENTS: u32 = 50; // Costo por minuto de alquiler

// Estructura que maneja la lógica de la estación, incluyendo el estado de los slots y las bicicletas.
pub struct Station {
    pub id: StationId,
    pub location: Location,
    pub slots: Vec<Slot>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Slot {
    pub index: usize,
    pub state: SlotState,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum SlotState {
    Empty,
    Occupied { bike_id: BikeId },
    Reserved { bike_id: BikeId },
}

impl Station {
    pub fn new(id: StationId, location: Location, num_slots: usize, num_bikes: usize) -> Self {
        let filename = format!("{}{}.json", OFFLINE_PREFIX, id);
        let mut slots = if let Ok(content) = std::fs::read_to_string(&filename) {
            println!(
                "[RECONECTANDO] Inventario previo detectado para estación {}. Cargando...",
                id
            );
            serde_json::from_str::<Vec<Slot>>(&content)
                .unwrap_or_else(|_| Self::default_slots(num_slots, num_bikes))
        } else {
            Self::default_slots(num_slots, num_bikes)
        };
        for slot in &mut slots {
            if let SlotState::Reserved { bike_id } = slot.state {
                println!(
                    "[RECONECTANDO] Revertida reserva del slot {} (Bici {}) debido a reinicio.",
                    slot.index, bike_id
                );
                slot.state = SlotState::Occupied { bike_id };
            }
        }
        Self {
            id,
            location,
            slots,
        }
    }

    fn default_slots(num_slots: usize, num_bikes: usize) -> Vec<Slot> {
        (0..num_slots)
            .map(|i| Slot {
                index: i,
                state: if i < num_bikes {
                    SlotState::Occupied {
                        bike_id: i as BikeId,
                    }
                } else {
                    SlotState::Empty
                },
            })
            .collect()
    }

    fn is_bike_available(&self, slot_index: usize) -> bool {
        matches!(
            self.slots.get(slot_index).map(|s| &s.state),
            Some(SlotState::Occupied { .. })
        )
    }

    fn reserve_bike(&mut self, slot_index: usize) -> Option<BikeId> {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Occupied { bike_id } = slot.state {
                slot.state = SlotState::Reserved { bike_id }; // Marcar el slot como reservado antes de commit
                return Some(bike_id);
            }
        }
        None
    }

    fn confirm_reservation(&mut self, slot_index: usize) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Reserved { .. } = slot.state {
                slot.state = SlotState::Empty; // Marcar el slot como vacío después de commit
            }
        }
    }

    fn is_slot_free(&self, slot_index: usize) -> bool {
        matches!(
            self.slots.get(slot_index).map(|s| &s.state),
            Some(SlotState::Empty)
        )
    }

    fn return_bike(&mut self, slot_index: usize, bike_id: BikeId) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            slot.state = SlotState::Occupied { bike_id }; // Marcar el slot como ocupado con la bicicleta devuelta
        }
    }

    fn cancel_reservation(&mut self, slot_index: usize, bike_id: BikeId) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Reserved { .. } = slot.state {
                slot.state = SlotState::Occupied { bike_id }; // Volver al estado ocupado con la misma bicicleta
            }
        }
    }

    fn calculate_amount(&self, start_secs: u64, end_secs: u64) -> u32 {
        let duration_secs = end_secs.saturating_sub(start_secs);
        let minutes = duration_secs.div_ceil(60);
        minutes as u32 * AMOUNT_PER_MINUTE_CENTS as u32
    }

    pub fn save_inventory(&self) {
        let filename = format!("{}{}.json", OFFLINE_PREFIX, self.id);
        if let Ok(json_content) = serde_json::to_string(&self.slots) {
            if let Err(e) = std::fs::write(&filename, json_content) {
                eprintln!(
                    "[ERROR PERSISTENCIA] No se pudo guardar el inventario: {}",
                    e
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub slot_index: usize,
    pub bike_id: BikeId,
    pub client_addr: Addr<ConnectionActor>, // Para saber a qué ConnectionActor responderle al final
    pub payment_voted_commit: bool,
    pub app_voted_commit: bool,
    pub started_at: SystemTime,
}

pub struct PendingValidation {
    pub msg: RequestMessage<RentRequest, ConnectionActor>,
    pub started_at: SystemTime,
}

pub struct StationActor {
    station: Station,
    central_server: Option<Sender<String>>,
    payment_service: Option<Sender<String>>,
    pending_transactions: HashMap<String, TransactionState>,
    server_addrs: Vec<String>,
    my_ip: String,
    payment_ip: String,
    pending_validations: HashMap<UserId, PendingValidation>,
}

impl StationActor {
    pub fn new(
        station: Station,
        server_addrs: Vec<String>,
        my_ip: String,
        payment_ip: String,
    ) -> Self {
        Self {
            station,
            central_server: None,
            payment_service: None,
            pending_transactions: HashMap::new(),
            server_addrs,
            my_ip,
            payment_ip,
            pending_validations: HashMap::new(),
        }
    }

    fn generate_rental_id(&self, bike_id: BikeId, user_id: UserId) -> String {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => format!("{}-{}-{}", bike_id, user_id, n.as_millis()),
            Err(_) => format!("{}-{}-{}", bike_id, user_id, 0), // fallback en caso de error
        }
    }

    fn client_id_from_rental(&self, rental_id: &str) -> Option<UserId> {
        rental_id.split('-').nth(1)?.parse().ok()
    }

    fn send_msg_to_payment(&mut self, msg: String) -> Result<(), ()> {
        if let Some(tx) = &self.payment_service {
            if tx.send(msg).is_err() {
                self.payment_service = None;
                return Err(());
            }
            Ok(())
        } else {
            Err(())
        }
    }

    fn check_transaction_state(&mut self, transaction_id: &str) {
        if self
            .pending_transactions
            .get(transaction_id)
            .map(|tx| tx.payment_voted_commit && tx.app_voted_commit)
            .unwrap_or(false)
        {
            // Si ambos votaron commit, confirmamos el alquiler
            if let Some(tx) = self.pending_transactions.remove(transaction_id) {
                self.station.confirm_reservation(tx.slot_index);
                self.station.save_inventory();
                tx.client_addr.do_send(RentConfirmed {
                    bike_id: tx.bike_id,
                    pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                    timestamp_secs: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Error al obtener tiempo")
                        .as_secs(),
                    rental_id: transaction_id.to_string(),
                });

                let commit_msg = CommitPayment {
                    transaction_id: transaction_id.to_string(),
                };

                let msg_serialized = commit_msg.serialize();
                let _ = self.send_msg_to_payment(msg_serialized);

                self.sync_with_central();
            }
        }
    }

    fn get_charges_filename(&self) -> String {
        format!("{}{}.json", PENDING_CHARGES_PREFIX, self.station.id)
    }

    fn get_rents_filename(&self) -> String {
        format!("{}{}.json", PENDING_RENTS_PREFIX, self.station.id)
    }

    fn get_json_rent(
        &self,
        rental_id: &str,
        user_id: UserId,
        bike_id: BikeId,
        card_token: &str,
    ) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error al obtener tiempo")
            .as_secs();
        format!(
            "{{\"rental_id\":\"{}\",\"user_id\":{},\"card_token\":\"{}\",\"bike_id\":{},\"timestamp\":{}}}\n",
            rental_id, user_id, card_token, bike_id, timestamp
        )
    }

    fn get_json_charge(&self, rental_id: &str, amount_cents: u32, bike_id: BikeId) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error al obtener tiempo")
            .as_secs();
        format!(
            "{{\"rental_id\":\"{}\",\"amount_cents\":{},\"bike_id\":{},\"timestamp\":{}}}\n",
            rental_id, amount_cents, bike_id, timestamp
        )
    }

    fn save_pending_rent(
        &self,
        rental_id: &str,
        user_id: UserId,
        bike_id: BikeId,
        card_token: &str,
    ) {
        let filename = self.get_rents_filename();
        let rent_json = self.get_json_rent(rental_id, user_id, bike_id, card_token);

        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)
            .and_then(|mut file| std::io::Write::write_all(&mut file, rent_json.as_bytes()))
            .expect("Error al guardar alquiler pendiente");
    }

    fn save_pending_charge(&self, rental_id: &str, amount_cents: u32, bike_id: BikeId) {
        let filename = self.get_charges_filename();
        let charge_json = self.get_json_charge(rental_id, amount_cents, bike_id);

        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(filename)
            .and_then(|mut file| std::io::Write::write_all(&mut file, charge_json.as_bytes()))
            .expect("Error al guardar cargo pendiente");
    }

    fn process_rents(&mut self) {
        let rents_filename = self.get_rents_filename();
        let mut lines_to_keep = Vec::<String>::new();
        if let Ok(contents) = std::fs::read_to_string(&rents_filename) {
            for line in contents.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                if let Ok(rent) = serde_json::from_str::<serde_json::Value>(line) {
                    let rental_id = rent
                        .get("rental_id")
                        .and_then(|v| v.as_str())
                        .expect("Error al parsear rental_id")
                        .to_string();
                    let card_token = rent
                        .get("card_token")
                        .and_then(|v| v.as_str())
                        .expect("Error al parsear card_token")
                        .to_string();

                    let bike_id = rent
                        .get("bike_id")
                        .and_then(|v| v.as_u64())
                        .expect("Error al parsear bike_id");

                    let payment_msg = ReservePayment {
                        card_token,
                        amount_cents: PRE_AUTH_AMOUNT_CENTS,
                        transaction_id: rental_id.clone(),
                    };
                    let msg_serialized = payment_msg.serialize();
                    let payment_ok = self.send_msg_to_payment(msg_serialized).is_ok();

                    let central_msg = OfflineRent {
                        rental_id: rental_id.clone(),
                        bike_id: bike_id as BikeId,
                        user_id: rent
                            .get("user_id")
                            .and_then(|v| v.as_u64())
                            .expect("Error al parsear user_id")
                            as UserId,
                    };
                    let central_msg_serialized = central_msg.serialize();
                    let central_ok = if let Some(ref sender) = self.central_server {
                        sender.send(central_msg_serialized).is_ok()
                    } else {
                        false
                    };

                    // 3. Verificamos que AMBOS se hayan enterado
                    if payment_ok && central_ok {
                        println!("Alquiler offline {} sincronizado con Payment y CentralServer exitosamente", rental_id);
                    } else {
                        // Si alguno de los dos falla (ej: payment volvió pero el central server sigue caído),
                        // guardamos la línea para reintentar en el próximo BatchUpdate.
                        println!(
                            "[BATCH-SYNC] Fallo parcial o total al sincronizar {}. Se reintentará.",
                            rental_id
                        );
                        lines_to_keep.push(line.to_string());
                    }
                }
            }
            if !lines_to_keep.is_empty() {
                std::fs::write(rents_filename, lines_to_keep.join("\n") + "\n")
                    .expect("Error al actualizar archivo de alquileres pendientes");
            } else {
                std::fs::remove_file(rents_filename).ok();
            }
        }
    }

    fn process_charges(&mut self) {
        let charges_filename = self.get_charges_filename();
        let mut lines_to_keep = Vec::<String>::new();
        if let Ok(contents) = std::fs::read_to_string(&charges_filename) {
            for line in contents.lines() {
                if line.trim().is_empty() {
                    continue;
                }

                if let Ok(charge) = serde_json::from_str::<serde_json::Value>(line) {
                    let rental_id = charge
                        .get("rental_id")
                        .and_then(|v| v.as_str())
                        .expect("Error al parsear rental_id")
                        .to_string();
                    let capture_msg = CapturePayment {
                        transaction_id: rental_id.clone(),
                        amount_cents: charge
                            .get("amount_cents")
                            .and_then(|v| v.as_u64())
                            .expect("Error al parsear amount_cents")
                            as u32,
                    };
                    let msg_serialized = capture_msg.serialize();
                    let payment_ok = self.send_msg_to_payment(msg_serialized).is_ok();

                    let central_msg = ReturnRent {
                        rental_id: rental_id.clone(),
                        bike_id: charge
                            .get("bike_id")
                            .and_then(|v| v.as_u64())
                            .expect("Error al parsear bike_id")
                            as BikeId,
                    };

                    let central_msg_serialized = central_msg.serialize();

                    let central_ok = if let Some(ref sender) = self.central_server {
                        sender.send(central_msg_serialized).is_ok()
                    } else {
                        false
                    };

                    if payment_ok && central_ok {
                        println!("Devolución offline {} sincronizada con Payment y CentralServer exitosamente", rental_id);
                    } else {
                        println!("[BATCH-SYNC] Fallo parcial o total al sincronizar cobro/devolución de {}. Se reintentará.", rental_id);
                        lines_to_keep.push(line.to_string());
                    }
                }
            }
            if !lines_to_keep.is_empty() {
                std::fs::write(charges_filename, lines_to_keep.join("\n") + "\n")
                    .expect("Error al actualizar archivo de cargos pendientes");
            } else {
                std::fs::remove_file(charges_filename).ok();
            }
        }
    }

    fn process_batch_updates(&mut self) {
        self.process_charges();
        self.process_rents();
    }

    fn sync_with_central(&mut self) {
        let available_bikes = self
            .station
            .slots
            .iter()
            .filter(|s| !matches!(s.state, SlotState::Empty))
            .count();
        let free_slots = self.station.slots.len() - available_bikes;

        let occupieds: Vec<String> = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Occupied { .. }))
            .map(|s| s.index.to_string())
            .collect();

        let frees: Vec<String> = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Empty))
            .map(|s| s.index.to_string())
            .collect();

        let occupied_map = occupieds.join(",");
        let free_map = frees.join(",");

        let status = StationStatus {
            station_id: self.station.id,
            location: Location {
                x: self.station.location.x,
                y: self.station.location.y,
            },
            available_bikes: available_bikes as u8,
            free_slots: free_slots as u8,
            updated_at_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            station_addr: self.my_ip.clone(),
            slots_occupied: occupied_map,
            slots_frees: free_map,
        };

        let update_msg = StationUpdate { station: status };
        let payload = format!("{}\n", update_msg.serialize());

        if let Some(ref sender) = self.central_server {
            if sender.send(payload).is_err() {
                println!("[RED] Conexión perdida con el Líder.");
                self.central_server = None;
            } else {
                println!("[CENTRAL] Estado sincronizado exitosamente.");
            }
        } else {
            println!("[NUEVA CONEXIÓN] Buscando Líder en la red (esperando conexión de fondo)...");
        }
    }

    fn start_central_connection(&mut self, ctx: &mut Context<Self>) {
        let station_addr = ctx.address();
        let server_addrs = self.server_addrs.clone();
        let my_ip = self.my_ip.clone();
        let station_id = self.station.id;
        let location = self.station.location.clone();
        let num_slots = self.station.slots.len();

        let occupieds: Vec<String> = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Occupied { .. }))
            .map(|s| s.index.to_string())
            .collect();

        let frees: Vec<String> = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Empty))
            .map(|s| s.index.to_string())
            .collect();

        let occupied_map = occupieds.join(",");
        let free_map = frees.join(",");

        std::thread::spawn(move || {
            let mut server_idx = 0;
            loop {
                let target_ip = &server_addrs[server_idx];
                println!("[RECONEXIÓN CENTRAL] Probando nodo {}...", target_ip);

                match TcpStream::connect(target_ip) {
                    Ok(mut stream) => {
                        let status = StationStatus {
                            station_id,
                            location: Location {
                                x: location.x,
                                y: location.y,
                            },
                            available_bikes: 0,
                            free_slots: num_slots as u8,
                            updated_at_secs: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                            station_addr: my_ip.clone(),
                            slots_occupied: occupied_map.clone(),
                            slots_frees: free_map.clone(),
                        };
                        let update_msg = StationUpdate { station: status };
                        let payload = format!("{}\n", update_msg.serialize());

                        if stream.write_all(payload.as_bytes()).is_ok() {
                            let _ =
                                stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
                            let mut buf = [0u8; 1024];
                            match stream.read(&mut buf) {
                                Ok(n) if n > 0 => {
                                    let response = String::from_utf8_lossy(&buf[..n]);
                                    if response.starts_with("NOT_LEADER") {
                                        println!(
                                            "[RECONEXIÓN CENTRAL] Nodo {} no es el Líder.",
                                            target_ip
                                        );
                                        server_idx = (server_idx + 1) % server_addrs.len();
                                        std::thread::sleep(std::time::Duration::from_secs(1));
                                        continue;
                                    }
                                }
                                Err(e)
                                    if e.kind() == std::io::ErrorKind::WouldBlock
                                        || e.kind() == std::io::ErrorKind::TimedOut =>
                                {
                                    println!(
                                        "[RECONEXIÓN CENTRAL] ¡Líder encontrado en {}!",
                                        target_ip
                                    );
                                    let _ = stream.set_read_timeout(None);

                                    let (tx, rx) = std::sync::mpsc::channel::<String>();
                                    station_addr.do_send(CentralServerConnected { sender: tx });

                                    let mut stream_writer = match stream.try_clone() {
                                        Ok(s) => s,
                                        Err(_) => {
                                            station_addr.do_send(CentralServerDisconnected);
                                            server_idx = (server_idx + 1) % server_addrs.len();
                                            continue;
                                        }
                                    };
                                    let mut stream_reader = match stream.try_clone() {
                                        Ok(s) => s,
                                        Err(_) => {
                                            station_addr.do_send(CentralServerDisconnected);
                                            server_idx = (server_idx + 1) % server_addrs.len();
                                            continue;
                                        }
                                    };
                                    let stream_to_shutdown = match stream.try_clone() {
                                        Ok(s) => s,
                                        Err(_) => {
                                            station_addr.do_send(CentralServerDisconnected);
                                            server_idx = (server_idx + 1) % server_addrs.len();
                                            continue;
                                        }
                                    };

                                    let station_addr_clone = station_addr.clone();
                                    let sender_handle = std::thread::spawn(move || {
                                        for msg in rx {
                                            let formatted = if msg.ends_with('\n') {
                                                msg
                                            } else {
                                                format!("{}\n", msg)
                                            };
                                            if stream_writer
                                                .write_all(formatted.as_bytes())
                                                .is_err()
                                            {
                                                break;
                                            }
                                            let _ = stream_writer.flush();
                                        }
                                        println!("[CENTRAL SENDER] Hilo de envío finalizado por error o desconexión.");
                                        let _ =
                                            stream_to_shutdown.shutdown(std::net::Shutdown::Both);
                                    });

                                    let receiver_station_addr = station_addr.clone();
                                    let receiver_handle = std::thread::spawn(move || {
                                        let mut buffer = [0; 4096];
                                        loop {
                                            match stream_reader.read(&mut buffer) {
                                                Ok(0) | Err(_) => {
                                                    println!("[CENTRAL RECEIVER] Conexión con central cerrada o caída.");
                                                    receiver_station_addr
                                                        .do_send(CentralServerDisconnected);
                                                    break;
                                                }
                                                Ok(n) => {
                                                    let data =
                                                        String::from_utf8_lossy(&buffer[..n]);
                                                    for line in data.lines() {
                                                        let message_text = line.trim();
                                                        if message_text.is_empty() {
                                                            continue;
                                                        }
                                                        let prefix = message_text
                                                            .split('|')
                                                            .next()
                                                            .unwrap_or("");
                                                        if let Some(message_type) =
                                                            MessageType::from_str(prefix)
                                                        {
                                                            match message_type {
                                                                MessageType::UserValidationResult => {
                                                                    receiver_station_addr.do_send(UserValidationResult::deserialize(message_text));
                                                                }
                                                                MessageType::ReservationRejected => {
                                                                    receiver_station_addr.do_send(ReservationRejected::deserialize(message_text));
                                                                }
                                                                _ => {
                                                                    println!("[CENTRAL RECEIVER] Mensaje no manejado del central server: {}", message_text);
                                                                }
                                                            }
                                                        } else {
                                                            println!("[CENTRAL RECEIVER] Tipo de mensaje desconocido: {}", prefix);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    });

                                    let _ = receiver_handle.join();
                                    let _ = sender_handle.join(); // wait for sender thread too
                                    station_addr_clone.do_send(CentralServerDisconnected);
                                }
                                _ => {
                                    server_idx = (server_idx + 1) % server_addrs.len();
                                }
                            }
                        } else {
                            server_idx = (server_idx + 1) % server_addrs.len();
                        }
                    }
                    Err(_) => {
                        server_idx = (server_idx + 1) % server_addrs.len();
                    }
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
    }

    fn abort_expired_transactions(&mut self) {
        let now = SystemTime::now();
        let expired_transactions: Vec<String> = self
            .pending_transactions
            .iter()
            .filter_map(|(tx_id, tx)| {
                if now
                    .duration_since(tx.started_at)
                    .unwrap_or_default()
                    .as_secs()
                    > TIMEOUT_SECS
                {
                    Some(tx_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for tx_id in expired_transactions {
            if let Some(tx) = self.pending_transactions.remove(&tx_id) {
                println!("[TIMEOUT] Transacción {} expirada. Abortando...", tx_id);
                let rollback_msg = RollbackPayment {
                    transaction_id: tx_id.clone(),
                };
                let rollback_msg_serialized = rollback_msg.serialize();
                let _ = self.send_msg_to_payment(rollback_msg_serialized);

                self.station.cancel_reservation(tx.slot_index, tx.bike_id);
                self.station.save_inventory();
                tx.client_addr.do_send(RentRejected {
                    reason: "Transacción expirada por timeout".to_string(),
                });
            }
        }
    }

    fn try_reconnect_payment(&mut self, ctx: &mut Context<Self>) {
        if self.payment_service.is_some() {
            return;
        }

        if let Ok(stream) = TcpStream::connect(&self.payment_ip) {
            println!(
                "[PAYMENT] ¡Conexión establecida con el servicio de pagos en {}!",
                self.payment_ip
            );

            let (tx, rx) = std::sync::mpsc::channel::<String>();
            let stream_writer = stream.try_clone().expect("Error clonando escritura");
            let mut stream_reader = stream.try_clone().expect("Error clonando lectura");
            let station_addr = ctx.address();

            std::thread::spawn(move || {
                let mut writer = stream_writer;
                for msg in rx {
                    if writer.write_all(msg.as_bytes()).is_err() {
                        break;
                    }
                    let _ = writer.flush();
                }
            });

            std::thread::spawn(move || {
                let mut buffer = [0; 1024];
                loop {
                    match stream_reader.read(&mut buffer) {
                        Ok(0) | Err(_) => {
                            println!("[PAYMENT] Conexión cerrada o caída.");
                            break;
                        }
                        Ok(n) => {
                            let data = String::from_utf8_lossy(&buffer[..n]);
                            for line in data.lines() {
                                let text = line.trim();
                                if text.is_empty() {
                                    continue;
                                }
                                let message_type = MessageType::deserialize(text);
                                match message_type {
                                    MessageType::VoteCommit => {
                                        station_addr.do_send(VoteCommit::deserialize(text))
                                    }
                                    MessageType::VoteAbort => {
                                        station_addr.do_send(VoteAbort::deserialize(text))
                                    }
                                    MessageType::ReservationRejected => {
                                        station_addr.do_send(ReservationRejected::deserialize(text))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            });

            self.payment_service = Some(tx);
        }
    }

    fn process_rent_request(
        &mut self,
        msg: RequestMessage<RentRequest, ConnectionActor>,
        time: SystemTime,
    ) {
        let bike_id = self
            .station
            .reserve_bike(msg.request.slot_index)
            .expect("Error al reservar bicicleta");
        self.station.save_inventory();
        let rental_id = self.generate_rental_id(bike_id, msg.request.user_id);
        let prepare_msg = PreparePayment {
            card_token: msg.request.card_token.clone(),
            amount_cents: PRE_AUTH_AMOUNT_CENTS,
            transaction_id: rental_id.clone(),
        };
        let msg_serialized = prepare_msg.serialize();

        match self.send_msg_to_payment(msg_serialized) {
            Ok(_) => {
                self.pending_transactions.insert(
                    prepare_msg.transaction_id.clone(),
                    TransactionState {
                        slot_index: msg.request.slot_index,
                        bike_id: bike_id,
                        client_addr: msg.response.clone(),
                        payment_voted_commit: false,
                        app_voted_commit: false,
                        started_at: time,
                    },
                );

                msg.response.do_send(Prepare {
                    transaction_id: rental_id,
                });
            }
            Err(_) => {
                // --- FLUJO OFFLINE
                println!("[ALERTA OFFLINE] Falló comunicación con Payment. Degradando 2PC...");

                self.station.confirm_reservation(msg.request.slot_index);
                self.station.save_inventory();
                self.sync_with_central();

                self.save_pending_rent(
                    &rental_id,
                    msg.request.user_id,
                    bike_id,
                    &msg.request.card_token,
                );

                msg.response.do_send(RentConfirmed {
                    bike_id: bike_id,
                    pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                    timestamp_secs: time
                        .duration_since(UNIX_EPOCH)
                        .expect("Error al obtener tiempo")
                        .as_secs(),
                    rental_id,
                });
            }
        }
    }
}

impl Actor for StationActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("StationActor iniciado. Registrando worker de BatchUpdate periódico...");

        self.start_central_connection(ctx);
        self.try_reconnect_payment(ctx);

        ctx.run_interval(std::time::Duration::from_secs(10), |act, ctx| {
            act.try_reconnect_payment(ctx);
            act.process_batch_updates();
            act.abort_expired_transactions();
            let ping_msg = format!("PING|{}\n", act.station.id);
            if let Some(ref sender) = act.central_server {
                let _ = sender.send(ping_msg);
            }
        });
    }
}

impl Handler<RequestMessage<RentRequest, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<RentRequest, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "StationActor recibiendo RentRequest para slot {}",
            msg.request.slot_index
        );

        if !self.station.is_bike_available(msg.request.slot_index) {
            msg.response.do_send(RentRejected {
                reason: "Bici no disponible".to_string(),
            });
            return;
        }

        let user_id = msg.request.user_id;
        let validation_msg = ValidateUser { user_id };
        let validation_msg_serialized = validation_msg.serialize();

        if let Some(ref sender) = self.central_server {
            if sender.send(validation_msg_serialized).is_ok() {
                self.pending_validations.insert(
                    user_id,
                    PendingValidation {
                        msg,
                        started_at: SystemTime::now(),
                    },
                );
                return;
            } else {
                println!("[RED] Conexión perdida con el Líder durante validación.");
                self.central_server = None;
            }
        }

        println!("[VALIDACIÓN OFFLINE] No se pudo contactar al Líder para validar usuario. Degradando a validación offline...");
        self.process_rent_request(msg, SystemTime::now());
    }
}

impl Handler<UserValidationResult> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: UserValidationResult, _ctx: &mut Self::Context) {
        let validation = self.pending_validations.remove(&msg.user_id);

        if !msg.is_valid {
            if let Some(validation) = validation {
                validation.msg.response.do_send(RentRejected {
                    reason: msg
                        .reason
                        .clone()
                        .unwrap_or("Usuario no válido".to_string()),
                });
            }
            return;
        }

        if let Some(validation) = validation {
            self.process_rent_request(validation.msg, validation.started_at);
        }
    }
}

impl Handler<RequestMessage<ReturnRequest, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<ReturnRequest, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let slot = msg.request.slot_index;
        let bike_id = msg.request.bike_id;
        let rental_id = msg.request.rental_id.clone();
        let started_at_secs = msg.request.started_at_secs;
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error al obtener tiempo")
            .as_secs();

        if self.station.is_slot_free(slot) {
            self.station.return_bike(slot, bike_id);
            self.station.save_inventory();
            let amount_cents = self.station.calculate_amount(started_at_secs, now_secs);
            let capture_msg = CapturePayment {
                transaction_id: rental_id,
                amount_cents,
            };

            let payment_msg = capture_msg.serialize();

            match self.send_msg_to_payment(payment_msg) {
                Ok(_) => {}
                Err(_) => {
                    // --- FLUJO OFFLINE
                    println!("[ALERTA OFFLINE] Falló comunicación con Payment durante devolución. Registrando cobro diferido...");

                    self.save_pending_charge(&msg.request.rental_id, amount_cents, bike_id);
                }
            }
            msg.response.do_send(ReturnConfirmed {
                charged_cents: amount_cents,
                timestamp_secs: now_secs,
            });

            self.sync_with_central();
        } else {
            msg.response.do_send(ReturnRejected {
                reason: "Slot no está libre".to_string(),
            });
        }
    }
}

// Mensaje para 2PC
// Mensajes que recibe de la App
impl Handler<RequestMessage<VoteCommit, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<VoteCommit, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        self.pending_transactions
            .entry(msg.request.transaction_id.clone())
            .and_modify(|tx| tx.app_voted_commit = true);
        self.check_transaction_state(&msg.request.transaction_id);
    }
}

impl Handler<RequestMessage<VoteAbort, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<VoteAbort, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let rollback_msg = RollbackPayment {
            transaction_id: msg.request.transaction_id.clone(),
        };

        let rollback_msg = rollback_msg.serialize();
        let _ = self.send_msg_to_payment(rollback_msg); //ignoramos, por timeout rollbackeara

        let transaction = self
            .pending_transactions
            .remove(&msg.request.transaction_id);
        if let Some(tx) = transaction {
            self.station.cancel_reservation(tx.slot_index, tx.bike_id);
            self.station.save_inventory();
        }
    }
}

// Mensajes que recibe de Payment
impl Handler<VoteCommit> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: VoteCommit, _ctx: &mut Self::Context) {
        self.pending_transactions
            .entry(msg.transaction_id.clone())
            .and_modify(|tx| tx.payment_voted_commit = true);
        self.check_transaction_state(&msg.transaction_id);
    }
}

impl Handler<VoteAbort> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: VoteAbort, _ctx: &mut Self::Context) {
        let return_msg = RentRejected {
            reason: "Pago rechazado".to_string(),
        };

        if let Some(tx) = self.pending_transactions.get(&msg.transaction_id) {
            tx.client_addr.do_send(return_msg);
        }

        let transaction = self.pending_transactions.remove(&msg.transaction_id);
        if let Some(tx) = transaction {
            self.station.cancel_reservation(tx.slot_index, tx.bike_id);
            self.station.save_inventory();
        }
    }
}

impl Handler<ReservationRejected> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: ReservationRejected, _ctx: &mut Self::Context) {
        let client_id = self.client_id_from_rental(&msg.transaction_id);

        // Enviar mensaje a central server para que bloquee al usuario
        if let Some(user_id) = client_id {
            let ban_msg = UserBanned {
                user_id,
                reason: format!("Bloqueado por no realizar pago: {}", msg.reason),
            };
            let ban_msg_serialized = ban_msg.serialize();
            if let Some(ref sender) = self.central_server {
                let _ = sender.send(ban_msg_serialized);
            }
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CentralServerConnected {
    pub sender: std::sync::mpsc::Sender<String>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CentralServerDisconnected;

impl Handler<CentralServerConnected> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: CentralServerConnected, _ctx: &mut Self::Context) {
        println!("[STATION] Conectado al Central Server. Habilitando comunicación.");
        self.central_server = Some(msg.sender);
        //self.sync_with_central();
        self.process_batch_updates();
    }
}

impl Handler<CentralServerDisconnected> for StationActor {
    type Result = ();

    fn handle(&mut self, _msg: CentralServerDisconnected, _ctx: &mut Self::Context) {
        if self.central_server.is_some() {
            println!("[STATION] Desconectado del Central Server. Deshabilitando comunicación.");
            self.central_server = None;
        }
    }
}
