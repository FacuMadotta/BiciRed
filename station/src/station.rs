use crate::connection::ConnectionActor;
use actix::prelude::*;
use common::*;
use serde_json;
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpStream;
use std::io::Read;
use std::sync::mpsc::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

const PENDING_RENTS_PREFIX: &str = "pending_rents_";
const PENDING_CHARGES_PREFIX: &str = "pending_charges_";
const PRE_AUTH_AMOUNT_CENTS: u32 = 100; // Harcodeo inicial para pre-autorización

// Estructura que maneja la lógica de la estación, incluyendo el estado de los slots y las bicicletas.
pub struct Station {
    pub id: StationId,
    pub location: Location,
    pub slots: Vec<Slot>,
}

pub struct Slot {
    pub index: usize,
    pub state: SlotState,
}

pub enum SlotState {
    Empty,
    Occupied { bike_id: BikeId },
    Reserved,
}

impl Station {
    pub fn new(id: StationId, location: Location, num_slots: usize, num_bikes: usize) -> Self {
        let slots = (0..num_slots)
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
            .collect();
        Self {
            id,
            location,
            slots,
        }
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
                slot.state = SlotState::Reserved; // Marcar el slot como reservado antes de commit
                return Some(bike_id);
            }
        }
        None
    }

    fn confirm_reservation(&mut self, slot_index: usize) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Reserved = slot.state {
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
}

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub slot_index: usize,
    pub bike_id: BikeId,
    pub client_addr: Addr<ConnectionActor>, // Para saber a qué ConnectionActor responderle al final
    pub payment_voted_commit: bool,
    pub central_voted_commit: bool,
}

pub struct StationActor {
    station: Station,
    central_server: Option<TcpStream>,
    payment_service: Sender<String>,
    pending_transactions: HashMap<String, TransactionState>,
    server_addrs: Vec<String>,
    my_ip: String,
}

impl StationActor {
    pub fn new(
        station: Station,
        payment_service: Sender<String>,
        server_addrs: Vec<String>,
        my_ip: String,
    ) -> Self {
        Self {
            station,
            central_server: None,
            payment_service,
            pending_transactions: HashMap::new(),
            server_addrs,
            my_ip,
        }
    }

    fn generate_rental_id(&self, bike_id: BikeId, user_id: UserId) -> String {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => format!("{}-{}-{}", bike_id, user_id, n.as_secs()),
            Err(_) => format!("{}-{}-{}", bike_id, user_id, 0), // fallback en caso de error
        }
    }

    fn send_msg_to_payment(&mut self, msg: String) -> Result<(), ()> {
        if let Err(_e) = self.payment_service.send(msg) {
            return Err(()); // Si falla el envío, asumimos que el Payment Service está offline
        }
        Ok(())
    }

    fn check_transaction_state(&mut self, transaction_id: &str) {
        if self
            .pending_transactions
            .get(transaction_id)
            .map(|tx| tx.payment_voted_commit && tx.central_voted_commit)
            .unwrap_or(false)
        {
            // Si ambos votaron commit, confirmamos el alquiler
            if let Some(tx) = self.pending_transactions.remove(transaction_id) {
                self.station.confirm_reservation(tx.slot_index);
                tx.client_addr.do_send(RentConfirmed {
                    bike_id: tx.bike_id,
                    pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                    timestamp_secs: 0, // Harcodeo inicial
                    rental_id: transaction_id.to_string(),
                });

                let commit_msg = CommitPayment {
                    transaction_id: transaction_id.to_string(),
                };

                let msg_serialized = commit_msg.serialize();
                self.send_msg_to_payment(msg_serialized);

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

    fn get_json_charge(&self, rental_id: &str, amount_cents: u64) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error al obtener tiempo")
            .as_secs();
        format!(
            "{{\"rental_id\":\"{}\",\"amount_cents\":{},\"timestamp\":{}}}\n",
            rental_id, amount_cents, timestamp
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

    fn save_pending_charge(&self, rental_id: &str, amount_cents: u64) {
        let filename = self.get_charges_filename();
        let charge_json = self.get_json_charge(rental_id, amount_cents);

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
                    let central_ok = if let Some(ref mut stream) = self.central_server {
                        stream.write_all(central_msg_serialized.as_bytes()).is_ok()
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

                    let central_ok = if let Some(ref mut stream) = self.central_server {
                        stream.write_all(central_msg_serialized.as_bytes()).is_ok()
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
        let available_bikes = self.station.slots.iter().filter(|s| !matches!(s.state, SlotState::Empty)).count();
        let free_slots = self.station.slots.len() - available_bikes;
    
        let status = StationStatus {
            station_id: self.station.id,
            location: Location { x: self.station.location.x, y: self.station.location.y },
            available_bikes: available_bikes as u8,
            free_slots: free_slots as u8,
            updated_at_secs: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            station_addr: self.my_ip.clone(),
        };
    
        let update_msg = StationUpdate { station: status };
        let payload = format!("{}\n", update_msg.serialize());
    
        if let Some(ref mut stream) = self.central_server {
            if stream.write_all(payload.as_bytes()).is_err() {
                println!("[RED] Conexión perdida con el Líder. Reconectando...");
                self.reconnect_to_leader(payload);
            } else {
                println!("[CENTRAL] Estado sincronizado exitosamente.");
            }
        } else {
            println!("[NUEVA CONEXIÓN] Buscando Líder en la red...");
            self.reconnect_to_leader(payload);
        }
    }
    
    fn reconnect_to_leader(&mut self, payload: String) {
        let mut server_idx = 0;
        loop {
            let target_ip = &self.server_addrs[server_idx];
            println!("[RECONEXIÓN] Probando nodo {}...", target_ip);
    
            if let Ok(mut stream) = TcpStream::connect(target_ip) {
                if stream.write_all(payload.as_bytes()).is_ok() {
                    stream.set_read_timeout(Some(std::time::Duration::from_secs(2))).unwrap();
                    let mut buf = [0u8; 1024];
                    
                    match stream.read(&mut buf) {
                        Ok(n) if n > 0 => {
                            let response = String::from_utf8_lossy(&buf[..n]);
                            if response.starts_with("NOT_LEADER") {
                                server_idx = (server_idx + 1) % self.server_addrs.len();
                                continue;
                            }
                        },
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                            println!("[RECONEXIÓN] ¡Nuevo Líder encontrado en {}!", target_ip);
                            stream.set_read_timeout(None).unwrap();
                            self.central_server = Some(stream);
                            return;
                        },
                        _ => {}
                    }
                }
            }
            server_idx = (server_idx + 1) % self.server_addrs.len();
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}

impl Actor for StationActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("StationActor iniciado. Registrando worker de BatchUpdate periódico...");

        self.sync_with_central();

        ctx.run_interval(std::time::Duration::from_secs(10), |act, _ctx| {
            act.process_batch_updates();
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
        if self.station.is_bike_available(msg.request.slot_index) {
            let bike_id = self
                .station
                .reserve_bike(msg.request.slot_index)
                .expect("Error al reservar bicicleta");
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
                            central_voted_commit: false,
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

                    self.save_pending_rent(
                        &rental_id,
                        msg.request.user_id,
                        bike_id,
                        &msg.request.card_token,
                    );

                    msg.response.do_send(RentConfirmed {
                        bike_id: bike_id,
                        pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                        timestamp_secs: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Error al obtener tiempo")
                            .as_secs(),
                        rental_id,
                    });
                }
            }
        } else {
            msg.response.do_send(RentRejected {
                reason: "Bici no disponible".to_string(),
            });
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

        if self.station.is_slot_free(slot) {
            self.station.return_bike(slot, bike_id);
            let capture_msg = CapturePayment {
                transaction_id: rental_id,
            };

            let payment_msg = capture_msg.serialize();

            match self.send_msg_to_payment(payment_msg) {
                Ok(_) => {
                    // --- FLUJO ONLINE, nose si hace falta que haga algo aca
                }
                Err(_) => {
                    // --- FLUJO OFFLINE
                    println!("[ALERTA OFFLINE] Falló comunicación con Payment durante devolución. Registrando cobro diferido...");

                    self.save_pending_charge(&msg.request.rental_id, 150); // Harcodeo inicial para cargo pendiente
                }
            }
            msg.response.do_send(ReturnConfirmed {
                charged_cents: 150, // Harcodeo inicial para cargo final, iria calculate_amount()
                timestamp_secs: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Error al obtener tiempo")
                    .as_secs(),
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
            .and_modify(|tx| tx.central_voted_commit = true);
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

        self.pending_transactions
            .remove(&msg.request.transaction_id);
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

        self.pending_transactions.remove(&msg.transaction_id);
    }
}

// Conexion con central server
