use actix::prelude::*;
use common::*;
use serde_json;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actors::ConnectionActor;
use crate::domain::*;

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub slot_index: usize,
    pub bike_id: BikeId,
    pub client_addr: Addr<ConnectionActor>,
    pub payment_voted_commit: bool,
    pub app_voted_commit: bool,
    pub started_at: SystemTime,
}

pub struct PendingValidation {
    pub msg: RequestMessage<RentRequest, ConnectionActor>,
    pub started_at: SystemTime,
}

pub struct PendingCharge {
    pub msg: RequestMessage<ReturnRequest, ConnectionActor>,
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
    pending_charges: HashMap<UserId, PendingCharge>, 
    active_rentals: HashMap<common::UserId, u64>,
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
            pending_charges: HashMap::new(),
            active_rentals: HashMap::new(),
        }
    }

    fn generate_rental_id(&self, bike_id: BikeId, user_id: UserId) -> String {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => format!("{}-{}-{}", bike_id, user_id, n.as_millis()),
            Err(_) => format!("{}-{}-{}", bike_id, user_id, 0),
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
            if let Some(tx) = self.pending_transactions.remove(transaction_id) {
                self.station.confirm_reservation(tx.slot_index);
                self.station.save_inventory();

                if let Some(user_id) = self.client_id_from_rental(transaction_id) {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    self.active_rentals.insert(user_id, now);
                }

                tx.client_addr.do_send(RentConfirmed {
                    bike_id: tx.bike_id,
                    pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                    timestamp_secs: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Error")
                        .as_secs(),
                    rental_id: transaction_id.to_string(),
                });

                let msg_serialized = CommitPayment {
                    transaction_id: transaction_id.to_string(),
                }
                .serialize();
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
            .expect("Error")
            .as_secs();
        format!("{{\"rental_id\":\"{}\",\"user_id\":{},\"card_token\":\"{}\",\"bike_id\":{},\"timestamp\":{}}}\n",
            rental_id, user_id, card_token, bike_id, timestamp)
    }

    fn get_json_charge(&self, rental_id: &str, amount_cents: u32, bike_id: BikeId) -> String {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Error")
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
                        .unwrap()
                        .to_string();
                    let card_token = rent
                        .get("card_token")
                        .and_then(|v| v.as_str())
                        .unwrap()
                        .to_string();
                    let bike_id = rent.get("bike_id").and_then(|v| v.as_u64()).unwrap();
                    let user_id = rent.get("user_id").and_then(|v| v.as_u64()).unwrap() as UserId;

                    let payment_msg = ReservePayment {
                        card_token,
                        amount_cents: PRE_AUTH_AMOUNT_CENTS,
                        transaction_id: rental_id.clone(),
                    }
                    .serialize();
                    let payment_ok = self.send_msg_to_payment(payment_msg).is_ok();

                    let central_msg = OfflineRent {
                        rental_id: rental_id.clone(),
                        bike_id: bike_id as BikeId,
                        user_id,
                    }
                    .serialize();
                    let central_ok = if let Some(ref sender) = self.central_server {
                        sender.send(central_msg).is_ok()
                    } else {
                        false
                    };

                    if payment_ok && central_ok {
                        println!("Alquiler offline {} sincronizado con Payment y CentralServer exitosamente", rental_id);
                    } else {
                        println!(
                            "[BATCH-SYNC] Fallo parcial o total al sincronizar {}. Se reintentará.",
                            rental_id
                        );
                        lines_to_keep.push(line.to_string());
                    }
                }
            }
            if !lines_to_keep.is_empty() {
                std::fs::write(rents_filename, lines_to_keep.join("\n") + "\n").expect("Error");
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
                        .unwrap()
                        .to_string();
                    let amount_cents =
                        charge.get("amount_cents").and_then(|v| v.as_u64()).unwrap() as u32;
                    let bike_id = charge.get("bike_id").and_then(|v| v.as_u64()).unwrap() as BikeId;

                    let msg_serialized = CapturePayment {
                        transaction_id: rental_id.clone(),
                        amount_cents,
                    }
                    .serialize();
                    let payment_ok = self.send_msg_to_payment(msg_serialized).is_ok();

                    let central_msg_serialized = ReturnRent {
                        rental_id: rental_id.clone(),
                        bike_id,
                    }
                    .serialize();
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
                std::fs::write(charges_filename, lines_to_keep.join("\n") + "\n").expect("Error");
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
        let occupied_map = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Occupied { .. }))
            .map(|s| s.index.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let free_map = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Empty))
            .map(|s| s.index.to_string())
            .collect::<Vec<_>>()
            .join(",");

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
            println!("[NUEVA CONEXIÓN] Buscando Líder en la red...");
        }
    }

    fn start_central_connection(&mut self, ctx: &mut Context<Self>) {
        let station_addr = ctx.address();
        let server_addrs = self.server_addrs.clone();
        let my_ip = self.my_ip.clone();
        let station_id = self.station.id;
        let location = self.station.location.clone();
        let num_slots = self.station.slots.len();
        let available_bikes = self.station.slots.iter().filter(|s| !matches!(s.state, SlotState::Empty)).count();

        let occupied_map = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Occupied { .. }))
            .map(|s| s.index.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let free_map = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Empty))
            .map(|s| s.index.to_string())
            .collect::<Vec<_>>()
            .join(",");

        std::thread::spawn(move || {
            let mut server_idx = 0;
            loop {
                let target_ip = &server_addrs[server_idx];
                match TcpStream::connect(target_ip) {
                    Ok(mut stream) => {
                        let status = StationStatus {
                            station_id,
                            location: Location {
                                x: location.x,
                                y: location.y,
                            },
                            available_bikes: available_bikes as u8,
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
                                    if String::from_utf8_lossy(&buf[..n]).starts_with("NOT_LEADER")
                                    {
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

                                    let mut stream_writer = stream.try_clone().unwrap();
                                    let mut stream_reader = stream.try_clone().unwrap();

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
                                    });

                                    let receiver_station_addr = station_addr.clone();
                                    let receiver_handle = std::thread::spawn(move || {
                                        let mut buffer = [0; 4096];
                                        loop {
                                            match stream_reader.read(&mut buffer) {
                                                Ok(0) | Err(_) => {
                                                    receiver_station_addr
                                                        .do_send(CentralServerDisconnected);
                                                    break;
                                                }
                                                Ok(n) => {
                                                    let data =
                                                        String::from_utf8_lossy(&buffer[..n]);
                                                    for line in data.lines() {
                                                        if line.trim().is_empty() {
                                                            continue;
                                                        }
                                                        let prefix =
                                                            line.split('|').next().unwrap_or("");
                                                        if let Some(msg_type) =
                                                            MessageType::from_str(prefix)
                                                        {
                                                            match msg_type {
                                                                MessageType::UserValidationResult => receiver_station_addr.do_send(UserValidationResult::deserialize(line)),
                                                                MessageType::ReservationRejected => receiver_station_addr.do_send(ReservationRejected::deserialize(line)),
                                                                _ => {}
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    });

                                    let _ = receiver_handle.join();
                                    let _ = sender_handle.join();
                                    station_addr.do_send(CentralServerDisconnected);
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
                let msg_serialized = RollbackPayment {
                    transaction_id: tx_id.clone(),
                }
                .serialize();
                let _ = self.send_msg_to_payment(msg_serialized);
                self.station.cancel_reservation(tx.slot_index, tx.bike_id);
                self.station.save_inventory();
                tx.client_addr.do_send(RentRejected {
                    reason: "Timeout".to_string(),
                });
            }
        }
    }

    fn try_reconnect_payment(&mut self, ctx: &mut Context<Self>) {
        if self.payment_service.is_some() {
            return;
        }
        if let Ok(stream) = TcpStream::connect(&self.payment_ip) {
            println!("[PAYMENT] ¡Conexión establecida!");
            let (tx, rx) = std::sync::mpsc::channel::<String>();
            let mut stream_writer = stream.try_clone().unwrap();
            let mut stream_reader = stream.try_clone().unwrap();
            let station_addr = ctx.address();

            std::thread::spawn(move || {
                for msg in rx {
                    if stream_writer.write_all(msg.as_bytes()).is_err() {
                        break;
                    }
                    let _ = stream_writer.flush();
                }
            });

            std::thread::spawn(move || {
                let mut buffer = [0; 1024];
                loop {
                    match stream_reader.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let data = String::from_utf8_lossy(&buffer[..n]);
                            for line in data.lines() {
                                if line.trim().is_empty() {
                                    continue;
                                }
                                match MessageType::deserialize(line.trim()) {
                                    MessageType::VoteCommit => {
                                        station_addr.do_send(VoteCommit::deserialize(line))
                                    }
                                    MessageType::VoteAbort => {
                                        station_addr.do_send(VoteAbort::deserialize(line))
                                    }
                                    MessageType::ReservationRejected => {
                                        station_addr.do_send(ReservationRejected::deserialize(line))
                                    }
                                    MessageType::PaymentResult => {
                                        station_addr.do_send(PaymentResult::deserialize(line))
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
                station_addr.do_send(PaymentServiceDisconnected);
            });
            self.payment_service = Some(tx);
        }
    }

    fn process_rent_request(
        &mut self,
        msg: RequestMessage<RentRequest, ConnectionActor>,
        time: SystemTime,
    ) {
        let bike_id = self.station.reserve_bike(msg.request.slot_index).unwrap();
        self.station.save_inventory();
        let rental_id = self.generate_rental_id(bike_id, msg.request.user_id);
        let msg_serialized = PreparePayment {
            card_token: msg.request.card_token.clone(),
            amount_cents: PRE_AUTH_AMOUNT_CENTS,
            transaction_id: rental_id.clone(),
        }
        .serialize();

        match self.send_msg_to_payment(msg_serialized) {
            Ok(_) => {
                self.pending_transactions.insert(
                    rental_id.clone(),
                    TransactionState {
                        slot_index: msg.request.slot_index,
                        bike_id,
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
                self.station.confirm_reservation(msg.request.slot_index);
                self.station.save_inventory();
                self.sync_with_central();
                self.save_pending_rent(
                    &rental_id,
                    msg.request.user_id,
                    bike_id,
                    &msg.request.card_token,
                );
                self.active_rentals.insert(
                    msg.request.user_id,
                    time.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                );
                msg.response.do_send(RentConfirmed {
                    bike_id,
                    pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                    timestamp_secs: time.duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    rental_id: rental_id.clone(),
                });
            }
        }
    }

    fn process_return_request(
        &mut self,
        msg: RequestMessage<ReturnRequest, ConnectionActor>,
        amount_cents: u32,
        time: SystemTime,
    ) {
        self.station
            .return_bike(msg.request.slot_index, msg.request.bike_id);
        self.station.save_inventory();
        if let Some(user_id) = self.client_id_from_rental(&msg.request.rental_id) {
            self.active_rentals.remove(&user_id);
        }

        msg.response.do_send(ReturnConfirmed {
                charged_cents: amount_cents,
                timestamp_secs: time.duration_since(UNIX_EPOCH).unwrap().as_secs(),
            });
        self.sync_with_central();
    }

    fn check_unreturned_bikes(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mut usuarios_a_banear = Vec::new();

        for (&user_id, &tiempo_inicio) in &self.active_rentals {
            if now.saturating_sub(tiempo_inicio) > TIME_TO_RETURN {
                usuarios_a_banear.push(user_id);
            }
        }

        for user_id in usuarios_a_banear {
            let ban_msg_serialized = UserBanned {
                user_id,
                reason: "Bicicleta robada / no devuelta a tiempo".to_string(),
            }
            .serialize();
            if let Some(ref sender) = self.central_server {
                let _ = sender.send(ban_msg_serialized);
            }
            self.active_rentals.remove(&user_id);
        }
    }
}

impl Actor for StationActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_central_connection(ctx);
        self.try_reconnect_payment(ctx);

        ctx.run_interval(std::time::Duration::from_secs(5), |act, ctx| {
            act.try_reconnect_payment(ctx);
            act.process_batch_updates();
            act.abort_expired_transactions();
            act.check_unreturned_bikes();
            if let Some(ref sender) = act.central_server {
                let _ = sender.send(format!("PING|{}\n", act.station.id));
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
        if !self.station.is_bike_available(msg.request.slot_index) {
            msg.response.do_send(RentRejected {
                reason: "Bici no disponible".to_string(),
            });
            return;
        }
        let validation_msg = ValidateUser {
            user_id: msg.request.user_id,
        }
        .serialize();
        if let Some(ref sender) = self.central_server {
            if sender.send(validation_msg).is_ok() {
                self.pending_validations.insert(
                    msg.request.user_id,
                    PendingValidation {
                        msg,
                        started_at: SystemTime::now(),
                    },
                );
                return;
            }
            self.central_server = None;
        }
        self.process_rent_request(msg, SystemTime::now());
    }
}
impl Handler<UserValidationResult> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: UserValidationResult, _ctx: &mut Self::Context) {
        let validation = self.pending_validations.remove(&msg.user_id);
        if !msg.is_valid {
            if let Some(v) = validation {
                v.msg.response.do_send(RentRejected {
                    reason: msg.reason.unwrap_or_default(),
                });
            }
            return;
        }
        if let Some(v) = validation {
            self.process_rent_request(v.msg, v.started_at);
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
        if self.station.is_slot_free(msg.request.slot_index) {
            let amount_cents = self.station.calculate_amount(
                msg.request.started_at_secs,
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            if self
                .send_msg_to_payment(
                    CapturePayment {
                        transaction_id: msg.request.rental_id.clone(),
                        amount_cents,
                    }
                    .serialize(),
                ) 
                .is_ok()
            {
                self.pending_charges.insert(
                        msg.request.user_id,
                        PendingCharge {
                            msg,
                            started_at: SystemTime::now(),
                        },
                    );
            } else {
                self.save_pending_charge(&msg.request.rental_id, amount_cents, msg.request.bike_id);
                self.process_return_request(msg, amount_cents, SystemTime::now());
            }
        } else {
            msg.response.do_send(ReturnRejected {
                reason: "Slot ocupado".to_string(),
            });
        }
    }
}

impl Handler<PaymentResult> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: PaymentResult, _ctx: &mut Self::Context) {
        let charge = self.pending_charges.remove(&self.client_id_from_rental(&msg.transaction_id).unwrap_or_default());
         
        if let Some(pending_charge) = charge {
            if msg.success {
                self.process_return_request(pending_charge.msg, msg.amount_cents, pending_charge.started_at);
            } else {
                pending_charge.msg.response.do_send(ReturnRejected {
                    reason: "Pago fallido".to_string(),
                });
            }
        } 
    }
}

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
        let _ = self.send_msg_to_payment(
            RollbackPayment {
                transaction_id: msg.request.transaction_id.clone(),
            }
            .serialize(),
        );
        if let Some(tx) = self
            .pending_transactions
            .remove(&msg.request.transaction_id)
        {
            self.station.cancel_reservation(tx.slot_index, tx.bike_id);
            self.station.save_inventory();
        }
    }
}
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
        if let Some(tx) = self.pending_transactions.remove(&msg.transaction_id) {
            tx.client_addr.do_send(RentRejected {
                reason: "Pago rechazado".to_string(),
            });
            self.station.cancel_reservation(tx.slot_index, tx.bike_id);
            self.station.save_inventory();
        }
    }
}
impl Handler<ReservationRejected> for StationActor {
    type Result = ();
    fn handle(&mut self, msg: ReservationRejected, _ctx: &mut Self::Context) {
        if let Some(user_id) = self.client_id_from_rental(&msg.transaction_id) {
            if let Some(ref sender) = self.central_server {
                let _ = sender.send(
                    UserBanned {
                        user_id,
                        reason: msg.reason,
                    }
                    .serialize(),
                );
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
        self.central_server = Some(msg.sender);
        //self.sync_with_central();
        self.process_batch_updates();
    }
}
impl Handler<CentralServerDisconnected> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: CentralServerDisconnected, _ctx: &mut Self::Context) {
        self.central_server = None;
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct PaymentServiceDisconnected;
impl Handler<PaymentServiceDisconnected> for StationActor {
    type Result = ();
    fn handle(&mut self, _msg: PaymentServiceDisconnected, _ctx: &mut Self::Context) {
        self.payment_service = None;
    }
}

