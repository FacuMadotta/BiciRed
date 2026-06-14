use common::BanNotification;
use rand::seq::SliceRandom;
use std::io::{Read, Write};
use std::net::TcpStream;

use common::{
    Deserializable, Location, MessageType, NearbyResponse, RentConfirmed, RentRejected,
    RentRequest, ReturnConfirmed, ReturnRejected, ReturnRequest, Serializable, StationStatus,
    UserId,
};

use crate::models::ActiveRental;

const MAX_RETRIES: u32 = 5;
const FILE_RENTAL: &str = "rental_state_";

pub struct AppClient {
    pub user_id: UserId,
    pub current_rental: Option<ActiveRental>,
    pub cached_stations: Vec<StationStatus>,
    pub is_blocked: bool,
    pub central_servers: Vec<String>,
    pub active_server_addr: String,
    pub actual_rental_id: Option<String>,
}

impl AppClient {
    pub fn new(user_id: UserId, servers: Vec<String>) -> Self {
        let mut rng = rand::thread_rng();
        let initial_server = servers.choose(&mut rng).cloned().unwrap_or_default();

        let current_rental = Self::load_rental_state(user_id);

        Self {
            user_id,
            current_rental,
            cached_stations: Vec::new(),
            is_blocked: false,
            central_servers: servers,
            active_server_addr: initial_server,
            actual_rental_id: None,
        }
    }

    fn load_rental_state(user_id: UserId) -> Option<ActiveRental> {
        let file_path = format!("{}{}.json", FILE_RENTAL, user_id);
        if let Ok(content) = std::fs::read_to_string(&file_path) {
            println!(
                "[GUARDADO] Encontrado alquiler activo previo en disco. Restaurando estado..."
            );
            serde_json::from_str::<ActiveRental>(&content).ok()
        } else {
            None
        }
    }

    fn save_rental_state(&self) {
        let file_path = format!("{}{}.json", FILE_RENTAL, self.user_id);
        if let Ok(json_content) = serde_json::to_string(&self.current_rental) {
            if let Err(e) = std::fs::write(&file_path, json_content) {
                eprintln!("[ERROR] No se pudo guardar el archivo de estado: {}", e);
            } else {
                println!("[GUARDADO] Alquiler respaldado en disco de forma segura.");
            }
        }
    }

    fn clear_rental_state(&self) {
        let file_path = format!("{}{}.json", FILE_RENTAL, self.user_id);
        let _ = std::fs::remove_file(file_path);
        println!("[GUARDADO] Historial de alquiler limpiado del disco.");
    }

    fn rotate_server(&mut self) {
        if let Some(pos) = self
            .central_servers
            .iter()
            .position(|x| x == &self.active_server_addr)
        {
            let next_idx = (pos + 1) % self.central_servers.len();
            self.active_server_addr = self.central_servers[next_idx].clone();
        } else if !self.central_servers.is_empty() {
            self.active_server_addr = self.central_servers[0].clone();
        }
    }

    pub fn send_tcp_request(addr: &str, payload: &str) -> Result<String, String> {
        let mut s = TcpStream::connect(addr).map_err(|e| format!("Fallo al conectar: {}", e))?;
        s.write_all(payload.as_bytes())
            .map_err(|e| format!("Error al escribir: {}", e))?;

        let mut buf = [0u8; 4096];
        let n = s
            .read(&mut buf)
            .map_err(|e| format!("Error al leer: {}", e))?;

        if n == 0 {
            return Err("Servidor desconectado.".to_string());
        }

        String::from_utf8(buf[..n].to_vec()).map_err(|e| format!("Error de formato UTF-8: {}", e))
    }

    pub fn query_central(&mut self, location: Location, radius: f64) {
        let mut connected = false;
        let query_msg = format!(
            "NEARBY_QUERY|{}|{}|{}|{}",
            self.user_id, location.x, location.y, radius
        );

        let mut retries = 0;

        while retries < MAX_RETRIES {
            match Self::send_tcp_request(&self.active_server_addr, &query_msg) {
                Ok(response_text) => {
                    if self.handle_central_response(response_text.trim()) {
                        connected = true;
                        break;
                    } else {
                        retries += 1;
                    }
                }
                Err(_) => {
                    println!(
                        "[ADVERTENCIA] Falló el nodo en {}. Rotando al siguiente...",
                        self.active_server_addr
                    );
                    self.rotate_server();
                    retries += 1;
                    std::thread::sleep(std::time::Duration::from_secs(2));
                }
            }
        }

        if !connected {
            println!("\n[OFFLINE] Server sin conexión. Mostrando caché local:");
            if self.cached_stations.is_empty() {
                println!(" - (Caché vacía)");
            } else {
                for st in &self.cached_stations {
                    println!(
                        " - Estación {} | Bicis: {} | Libres: {}",
                        st.station_id, st.available_bikes, st.free_slots
                    );
                }
            }
        }
    }

    fn handle_central_response(&mut self, text: &str) -> bool {
        let msg_type = MessageType::deserialize(text);

        match msg_type {
            MessageType::NearbyResponse => {
                let response = NearbyResponse::deserialize(text);
                println!(
                    "\n[CENTRAL] {} estaciones encontradas:",
                    response.stations.len()
                );
                for st in &response.stations {
                    println!(
                        " - Estación {} | Bicis: {} | Libres: {}",
                        st.station_id, st.available_bikes, st.free_slots
                    );
                    println!("   [Slots con Bici]: [{}]", st.slots_occupied);
                    println!("   [Slots Libres]  : [{}]", st.slots_frees);
                    println!("------------------------------------------------");
                }
                self.cached_stations = response.stations;
                true
            }
            MessageType::NotReplica => {
                let parts: Vec<&str> = text.split('|').collect();
                if parts.len() > 1 {
                    let new_addr = parts[1].to_string();
                    println!(
                        "[INFO] El nodo es Líder. Redirigiendo y guardando réplica: {}",
                        new_addr
                    );
                    self.active_server_addr = new_addr;
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
                false
            }
            MessageType::BanNotification => {
                let ban_msg = BanNotification::deserialize(text);
                println!(
                    "\n[BAN] Has sido bloqueado por el servidor. Razón: {}",
                    ban_msg.reason
                );
                self.is_blocked = true;
                true
            }
            _ => {
                println!("[ERROR] Respuesta inesperada: {}", text);
                self.rotate_server();
                std::thread::sleep(std::time::Duration::from_secs(2));
                false
            }
        }
    }

    pub fn rent_station(&mut self, addr: &str, slot_index: usize, card_token: &str) {
        if self.current_rental.is_some() {
            println!("\n[ERROR] Ya tenés un alquiler en curso. Devolvé la bici actual antes de sacar otra.");
            return;
        }

        if self.is_blocked {
            println!("\n[ERROR] Tu cuenta está bloqueada.");
            return;
        }

        let mut stream = match std::net::TcpStream::connect(addr) {
            Ok(s) => s,
            Err(e) => {
                println!("\n[ERROR DE RED] Fallo al conectar con la estación: {}", e);
                return;
            }
        };

        let req = RentRequest {
            user_id: self.user_id,
            slot_index,
            card_token: card_token.to_string(),
        };

        if let Err(e) = stream.write_all(req.serialize().as_bytes()) {
            println!("\n[ERROR DE RED] Error al enviar petición: {}", e);
            return;
        }

        let transaction_id = match self.process_prepare_phase(&mut stream) {
            Some(id) => id,
            None => return,
        };

        self.actual_rental_id = Some(transaction_id.clone());
        self.process_commit_phase(&mut stream, &transaction_id);
    }

    fn process_prepare_phase(&mut self, stream: &mut TcpStream) -> Option<String> {
        let mut buf = [0u8; 4096];
        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => {
                println!("\n[ERROR] La estación cerró la conexión inesperadamente.");
                return None;
            }
            Ok(bytes) => bytes,
        };

        let prepare_text = String::from_utf8_lossy(&buf[..n]).trim().to_string();
        let msg_type = MessageType::deserialize(&prepare_text);

        match msg_type {
            MessageType::RentConfirmed => {
                let conf = RentConfirmed::deserialize(&prepare_text);
                self.actual_rental_id = Some(conf.rental_id.clone());
                println!(
                    "\n[ÉXITO PAYMENT OFFLINE] Bici {} liberada. Pre-auth: ${}",
                    conf.bike_id, conf.pre_auth_cents
                );
                self.current_rental = Some(crate::models::ActiveRental {
                    bike_id: conf.bike_id,
                    started_at_secs: conf.timestamp_secs,
                    pre_auth_cents: conf.pre_auth_cents,
                    station_id: 0,
                });
                self.save_rental_state();
                None
            }
            MessageType::RentRejected => {
                let rej = RentRejected::deserialize(&prepare_text);
                println!("\n[RECHAZO] No se pudo alquilar: {}", rej.reason);
                None
            }
            MessageType::Prepare => {
                let parts: Vec<&str> = prepare_text.split('|').collect();
                if parts.len() > 1 {
                    Some(parts[1].to_string())
                } else {
                    println!("\n[ERROR] Formato de PREPARE inválido.");
                    None
                }
            }
            _ => {
                println!(
                    "\n[ERROR] Se esperaba PREPARE o CONFIRMACIÓN, se recibió: {}",
                    prepare_text
                );
                None
            }
        }
    }

    fn process_commit_phase(&mut self, stream: &mut TcpStream, transaction_id: &str) {
        let vote = format!("VOTE_COMMIT|{}", transaction_id);

        if let Err(e) = stream.write_all(vote.as_bytes()) {
            println!("\n[ERROR DE RED] Error al enviar voto: {}", e);
            return;
        }

        let mut buf = [0u8; 4096];
        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => {
                println!("\n[ERROR] La estación se cayó antes del commit final.");
                return;
            }
            Ok(bytes) => bytes,
        };

        let final_text = String::from_utf8_lossy(&buf[..n]).trim().to_string();
        let final_msg_type = MessageType::deserialize(&final_text);

        match final_msg_type {
            MessageType::RentConfirmed => {
                let conf = RentConfirmed::deserialize(&final_text);
                println!(
                    "\n[ÉXITO] Bici {} liberada. Pre-auth: ${}",
                    conf.bike_id, conf.pre_auth_cents
                );
                self.current_rental = Some(crate::models::ActiveRental {
                    bike_id: conf.bike_id,
                    started_at_secs: conf.timestamp_secs,
                    pre_auth_cents: conf.pre_auth_cents,
                    station_id: 0,
                });
                self.save_rental_state();
            }
            MessageType::RentRejected => {
                let rej = RentRejected::deserialize(&final_text);
                println!("\n[RECHAZO] No se pudo alquilar: {}", rej.reason);
            }
            _ => println!("\n[ERROR] Respuesta final inesperada: {}", final_text),
        }
    }

    pub fn return_station(&mut self, addr: &str, slot_index: usize) {
        let rental = match &self.current_rental {
            Some(r) => r,
            None => {
                println!("\n[ERROR] No tenés ninguna bici para devolver.");
                return;
            }
        };

        let req = ReturnRequest {
            user_id: self.user_id,
            bike_id: rental.bike_id,
            slot_index,
            started_at_secs: rental.started_at_secs,
            rental_id: self
                .actual_rental_id
                .clone()
                .unwrap_or_else(|| "unknown".into()),
        };

        match Self::send_tcp_request(addr, &req.serialize()) {
            Ok(response_text) => {
                let text = response_text.trim();
                let msg_type = MessageType::deserialize(text);

                match msg_type {
                    MessageType::ReturnConfirmed => {
                        let conf = ReturnConfirmed::deserialize(text);
                        println!(
                            "\n[ÉXITO] Devolución procesada. Cargo: ${}",
                            conf.charged_cents
                        );
                        self.current_rental = None;
                        self.clear_rental_state();
                    }
                    MessageType::ReturnRejected => {
                        let rej = ReturnRejected::deserialize(text);
                        println!("\n[RECHAZO] Falló la devolución: {}", rej.reason);
                    }
                    _ => println!("\n[ERROR] Respuesta inesperada: {}", text),
                }
            }
            Err(e) => println!("\n[ERROR DE RED] {}", e),
        }
    }
}
