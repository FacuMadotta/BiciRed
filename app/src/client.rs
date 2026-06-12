use rand::seq::SliceRandom;
use std::io::{Read, Write};
use std::net::TcpStream;

use common::{
    Deserializable, Location, MessageType, NearbyResponse, RentConfirmed, RentRejected,
    RentRequest, ReturnConfirmed, ReturnRejected, ReturnRequest, Serializable, StationStatus,
    UserId,
};

use crate::models::ActiveRental;

pub struct AppClient {
    pub user_id: UserId,
    pub current_rental: Option<ActiveRental>,
    pub cached_stations: Vec<StationStatus>,
    pub is_blocked: bool,
    pub central_servers: Vec<String>,
    pub active_server_addr: String,
}

impl AppClient {
    pub fn new(user_id: UserId, servers: Vec<String>) -> Self {
        let mut rng = rand::thread_rng();
        let initial_server = servers.choose(&mut rng).cloned().unwrap_or_default();

        let file_path = format!("rental_state_{}.json", user_id);
        let current_rental = if let Ok(content) = std::fs::read_to_string(&file_path) {
            println!("[OFFLINE/ONLINE] Encontrado alquiler activo previo en disco. Restaurando estado...");
            serde_json::from_str::<ActiveRental>(&content).ok()
        } else {
            None
        };

        Self {
            user_id,
            current_rental,
            cached_stations: Vec::new(),
            is_blocked: false,
            central_servers: servers,
            active_server_addr: initial_server,
        }
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

    fn send_tcp_request(addr: &str, payload: &str) -> Result<String, String> {
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
        let query_msg = format!("NEARBY_QUERY|{}|{}|{}|{}", self.user_id, location.x, location.y, radius);

        let mut retries = 0;
        const MAX_RETRIES: usize = 5;

        while retries < MAX_RETRIES {
            match Self::send_tcp_request(&self.active_server_addr, &query_msg) {
                Ok(response_text) => {
                    let text = response_text.trim();
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
                            }
                            self.cached_stations = response.stations;
                            connected = true;
                            break;
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
                                retries += 1;
                                continue;
                            }
                        }
                        MessageType::BanNotification => {
                            println!("\n[BAN] Has sido bloqueado por el servidor. Razón: {}", text);
                            self.is_blocked = true;
                            break;
                        }
                        _ => {
                            println!("[ERROR] Respuesta inesperada: {}", text);
                            self.rotate_server();
                            retries += 1;
                        }
                    }
                }
                Err(_) => {
                    println!(
                        "[ADVERTENCIA] Falló el nodo en {}. Rotando al siguiente...",
                        self.active_server_addr
                    );
                    self.rotate_server();
                    retries += 1;
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

    pub fn rent_station(&mut self, addr: &str, slot_index: usize, card_token: &str) {
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

        let mut buf = [0u8; 4096];

        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => {
                println!("\n[ERROR] La estación cerró la conexión inesperadamente.");
                return;
            }
            Ok(bytes) => bytes,
        };

        let prepare_text = String::from_utf8_lossy(&buf[..n]).trim().to_string();
        let msg_type = MessageType::deserialize(&prepare_text);

        if msg_type != MessageType::Prepare {
            println!(
                "\n[ERROR] Se esperaba fase PREPARE, se recibió: {}",
                prepare_text
            );
            return;
        }

        let parts: Vec<&str> = prepare_text.split('|').collect();

        let transaction_id = parts[1];

        let vote = if self.current_rental.is_some() {
            format!("VOTE_ABORT|{}", transaction_id)
        } else {
            format!("VOTE_COMMIT|{}", transaction_id)
        };

        if let Err(e) = stream.write_all(vote.as_bytes()) {
            println!("\n[ERROR DE RED] Error al enviar voto: {}", e);
            return;
        }

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

                let file_path = format!("rental_state_{}.json", self.user_id);
                if let Ok(json_content) = serde_json::to_string(&self.current_rental) {
                    if let Err(e) = std::fs::write(&file_path, json_content) {
                        eprintln!("[ERROR PERSISTENCIA] No se pudo guardar el archivo de estado: {}", e);
                    } else {
                        println!("[OFFLINE/ONLINE] Alquiler respaldado en disco de forma segura.");
                    }
                }
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
            rental_id: format!("{}-{}", self.user_id, rental.started_at_secs),
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

                        let file_path = format!("rental_state_{}.json", self.user_id);
                        let _ = std::fs::remove_file(file_path);
                        println!("[OFFLINE/ONLINE] Historial de alquiler limpiado del disco.");
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
