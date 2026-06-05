use std::io::{Read, Write};
use std::net::TcpStream;

use common::{
    UserId, StationStatus, Location, MessageType,
    RentRequest, ReturnRequest, RentConfirmed, RentRejected, 
    ReturnConfirmed, ReturnRejected, NearbyResponse,
    Serializable, Deserializable
};

use crate::models::ActiveRental;

pub struct AppClient {
    pub user_id: UserId,
    pub current_rental: Option<ActiveRental>,
    pub cached_stations: Vec<StationStatus>,
    pub is_blocked: bool,
    pub central_servers: Vec<String>,
}

impl AppClient {
    pub fn new(user_id: UserId, servers: Vec<String>) -> Self {
        Self {
            user_id,
            current_rental: None,
            cached_stations: Vec::new(),
            is_blocked: false,
            central_servers: servers,
        }
    }

    fn send_tcp_request(addr: &str, payload: &str) -> Result<String, String> {
        let mut s = TcpStream::connect(addr).map_err(|e| format!("Fallo al conectar: {}", e))?;
        s.write_all(payload.as_bytes()).map_err(|e| format!("Error al escribir: {}", e))?;

        let mut buf = [0u8; 4096];
        let n = s.read(&mut buf).map_err(|e| format!("Error al leer: {}", e))?;
        
        if n == 0 {
            return Err("Servidor desconectado.".to_string());
        }

        String::from_utf8(buf[..n].to_vec()).map_err(|e| format!("Error de formato UTF-8: {}", e))
    }

    pub fn query_central(&mut self, location: Location, radius: f64) {
        let mut connected = false;
        let query_msg = format!("NEARBY_QUERY|{}|{}|{}", location.x, location.y, radius);

        for addr in &self.central_servers {
            match Self::send_tcp_request(addr, &query_msg) {
                Ok(response_text) => {
                    let text = response_text.trim();
                    if text.starts_with("NEARBY_RESPONSE") {
                        let response = NearbyResponse::deserialize(text);
                        println!("\n[CENTRAL] {} estaciones encontradas:", response.stations.len());
                        for st in &response.stations {
                            println!(" - Estación {} | Bicis: {} | Libres: {}", 
                                st.station_id, st.available_bikes, st.free_slots);
                        }
                        
                        self.cached_stations = response.stations;
                        connected = true;
                        break; 
                    }
                }
                Err(_) => println!("Falló el nodo en {}. Intentando con el siguiente...", addr),
            }
        }

        if !connected {
            println!("\n[OFFLINE] Server sin conexión. Mostrando caché local:");
            if self.cached_stations.is_empty() {
                println!(" - (Caché vacía)");
            } else {
                for st in &self.cached_stations {
                    println!(" - Estación {} | Bicis: {} | Libres: {}", 
                        st.station_id, st.available_bikes, st.free_slots);
                }
            }
        }
    }

    pub fn rent_station(&mut self, addr: &str, slot_index: usize, card_token: &str) {
        if self.is_blocked {
            println!("\n[ERROR] Tu cuenta está bloqueada.");
            return;
        }
        if self.current_rental.is_some() {
            println!("\n[ERROR] Ya tenés un alquiler en curso.");
            return;
        }

        let req = RentRequest { user_id: self.user_id, slot_index, card_token: card_token.to_string() };
        
        match Self::send_tcp_request(addr, &req.serialize()) {
            Ok(response_text) => {
                let text = response_text.trim();
                let msg_type = MessageType::deserialize(text);
                
                match msg_type {
                    MessageType::RentConfirmed => {
                        let conf = RentConfirmed::deserialize(text);
                        println!("\n[ÉXITO] Bici {} liberada. Pre-auth: ¢{}", conf.bike_id, conf.pre_auth_cents);
                        self.current_rental = Some(ActiveRental {
                            bike_id: conf.bike_id,
                            started_at_secs: conf.timestamp_secs,
                            pre_auth_cents: conf.pre_auth_cents,
                            station_id: 0, 
                        });
                    }
                    MessageType::RentRejected => {
                        let rej = RentRejected::deserialize(text);
                        println!("\n[RECHAZO] No se pudo alquilar: {}", rej.reason);
                    }
                    _ => println!("\n[ERROR] Respuesta inesperada: {}", text),
                }
            }
            Err(e) => println!("\n[ERROR DE RED] {}", e),
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
        };

        match Self::send_tcp_request(addr, &req.serialize()) {
            Ok(response_text) => {
                let text = response_text.trim();
                let msg_type = MessageType::deserialize(text);
                
                match msg_type {
                    MessageType::ReturnConfirmed => {
                        let conf = ReturnConfirmed::deserialize(text);
                        println!("\n[ÉXITO] Devolución procesada. Cargo: ${}", conf.charged_cents);
                        self.current_rental = None; 
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
