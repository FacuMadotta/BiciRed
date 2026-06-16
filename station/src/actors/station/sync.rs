use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use actix::prelude::*;
use common::*;

use crate::domain::SlotState;

use super::messages::{
    CentralServerConnected, CentralServerDisconnected, PaymentServiceDisconnected,
    RequestFreshPayload,
};
use super::StationActor;

impl StationActor {
    pub fn send_to_payment(&mut self, msg: String) -> Result<(), ()> {
        if let Some(tx) = &self.payment_service {
            if tx.send(msg).is_ok() {
                return Ok(());
            }
            self.payment_service = None;
        }
        Err(())
    }

    pub fn sync_with_central(&mut self) {
        let payload = self.build_station_update_payload();

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

    pub fn build_station_update_payload(&self) -> String {
        let available_bikes = self
            .station
            .slots
            .iter()
            .filter(|s| !matches!(s.state, SlotState::Empty))
            .count();
        let free_slots = self.station.slots.len() - available_bikes;

        let slots_occupied = self
            .station
            .slots
            .iter()
            .filter(|s| matches!(s.state, SlotState::Occupied { .. }))
            .map(|s| s.index.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let slots_free = self
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
                .unwrap_or_default()
                .as_secs(),
            station_addr: self.my_ip.clone(),
            slots_occupied,
            slots_frees: slots_free,
        };

        format!("{}\n", StationUpdate { station: status }.serialize())
    }

    pub fn start_central_connection(&mut self, ctx: &mut Context<Self>) {
        let station_addr = ctx.address();
        let server_addrs = self.server_addrs.clone();

        std::thread::spawn(move || {
            let mut server_idx = 0;
            loop {
                let target_ip = &server_addrs[server_idx];
                match TcpStream::connect(target_ip) {
                    Ok(mut stream) => {
                        let (tx_fresh, rx_fresh) = mpsc::channel();
                        station_addr.do_send(RequestFreshPayload { sender: tx_fresh });
                        let fresh_payload = rx_fresh.recv().unwrap_or_default();

                        if stream.write_all(fresh_payload.as_bytes()).is_err() {
                            server_idx = (server_idx + 1) % server_addrs.len();
                            std::thread::sleep(std::time::Duration::from_secs(1));
                            continue;
                        }

                        let _ = stream.set_read_timeout(Some(std::time::Duration::from_secs(2)));
                        let mut buf = [0u8; 1024];

                        match stream.read(&mut buf) {
                            Ok(n) if n > 0 => {
                                if String::from_utf8_lossy(&buf[..n]).starts_with("NOT_LEADER") {
                                    server_idx = (server_idx + 1) % server_addrs.len();
                                    std::thread::sleep(std::time::Duration::from_secs(1));
                                    continue;
                                }
                            }
                            Err(e)
                                if e.kind() == std::io::ErrorKind::WouldBlock
                                    || e.kind() == std::io::ErrorKind::TimedOut =>
                            {
                                // Timeout de lectura: el servidor es el líder y no respondió nada extra
                                println!(
                                    "[RECONEXIÓN CENTRAL] ¡Líder encontrado en {}!",
                                    target_ip
                                );
                            }
                            _ => {
                                // Error de conexión o cierre prematuro
                                server_idx = (server_idx + 1) % server_addrs.len();
                                std::thread::sleep(std::time::Duration::from_secs(1));
                                continue;
                            }
                        }

                        let _ = stream.set_read_timeout(None);
                        let (tx, rx) = mpsc::channel::<String>();
                        station_addr.do_send(CentralServerConnected { sender: tx });

                        let (mut stream_writer, mut stream_reader) =
                            match (stream.try_clone(), stream.try_clone()) {
                                (Ok(writer), Ok(reader)) => (writer, reader),
                                (Err(err), _) | (_, Err(err)) => {
                                    eprintln!("[RECONEXIÓN CENTRAL] Error clonando stream: {err}");
                                    server_idx = (server_idx + 1) % server_addrs.len();
                                    std::thread::sleep(std::time::Duration::from_secs(1));
                                    continue;
                                }
                            };

                        // Hilo escritor: envía mensajes al central mientras haya conexión
                        let sender_handle = std::thread::spawn(move || {
                            for msg in rx {
                                let formatted = if msg.ends_with('\n') {
                                    msg
                                } else {
                                    format!("{}\n", msg)
                                };
                                if stream_writer.write_all(formatted.as_bytes()).is_err() {
                                    break;
                                }
                                let _ = stream_writer.flush();
                            }
                        });

                        // Hilo lector: recibe mensajes del central y los despacha al actor. Cuando el socket se cierra, notifica la desconexión.
                        let receiver_station_addr = station_addr.clone();
                        let receiver_handle = std::thread::spawn(move || {
                            let mut buffer = [0; 4096];
                            loop {
                                match stream_reader.read(&mut buffer) {
                                    Ok(0) | Err(_) => {
                                        receiver_station_addr.do_send(CentralServerDisconnected);
                                        break;
                                    }
                                    Ok(n) => {
                                        let data = String::from_utf8_lossy(&buffer[..n]);
                                        dispatch_central_messages(&receiver_station_addr, &data);
                                    }
                                }
                            }
                        });

                        // Bloquear el hilo de reconexión hasta que la sesión termine
                        let _ = receiver_handle.join();
                        let _ = sender_handle.join();
                        station_addr.do_send(CentralServerDisconnected);
                    }
                    Err(_) => {
                        server_idx = (server_idx + 1) % server_addrs.len();
                    }
                }
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        });
    }

    pub fn try_reconnect_payment(&mut self, ctx: &mut Context<Self>) {
        if self.payment_service.is_some() {
            return;
        }
        if let Ok(stream) = TcpStream::connect(&self.payment_ip) {
            println!("[PAYMENT] ¡Conexión establecida!");
            let (tx, rx) = mpsc::channel::<String>();
            let (mut stream_writer, mut stream_reader) =
                match (stream.try_clone(), stream.try_clone()) {
                    (Ok(writer), Ok(reader)) => (writer, reader),
                    (Err(err), _) | (_, Err(err)) => {
                        eprintln!("[PAYMENT] Error clonando stream: {err}");
                        return;
                    }
                };
            let station_addr = ctx.address();

            // Hilo escritor: reenvía mensajes al servicio de pagos
            std::thread::spawn(move || {
                for msg in rx {
                    if stream_writer.write_all(msg.as_bytes()).is_err() {
                        break;
                    }
                    let _ = stream_writer.flush();
                }
            });

            // Hilo lector: recibe respuestas del servicio de pagos y las despacha al actor. Al cerrarse el socket, notifica la desconexión.
            std::thread::spawn(move || {
                let mut buffer = [0; 1024];
                loop {
                    match stream_reader.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            let data = String::from_utf8_lossy(&buffer[..n]);
                            dispatch_payment_messages(&station_addr, &data);
                        }
                    }
                }
                station_addr.do_send(PaymentServiceDisconnected);
            });

            self.payment_service = Some(tx);
        }
    }
}

fn dispatch_central_messages(station_addr: &Addr<StationActor>, data: &str) {
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let prefix = line.split('|').next().unwrap_or("");
        if let Some(msg_type) = MessageType::from_string(prefix) {
            match msg_type {
                MessageType::UserValidationResult => {
                    station_addr.do_send(UserValidationResult::deserialize(line))
                }
                MessageType::ReservationRejected => {
                    station_addr.do_send(ReservationRejected::deserialize(line))
                }
                MessageType::ReturnRent => station_addr.do_send(ReturnRent::deserialize(line)),
                _ => {}
            }
        }
    }
}

fn dispatch_payment_messages(station_addr: &Addr<StationActor>, data: &str) {
    for line in data.lines() {
        if line.trim().is_empty() {
            continue;
        }
        match MessageType::deserialize(line.trim()) {
            MessageType::VoteCommit => station_addr.do_send(VoteCommit::deserialize(line)),
            MessageType::VoteAbort => station_addr.do_send(VoteAbort::deserialize(line)),
            MessageType::ReservationRejected => {
                station_addr.do_send(ReservationRejected::deserialize(line))
            }
            MessageType::PaymentResult => station_addr.do_send(PaymentResult::deserialize(line)),
            _ => {}
        }
    }
}
