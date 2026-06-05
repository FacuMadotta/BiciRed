use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::net::TcpStream;
use actix::prelude::*;
use std::io::Write;
use crate::messages_actors::*;
use std::io::Read;
use crate::messages_actors::IncomingData;
use crate::messages_actors::NewConnectionMessage;

pub struct ConnectionActor {
    pub socket: TcpStream,
    pub server_addr: Addr<CentralServerActor>,
    pub elector_addr: Addr<ElectorActor>,
}

impl ConnectionActor {
    pub fn new(socket: TcpStream, server: Addr<CentralServerActor>, elector: Addr<ElectorActor>) -> Self {
        Self { socket, server_addr: server, elector_addr: elector }
    }
}

impl Actor for ConnectionActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        if let Ok(mut stream_clone) = self.socket.try_clone() {
            let addr = ctx.address();
            
            std::thread::spawn(move || {
                let mut buf = [0; 1024];
                while let Ok(n) = stream_clone.read(&mut buf) {
                    if n == 0 { 
                        println!("[SERVER] El cliente cerró la conexión.");
                        break; 
                    }
                    addr.do_send(IncomingData(buf[..n].to_vec()));
                }
            });
        } else {
            println!("[SERVER] Error al clonar socket para ConnectionActor");
            ctx.stop();
        }
    }
}

impl Handler<IncomingData> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: IncomingData, ctx: &mut Self::Context) {
        if let Ok(text) = String::from_utf8(msg.0) {
            let message_text = text.trim();
            if message_text.is_empty() { return; }

            let parts: Vec<&str> = message_text.split('|').collect();
            if parts.is_empty() { return; }

            match parts[0] {
                "STATION_UPDATE" => {
                    let update = StationUpdate::deserialize(message_text);
                    self.server_addr.do_send(StationUpdateMessage {
                        station: update.station,
                    });
                }
                "NEARBY_QUERY" => {
                    if parts.len() == 4 {
                        let x: f64 = parts[1].parse().unwrap_or(0.0);
                        let y: f64 = parts[2].parse().unwrap_or(0.0);
                        let radius: f64 = parts[3].parse().unwrap_or(0.0);

                        self.server_addr.do_send(NearbyStationsRequestMessage {
                            location: Location { x, y },
                            radius,
                            response_addr: ctx.address(),
                        });
                    }
                }
                _ => {
                    println!("[SERVER] Tipo de mensaje no manejado en esta fase: {}", parts[0]);
                }
            }
        }
    }
}

impl Handler<NearbyStationsResponseMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: NearbyStationsResponseMessage, _ctx: &mut Self::Context) {
        let response = NearbyResponse { stations: msg.stations };
        let response_text = response.serialize();
        let _ = self.socket.write_all(response_text.as_bytes());
    }
}

pub struct CentralServerActor {
    pub station_table: HashMap<StationId, StationStatus>,
}

impl CentralServerActor {
    pub fn new() -> Self {
        Self { station_table: HashMap::new() }
    }
}

impl Actor for CentralServerActor {
    type Context = Context<Self>;
}

impl Handler<StationUpdateMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: StationUpdateMessage, _ctx: &mut Context<Self>) {
        println!("[SERVER] Actualizando datos de la estación ID: {}", msg.station.station_id);
        self.station_table.insert(msg.station.station_id, msg.station);
    }
}

impl Handler<NearbyStationsRequestMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: NearbyStationsRequestMessage, _ctx: &mut Context<Self>) {
        let mut nearby = Vec::new();

        for station in self.station_table.values() {
            let distance = ((station.location.x - msg.location.x).powi(2) 
                + (station.location.y - msg.location.y).powi(2)).sqrt();
            
            if distance <= msg.radius {
                nearby.push(StationStatus {
                    station_id: station.station_id,
                    location: Location { x: station.location.x, y: station.location.y },
                    available_bikes: station.available_bikes,
                    free_slots: station.free_slots,
                    updated_at_secs: station.updated_at_secs,
                });
            }
        }

        msg.response_addr.do_send(NearbyStationsResponseMessage { stations: nearby });
    }
}

pub struct ElectorActor {
    pub server_id: ServerId,
    pub central_server_addr: Addr<CentralServerActor>,
    pub is_leader: bool,
    pub leader_id: Option<ServerId>,
    pub connection_addrs: Vec<Addr<ConnectionActor>>,
}

impl ElectorActor {
    pub fn new(server_id: ServerId, central: Addr<CentralServerActor>) -> Self {
        Self { server_id, central_server_addr: central, is_leader: false, leader_id: None, connection_addrs: Vec::new() }
    }
}

impl Actor for ElectorActor {
    type Context = Context<Self>;
}

pub struct SpawnerActor {
    pub server_addr: Addr<CentralServerActor>,
    pub elector_addr: Addr<ElectorActor>,
}

impl Actor for SpawnerActor {
    type Context = Context<Self>;
}

impl Handler<NewConnectionMessage> for SpawnerActor {
    type Result = ();

    fn handle(&mut self, msg: NewConnectionMessage, _ctx: &mut Self::Context) {
        println!("[SERVER] Spawner recibiendo socket. Levantando ConnectionActor...");
        ConnectionActor::new(msg.0, self.server_addr.clone(), self.elector_addr.clone()).start();
    }
}
