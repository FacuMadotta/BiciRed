use crate::messages_actors::IncomingData;
use crate::messages_actors::NewConnectionMessage;
use crate::messages_actors::*;
use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use std::time::Instant;

pub struct ConnectionActor {
    pub server_id: ServerId,
    pub peer_id: Option<ServerId>,
    pub peer_addr: Option<String>,
    pub reconnect_on_stop: bool,
    pub socket: TcpStream,
    pub server_addr: Addr<CentralServerActor>,
    pub elector_addr: Addr<ElectorActor>,
}

impl ConnectionActor {
    pub fn new_incoming(
        server_id: ServerId,
        socket: TcpStream,
        server: Addr<CentralServerActor>,
        elector: Addr<ElectorActor>,
    ) -> Self {
        Self {
            server_id,
            peer_id: None,
            peer_addr: None,
            reconnect_on_stop: false,
            socket,
            server_addr: server,
            elector_addr: elector,
        }
    }

    pub fn new_outgoing(
        server_id: ServerId,
        peer_id: ServerId,
        peer_addr: String,
        socket: TcpStream,
        server: Addr<CentralServerActor>,
        elector: Addr<ElectorActor>,
    ) -> Self {
        Self {
            server_id,
            peer_id: Some(peer_id),
            peer_addr: Some(peer_addr),
            reconnect_on_stop: true,
            socket,
            server_addr: server,
            elector_addr: elector,
        }
    }
}

impl Actor for ConnectionActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        if self.peer_id.is_some() {
            let hello_msg = format!("HELLO|{}\n", self.server_id);
            let _ = self.socket.write_all(hello_msg.as_bytes());
        }

        if let Ok(mut stream_clone) = self.socket.try_clone() {
            let addr = ctx.address();

            std::thread::spawn(move || {
                let mut buf = [0; 1024];
                while let Ok(n) = stream_clone.read(&mut buf) {
                    if n == 0 {
                        println!("[SERVER] El cliente cerró la conexión.");
                        addr.do_send(ConnectionClosed);
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

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        if let Some(peer_id) = self.peer_id {
            self.elector_addr
                .do_send(RemovePeerMessage { server_id: peer_id });
            self.server_addr
                .do_send(RemovePeerMessage { server_id: peer_id });
        }

        if !self.reconnect_on_stop {
            return;
        }

        let Some(peer_addr) = self.peer_addr.clone() else {
            return;
        };

        let peer_id = self.peer_id;
        let server_addr = self.server_addr.clone();

        thread::spawn(move || loop {
            match TcpStream::connect(&peer_addr) {
                Ok(socket) => {
                    if let Some(target_peer_id) = peer_id {
                        println!(
                            "[ELECTION] Reconectado con peer {} en {}",
                            target_peer_id, peer_addr
                        );
                        server_addr.do_send(PeerConnectedMessage {
                            peer_id: target_peer_id,
                            peer_addr: peer_addr.clone(),
                            socket,
                        });
                    }
                    break;
                }
                Err(err) => {
                    eprintln!(
                        "[ELECTION] Reintentando conexión con {}: {}",
                        peer_addr, err
                    );
                    thread::sleep(Duration::from_secs(1));
                }
            }
        });
    }
}

impl Handler<IncomingData> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: IncomingData, ctx: &mut Self::Context) {
        if let Ok(text) = String::from_utf8(msg.0) {
            for line in text.lines() {
                let message_text = line.trim();
                if message_text.is_empty() {
                    continue;
                }

                let parts: Vec<&str> = message_text.split('|').collect();
                if parts.is_empty() {
                    continue;
                }

                match parts[0] {
                    "HELLO" => {
                        if parts.len() == 2 {
                            if let Ok(server_id) = parts[1].parse::<ServerId>() {
                                let ip_addr = self.socket.peer_addr().map(|addr| addr.to_string()).ok();
                                let msg = RegisterPeerConnectionMessage {
                                    server_id,
                                    connection_addr: ctx.address(),
                                    peer_addr: ip_addr,
                                };
                                self.elector_addr.do_send(msg.clone());
                                self.server_addr.do_send(msg);
                                self.peer_id = Some(server_id);
                            }
                        }
                    }
                    "STATION_UPDATE" => {
                        let update = StationUpdate::deserialize(message_text);
                        self.server_addr.do_send(StationUpdateMessage {
                            station: update.station,
                            response_addr: ctx.address(),
                        });
                    }
                    "IS_ALIVE" => {
                        let is_alive = IsAlive::deserialize(message_text);
                        self.elector_addr.do_send(LeaderAliveMessage {
                            leader_id: is_alive.leader_id,
                        });
                    }
                    "ELECTION_ACK" | "ACK" => {
                        self.elector_addr.do_send(ElectionAckMessage);
                    }
                    "ELECTION" => {
                        if parts.len() == 2 {
                            if let Ok(server_id) = parts[1].parse::<ServerId>() {
                                let ip_addr = self.socket.peer_addr().map(|addr| addr.to_string()).ok();
                                self.elector_addr.do_send(RegisterPeerConnectionMessage {
                                    server_id,
                                    connection_addr: ctx.address(),
                                    peer_addr: ip_addr,
                                });
                                self.elector_addr
                                    .do_send(LeaderElectionMessage { server_id });
                            }
                        }
                    }
                    "COORDINATOR" => {
                        if parts.len() == 2 {
                            if let Ok(leader_id) = parts[1].parse::<ServerId>() {
                                self.elector_addr
                                    .do_send(LeaderAnnouncementMessage { leader_id });
                            }
                        }
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
                    "REPLICA_SYNC" => {
                        if parts.len() >= 2 {
                            let mut new_table = HashMap::new();
                            if !parts[1].is_empty() {
                                for st_str in parts[1].split(';') {
                                    let props: Vec<&str> = st_str.split(',').collect();
                                    if props.len() == 6 {
                                        if let (Ok(id), Ok(x), Ok(y), Ok(bikes), Ok(slots), Ok(ts)) = (
                                            props[0].parse(), props[1].parse(), props[2].parse(),
                                            props[3].parse(), props[4].parse(), props[5].parse()
                                        ) {
                                            new_table.insert(id, StationStatus {
                                                station_id: id,
                                                location: Location { x, y },
                                                available_bikes: bikes,
                                                free_slots: slots,
                                                updated_at_secs: ts,
                                            });
                                        }
                                    }
                                }
                            }
                            self.server_addr.do_send(ReplicaSyncMessage {
                                station_table: new_table,
                            });
                        }
                    }
                    _ => {
                        println!(
                            "[SERVER] Tipo de mensaje no manejado en esta fase: {}",
                            parts[0]
                        );
                    }
                }
            }
        }
    }
}

impl Handler<NearbyStationsResponseMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: NearbyStationsResponseMessage, _ctx: &mut Self::Context) {
        let response = NearbyResponse {
            stations: msg.stations,
        };
        let response_text = response.serialize();
        let _ = self.socket.write_all(response_text.as_bytes());
        let _ = self.socket.write_all(b"\n");
    }
}

impl Handler<ConnectionClosed> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, _msg: ConnectionClosed, ctx: &mut Self::Context) {
        ctx.stop();
    }
}

impl Handler<RegisterPeerConnectionMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, msg: RegisterPeerConnectionMessage, _ctx: &mut Self::Context) {
        self.peer_servers.insert(msg.server_id, msg.connection_addr);
    }
}

impl Handler<RemovePeerMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, msg: RemovePeerMessage, _ctx: &mut Self::Context) {
        self.peer_servers.remove(&msg.server_id);
    }
}

impl Handler<LeaderAliveMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: LeaderAliveMessage, _ctx: &mut Self::Context) {
        let alive_msg = format!("IS_ALIVE|{}\n", msg.leader_id);
        let _ = self.socket.write_all(alive_msg.as_bytes());
    }
}

impl Handler<SendElectionMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: SendElectionMessage, _ctx: &mut Self::Context) {
        let election_msg = format!("ELECTION|{}\n", msg.candidate_id);
        let _ = self.socket.write_all(election_msg.as_bytes());
    }
}

impl Handler<SendElectionAckMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, _msg: SendElectionAckMessage, _ctx: &mut Self::Context) {
        let _ = self.socket.write_all(b"ELECTION_ACK\n");
    }
}

impl Handler<SendCoordinatorMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: SendCoordinatorMessage, _ctx: &mut Self::Context) {
        let coord_msg = format!("COORDINATOR|{}\n", msg.leader_id);
        let _ = self.socket.write_all(coord_msg.as_bytes());
    }
}

pub struct CentralServerActor {
    pub server_id: ServerId,
    pub is_leader: bool,
    pub leader_id: Option<ServerId>,
    pub station_table: HashMap<StationId, StationStatus>,
    pub peers: HashMap<ServerId, Addr<ConnectionActor>>,
    pub elector_addr: Option<Addr<ElectorActor>>,
    pub peer_addrs: HashMap<ServerId, String>,
}

impl CentralServerActor {
    pub fn new(server_id: ServerId, peer_addrs: std::collections::HashMap<ServerId, String>) -> Self {
        Self {
            server_id,
            is_leader: false,
            leader_id: None,
            station_table: HashMap::new(),
            peers: HashMap::new(),
            peer_addrs,
            elector_addr: None,
        }
    }
}

impl Actor for CentralServerActor {
    type Context = Context<Self>;
}

impl Handler<StationUpdateMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: StationUpdateMessage, _ctx: &mut Context<Self>) {
        if !self.is_leader {
            let leader_addr = if let Some(l_id) = self.leader_id {
                self.peer_addrs.get(&l_id).cloned().unwrap_or_default()
            } else {
                String::new()
            };
            
            msg.response_addr.do_send(RejectNotLeaderMessage { leader_addr });
            return;
        }
        println!("[SERVER LÍDER] Actualizando estación ID: {}", msg.station.station_id);
        self.station_table.insert(msg.station.station_id, msg.station);

        for peer_con in self.peers.values() {
            peer_con.do_send(SendReplicaSyncMessage {
                station_table: self.station_table.clone(),
            });
        }
    }
}

impl Handler<NearbyStationsRequestMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: NearbyStationsRequestMessage, _ctx: &mut Context<Self>) {
        let mut nearby = Vec::new();
        if self.is_leader {
            let replica_addr = self.peer_addrs.values().next().cloned().unwrap_or_default();
            msg.response_addr.do_send(RejectNotReplicaMessage { replica_addr });
            return;
        }

        for station in self.station_table.values() {
            let distance = ((station.location.x - msg.location.x).powi(2)
                + (station.location.y - msg.location.y).powi(2))
            .sqrt();

            if distance <= msg.radius {
                nearby.push(StationStatus {
                    station_id: station.station_id,
                    location: Location {
                        x: station.location.x,
                        y: station.location.y,
                    },
                    available_bikes: station.available_bikes,
                    free_slots: station.free_slots,
                    updated_at_secs: station.updated_at_secs,
                });
            }
        }

        msg.response_addr
            .do_send(NearbyStationsResponseMessage { stations: nearby });
    }
}

pub struct ElectorActor {
    pub server_id: ServerId,
    pub central_server_addr: Addr<CentralServerActor>,
    pub is_leader: bool,
    pub leader_id: Option<ServerId>,
    pub peer_servers: HashMap<ServerId, Addr<ConnectionActor>>,
    pub leader_timeout: Instant,
    pub election_in_progress: bool,
    pub can_be_leader: bool,
}

impl ElectorActor {
    pub fn new(server_id: ServerId, central: Addr<CentralServerActor>) -> Self {
        Self {
            server_id,
            central_server_addr: central,
            is_leader: false,
            leader_id: None,
            peer_servers: HashMap::new(),
            leader_timeout: Instant::now(),
            election_in_progress: false,
            can_be_leader: false,
        }
    }
}

impl Actor for ElectorActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_bully_timeout_monitor(ctx);
    }
}

pub struct SpawnerActor {
    pub server_id: ServerId,
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
        ConnectionActor::new_incoming(
            self.server_id,
            msg.0,
            self.server_addr.clone(),
            self.elector_addr.clone(),
        )
        .start();
    }
}

impl Handler<RejectNotLeaderMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: RejectNotLeaderMessage, _ctx: &mut Self::Context) {
        let text = format!("NOT_LEADER|{}\n", msg.leader_addr);
        let _ = self.socket.write_all(text.as_bytes());
    }
}

impl Handler<RejectNotReplicaMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: RejectNotReplicaMessage, _ctx: &mut Self::Context) {
        let text = format!("NOT_REPLICA|{}\n", msg.replica_addr);
        let _ = self.socket.write_all(text.as_bytes());
    }
}

impl Handler<RoleUpdateMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: RoleUpdateMessage, _ctx: &mut Context<Self>) {
        println!("[SERVER] Actualizando rol - ¿Soy líder?: {}, ID del líder: {:?}", 
                 msg.is_leader, msg.leader_id);
                 
        self.is_leader = msg.is_leader;
        self.leader_id = msg.leader_id;
    }
}

impl Handler<SendReplicaSyncMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: SendReplicaSyncMessage, _ctx: &mut Self::Context) {
        let mut stations_str = Vec::new();
        for st in msg.station_table.values() {
            stations_str.push(format!("{},{},{},{},{},{}",
                st.station_id,
                st.location.x,
                st.location.y,
                st.available_bikes,
                st.free_slots,
                st.updated_at_secs
            ));
        }

        let payload = format!("REPLICA_SYNC|{}\n", stations_str.join(";"));
        let _ = self.socket.write_all(payload.as_bytes());
    }
}

impl Handler<ReplicaSyncMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: ReplicaSyncMessage, _ctx: &mut Context<Self>) {
        if !self.is_leader {
            println!("[SERVER RÉPLICA] Sincronizando tabla de estaciones desde el Líder. Total estaciones: {}", msg.station_table.len());
            self.station_table = msg.station_table;
        }
    }
}
