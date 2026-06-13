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
                                let ip_addr =
                                    self.socket.peer_addr().map(|addr| addr.to_string()).ok();
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
                    "VALIDATE_USER" => {
                        let validate_msg = ValidateUser::deserialize(message_text);
                        self.server_addr.do_send(ValidateUserMessage {
                            user_id: validate_msg.user_id,
                            response_addr: ctx.address(),
                        });
                    }
                    "USER_BANNED" => {
                        let ban_info = UserBanned::deserialize(message_text);
                        self.server_addr.do_send(ban_info);
                    }
                    // "IS_ALIVE" => {
                    //     let is_alive = IsAlive::deserialize(message_text);
                    //     self.elector_addr.do_send(LeaderAliveMessage {
                    //         leader_id: is_alive.leader_id,
                    //     });
                    // }
                    "ELECTION_ACK" | "ACK" => {
                        self.elector_addr.do_send(ElectionAckMessage);
                    }
                    "ELECTION" => {
                        if parts.len() == 2 {
                            if let Ok(server_id) = parts[1].parse::<ServerId>() {
                                let ip_addr =
                                    self.socket.peer_addr().map(|addr| addr.to_string()).ok();
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
                        if parts.len() == 5 {
                            let user_id: UserId = parts[1].parse().unwrap_or(0);
                            let x: f64 = parts[2].trim().parse().unwrap_or(0.0);
                            let y: f64 = parts[3].trim().parse().unwrap_or(0.0);
                            let radius: f64 = parts[4].trim().parse().unwrap_or(0.0);

                            self.server_addr.do_send(NearbyStationsRequestMessage {
                                user_id,
                                location: Location { x, y },
                                radius,
                                response_addr: ctx.address(),
                            });
                        }
                    }
                    "REPLICA_SYNC" => {
                        if parts.len() >= 3 {
                            let mut new_table = HashMap::new();
                            if !parts[1].is_empty() {
                                for st_str in parts[1].split(';') {
                                    let props: Vec<&str> = st_str.split('#').collect();
                                    if props.len() == 9 {
                                        if let (
                                            Ok(id),
                                            Ok(x),
                                            Ok(y),
                                            Ok(bikes),
                                            Ok(slots),
                                            Ok(ts),
                                        ) = (
                                            props[0].parse(),
                                            props[1].parse(),
                                            props[2].parse(),
                                            props[3].parse(),
                                            props[4].parse(),
                                            props[5].parse(),
                                        ) {
                                            new_table.insert(
                                                id,
                                                StationStatus {
                                                    station_id: id,
                                                    location: Location { x, y },
                                                    available_bikes: bikes,
                                                    free_slots: slots,
                                                    updated_at_secs: ts,
                                                    station_addr: props[6].to_string(),
                                                    slots_occupied: props[7].to_string(),
                                                    slots_frees: props[8].to_string(),
                                                },
                                            );
                                        }
                                    }
                                }
                            }

                            let mut banned_users = HashMap::new();
                            if parts.len() >= 3 && !parts[2].is_empty() {
                                for user_str in parts[2].split(';') {
                                    let props: Vec<&str> = user_str.split(',').collect();
                                    if props.len() == 2 {
                                        if let Ok(user_id) = props[0].parse() {
                                            banned_users.insert(user_id, props[1].to_string());
                                        }
                                    }
                                }
                            }

                            self.server_addr.do_send(ReplicaSyncMessage {
                                station_table: new_table,
                                banned_users: banned_users,
                            });
                        }
                    }
                    "PING" => {
                        if parts.len() == 2 {
                            if let Ok(id) = parts[1].parse::<StationId>() {
                                self.server_addr.do_send(UpdateStationTimestamp { station_id: id });
                            }
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
        if let Some(peer_id) = self.peer_id {
            println!("[SERVER] Conexión cerrada con el peer {}", peer_id);
            self.elector_addr.do_send(PeerDisconnectedMessage { server_id: peer_id });
        }
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

impl Handler<BanNotification> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: BanNotification, _ctx: &mut Self::Context) {
        let ban_msg = msg.serialize();
        let _ = self.socket.write_all(ban_msg.as_bytes());
        let _ = self.socket.write_all(b"\n");
    }
}

impl Handler<UserValidationResult> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: UserValidationResult, _ctx: &mut Self::Context) {
        let validation_msg = msg.serialize();
        let _ = self.socket.write_all(validation_msg.as_bytes());
        let _ = self.socket.write_all(b"\n");
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
    pub users_banned: HashMap<UserId, String>,
}

impl CentralServerActor {
    pub fn new(
        server_id: ServerId,
        peer_addrs: std::collections::HashMap<ServerId, String>,
    ) -> Self {
        Self {
            server_id,
            is_leader: false,
            leader_id: None,
            station_table: HashMap::new(),
            peers: HashMap::new(),
            peer_addrs,
            elector_addr: None,
            users_banned: HashMap::new(),
        }
    }

    fn broadcast_replica_sync(&self) {
        for peer_con in self.peers.values() {
            peer_con.do_send(SendReplicaSyncMessage {
                station_table: self.station_table.clone(),
                banned_users: self.users_banned.clone(),
            });
        }
    }
}

impl Actor for CentralServerActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("[SERVER] Iniciando recolector de basura de estaciones...");
        ctx.run_interval(std::time::Duration::from_secs(15), |act, _ctx| {
            if !act.is_leader {
                return;
            }

            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();

            let mut estaciones_muertas = Vec::new();
            for (id, station) in &act.station_table {
                if now.saturating_sub(station.updated_at_secs) > 30 {
                    estaciones_muertas.push(*id);
                }
            }

            if !estaciones_muertas.is_empty() {
                for id in estaciones_muertas {
                    println!("[SERVER LÍDER] Estación {} eliminada por inactividad.", id);
                    act.station_table.remove(&id);
                }
                act.broadcast_replica_sync();
            }
        });
    }
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

            msg.response_addr
                .do_send(RejectNotLeaderMessage { leader_addr });
            return;
        }
        println!(
            "[SERVER LÍDER] Actualizando estación ID: {}",
            msg.station.station_id
        );
        self.station_table
            .insert(msg.station.station_id, msg.station);

        self.broadcast_replica_sync();
    }
}

impl Handler<UserBanned> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: UserBanned, _ctx: &mut Context<Self>) {
        println!(
            "[SERVER] Usuario {} ha sido baneado. Razón: {}",
            msg.user_id, msg.reason
        );

        self.users_banned.insert(msg.user_id, msg.reason.clone());
        self.broadcast_replica_sync();
    }
}

impl Handler<ValidateUserMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: ValidateUserMessage, _ctx: &mut Context<Self>) {
        println!("[SERVER] Validando usuario {}", msg.user_id);

        let validation_result = if let Some(reason) = self.users_banned.get(&msg.user_id) {
            UserValidationResult {
                user_id: msg.user_id,
                is_valid: false,
                reason: Some(reason.clone()),
            }
        } else {
            UserValidationResult {
                user_id: msg.user_id,
                is_valid: true,
                reason: None,
            }
        };

        msg.response_addr.do_send(validation_result);
    }
}

impl Handler<NearbyStationsRequestMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: NearbyStationsRequestMessage, _ctx: &mut Context<Self>) {
        let mut nearby = Vec::new();
        if self.is_leader {
            let replica_addr = self
                .peer_addrs
                .iter()
                .filter(|(&id, _)| id != self.server_id)
                .map(|(_, addr)| addr.clone())
                .next()
                .unwrap_or_default();
            msg.response_addr
                .do_send(RejectNotReplicaMessage { replica_addr });
            return;
        }

        if let Some(reason) = self.users_banned.get(&msg.user_id) {
            msg.response_addr.do_send(BanNotification {
                reason: reason.clone(),
            });
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
                    station_addr: station.station_addr.clone(),
                    slots_occupied: station.slots_occupied.clone(),
                    slots_frees: station.slots_frees.clone(),
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
        ctx.run_later(std::time::Duration::from_secs(3), |act, _ctx| {
            if act.leader_id.is_none() {
                println!("[ELECTION] Arranque de nodo. Iniciando elección inicial...");
                act.init_election();
            }
        });
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
        println!(
            "[SERVER] Actualizando rol - ¿Soy líder?: {}, ID del líder: {:?}",
            msg.is_leader, msg.leader_id
        );

        self.is_leader = msg.is_leader;
        self.leader_id = msg.leader_id;
    }
}

impl Handler<SendReplicaSyncMessage> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: SendReplicaSyncMessage, _ctx: &mut Self::Context) {
        let mut stations_str = Vec::new();
        for st in msg.station_table.values() {
            stations_str.push(format!(
                "{}#{}#{}#{}#{}#{}#{}#{}#{}",
                st.station_id,
                st.location.x,
                st.location.y,
                st.available_bikes,
                st.free_slots,
                st.updated_at_secs,
                st.station_addr,
                st.slots_occupied,
                st.slots_frees
            ));
        }

        let mut banned_users_str = Vec::new();
        for (user_id, reason) in &msg.banned_users {
            banned_users_str.push(format!("{},{}", user_id, reason));
        }

        let payload = format!(
            "REPLICA_SYNC|{}|{}\n",
            stations_str.join(";"),
            banned_users_str.join(";")
        );
        let _ = self.socket.write_all(payload.as_bytes());
    }
}

impl Handler<ReplicaSyncMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: ReplicaSyncMessage, _ctx: &mut Context<Self>) {
        if !self.is_leader {
            println!("[SERVER RÉPLICA] Sincronizando tabla de estaciones desde el Líder. Total estaciones: {}, Usuarios baneados: {}", msg.station_table.len(), msg.banned_users.len());
            self.station_table = msg.station_table;
            self.users_banned = msg.banned_users;
        }
    }
}
