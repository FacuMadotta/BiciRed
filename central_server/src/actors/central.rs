use actix::prelude::*;
use common::*;
use std::collections::HashMap;

use crate::actors::{ConnectionActor, ElectorActor};
use crate::messages_actors::*;

const FILE_BANNED: &str = "banned_users_";

pub struct CentralServerActor {
    pub server_id: ServerId,
    pub is_leader: bool,
    pub leader_id: Option<ServerId>,
    pub station_table: HashMap<StationId, StationStatus>,
    pub peers: HashMap<ServerId, Addr<ConnectionActor>>,
    pub elector_addr: Option<Addr<ElectorActor>>,
    pub peer_addrs: HashMap<ServerId, String>,
    pub users_banned: HashMap<UserId, String>,
    pub indultos: std::collections::HashSet<UserId>,
}

impl CentralServerActor {
    pub fn new(
        server_id: ServerId,
        peer_addrs: std::collections::HashMap<ServerId, String>,
    ) -> Self {
        let filename = format!("{}{}.json", FILE_BANNED, server_id);
        let users_banned = if let Ok(content) = std::fs::read_to_string(&filename) {
            println!("[SERVER] Restaurando lista de usuarios baneados desde el disco...");
            serde_json::from_str(&content).unwrap_or_else(|_| HashMap::new())
        } else {
            HashMap::new()
        };
        Self {
            server_id,
            is_leader: false,
            leader_id: None,
            station_table: HashMap::new(),
            peers: HashMap::new(),
            peer_addrs,
            elector_addr: None,
            users_banned,
            indultos: std::collections::HashSet::new(),
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

    fn save_banned_users(&self) {
        let filename = format!("{}{}.json", FILE_BANNED, self.server_id);
        if let Ok(json_content) = serde_json::to_string(&self.users_banned) {
            if let Err(e) = std::fs::write(&filename, json_content) {
                eprintln!("[SERVER] Error al guardar baneados en disco: {}", e);
            }
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

            let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                Err(err) => {
                    eprintln!("Error obteniendo timestamp: {err}");
                    return;
                }
            };

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
        if self.indultos.remove(&msg.user_id) {
            println!("[CENTRAL] Falso positivo evitado. El usuario {} ya había devuelto la bici en otra estación.", msg.user_id);
            return;
        }
        println!(
            "[SERVER] Usuario {} ha sido baneado. Razón: {}",
            msg.user_id, msg.reason
        );
        self.users_banned.insert(msg.user_id, msg.reason.clone());
        self.broadcast_replica_sync();
        self.save_banned_users();
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

impl Handler<RoleUpdateMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: RoleUpdateMessage, _ctx: &mut Context<Self>) {
        println!(
            "[SERVER] Actualizando rol - ¿Soy líder?: {}, ID del líder: {:?}",
            msg.is_leader, msg.leader_id
        );
        self.is_leader = msg.is_leader;
        self.leader_id = msg.leader_id;

        if self.is_leader {
            println!(
                "[SERVER LÍDER] ¡Liderazgo asumido por elección! Sincronizando clúster completo..."
            );
            self.broadcast_replica_sync();
        }
    }
}

impl Handler<ReplicaSyncMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: ReplicaSyncMessage, _ctx: &mut Context<Self>) {
        if !self.is_leader {
            println!("[SERVER RÉPLICA] Sincronizando tabla de estaciones desde el Líder. Total estaciones: {}, Usuarios baneados: {}", msg.station_table.len(), msg.banned_users.len());
            self.station_table = msg.station_table;
            self.users_banned = msg.banned_users;
            self.save_banned_users();
        }
    }
}

impl Handler<PeerConnectedMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: PeerConnectedMessage, ctx: &mut Self::Context) {
        println!(
            "[SERVER] Recibida nueva conexión cruda para el peer {}. Iniciando actor...",
            msg.peer_id
        );

        self.peer_addrs.insert(msg.peer_id, msg.peer_addr.clone());

        let connection_addr = ConnectionActor::new_outgoing(
            self.server_id,
            msg.peer_id,
            msg.peer_addr,
            msg.socket,
            ctx.address(),
            self.elector_addr
                .clone()
                .expect("ElectorActor no registrado en CentralServerActor"),
        )
        .start();

        self.peers.insert(msg.peer_id, connection_addr.clone());

        if self.is_leader {
            println!(
                "[SERVER LÍDER] Conexión saliente establecida con {}. Sincronizando estado...",
                msg.peer_id
            );
            self.broadcast_replica_sync();
        }

        if let Some(elector_addr) = &self.elector_addr {
            let msg = RegisterPeerConnectionMessage {
                server_id: msg.peer_id,
                connection_addr: connection_addr.clone(),
                peer_addr: None,
            };
            elector_addr.do_send(msg);
        }
    }
}

impl Handler<RegisterElectionActor> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: RegisterElectionActor, _ctx: &mut Self::Context) {
        println!("[SERVER] ElectorActor registrado en CentralServerActor");
        self.elector_addr = Some(msg.elector_addr);
    }
}

impl Handler<RemovePeerMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: RemovePeerMessage, _ctx: &mut Self::Context) {
        println!(
            "[SERVER] Removiendo peer {} de la tabla de peers",
            msg.server_id
        );
        self.peers.remove(&msg.server_id);
    }
}

impl Handler<RegisterPeerConnectionMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: RegisterPeerConnectionMessage, _ctx: &mut Self::Context) {
        let ip_info = msg
            .peer_addr
            .unwrap_or_else(|| "IP Desconocida".to_string());
        println!(
            "[SERVER] Registrando conexión del peer {} (IP: {}) en la tabla de peers",
            msg.server_id, ip_info
        );
        self.peers.insert(msg.server_id, msg.connection_addr);
        if self.is_leader {
            println!(
                "[SERVER LÍDER] ¡Peer {} conectado! Enviando sincronización de bienvenida...",
                msg.server_id
            );
            self.broadcast_replica_sync();
        }
    }
}

impl Handler<UpdateStationTimestamp> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: UpdateStationTimestamp, _ctx: &mut Context<Self>) {
        if let Some(station) = self.station_table.get_mut(&msg.station_id) {
            let now = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
                Ok(duration) => duration.as_secs(),
                Err(err) => {
                    eprintln!("Error obteniendo timestamp: {err}");
                    return;
                }
            };
            station.updated_at_secs = now;
        }
    }
}

impl Handler<ReturnRent> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: ReturnRent, _ctx: &mut Self::Context) {
        let parts: Vec<&str> = msg.rental_id.split('-').collect();
        if parts.len() >= 2 {
            if let Ok(user_id) = parts[1].parse::<UserId>() {
                self.indultos.insert(user_id);
            }
        }
    }
}
