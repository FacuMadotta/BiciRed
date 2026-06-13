use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::actors::{CentralServerActor, ConnectionActor};
use crate::messages_actors::*;

const COORDINATOR_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

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

    pub fn start_bully_timeout_monitor(&mut self, ctx: &mut Context<Self>) {
        ctx.run_interval(TIMEOUT_CHECK_INTERVAL, |actor, _ctx| {
            if actor.election_in_progress {
                if actor.leader_timeout.elapsed() >= COORDINATOR_TIMEOUT {
                    if actor.can_be_leader {
                        println!("[ELECTION] Coordinator timeout. Me proclamo lider");
                        actor.announce_leader();
                    } else {
                        println!("[ELECTION] El nodo mayor falló en asumir. Reiniciando...");
                        actor.init_election();
                    }
                }
            }
        });
    }

    pub fn reset_leader_timeout(&mut self) {
        self.election_in_progress = false;
        self.can_be_leader = true;
    }

    pub fn init_election(&mut self) {
        self.election_in_progress = true;
        self.can_be_leader = true;
        self.leader_id = None;
        self.leader_timeout = Instant::now();

        let mut higher_peer_found = false;
        for (&peer_id, addr) in &self.peer_servers {
            if peer_id > self.server_id {
                higher_peer_found = true;
                addr.do_send(SendElectionMessage {
                    candidate_id: self.server_id,
                });
            }
        }

        if !higher_peer_found {
            self.announce_leader();
        }
    }

    pub fn announce_leader(&mut self) {
        self.is_leader = true;
        self.leader_id = Some(self.server_id);
        self.election_in_progress = false;
        self.can_be_leader = true;
        for addr in self.peer_servers.values() {
            addr.do_send(SendCoordinatorMessage {
                leader_id: self.server_id,
            });
        }
        self.central_server_addr.do_send(RoleUpdateMessage {
            is_leader: self.is_leader,
            leader_id: self.leader_id,
        });
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

impl Handler<LeaderElectionMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, msg: LeaderElectionMessage, _ctx: &mut Self::Context) {
        if msg.server_id < self.server_id {
            if let Some(addr) = self.peer_servers.get(&msg.server_id) {
                addr.do_send(SendElectionAckMessage);
            }

            if !self.election_in_progress {
                self.init_election();
            }
        }
    }
}

impl Handler<LeaderAnnouncementMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, msg: LeaderAnnouncementMessage, _ctx: &mut Self::Context) {
        self.is_leader = false;
        self.leader_id = Some(msg.leader_id);
        self.reset_leader_timeout();

        self.central_server_addr.do_send(RoleUpdateMessage {
            is_leader: self.is_leader,
            leader_id: self.leader_id,
        });
    }
}

impl Handler<ElectionAckMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, _msg: ElectionAckMessage, _ctx: &mut Self::Context) {
        self.can_be_leader = false;
        self.leader_timeout = Instant::now();
    }
}

impl Handler<PeerDisconnectedMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, msg: PeerDisconnectedMessage, _ctx: &mut Self::Context) {
        self.peer_servers.remove(&msg.server_id);

        if Some(msg.server_id) == self.leader_id {
            println!("[ELECTION] ¡El Líder ha caído! Iniciando algoritmo Bully...");

            self.leader_id = None;
            self.is_leader = false;
            self.election_in_progress = true;

            self.central_server_addr.do_send(RoleUpdateMessage {
                is_leader: false,
                leader_id: None,
            });

            self.init_election();
        }
    }
}
