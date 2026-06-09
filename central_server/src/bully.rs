use actix::prelude::*;
use std::time::{Duration, Instant};

use crate::actors::ElectorActor;
use crate::messages_actors::*;

const LEADER_TIMEOUT: Duration = Duration::from_secs(5);
const COORDINATOR_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

impl ElectorActor {
    pub fn start_bully_timeout_monitor(&mut self, ctx: &mut Context<Self>) {
        ctx.run_interval(TIMEOUT_CHECK_INTERVAL, |actor, _ctx| {
            if actor.election_in_progress {
                if actor.leader_timeout.elapsed() >= COORDINATOR_TIMEOUT {
                    if actor.can_be_leader {
                        println!("[ELECTION] Coordinator timeout. Me proclamo lider");
                        actor.announce_leader();
                    } else {
                        actor.init_election();
                    }
                }
                return;
            }

            if actor.is_leader {
                actor.leader_timeout = Instant::now();
                actor.send_alive_to_peers();
                return;
            }

            if actor.leader_timeout.elapsed() >= LEADER_TIMEOUT {
                actor.init_election();
            }
        });
    }

    pub fn reset_leader_timeout(&mut self) {
        self.leader_timeout = Instant::now();
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
        self.leader_timeout = Instant::now();
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

    pub fn send_alive_to_peers(&self) {
        for addr in self.peer_servers.values() {
            addr.do_send(LeaderAliveMessage {
                leader_id: self.server_id,
            });
        }
    }
}

impl Handler<LeaderAliveMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, msg: LeaderAliveMessage, _ctx: &mut Self::Context) {
        if self.leader_id != Some(msg.leader_id) {
            self.is_leader = false;
            self.leader_id = Some(msg.leader_id);

            self.central_server_addr.do_send(RoleUpdateMessage {
                is_leader: self.is_leader,
                leader_id: self.leader_id,
            });
        }

        self.reset_leader_timeout();
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
