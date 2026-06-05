use actix::prelude::*;
use std::time::{Duration, Instant};

use crate::actors::ElectorActor;
use crate::messages_actors::*;

const LEADER_TIMEOUT: Duration = Duration::from_secs(5);
const COORDINATOR_TIMEOUT: Duration = Duration::from_secs(5);
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);

impl ElectorActor {
    pub fn start_bully_timeout_monitor(&mut self, ctx: &mut Context<Self>) {

        
        ctx.run_interval(TIMEOUT_CHECK_INTERVAL, |actor, ctx| {
            if actor.election_in_progress {
                if actor.leader_timeout.elapsed() >= COORDINATOR_TIMEOUT {
                    println!("[ELECTION] Coordinator timeout. Me proclamo lider");
                    actor.announce_leader();
                }
                return;
            }
            if actor.is_leader {
                actor.leader_timeout = Instant::now();
                actor.send_alive_to_peers();
                return;
            }

            if actor.election_in_progress {
                return;
            }

            if actor.leader_timeout.elapsed() >= LEADER_TIMEOUT {
                actor.election_in_progress = true;
                ctx.address().do_send(LeaderElectionMessage {
                    server_id: actor.server_id,
                });
            }
        });
    }

    pub fn reset_leader_timeout(&mut self) {
        self.leader_timeout = Instant::now();
        self.election_in_progress = false;
    }

    pub fn init_election(&mut self) {
        self.election_in_progress = true;
        self.leader_id = None;
        for (&peer_id, addr) in &self.peer_servers {
            if peer_id > self.server_id {
                addr.do_send(SendElectionMessage {
                    candidate_id: self.server_id,
                });
            }
        }
    }

    pub fn announce_leader(&mut self) {
        self.is_leader = true;
        self.leader_id = Some(self.server_id);
        for addr in self.peer_servers.values() {
            addr.do_send(SendCoordinatorMessage {
                leader_id: self.server_id,
            });
        }
    }

    pub fn send_alive_to_peers(&self) {
        for addr in self.peer_servers.values() {
            addr.do_send(LeaderAliveMessage);
        }
    }
}

impl Handler<LeaderAliveMessage> for ElectorActor {
    type Result = ();

    fn handle(&mut self, _msg: LeaderAliveMessage, _ctx: &mut Self::Context) {
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

        // self.central_server_addr.do_send(NewLeaderMessage { leader_id: msg.leader_id, 
        //     self.peer_servers.get(&msg.leader_id) });
    }
}
