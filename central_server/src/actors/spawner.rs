use actix::prelude::*;
use common::ServerId;

use crate::actors::{CentralServerActor, ConnectionActor, ElectorActor};
use crate::messages_actors::NewConnectionMessage;

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
