use actix::prelude::*;
use common::NewConnectionMessage;

use crate::actors::{ConnectionActor, StationActor};

pub struct SpawnerActor {
    pub station_addr: Addr<StationActor>,
}

impl Actor for SpawnerActor {
    type Context = Context<Self>;
}

impl Handler<NewConnectionMessage> for SpawnerActor {
    type Result = ();

    fn handle(&mut self, msg: NewConnectionMessage, _ctx: &mut Self::Context) {
        println!("[STATION] Spawner recibiendo socket. Levantando ConnectionActor...");
        ConnectionActor::new(msg.0, self.station_addr.clone()).start();
    }
}
