use actix::prelude::*;
use common::NewConnectionMessage;

use crate::service::PaymentServiceActor;

use super::ConnectionActor;

pub struct SpawnerActor {
    pub payment_service_addr: Addr<PaymentServiceActor>,
}

impl Actor for SpawnerActor {
    type Context = Context<Self>;
}

impl Handler<NewConnectionMessage> for SpawnerActor {
    type Result = ();

    fn handle(&mut self, msg: NewConnectionMessage, _ctx: &mut Self::Context) {
        println!("[BANK] Nueva conexión recibida. Levantando ConnectionActor...");
        ConnectionActor::new(msg.0, self.payment_service_addr.clone()).start();
    }
}
