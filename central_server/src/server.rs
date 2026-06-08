use crate::actors::{CentralServerActor, ConnectionActor};
use crate::messages_actors::*;
use actix::prelude::*;

impl Handler<PeerConnectedMessage> for CentralServerActor {
    type Result = ();

    fn handle(&mut self, msg: PeerConnectedMessage, ctx: &mut Self::Context) {
        println!(
            "[SERVER] Recibida nueva conexión cruda para el peer {}. Iniciando actor...",
            msg.peer_id
        );

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

        if let Some(elector_addr) = &self.elector_addr {
            let msg = RegisterPeerConnectionMessage {
                server_id: msg.peer_id,
                connection_addr: connection_addr.clone(),
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
        println!(
            "[SERVER] Registrando conexión del peer {} en la tabla de peers",
            msg.server_id
        );
        self.peers.insert(msg.server_id, msg.connection_addr);
    }
}
