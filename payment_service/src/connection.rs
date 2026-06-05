use actix::prelude::*;
use std::io::{Read, Write};
use std::net::TcpStream;
use common::*;
use crate::PaymentServiceActor;

// Actor de conexión TCP
pub struct ConnectionActor {
    pub socket: TcpStream,
    pub payment_service_addr: Addr<PaymentServiceActor>,
}

impl ConnectionActor {
    pub fn new(socket: TcpStream, payment_service_addr: Addr<PaymentServiceActor>) -> Self {
        Self {
            socket,
            payment_service_addr,
        }
    }

    fn send_response(&mut self, response_text: &str) {
        let response_formatted = format!("{}\n", response_text);
        if let Err(e) = self.socket.write_all(response_formatted.as_bytes()) {
            println!("[BANK] Error escribiendo en socket: {}", e);
        }
    }
}

impl Actor for ConnectionActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let mut stream_clone = match self.socket.try_clone() {
            Ok(s) => s,
            Err(e) => {
                println!("[BANK] Error clonando socket: {}", e);
                ctx.stop();
                return;
            }
        };

        let addr = ctx.address();
        std::thread::spawn(move || {
            let mut buffer = [0; 1024];
            loop {
                match stream_clone.read(&mut buffer) {
                    Ok(0) => {
                        addr.do_send(ConnectionClosed);
                        break;
                    }
                    Ok(n) => {
                        let data = buffer[..n].to_vec();
                        addr.do_send(IncomingData(data));
                    }
                    Err(_) => {
                        addr.do_send(ConnectionClosed);
                        break;
                    }
                }
            }
        });
    }
}

impl Handler<IncomingData> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: IncomingData, ctx: &mut Self::Context) {
        // Falta definir
    }
}

impl Handler<ConnectionClosed> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, _msg: ConnectionClosed, ctx: &mut Self::Context) {
        println!("[BANK] Conexión cerrada con el cliente.");
        ctx.stop();
    }
}

// Actor Spawner para aceptar conexiones
pub struct SpawnerActor {
    pub payment_service_addr: Addr<PaymentServiceActor>,
}

impl Actor for SpawnerActor {
    type Context = Context<Self>;
}

impl Handler<NewConnectionMessage> for SpawnerActor {
    type Result = ();

    fn handle(&mut self, msg: NewConnectionMessage, _ctx: &mut Self::Context) {
        println!("[BANK] Spawner recibiendo socket. Levantando ConnectionActor...");
        ConnectionActor::new(msg.0, self.payment_service_addr.clone()).start();
    }
}
