use crate::PaymentServiceActor;
use actix::prelude::*;
use common::*;
use std::io::{Read, Write};
use std::net::TcpStream;

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

    fn send_message<T>(&mut self, message_text: &str, ctx: &mut <Self as Actor>::Context)
    where
        T: Deserializable + 'static + Send,
        PaymentServiceActor: Handler<RequestMessage<T, ConnectionActor>>,
    {
        let request_data = T::deserialize(message_text);
        self.payment_service_addr.do_send(RequestMessage {
            request: request_data,
            response: ctx.address(),
        });
    }

    fn send_response<T>(&mut self, response: T)
    where
        T: Serializable,
    {
        let response_text = response.serialize();
        let _ = self.socket.write_all(response_text.as_bytes());
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
        if let Ok(text) = String::from_utf8(msg.0) {
            let message_text = text.trim();
            let message_type = MessageType::deserialize(message_text);

            match message_type {
                MessageType::PreparePayment => {
                    self.send_message::<PreparePayment>(message_text, ctx)
                }
                MessageType::CommitPayment => self.send_message::<CommitPayment>(message_text, ctx),
                MessageType::CapturePayment => {
                    self.send_message::<CapturePayment>(message_text, ctx)
                }
                MessageType::RollbackPayment => {
                    self.send_message::<RollbackPayment>(message_text, ctx)
                }
                MessageType::ReservePayment => {
                    self.send_message::<ReservePayment>(message_text, ctx)
                }
                _ => {
                    println!("Mensaje desconocido recibido: {}", message_text);
                }
            }
        }
    }
}

impl Handler<VoteCommit> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: VoteCommit, _ctx: &mut Self::Context) {
        println!(
            "[BANK] Enviando VoteCommit para transaction_id {}",
            msg.transaction_id()
        );
        self.send_response(msg);
    }
}

impl Handler<VoteAbort> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: VoteAbort, _ctx: &mut Self::Context) {
        println!(
            "[BANK] Enviando VoteAbort para transaction_id {}",
            msg.transaction_id()
        );
        self.send_response(msg);
    }
}

impl Handler<ReservationRejected> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: ReservationRejected, _ctx: &mut Self::Context) {
        println!(
            "[BANK] Enviando ReservationRejected para transaction_id {}: {}",
            msg.transaction_id, msg.reason
        );
        self.send_response(msg);
    }
}

impl Handler<PaymentResult> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: PaymentResult, _ctx: &mut Self::Context) {
        println!(
            "[BANK] Enviando PaymentResult para transaction_id {}: success={}",
            msg.transaction_id, msg.success
        );
        self.send_response(msg);
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
