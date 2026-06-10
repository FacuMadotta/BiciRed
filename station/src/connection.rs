use crate::StationActor;
use actix::prelude::*;
use common::*;
use std::io::{Read, Write};
use std::net::TcpStream;

// Actor que maneja la conexión con un cliente, recibiendo solicitudes, y enviando respuestas.
pub struct ConnectionActor {
    pub socket: TcpStream,
    pub station_addr: Addr<StationActor>,
}

impl ConnectionActor {
    pub fn new(socket: TcpStream, station: Addr<StationActor>) -> Self {
        Self {
            socket,
            station_addr: station,
        }
    }

    fn send_message<T>(&mut self, message_text: &str, ctx: &mut <Self as Actor>::Context)
    where
        T: Deserializable + 'static + Send,
        StationActor: Handler<RequestMessage<T, ConnectionActor>>,
    {
        let request_data = T::deserialize(message_text);
        self.station_addr.do_send(RequestMessage {
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
            Err(_e) => {
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
                    Err(e) => {
                        println!("Error leyendo del socket: {}", e);
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
                MessageType::RentRequest => self.send_message::<RentRequest>(message_text, ctx),
                MessageType::ReturnRequest => self.send_message::<ReturnRequest>(message_text, ctx),
                MessageType::VoteCommit => self.send_message::<VoteCommit>(message_text, ctx),
                MessageType::VoteAbort => self.send_message::<VoteAbort>(message_text, ctx),
                _ => {
                    println!("Mensaje desconocido recibido: {}", message_text);
                }
            }
        }
    }
}

impl Handler<ConnectionClosed> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, _msg: ConnectionClosed, ctx: &mut Self::Context) {
        println!("Cerrando conexión con cliente.");
        ctx.stop();
    }
}

impl Handler<RentConfirmed> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: RentConfirmed, _ctx: &mut Self::Context) {
        self.send_response(msg);
    }
}

impl Handler<RentRejected> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: RentRejected, _ctx: &mut Self::Context) {
        self.send_response(msg);
    }
}

impl Handler<ReturnConfirmed> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: ReturnConfirmed, _ctx: &mut Self::Context) {
        self.send_response(msg);
    }
}

impl Handler<ReturnRejected> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: ReturnRejected, _ctx: &mut Self::Context) {
        self.send_response(msg);
    }
}

// Mensajes de 2PC
impl Handler<PreparePayment> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: PreparePayment, _ctx: &mut Self::Context) {
        self.send_response(msg);
    }
}
impl Handler<Prepare> for ConnectionActor {
    type Result = ();

    fn handle(&mut self, msg: Prepare, _ctx: &mut Self::Context) {
        self.send_response(msg);
    }
}

// Actor que permite levantar un nuevo ConnectionActor por cada nueva conexión entrante, recibiendo los sockets desde el Acceptor.
pub struct SpawnerActor {
    pub station_addr: Addr<StationActor>,
}

impl Actor for SpawnerActor {
    type Context = Context<Self>;
}

impl Handler<NewConnectionMessage> for SpawnerActor {
    type Result = ();

    fn handle(&mut self, msg: NewConnectionMessage, _ctx: &mut Self::Context) {
        println!("Spawner recibiendo socket. Levantando ConnectionActor...");
        ConnectionActor::new(msg.0, self.station_addr.clone()).start();
    }
}
