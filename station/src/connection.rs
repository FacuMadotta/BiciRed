use actix::prelude::*;
use common::*;
use std::net::TcpStream;

// Actor que maneja la conexión con un cliente, recibiendo solicitudes, y enviando respuestas.
pub struct ConnectionActor {
    pub socket: TcpStream,
    pub station_addr: Addr<StationActor>,
}

impl ConnectionActor {
    pub fn new(socket: TcpStream, station: Addr<StationActor>) -> Self {
        Self { socket, station_addr: station }
    }

    fn send_message<T>(&mut self, message_text: &str, ctx: &mut <Self as Actor>::Context) 
    where 
        T: Deserializable + 'static,
        StationActor: Handler<RequestMessage<T>>,
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
        let _ = self.socket.set_nonblocking(true);
        if let Ok(stream_clone) = self.socket.try_clone() {
            ctx.add_stream(SocketStream(stream_clone)); // Se usa el stream para leer datos de la conexión sin bloquear el actor.
        } else {
            println!("Failed to clone socket for ConnectionActor");
            ctx.stop();
        }
    }
}

pub struct RequestMessage<T> {
    pub request: T,
    pub response: Addr<ConnectionActor>,
}

impl<T> Message for RequestMessage<T> where T: Send + 'static {
    type Result = ();
}

impl StreamHandler<std::io::Result<Vec<u8>>> for ConnectionActor {
    fn handle(&mut self, msg: std::io::Result<Vec<u8>>, _ctx: &mut Self::Context) {
        match msg {
            Ok(data) => {
                if let Ok(text) = String::from_utf8(data) {
                    let message_text = text.trim();
                    let message_type = MessageType::deserialize(message_text);
                    
                    match message_type {
                        MessageType::RentRequest => self.send_message::<RentRequest>(message_text, _ctx),
                        MessageType::ReturnRequest => self.send_message::<ReturnRequest>(message_text, _ctx),
                        _ => {
                            println!("Unknown message type received: {}", message_text);
                        }
                    }
                    
                }
            }
            Err(e) => {
                println!("Error reading from socket: {}", e);
                _ctx.stop();
            }
        }
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