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

impl StreamHandler<std::io::Result<Vec<u8>>> for ConnectionActor {
    fn handle(&mut self, msg: std::io::Result<Vec<u8>>, _ctx: &mut Self::Context) {
        match msg {
            Ok(data) => {
                if let Ok(text) = String::from_utf8(data) {
                    let message_text = text.trim();
                    let message_type = MessageType::deserialize(message_text);
                    
                    match message_type {
                        MessageType::RentRequest => {
                            let rent_request = RentRequest::deserialize(message_text);
                            self.station_addr.do_send(rent_request);
                        }
                        MessageType::ReturnRequest => {
                            let return_request = ReturnRequest::deserialize(message_text);
                            self.station_addr.do_send(return_request);
                        }
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