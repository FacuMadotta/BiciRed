use actix::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::io;

// Actor genérico que escucha en una dirección TCP y crea un handler para cada conexión.
pub struct Acceptor {
    address: String,
    handler_factory: Box<dyn Fn(TcpStream) + Send + Sync + 'static>, // Este handler nos permite que cada parte del sistema pueda definir cómo 
                                                                    // manejar nuevas conexiones sin acoplar el Acceptor a un tipo específico de Actor.

}

impl Acceptor {
    pub fn new<F>(address: String, f: F) -> Self 
    where 
        F: Fn(TcpStream) + Send + Sync + 'static 
    {
        Self {
            address,
            handler_factory: Box::new(f),
        }
    }
}

impl Actor for Acceptor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let listener = TcpListener::bind(&self.address).expect("Failed to bind Acceptor");
        listener.set_nonblocking(true).expect("Failed to set non-blocking");
        
        println!("Acceptor listening on {}", self.address);

        // Se agrega el listener al runtime de Actix como un stream
        ctx.add_stream(AcceptorStream(listener));
    }
}

// Sirve para convertir un TcpListener en un Stream de Actix. Esto permite que el acceptor no bloquee el hilo principal y pueda manejar múltiples conexiones de forma asíncrona.
struct AcceptorStream(TcpListener);

impl futures::Stream for AcceptorStream {
    type Item = io::Result<TcpStream>;

    fn poll_next(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        match self.0.accept() {
            Ok((stream, _)) => std::task::Poll::Ready(Some(Ok(stream))),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => std::task::Poll::Pending,
            Err(e) => std::task::Poll::Ready(Some(Err(e))),
        }
    }
}

impl StreamHandler<io::Result<TcpStream>> for Acceptor {
    fn handle(&mut self, res: io::Result<TcpStream>, _ctx: &mut Self::Context) {
        match res {
            Ok(stream) => {
                // Se ejecuta la fábrica para crear el handler directamente
                (self.handler_factory)(stream);
            }
            Err(e) => eprintln!("Acceptor error: {}", e),
        }
    }
}
