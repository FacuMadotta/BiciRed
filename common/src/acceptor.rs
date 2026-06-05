use std::net::{TcpListener, TcpStream};
use std::thread;


pub struct Acceptor {
    address: String,
    handler_factory: Box<dyn Fn(TcpStream) + Send + Sync + 'static>,
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
    
    pub fn start(self) {
        thread::spawn(move || {
            let listener = TcpListener::bind(&self.address).expect("Error en bindear el Acceptor");
            println!("Acceptor escuchando en {}", self.address);
            
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => {
                        (self.handler_factory)(s);
                    }
                    Err(e) => eprintln!("Error aceptando conexión: {}", e),
                }
            }
        });
    }
}
