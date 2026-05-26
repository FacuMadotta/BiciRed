use std::env;
use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;


fn handle_client(mut socket: TcpStream) {
    let mut buf = [0u8; 1024];
    match socket.read(&mut buf) {
        // Recepcion de datos del cliente y bicicleta a alquilar
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let listening_ip = args.get(1).map(|s| s.as_str());
    let Some(ip) = listening_ip else {
        eprintln!("No IP provided");
        return;
    };
    println!("Station starting and listening on {}", ip);

    let listener = TcpListener::bind(ip).expect("failed to bind");
    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                thread::spawn(|| handle_client(s));
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
}
