use std::env;
use std::net::TcpStream;
use std::io::{Read, Write};
use actix::prelude::*;
mod station;
mod connection;
use station::{StationActor, Station};
use connection::{SpawnerActor};
use common::*;

#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        println!("Usage: station <ip_estacion> <ip_servidor_central> <ip_payment_service>");
        return Ok(());
    }
    
    let ip = args[1].clone();
    let central_ip = &args[2];
    let payment_ip = &args[3];

    println!("Station iniciada en {}", ip);

    let central_socket = TcpStream::connect(central_ip)
        .expect("Error conectando al servidor central");

    let payment_socket = TcpStream::connect(payment_ip)
        .expect("Error conectando al servicio de pagos");

    // Esto esta harcodeado inicialmente.
    let my_location = Location { x: 0.0, y: 0.0 }; 
    let station_data = Station::new(1, my_location, 10);
    
    let station_addr = StationActor::create(move |ctx| {
        let actor_address = ctx.address();
        let payment_tx = start_payment_gateway(payment_socket, actor_address.clone());
        StationActor::new(station_data, central_socket, payment_tx)
    });

    let spawner = SpawnerActor { station_addr: station_addr.clone() }.start();

    Acceptor::new(ip, move |stream| {
        println!("Nueva conexión aceptada, levantando ConnectionActor");
        spawner.do_send(NewConnectionMessage(stream));
    }).start();

    println!("Station iniciada. Presiona Ctrl+C para detener.");
    
    std::future::pending::<()>().await;
    Ok(())
}

pub fn start_payment_gateway(stream: TcpStream, station_addr: Addr<StationActor>) -> std::sync::mpsc::Sender<String> {
    let stream_writer = stream.try_clone().expect("Error clonando el stream para escritura");
    let mut stream_reader = stream.try_clone().expect("Error clonando el stream para lectura");
    let (tx, rx) = std::sync::mpsc::channel::<String>();

    std::thread::spawn(move || {
        let mut writer = stream_writer;
        for message in rx {
            if let Err(e) = writer.write_all(message.as_bytes()) {
                eprintln!("Error escribiendo al servicio de pagos: {}", e);
                break;
            }
            let _ = writer.flush();
        }
    });

    std::thread::spawn(move || {
        let mut buffer = [0; 1024];
        loop {
            match stream_reader.read(&mut buffer) {
                Ok(0) => {
                    println!("Conexión cerrada por el servicio de pagos");
                    break;
                }
                Ok(n) => {
                    let text = String::from_utf8_lossy(&buffer[..n]);   
                    let message_text = text.trim();
                    let message_type = MessageType::deserialize(message_text);
                    match message_type {
                        MessageType::VoteCommit => {
                            let vote_msg = VoteCommit::deserialize(message_text);
                            station_addr.do_send(vote_msg);
                        }
                        MessageType::VoteAbort => {
                            let vote_msg = VoteAbort::deserialize(message_text);
                            station_addr.do_send(vote_msg);
                        }
                        _ => {
                            eprintln!("Mensaje desconocido recibido del servicio de pagos: {}", message_text);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error leyendo del servicio de pagos: {}", e);
                    break;
                }
            }
        }
    });
    tx 
}