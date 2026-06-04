use std::env;
use std::net::TcpStream;
use actix::prelude::*;
mod station;
mod connection;
use station::{StationActor, Station};
use connection::ConnectionActor;
use common::*;

#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: station <ip_estacion> <ip_servidor_central>");
        return Ok(());
    }
    
    let ip = args[1].clone();
    let central_ip = &args[2];

    println!("Station starting on {}", ip);

    let central_socket = TcpStream::connect(central_ip)
        .expect("Could not connect to Central Server");

    // Esto esta harcodeado inicialmente.
    let my_location = Location { x: 0.0, y: 0.0 }; 
    let station_data = Station::new(1, my_location, 10);
    
    let station_addr = StationActor::new(station_data, central_socket).start();

    Acceptor::new(ip, move |stream| {
        println!("New connection accepted, spawning ConnectionActor");
        ConnectionActor::new(stream, station_addr.clone()).start();
    }).start();

    println!("Station is running. Press Ctrl+C to stop.");
    
    futures::future::pending::<()>().await;
    Ok(())
}
