use actix::prelude::*;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
mod connection;
mod station;
use common::*;
use connection::SpawnerActor;
use station::{Station, StationActor};

#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        println!("Usage: station <id> <servers.csv> <stations.csv> <ip_payment_service>");
        return Ok(());
    }

    let my_id: StationId = args[1].parse().expect("ID de estación inválido");
    let servers_csv_path = &args[2];
    let stations_csv_path = &args[3];
    let payment_ip = &args[4];

    let (mut my_ip, my_slots, my_bikes, my_location) = load_stations_csv(stations_csv_path, my_id)
        .expect("No se encontró el ID de la estación en stations.csv");

    my_ip = my_ip.trim().to_string();

    let my_ip_for_actor = my_ip.clone();

    println!(
        "Iniciando Station {} en IP: {} | Slots: {} | Bicis iniciales: {}",
        my_id, my_ip, my_slots, my_bikes
    );

    let server_nodes = load_servers_csv(servers_csv_path).expect("Error leyendo servers.csv");
    let server_addrs: Vec<String> = server_nodes.into_iter().map(|n| n.addr).collect();

    let payment_socket =
        TcpStream::connect(payment_ip);

    let station_data = Station::new(
        my_id,
        Location {
            x: my_location.x,
            y: my_location.y,
        },
        my_slots,
        my_bikes,
    );

    let station_addr = StationActor::create(move |ctx| {
        let actor_address = ctx.address();
        let payment_tx = start_payment_gateway(payment_socket, actor_address.clone());
        StationActor::new(station_data, payment_tx, server_addrs, my_ip_for_actor) 
    });

    let spawner = SpawnerActor {
        station_addr: station_addr.clone(),
    }
    .start();

    Acceptor::new(my_ip, move |stream| { 
        println!("Nueva conexión aceptada, levantando ConnectionActor");
        spawner.do_send(NewConnectionMessage(stream));
    })
    .start();

    println!("Station iniciada. Presiona Ctrl+C para detener.");

    std::future::pending::<()>().await;
    Ok(())
}

pub fn start_payment_gateway(
    stream: std::io::Result<TcpStream>,
    station_addr: Addr<StationActor>,
) -> std::sync::mpsc::Sender<String> {
    let (tx, rx) = std::sync::mpsc::channel::<String>();

    match stream {
        Ok(s) => {
            let stream_writer = s.try_clone().expect("Error clonando el stream para escritura");
            let mut stream_reader = s.try_clone().expect("Error clonando el stream para lectura");
            std::thread::spawn(move || {
                println!("[PAYMENT] Conexión establecida con el servicio de pagos.");
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
                                MessageType::ReservationRejected => {
                                    let reject_msg = ReservationRejected::deserialize(message_text);
                                    station_addr.do_send(reject_msg);
                                }
                                _ => {
                                    eprintln!(
                                        "Mensaje desconocido recibido del servicio de pagos: {}",
                                        message_text
                                    );
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
        }
        Err(e) => {
            println!("\n[ALERTA OFFLINE] No se pudo conectar al Payment Service: {}", e);
            println!("[ALERTA OFFLINE] La Estación operará de forma local y guardará los cobros para más tarde.\n");
        }
    }
    tx
}

