use actix::prelude::*;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
mod connection;
mod station;
use common::*;
use connection::SpawnerActor;
use station::{Station, StationActor};
use std::time::{SystemTime, UNIX_EPOCH};

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

    println!(
        "Iniciando Station {} en IP: {} | Slots: {} | Bicis iniciales: {}",
        my_id, my_ip, my_slots, my_bikes
    );

    let server_nodes = load_servers_csv(servers_csv_path).expect("Error leyendo servers.csv");
    let server_addrs: Vec<String> = server_nodes.into_iter().map(|n| n.addr).collect();

    let central_socket = connect_and_register_to_central(
        &server_addrs,
        my_id,
        Location {
            x: my_location.x,
            y: my_location.y,
        },
        &my_ip,
        my_bikes,
        my_slots - my_bikes,
    );

    let payment_socket =
        TcpStream::connect(payment_ip).expect("Error conectando al servicio de pagos");

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
        StationActor::new(station_data, central_socket, payment_tx)
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
    stream: TcpStream,
    station_addr: Addr<StationActor>,
) -> std::sync::mpsc::Sender<String> {
    let stream_writer = stream
        .try_clone()
        .expect("Error clonando el stream para escritura");
    let mut stream_reader = stream
        .try_clone()
        .expect("Error clonando el stream para lectura");
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
    tx
}

pub fn connect_and_register_to_central(
    central_servers: &[String],
    station_id: StationId,
    location: Location,
    my_listening_ip: &str,
    available_bikes: usize,
    free_slots: usize,
) -> TcpStream {
    let mut server_idx = 0;
    let mut current_ip = central_servers.first().cloned().unwrap_or_default();

    loop {
        println!(
            "[STATION] Intentando conectar al CentralServer en {}...",
            current_ip
        );

        let mut stream = match TcpStream::connect(&current_ip) {
            Ok(s) => s,
            Err(e) => {
                println!(
                    "[ADVERTENCIA] Falló nodo {}: {}. Probando siguiente...",
                    current_ip, e
                );
                server_idx = (server_idx + 1) % central_servers.len();
                current_ip = central_servers[server_idx].clone();
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            }
        };

        let status = StationStatus {
            station_id,
            location: Location {
                x: location.x,
                y: location.y,
            },
            available_bikes: available_bikes as u8,
            free_slots: free_slots as u8,
            updated_at_secs: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            station_addr: my_listening_ip.to_string(),
        };

        let payload = format!("STATION_UPDATE|{}\n", status.serialize());
        if stream.write_all(payload.as_bytes()).is_err() {
            println!("[ERROR] Error al enviar registro. Rotando nodo...");
            server_idx = (server_idx + 1) % central_servers.len();
            current_ip = central_servers[server_idx].clone();
            continue;
        }

        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();

        let mut buf = [0u8; 1024];
        match stream.read(&mut buf) {
            Ok(0) => {
                println!("[ERROR] El servidor cerró la conexión inesperadamente.");
                server_idx = (server_idx + 1) % central_servers.len();
                current_ip = central_servers[server_idx].clone();
                continue;
            }
            Ok(n) => {
                let response = String::from_utf8_lossy(&buf[..n]).trim().to_string();

                if response.starts_with("NOT_LEADER") {
                    let parts: Vec<&str> = response.split('|').collect();
                    if parts.len() > 1 && !parts[1].is_empty() {
                        let leader_ip = parts[1].to_string();
                        println!(
                            "[RED DIRECCIÓN] El nodo era réplica. Redirigiendo al Líder real: {}",
                            leader_ip
                        );
                        current_ip = leader_ip;
                        continue;
                    }
                }
                println!("[STATION] Conectado y registrado exitosamente en el Líder.");
                stream.set_read_timeout(None).unwrap();
                return stream;
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    println!("[INFO] El servidor aceptó la actualización silenciosamente (Es el Líder). Socket retenido.");
                    stream.set_read_timeout(None).unwrap();
                    return stream;
                } else {
                    println!("[ERROR] Error de lectura en el socket: {}", e);
                    server_idx = (server_idx + 1) % central_servers.len();
                    current_ip = central_servers[server_idx].clone();
                    continue;
                }
            }
        }
    }
}
