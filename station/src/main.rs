use actix::prelude::*;
use std::env;
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

    let payment_ip = args[4].clone();

    let station_data = Station::new(
        my_id,
        Location {
            x: my_location.x,
            y: my_location.y,
        },
        my_slots,
        my_bikes,
    );

    let station_addr = StationActor::create(move |_ctx| {
        StationActor::new(station_data, server_addrs, my_ip_for_actor, payment_ip) 
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

