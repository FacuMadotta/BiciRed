use common::{load_servers_csv, Location};
use std::env;
use std::io::{self, Write};

mod client;
mod models;
use client::AppClient;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Uso: app <id> <ruta_al_archivo_servers.csv>");
        return;
    }

    let id = args[1].parse::<u32>().unwrap_or(0);
    let csv_path = &args[2];

    let server_nodes = match load_servers_csv(csv_path) {
        Ok(nodes) => nodes,
        Err(e) => {
            println!("Error fatal al leer el archivo CSV '{}': {}", csv_path, e);
            return;
        }
    };

    if server_nodes.is_empty() {
        println!("Error: El archivo CSV está vacío o no tiene servidores válidos.");
        return;
    }

    let server_addrs: Vec<String> = server_nodes.into_iter().map(|node| node.addr).collect();

    let mut app = AppClient::new(id, server_addrs);
    let mut input = String::new();

    println!(
        "=== Bienvenido a BiciRed App (Usuario: {}) ===",
        app.user_id
    );

    loop {
        println!("\n------------------------------------------------");
        println!("1) Consultar Estaciones (CentralServer)");
        println!("2) Alquilar Bicicleta (Station)");
        println!("3) Devolver Bicicleta (Station)");
        println!("4) Salir");
        print!("> ");

        let _ = io::stdout().flush();
        input.clear();
        if io::stdin().read_line(&mut input).is_err() {
            println!("Error de lectura de consola");
            break;
        }

        match input.trim() {
            "1" => {
                let mut x_input = String::new();
                print!("> Ingresá tu coordenada X actual: ");
                io::stdout().flush().unwrap();
                io::stdin().read_line(&mut x_input).unwrap();
                let x: f64 = x_input.trim().parse().unwrap_or(0.0);

                let mut y_input = String::new();
                print!("> Ingresá tu coordenada Y actual: ");
                io::stdout().flush().unwrap();
                io::stdin().read_line(&mut y_input).unwrap();
                let y: f64 = y_input.trim().parse().unwrap_or(0.0);

                let mut r_input = String::new();
                print!("> Ingresá el radio de búsqueda: ");
                io::stdout().flush().unwrap();
                io::stdin().read_line(&mut r_input).unwrap();
                let radius: f64 = r_input.trim().parse().unwrap_or(5.0);

                app.query_central(Location { x, y }, radius)
            }
            "2" => {
                if app.cached_stations.is_empty() {
                    println!("Primero tenés que consultar las estaciones (Opción 1).");
                    continue;
                }

                let mut station_input = String::new();
                print!("> Ingresá el ID de la estación: ");
                io::stdout().flush().unwrap();
                io::stdin().read_line(&mut station_input).unwrap();
                let target_id: usize = station_input.trim().parse().unwrap_or(0);
                let target_addr = app
                    .cached_stations
                    .iter()
                    .find(|s| s.station_id == target_id as u32)
                    .map(|s| s.station_addr.clone());

                if let Some(addr) = target_addr {
                    let mut slot_input = String::new();
                    print!("> Ingresá el número de slot: ");
                    io::stdout().flush().unwrap();
                    io::stdin().read_line(&mut slot_input).unwrap();
                    let slot_index: usize = slot_input.trim().parse().unwrap_or(0);

                    let mut card_input = String::new();
                    print!("> Ingresá tu token de tarjeta: ");
                    io::stdout().flush().unwrap();
                    io::stdin().read_line(&mut card_input).unwrap();

                    app.rent_station(&addr, slot_index, card_input.trim());
                } else {
                    println!("Estación no encontrada en la caché local.");
                }
            }
            "3" => {
                if app.current_rental.is_none() {
                    println!("\n[ERROR] No tenés ninguna bici alquilada actualmente.");
                    continue;
                }

                if app.cached_stations.is_empty() {
                    println!("Primero tenés que consultar las estaciones (Opción 1) para buscar a dónde devolverla.");
                    continue;
                }

                let mut station_input = String::new();
                print!("> Ingresá el ID de la estación para devolver la bici: ");
                io::stdout().flush().unwrap();
                io::stdin().read_line(&mut station_input).unwrap();
                let target_id: usize = station_input.trim().parse().unwrap_or(0);

                let target_addr = app
                    .cached_stations
                    .iter()
                    .find(|s| s.station_id == target_id as u32)
                    .map(|s| s.station_addr.clone());

                if let Some(addr) = target_addr {
                    let mut slot_input = String::new();
                    print!("> Ingresá el número de slot libre: ");
                    io::stdout().flush().unwrap();
                    io::stdin().read_line(&mut slot_input).unwrap();
                    let slot_index: usize = slot_input.trim().parse().unwrap_or(0);
                    app.return_station(&addr, slot_index);
                } else {
                    println!("Estación no encontrada en la caché local.");
                }
            }
            "4" => {
                println!("Cerrando aplicación...");
                break;
            }
            other => println!("Opción desconocida: {}", other),
        }
    }
}
