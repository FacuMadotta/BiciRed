use common::{load_servers_csv, Location};
use std::env;
use std::io::{self, Write};

mod client;
mod models;
use client::AppClient;

fn read_input(prompt: &str) -> String {
    let mut input = String::new();
    print!("{}", prompt);
    io::stdout().flush().unwrap();
    io::stdin().read_line(&mut input).unwrap();
    input.trim().to_string()
}

fn handle_query_option(app: &mut AppClient) {
    let x: f64 = read_input("> Ingresá tu coordenada X actual: ")
        .parse()
        .unwrap_or(0.0);
    let y: f64 = read_input("> Ingresá tu coordenada Y actual: ")
        .parse()
        .unwrap_or(0.0);
    let radius: f64 = read_input("> Ingresá el radio de búsqueda: ")
        .parse()
        .unwrap_or(5.0);

    app.query_central(Location { x, y }, radius)
}

fn handle_rent_option(app: &mut AppClient) {
    if app.cached_stations.is_empty() {
        println!("Primero tenés que consultar las estaciones (Opción 1).");
        return;
    }

    let target_id: usize = read_input("> Ingresá el ID de la estación: ")
        .parse()
        .unwrap_or(0);

    let target_addr = app
        .cached_stations
        .iter()
        .find(|s| s.station_id == target_id as u32)
        .map(|s| s.station_addr.clone());

    if let Some(addr) = target_addr {
        let slot_index: usize = read_input("> Ingresá el número de slot: ")
            .parse()
            .unwrap_or(0);
        let card_token = read_input("> Ingresá tu token de tarjeta: ");

        app.rent_station(&addr, slot_index, &card_token);
    } else {
        println!("Estación no encontrada en la caché local.");
    }
}

fn handle_return_option(app: &mut AppClient) {
    if app.current_rental.is_none() {
        println!("\n[ERROR] No tenés ninguna bici alquilada actualmente.");
        return;
    }

    if app.cached_stations.is_empty() {
        println!(
            "Primero tenés que consultar las estaciones (Opción 1) para buscar a dónde devolverla."
        );
        return;
    }

    let target_id: usize = read_input("> Ingresá el ID de la estación para devolver la bici: ")
        .parse()
        .unwrap_or(0);

    let target_addr = app
        .cached_stations
        .iter()
        .find(|s| s.station_id == target_id as u32)
        .map(|s| s.station_addr.clone());

    if let Some(addr) = target_addr {
        let slot_index: usize = read_input("> Ingresá el número de slot libre: ")
            .parse()
            .unwrap_or(0);
        app.return_station(&addr, slot_index);
    } else {
        println!("Estación no encontrada en la caché local.");
    }
}

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

        let op = read_input("> ");

        match op.as_str() {
            "1" => {
                handle_query_option(&mut app);
            }
            "2" => {
                handle_rent_option(&mut app);
            }
            "3" => {
                handle_return_option(&mut app);
            }
            "4" => {
                println!("Cerrando aplicación...");
                break;
            }
            other => println!("Opción desconocida: {}", other),
        }
    }
}
