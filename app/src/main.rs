use common::{load_servers_csv, Location};
use std::env;
use std::io::{self, Write};

mod client;
mod models;
use client::AppClient;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("Uso: app <ruta_al_archivo_servers.csv>");
        return;
    }

    let csv_path = &args[1];

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

    let mut app = AppClient::new(99, server_addrs);
    let test_station_ip = "127.0.0.1:9000"; // Lo manda el servidor internamente; y poder saber cuál es la ip.

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
            "1" => app.query_central(Location { x: 10.5, y: 20.0 }, 5.0),
            "2" => {
                let mut card_input = String::new();

                print!("> Ingresá tu token de tarjeta: ");
                io::stdout().flush().unwrap();
                io::stdin().read_line(&mut card_input).unwrap();

                app.rent_station(test_station_ip, 0, card_input.trim());
            }
            "3" => app.return_station(test_station_ip, 1),
            "4" => {
                println!("Cerrando aplicación...");
                break;
            }
            other => println!("Opción desconocida: {}", other),
        }
    }
}
