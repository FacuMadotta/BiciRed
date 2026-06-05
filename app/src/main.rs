use std::io::{self, Write};
use common::Location;

mod client;
mod models;
use client::AppClient;

fn main() {
    let mut app = AppClient::new(99, vec!["127.0.0.1:8080".to_string()]); // Se me ocurrio hacerlo en archivo csv; se lea y sea random de ahi (FORMATO, id_servidor, ip, puerto)
    let test_station_ip = "127.0.0.1:9000"; // yo lo estoy probando con el comando ns 127.0.0.1:8080 y mandandole mensajes que puede hacer la estación. Pero la misma idea que el servidor con el id de estacion, cant de bicis, etc (formato, id_estacion, ip, puerto, cantidad_bicis, cantidad_slots)
    
    let mut input = String::new();
    
    println!("=== Bienvenido a BiciRed App (Usuario: {}) ===", app.user_id);
    
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
            "1" => app.query_central(Location { x: 10.0, y: 20.0 }, 5.0),
            "2" => app.rent_station(test_station_ip, 0, "VISA_5555"), // Estaria bueno pedirle el token al iniciar la app o pedirselo cuando ejecuta esta acción ?
            "3" => app.return_station(test_station_ip, 1),
            "4" => { 
                println!("Cerrando aplicación..."); 
                break; 
            },
            other => println!("Opción desconocida: {}", other),
        }
    }
}
