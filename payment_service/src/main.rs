use actix::prelude::*;
use common::{Acceptor, NewConnectionMessage};
use std::collections::HashMap;
use std::env;
use std::fs;
mod connection;
mod service;
use connection::SpawnerActor;
use service::PaymentServiceActor;

#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        println!("Usage: payment_service <ip_puerto> <tarjetas.csv>");
        return Ok(());
    }

    let ip = args[1].clone();
    let tarjetas_csv = args[2].clone();
    println!("[BANK] Iniciando PaymentService en {}", ip);

    let tarjetas_db = cargar_tarjetas(&tarjetas_csv);
    println!(
        "[BANK] Base de datos cargada. Tarjetas registradas: {}",
        tarjetas_db.len()
    );

    let payment_service_addr = PaymentServiceActor::new(tarjetas_db, tarjetas_csv.clone()).start();

    let spawner_addr = SpawnerActor {
        payment_service_addr: payment_service_addr.clone(),
    }
    .start();

    Acceptor::new(ip, move |stream| {
        spawner_addr.do_send(NewConnectionMessage(stream));
    })
    .start();

    println!("[BANK] Servidor de pagos iniciado. Presiona Ctrl+C para detener.");

    std::future::pending::<()>().await;
    Ok(())
}

fn cargar_tarjetas(ruta: &str) -> HashMap<String, u32> {
    let mut db = HashMap::new();
    if let Ok(contenido) = fs::read_to_string(ruta) {
        for linea in contenido.lines() {
            let partes: Vec<&str> = linea.split(',').collect();
            if partes.len() == 2 {
                let token = partes[0].to_string();
                if let Ok(saldo) = partes[1].trim().parse::<u32>() {
                    db.insert(token, saldo);
                }
            }
        }
    }
    db
}
