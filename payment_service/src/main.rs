use actix::prelude::*;
use common::{Acceptor, NewConnectionMessage};
use std::collections::HashMap;
use std::env;

mod actors;
mod service;
#[cfg(test)]
mod test_payment;

use actors::SpawnerActor;
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

    let tarjetas_db = load_cards_from_csv(&tarjetas_csv);
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

fn load_cards_from_csv(path: &str) -> HashMap<String, u32> {
    let mut db = HashMap::new();
    let Ok(content) = std::fs::read_to_string(path) else {
        eprintln!("[BANK] No se pudo leer el archivo de tarjetas: {}", path);
        return db;
    };
    for line in content.lines() {
        let parts: Vec<&str> = line.split(',').collect();
        if let [token, balance] = parts.as_slice() {
            if let Ok(saldo) = balance.trim().parse::<u32>() {
                db.insert(token.to_string(), saldo);
            }
        }
    }
    db
}
