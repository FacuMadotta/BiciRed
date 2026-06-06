use std::collections::HashMap;
use std::env;
use actix::prelude::*;
use common::{Acceptor, NewConnectionMessage};
mod connection;
mod service;
use connection::{SpawnerActor};
use service::PaymentServiceActor;


#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: payment_service <ip_puerto>");
        return Ok(());
    }

    let ip = args[1].clone();
    println!("[BANK] Iniciando PaymentService en {}", ip);

    let payment_service_addr = PaymentServiceActor::new().start();

    let spawner_addr = SpawnerActor { payment_service_addr: payment_service_addr.clone() }.start();

    Acceptor::new(ip, move |stream| {
        spawner_addr.do_send(NewConnectionMessage(stream));
    })
    .start();

    println!("[BANK] Servidor de pagos iniciado. Presiona Ctrl+C para detener.");

    std::future::pending::<()>().await;
    Ok(())
}
