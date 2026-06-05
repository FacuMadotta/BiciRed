use std::env;
use actix::prelude::*;
use common::Acceptor;

mod actors;
mod messages_actors;

use actors::{CentralServerActor, ConnectionActor, ElectorActor, SpawnerActor};
use messages_actors::NewConnectionMessage;

#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: central_server <ip_servidor>");
        return Ok(());
    }
    
    let ip = args[1].clone();
    println!("CentralServer starting on {}", ip);

    let server_addr = CentralServerActor::new().start();
    let elector_addr = ElectorActor::new(1, server_addr.clone()).start();

    let spawner_addr = SpawnerActor {
        server_addr: server_addr.clone(),
        elector_addr: elector_addr.clone(),
    }.start();

    Acceptor::new(ip, move |stream| {
        spawner_addr.do_send(NewConnectionMessage(stream));
    }).start();
    
    futures::future::pending::<()>().await;
    Ok(())
}