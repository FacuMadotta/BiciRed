use std::env;
use actix::prelude::*;
use common::{Acceptor, load_servers_csv};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

mod bully;
mod actors;
mod messages_actors;

use actors::{CentralServerActor, ConnectionActor, ElectorActor, SpawnerActor};
use common::ServerId;
use messages_actors::{NewConnectionMessage, RegisterPeerConnectionMessage};

#[actix::main]
async fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        println!("Usage: central_server <server_id> <ip_servidor> <servers.csv>");
        return Ok(());
    }
    
    let server_id = match args[1].parse::<ServerId>() {
        Ok(id) => id,
        Err(_) => {
            println!("[SERVER] server_id inválido: {}", args[1]);
            return Ok(());
        }
    };

    let ip = args[2].clone();
    let servers_csv = args[3].clone();
    println!("CentralServer {} starting on {}", server_id, ip);

    let server_nodes = match load_servers_csv(&servers_csv) {
        Ok(nodes) => nodes,
        Err(err) => {
            println!("[SERVER] Error leyendo CSV de servidores '{}': {}", servers_csv, err);
            return Ok(());
        }
    };

    let server_addr = CentralServerActor::new().start();
    let elector_addr = ElectorActor::new(server_id, server_addr.clone()).start();

    let spawner_addr = SpawnerActor {
        server_id,
        server_addr: server_addr.clone(),
        elector_addr: elector_addr.clone(),
    }.start();

    for peer in server_nodes.into_iter().filter(|node| node.addr != ip) {
        let server_addr = server_addr.clone();
        let elector_addr = elector_addr.clone();
        thread::spawn(move || {
            loop {
                match TcpStream::connect(&peer.addr) {
                    Ok(socket) => {
                        println!("[ELECTION] Conectado al peer {} en {}", peer.id, peer.addr);
                        let connection_addr = ConnectionActor::new(server_id, socket, server_addr.clone(), elector_addr.clone()).start();
                        elector_addr.do_send(RegisterPeerConnectionMessage {
                            server_id: peer.id,
                            connection_addr,
                        });
                        break;
                    }
                    Err(err) => {
                        eprintln!("[ELECTION] No se pudo conectar con peer {} ({}): {}", peer.id, peer.addr, err);
                        thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        });
    }

    Acceptor::new(ip, move |stream| {
        spawner_addr.do_send(NewConnectionMessage(stream));
    }).start();
    
    futures::future::pending::<()>().await;
    Ok(())
}