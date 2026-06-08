use actix::prelude::*;
use common::{load_servers_csv, Acceptor};
use std::env;
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

mod actors;
mod bully;
mod messages_actors;
mod server;

use actors::{CentralServerActor, ElectorActor, SpawnerActor};
use common::ServerId;
use messages_actors::{NewConnectionMessage, PeerConnectedMessage, RegisterElectionActor};

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
            println!(
                "[SERVER] Error leyendo CSV de servidores '{}': {}",
                servers_csv, err
            );
            return Ok(());
        }
    };

    let server_addr = CentralServerActor::new(server_id).start();
    let elector_addr = ElectorActor::new(server_id, server_addr.clone()).start();
    server_addr.do_send(RegisterElectionActor {
        elector_addr: elector_addr.clone(),
    });

    let spawner_addr = SpawnerActor {
        server_id,
        server_addr: server_addr.clone(),
        elector_addr: elector_addr.clone(),
    }
    .start();

    for peer in server_nodes
        .into_iter()
        .filter(|node| node.id > server_id && node.addr != ip)
    {
        let server_addr = server_addr.clone();
        thread::spawn(move || loop {
            match TcpStream::connect(&peer.addr) {
                Ok(socket) => {
                    println!("[ELECTION] Conectado al peer {} en {}", peer.id, peer.addr);
                    server_addr.do_send(PeerConnectedMessage {
                        peer_id: peer.id,
                        peer_addr: peer.addr.clone(),
                        socket,
                    });
                    break;
                }
                Err(err) => {
                    eprintln!(
                        "[ELECTION] No se pudo conectar con peer {} ({}): {}",
                        peer.id, peer.addr, err
                    );
                    thread::sleep(Duration::from_secs(1));
                }
            }
        });
    }

    Acceptor::new(ip, move |stream| {
        spawner_addr.do_send(NewConnectionMessage(stream));
    })
    .start();

    futures::future::pending::<()>().await;
    Ok(())
}
