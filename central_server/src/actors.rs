use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::net::TcpStream;

pub struct ConnectionActor {
    pub socket: TcpStream,
    pub server_addr: Addr<CentralServerActor>,
    pub elector_addr: Addr<ElectorActor>,
}

impl ConnectionActor {
    pub fn new(socket: TcpStream, server: Addr<CentralServerActor>, elector: Addr<ElectorActor>) -> Self {
        Self { socket, server_addr: server, elector_addr: elector }
    }
}

impl Actor for ConnectionActor {
    type Context = Context<Self>;
}

pub struct CentralServerActor {
    pub station_table: HashMap<StationId, StationStatus>,
}

impl CentralServerActor {
    pub fn new() -> Self {
        Self { station_table: HashMap::new() }
    }
}

impl Actor for CentralServerActor {
    type Context = Context<Self>;
}

pub struct ElectorActor {
    pub server_id: ServerId,
    pub central_server_addr: Addr<CentralServerActor>,
    pub is_leader: bool,
    pub leader_id: Option<ServerId>,
    pub connection_addrs: Vec<Addr<ConnectionActor>>,
}

impl ElectorActor {
    pub fn new(server_id: ServerId, central: Addr<CentralServerActor>) -> Self {
        Self { server_id, central_server_addr: central, is_leader: false, leader_id: None, connection_addrs: Vec::new() }
    }
}

impl Actor for ElectorActor {
    type Context = Context<Self>;
}

