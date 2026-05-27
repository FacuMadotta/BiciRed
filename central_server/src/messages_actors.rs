use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::net::TcpStream;

// Connection --> Central
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct StationUpdateMessage {
    pub station: StationStatus,
}

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct LeaderAliveMessage;

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct LeaderElectionMessage {
    pub server_id: ServerId,
}

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct LeaderAnnouncementMessage {
    pub leader_id: ServerId,
}

// Election --> Connection
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct SendElectionAckMessage;

// Election --> Connection
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct SendCoordinatorMessage {
    pub leader_id: ServerId,
}

// Election --> Connection
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct SendElectionMessage {
    pub candidate_id: ServerId,
}

// Connection --> Central
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct NearbyStationsRequestMessage {
    pub location: Location,
    pub radius: f64,
    pub response_addr: Addr<ConnectionActor>,
}

// Central --> Connection
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct NearbyStationsResponseMessage {
    pub stations: Vec<StationStatus>,
}
