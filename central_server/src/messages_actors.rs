use crate::actors::ConnectionActor;
use crate::actors::ElectorActor;
use actix::prelude::*;
use common::*;
// Connection --> Central
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct StationUpdateMessage {
    pub station: StationStatus,
    pub response_addr: Addr<ConnectionActor>,
}

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct LeaderAliveMessage {
    pub leader_id: ServerId,
}

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct LeaderElectionMessage {
    pub server_id: ServerId,
}

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct RegisterPeerConnectionMessage {
    pub server_id: ServerId,
    pub connection_addr: Addr<ConnectionActor>,
    pub peer_addr: Option<String>,
}

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct RemovePeerMessage {
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

// Connection --> Election
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct ElectionAckMessage; // Mensaje de Ok de mayores id

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

#[derive(Message)]
#[rtype(result = "()")]
pub struct IncomingData(pub Vec<u8>);

#[derive(Message)]
#[rtype(result = "()")]
pub struct NewConnectionMessage(pub std::net::TcpStream);

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct PeerConnectedMessage {
    pub peer_id: ServerId,
    pub peer_addr: String,
    pub socket: std::net::TcpStream,
}

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct RegisterElectionActor {
    pub elector_addr: Addr<ElectorActor>,
}

// Election --> Central
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct RoleUpdateMessage {
    pub is_leader: bool,
    pub leader_id: Option<ServerId>,
}

// Central --> Connection
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct RejectNotLeaderMessage {
    pub leader_addr: String,
}

// Central --> Connection
#[derive(Message, Debug, Clone)]
#[rtype(result = "()")]
pub struct RejectNotReplicaMessage {
    pub replica_addr: String,
}
