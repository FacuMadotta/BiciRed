use crate::entities::*;
use actix::prelude::*;
use crate::message_types::MessageType;

#[derive(Message)]
#[rtype(result = "()")]
pub struct RentRequest {
    pub user_id: UserId,
    pub slot_index: usize,
    pub card_token: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ReturnRequest {
    pub user_id: UserId,
    pub bike_id: BikeId,
    pub slot_index: usize,
    pub started_at_secs: u64,
    pub rental_id: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RentConfirmed {
    pub bike_id: BikeId,
    pub pre_auth_cents: u32,
    pub timestamp_secs: u64,
    pub rental_id: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RentRejected {
    pub reason: String,
}

#[derive(Message)]
#[rtype(result = "()")] 
pub struct ReturnConfirmed {
    pub charged_cents: u32,
    pub timestamp_secs: u64,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ReturnRejected {
    pub reason: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct StationUpdate {
    pub station: StationStatus,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct IsAlive;

#[derive(Message)]
#[rtype(result = "()")]
pub struct Election {
    pub candidate_id: ServerId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Coordinator {
    pub leader_id: ServerId,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Ack;

#[derive(Message)]
#[rtype(result = "()")]
pub struct NearbyResponse {
    pub stations: Vec<StationStatus>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct IncomingData(pub Vec<u8>);

#[derive(Message)]
#[rtype(result = "()")]
pub struct ConnectionClosed;

#[derive(Message)]
#[rtype(result = "()")]
pub struct NewConnectionMessage(pub std::net::TcpStream);

#[derive(Message)]
#[rtype(result = "()")]
pub struct PreparePayment {
    pub transaction_id: String,
    pub amount_cents: u32,
    pub card_token: String,
}

pub trait TransactionMessage {
    fn new(id: String) -> Self;
    fn transaction_id(&self) -> String;
    fn message_type() -> MessageType;
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct VoteCommit {
    pub transaction_id: String,
}

impl TransactionMessage for VoteCommit {
    fn new(id: String) -> Self {
        Self { transaction_id: id }
    }
    fn transaction_id(&self) -> String {
        self.transaction_id
    }
    fn message_type() -> MessageType {
        MessageType::VoteCommit
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct VoteAbort {
    pub transaction_id: String,
}

impl TransactionMessage for VoteAbort {
    fn new(id: String) -> Self {
        Self { transaction_id: id }
    }
    fn transaction_id(&self) -> String {
        self.transaction_id
    }
    fn message_type() -> MessageType {
        MessageType::VoteAbort
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CommitPayment {
    pub transaction_id: String,
}

impl TransactionMessage for CommitPayment {
    fn new(id: String) -> Self {
        Self { transaction_id: id }
    }
    fn transaction_id(&self) -> String {
        self.transaction_id
    }
    fn message_type() -> MessageType {
        MessageType::CommitPayment
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CapturePayment {
    pub transaction_id: String,
}

impl TransactionMessage for CapturePayment {
    fn new(id: String) -> Self {
        Self { transaction_id: id }
    }
    fn transaction_id(&self) -> String {
        self.transaction_id
    }
    fn message_type() -> MessageType {
        MessageType::CapturePayment
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct RollbackPayment {
    pub transaction_id: String,
}

impl TransactionMessage for RollbackPayment {
    fn new(id: String) -> Self {
        Self { transaction_id: id }
    }
    fn transaction_id(&self) -> String {
        self.transaction_id
    }
    fn message_type() -> MessageType {
        MessageType::RollbackPayment
    }
}