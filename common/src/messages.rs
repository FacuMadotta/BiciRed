use crate::entities::*;

pub struct RentRequest {
    pub user_id: UserId,
    pub slot_index: usize,
    pub card_token: String,
}

pub struct ReturnRequest {
    pub user_id: UserId,
    pub bike_id: BikeId,
    pub slot_index: usize,
    pub started_at_secs: u64,
}

pub struct RentConfirmed {
    pub bike_id: BikeId,
    pub pre_auth_cents: u32,
    pub timestamp_secs: u64,
}

pub struct RentRejected {
    pub reason: String,
}

pub struct ReturnConfirmed {
    pub charged_cents: u32,
    pub timestamp_secs: u64,
}

pub struct ReturnRejected {
    pub reason: String,
}

pub struct StationUpdate {
    pub station: StationStatus,
}

pub struct IsAlive;

pub struct Election {
    pub candidate_id: ServerId,
}

pub struct Coordinator {
    pub leader_id: ServerId,
}

pub struct Ack;

pub struct NearbyResponse {
    pub stations: Vec<StationStatus>,
}
