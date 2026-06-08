pub type StationId = u32;
pub type BikeId = u32;
pub type UserId = u32;
pub type ServerId = u32;

#[derive(Debug, Clone)]
pub struct Location {
    pub x: f64,
    pub y: f64,
}

#[derive(Debug, Clone)]
pub struct StationStatus {
    pub station_id: StationId,
    pub location: Location,
    pub available_bikes: u8,
    pub free_slots: u8,
    pub updated_at_secs: u64,
}

#[derive(Debug, Clone)]
pub struct ServerNode {
    pub id: ServerId,
    pub addr: String,
}
