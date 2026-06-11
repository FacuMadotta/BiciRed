use common::{BikeId, StationId};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveRental {
    pub bike_id: BikeId,
    pub started_at_secs: u64,
    pub pre_auth_cents: u32,
    pub station_id: StationId,
}
