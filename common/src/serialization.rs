use crate::entities::*;
use crate::message_types::MessageType;
use crate::messages::*;

pub trait Serializable {
    fn serialize(&self) -> String;
}

impl Serializable for MessageType {
    fn serialize(&self) -> String {
        self.as_str().to_string()
    }
}

impl Serializable for RentRequest {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            MessageType::RentRequest.as_str(),
            self.user_id,
            self.slot_index,
            self.card_token,
        )
    }
}

impl Serializable for ReturnRequest {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}|{}",
            MessageType::ReturnRequest.as_str(),
            self.user_id,
            self.bike_id,
            self.slot_index,
            self.started_at_secs,
            self.rental_id
        )
    }
}

impl Serializable for RentConfirmed {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            MessageType::RentConfirmed.as_str(),
            self.bike_id,
            self.pre_auth_cents,
            self.timestamp_secs,
            self.rental_id
        )
    }
}

impl Serializable for RentRejected {
    fn serialize(&self) -> String {
        format!("{}|{}", MessageType::RentRejected.as_str(), &self.reason)
    }
}

impl Serializable for ReturnConfirmed {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}",
            MessageType::ReturnConfirmed.as_str(),
            self.charged_cents,
            self.timestamp_secs
        )
    }
}

impl Serializable for ReturnRejected {
    fn serialize(&self) -> String {
        format!("{}|{}", MessageType::ReturnRejected.as_str(), &self.reason)
    }
}

impl Serializable for StationUpdate {
    fn serialize(&self) -> String {
        format!(
            "{}|{}",
            MessageType::StationUpdate.as_str(),
            serialize_station_status(&self.station)
        )
    }
}

impl Serializable for IsAlive {
    fn serialize(&self) -> String {
        format!("{}|{}", MessageType::IsAlive.as_str(), self.leader_id)
    }
}

impl Serializable for Election {
    fn serialize(&self) -> String {
        format!("{}|{}", MessageType::Election.as_str(), self.candidate_id)
    }
}

impl Serializable for Coordinator {
    fn serialize(&self) -> String {
        format!("{}|{}", MessageType::Coordinator.as_str(), self.leader_id)
    }
}

impl Serializable for Ack {
    fn serialize(&self) -> String {
        MessageType::Ack.serialize()
    }
}

impl Serializable for NearbyResponse {
    fn serialize(&self) -> String {
        let stations = self
            .stations
            .iter()
            .map(serialize_station_status)
            .collect::<Vec<_>>()
            .join(";");

        format!(
            "{}|{}|{}",
            MessageType::NearbyResponse.as_str(),
            self.stations.len(),
            stations
        )
    }
}

impl Serializable for Location {
    fn serialize(&self) -> String {
        format!("{},{}", self.x, self.y)
    }
}

impl Serializable for StationStatus {
    fn serialize(&self) -> String {
        serialize_station_status(self)
    }
}

impl Serializable for PreparePayment {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            MessageType::PreparePayment.as_str(),
            self.transaction_id,
            self.amount_cents,
            self.card_token
        )
    }
}

impl Serializable for ReservePayment {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            MessageType::ReservePayment.as_str(),
            self.transaction_id,
            self.amount_cents,
            self.card_token
        )
    }
}

impl Serializable for ReservationRejected {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}",
            MessageType::ReservationRejected.as_str(),
            self.transaction_id,
            self.reason
        )
    }
}

impl Serializable for OfflineRent {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            MessageType::OfflineRent.as_str(),
            self.rental_id,
            self.bike_id,
            self.user_id
        )
    }
}

impl Serializable for ReturnRent {
    fn serialize(&self) -> String {
        format!(
            "{}|{}|{}",
            MessageType::ReturnRent.as_str(),
            self.rental_id,
            self.bike_id
        )
    }
}

impl<T: TransactionMessage> Serializable for T {
    fn serialize(&self) -> String {
        format!("{}|{}", T::message_type().as_str(), self.transaction_id())
    }
}

fn serialize_station_status(station: &StationStatus) -> String {
    format!(
        "{}|{}|{}|{}|{}|{}|{}",
        station.station_id,
        station.location.x,
        station.location.y,
        station.available_bikes,
        station.free_slots,
        station.updated_at_secs,
        station.station_addr
    )
}
