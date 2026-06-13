use crate::entities::*;
use crate::message_types::MessageType;
use crate::messages::*;

pub trait Deserializable {
    fn deserialize(input: &str) -> Self;
}

impl Deserializable for MessageType {
    fn deserialize(input: &str) -> Self {
        let prefix = input.split("|").next().unwrap_or("");
        MessageType::from_str(prefix).expect("Invalid message type")
    }
}

impl Deserializable for RentRequest {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 4);
        Self {
            user_id: parts[1].parse().expect("Invalid user_id"),
            slot_index: parts[2].parse().expect("Invalid slot_index"),
            card_token: parts[3].to_string(),
        }
    }
}

impl Deserializable for ReturnRequest {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 6);
        Self {
            user_id: parts[1].parse().expect("Invalid user_id"),
            bike_id: parts[2].parse().expect("Invalid bike_id"),
            slot_index: parts[3].parse().expect("Invalid slot_index"),
            started_at_secs: parts[4].parse().expect("Invalid started_at_secs"),
            rental_id: parts[5].to_string(),
        }
    }
}

impl Deserializable for RentConfirmed {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 5);
        Self {
            bike_id: parts[1].parse().expect("Invalid bike_id"),
            pre_auth_cents: parts[2].parse().expect("Invalid pre_auth_cents"),
            timestamp_secs: parts[3].parse().expect("Invalid timestamp_secs"),
            rental_id: parts[4].to_string(),
        }
    }
}

impl Deserializable for RentRejected {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            reason: parts[1].to_string(),
        }
    }
}

impl Deserializable for ReturnConfirmed {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 3);
        Self {
            charged_cents: parts[1].parse().expect("Invalid charged_cents"),
            timestamp_secs: parts[2].parse().expect("Invalid timestamp_secs"),
        }
    }
}

impl Deserializable for ReturnRejected {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            reason: parts[1].to_string(),
        }
    }
}

impl Deserializable for StationUpdate {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.splitn(2, '|').collect();
        assert!(parts.len() == 2);
        Self {
            station: StationStatus::deserialize(parts[1]),
        }
    }
}

impl Deserializable for IsAlive {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            leader_id: parts[1].parse().expect("Invalid leader_id"),
        }
    }
}

impl Deserializable for Election {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            candidate_id: parts[1].parse().expect("Invalid candidate_id"),
        }
    }
}

impl Deserializable for Coordinator {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            leader_id: parts[1].parse().expect("Invalid leader_id"),
        }
    }
}

impl Deserializable for Ack {
    fn deserialize(_input: &str) -> Self {
        Self
    }
}

impl Deserializable for NearbyResponse {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.splitn(3, '|').collect();
        assert!(parts.len() == 3);

        let expected_len: usize = parts[1].parse().expect("Invalid stations length");
        let stations = if parts[2].is_empty() {
            Vec::new()
        } else {
            parts[2]
                .split(';')
                .map(StationStatus::deserialize)
                .collect::<Vec<_>>()
        };

        assert!(stations.len() == expected_len);
        Self { stations }
    }
}

impl Deserializable for Location {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split(',').collect();
        assert!(parts.len() == 2);
        Self {
            x: parts[0].parse().expect("Invalid x coordinate"),
            y: parts[1].parse().expect("Invalid y coordinate"),
        }
    }
}

impl Deserializable for StationStatus {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 9);
        Self {
            station_id: parts[0].parse().expect("Invalid station_id"),
            location: Location {
                x: parts[1].parse().expect("Invalid x coordinate"),
                y: parts[2].parse().expect("Invalid y coordinate"),
            },
            available_bikes: parts[3].parse().expect("Invalid available_bikes"),
            free_slots: parts[4].parse().expect("Invalid free_slots"),
            updated_at_secs: parts[5].parse().expect("Invalid updated_at_secs"),
            station_addr: parts[6].to_string(),
            slots_occupied: parts[7].to_string(),
            slots_frees: parts[8].to_string(),
        }
    }
}

impl Deserializable for ReservePayment {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 4);
        Self {
            transaction_id: parts[1].parse().expect("Invalid transaction_id"),
            amount_cents: parts[2].parse().expect("Invalid amount_cents"),
            card_token: parts[3].to_string(),
        }
    }
}

impl Deserializable for ReservationRejected {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 3);
        Self {
            transaction_id: parts[1].parse().expect("Invalid transaction_id"),
            reason: parts[2].to_string(),
        }
    }
}

impl Deserializable for OfflineRent {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 4);
        Self {
            rental_id: parts[1].parse().expect("Invalid rental_id"),
            bike_id: parts[2].parse().expect("Invalid bike_id"),
            user_id: parts[3].parse().expect("Invalid user_id"),
        }
    }
}

impl Deserializable for ReturnRent {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 3);
        Self {
            rental_id: parts[1].parse().expect("Invalid rental_id"),
            bike_id: parts[2].parse().expect("Invalid bike_id"),
        }
    }
}

impl Deserializable for PreparePayment {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 4);
        Self {
            transaction_id: parts[1].parse().expect("Invalid transaction_id"),
            amount_cents: parts[2].parse().expect("Invalid amount_cents"),
            card_token: parts[3].to_string(),
        }
    }
}

impl<T: TransactionMessage> Deserializable for T {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        let id = parts[1].parse().expect("Invalid transaction_id");
        Self::new(id)
    }
}

impl Deserializable for CapturePayment {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 3);
        Self {
            transaction_id: parts[1].parse().expect("Invalid transaction_id"),
            amount_cents: parts[2].parse().expect("Invalid amount_cents"),
        }
    }
}

impl Deserializable for UserBanned {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 3);
        Self {
            user_id: parts[1].parse().expect("Invalid user_id"),
            reason: parts[2].to_string(),
        }
    }
}

impl Deserializable for BanNotification {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            reason: parts[1].to_string(),
        }
    }
}

impl Deserializable for ValidateUser {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 2);
        Self {
            user_id: parts[1].parse().expect("Invalid user_id"),
        }
    }
}

impl Deserializable for UserValidationResult {
    fn deserialize(input: &str) -> Self {
        let parts: Vec<&str> = input.split('|').collect();
        assert!(parts.len() == 4);
        let is_valid = match parts[2] {
            "VALID" => true,
            "INVALID" => false,
            _ => parts[2].parse().expect("Invalid is_valid value"),
        };
        Self {
            user_id: parts[1].parse().expect("Invalid user_id"),
            is_valid,
            reason: if is_valid {
                None
            } else {
                Some(parts[3].to_string())
            },
        }
    }
}
