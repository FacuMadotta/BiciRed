use serde_json;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use common::{BikeId, StationId, UserId};

use crate::domain::{PENDING_CHARGES_PREFIX, PENDING_RENTS_PREFIX};

pub fn get_rents_filename(station_id: StationId) -> String {
    format!("{}{}.json", PENDING_RENTS_PREFIX, station_id)
}

pub fn get_charges_filename(station_id: StationId) -> String {
    format!("{}{}.json", PENDING_CHARGES_PREFIX, station_id)
}

pub fn build_rent_json(
    rental_id: &str,
    user_id: UserId,
    bike_id: BikeId,
    card_token: &str,
) -> String {
    let timestamp = current_timestamp_secs();
    format!(
        "{{\"rental_id\":\"{}\",\"user_id\":{},\"card_token\":\"{}\",\"bike_id\":{},\"timestamp\":{}}}\n",
        rental_id, user_id, card_token, bike_id, timestamp
    )
}

pub fn build_charge_json(rental_id: &str, amount_cents: u32, bike_id: BikeId) -> String {
    let timestamp = current_timestamp_secs();
    format!(
        "{{\"rental_id\":\"{}\",\"amount_cents\":{},\"bike_id\":{},\"timestamp\":{}}}\n",
        rental_id, amount_cents, bike_id, timestamp
    )
}

pub fn save_pending_rent(
    station_id: StationId,
    rental_id: &str,
    user_id: UserId,
    bike_id: BikeId,
    card_token: &str,
) {
    let filename = get_rents_filename(station_id);
    let json = build_rent_json(rental_id, user_id, bike_id, card_token);
    append_to_file(&filename, &json);
}

pub fn save_pending_charge(
    station_id: StationId,
    rental_id: &str,
    amount_cents: u32,
    bike_id: BikeId,
) {
    let filename = get_charges_filename(station_id);
    let json = build_charge_json(rental_id, amount_cents, bike_id);
    append_to_file(&filename, &json);
}

fn append_to_file(filename: &str, content: &str) {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(filename)
        .and_then(|mut file| file.write_all(content.as_bytes()))
        .unwrap_or_else(|e| {
            eprintln!(
                "[ERROR PERSISTENCIA] No se pudo escribir en {}: {}",
                filename, e
            )
        });
}

pub struct PendingRentRecord {
    pub rental_id: String,
    pub card_token: String,
    pub bike_id: BikeId,
    pub user_id: UserId,
}

pub struct PendingChargeRecord {
    pub rental_id: String,
    pub amount_cents: u32,
    pub bike_id: BikeId,
}

pub fn parse_rent_record(line: &str) -> Option<PendingRentRecord> {
    if line.trim().is_empty() {
        return None;
    }
    let rent = serde_json::from_str::<serde_json::Value>(line).ok()?;
    Some(PendingRentRecord {
        rental_id: rent.get("rental_id")?.as_str()?.to_string(),
        card_token: rent.get("card_token")?.as_str()?.to_string(),
        bike_id: rent.get("bike_id")?.as_u64()? as BikeId,
        user_id: rent.get("user_id")?.as_u64()? as UserId,
    })
}

pub fn parse_charge_record(line: &str) -> Option<PendingChargeRecord> {
    if line.trim().is_empty() {
        return None;
    }
    let charge = serde_json::from_str::<serde_json::Value>(line).ok()?;
    Some(PendingChargeRecord {
        rental_id: charge.get("rental_id")?.as_str()?.to_string(),
        amount_cents: charge.get("amount_cents")?.as_u64()? as u32,
        bike_id: charge.get("bike_id")?.as_u64()? as BikeId,
    })
}

pub fn flush_pending_file(filename: &str, lines_to_keep: &[String]) {
    if lines_to_keep.is_empty() {
        std::fs::remove_file(filename).ok();
    } else {
        std::fs::write(filename, lines_to_keep.join("\n") + "\n").unwrap_or_else(|e| {
            eprintln!(
                "[ERROR PERSISTENCIA] No se pudo actualizar {}: {}",
                filename, e
            )
        });
    }
}

fn current_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
