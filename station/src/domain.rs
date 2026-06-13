use common::{BikeId, Location, StationId};
use serde::{Deserialize, Serialize};

pub const PENDING_RENTS_PREFIX: &str = "pending_rents_";
pub const PENDING_CHARGES_PREFIX: &str = "pending_charges_";
pub const OFFLINE_PREFIX: &str = "inventario_estacion_";
pub const TIMEOUT_SECS: u64 = 30;
pub const PRE_AUTH_AMOUNT_CENTS: u32 = 100;
pub const AMOUNT_PER_MINUTE_CENTS: u32 = 50;
pub const TIME_TO_RETURN: u64 = 60;

#[derive(Serialize, Deserialize, Clone)]
pub struct Slot {
    pub index: usize,
    pub state: SlotState,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum SlotState {
    Empty,
    Occupied { bike_id: BikeId },
    Reserved { bike_id: BikeId },
}

pub struct Station {
    pub id: StationId,
    pub location: Location,
    pub slots: Vec<Slot>,
}

impl Station {
    pub fn new(id: StationId, location: Location, num_slots: usize, num_bikes: usize) -> Self {
        let filename = format!("{}{}.json", OFFLINE_PREFIX, id);
        let mut slots = if let Ok(content) = std::fs::read_to_string(&filename) {
            println!(
                "[RECONECTANDO] Inventario previo detectado para estación {}. Cargando...",
                id
            );
            serde_json::from_str::<Vec<Slot>>(&content)
                .unwrap_or_else(|_| Self::default_slots(num_slots, num_bikes))
        } else {
            Self::default_slots(num_slots, num_bikes)
        };

        for slot in &mut slots {
            if let SlotState::Reserved { bike_id } = slot.state {
                println!(
                    "[RECONECTANDO] Revertida reserva del slot {} (Bici {}) debido a reinicio.",
                    slot.index, bike_id
                );
                slot.state = SlotState::Occupied { bike_id };
            }
        }
        Self {
            id,
            location,
            slots,
        }
    }

    fn default_slots(num_slots: usize, num_bikes: usize) -> Vec<Slot> {
        (0..num_slots)
            .map(|i| Slot {
                index: i,
                state: if i < num_bikes {
                    SlotState::Occupied {
                        bike_id: i as BikeId,
                    }
                } else {
                    SlotState::Empty
                },
            })
            .collect()
    }

    pub fn is_bike_available(&self, slot_index: usize) -> bool {
        matches!(
            self.slots.get(slot_index).map(|s| &s.state),
            Some(SlotState::Occupied { .. })
        )
    }

    pub fn reserve_bike(&mut self, slot_index: usize) -> Option<BikeId> {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Occupied { bike_id } = slot.state {
                slot.state = SlotState::Reserved { bike_id };
                return Some(bike_id);
            }
        }
        None
    }

    pub fn confirm_reservation(&mut self, slot_index: usize) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Reserved { .. } = slot.state {
                slot.state = SlotState::Empty;
            }
        }
    }

    pub fn is_slot_free(&self, slot_index: usize) -> bool {
        matches!(
            self.slots.get(slot_index).map(|s| &s.state),
            Some(SlotState::Empty)
        )
    }

    pub fn return_bike(&mut self, slot_index: usize, bike_id: BikeId) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            slot.state = SlotState::Occupied { bike_id };
        }
    }

    pub fn cancel_reservation(&mut self, slot_index: usize, bike_id: BikeId) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Reserved { .. } = slot.state {
                slot.state = SlotState::Occupied { bike_id };
            }
        }
    }

    pub fn calculate_amount(&self, start_secs: u64, end_secs: u64) -> u32 {
        let duration_secs = end_secs.saturating_sub(start_secs);
        let minutes = duration_secs.div_ceil(60);
        minutes as u32 * AMOUNT_PER_MINUTE_CENTS as u32
    }

    pub fn save_inventory(&self) {
        let filename = format!("{}{}.json", OFFLINE_PREFIX, self.id);
        if let Ok(json_content) = serde_json::to_string(&self.slots) {
            if let Err(e) = std::fs::write(&filename, json_content) {
                eprintln!(
                    "[ERROR PERSISTENCIA] No se pudo guardar el inventario: {}",
                    e
                );
            }
        }
    }
}
