use actix::prelude::*;
use common::*;
use std::net::TcpStream;


// Estructura que maneja la lógica de la estación, incluyendo el estado de los slots y las bicicletas.
pub struct Station {
    pub id: StationId,
    pub location: Location,
    pub slots: Vec<Slot>,
}

pub struct Slot {
    pub index: usize,
    pub state: SlotState,
}

pub enum SlotState {
    Empty,
    Occupied { bike_id: BikeId },
    Reserved,
}

impl Station {
    pub fn new(id: StationId, location: Location, num_slots: usize) -> Self {
        let slots = (0..num_slots)
            .map(|i| Slot { index: i, state: SlotState::Occupied { bike_id: i as BikeId } }) // Inicialmente todos los slots ocupados con bicicletas con IDs iguales al índice del slot.
            .collect();
        Self { id, location, slots }
    }

    fn is_slot_available(&self, slot_index: usize) -> bool {
        matches!(self.slots.get(slot_index).map(|s| &s.state), Some(SlotState::Occupied { .. }))
    }

    fn lock_bike(&mut self, slot_index: usize) -> Option<BikeId> {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Occupied { bike_id } = slot.state {
                slot.state = SlotState::Reserved; // Marcar el slot como reservado
                return Some(bike_id);
            }
        }
    }

    fn get_bike_id(&self, slot_index: usize) -> Option<BikeId> {
        if let Some(slot) = self.slots.get(slot_index) {
            if let SlotState::Occupied { bike_id } = slot.state {
                return Some(bike_id);
            }
        }
    }
}

pub struct StationActor {
    station: Station,
    central_server: TcpStream,
}

impl StationActor {
    pub fn new(station: Station, central_server: TcpStream) -> Self {
        Self { station, central_server }
    }
}

impl Actor for StationActor {
    type Context = Context<Self>;
}

impl Handler<RequestMessage<RentRequest>> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<RentRequest>, _ctx: &mut Self::Context) {
        if self.is_slot_available(msg.request.slot_index) { 
            self.lock_bike(msg.request.slot_index);
            msg.response.do_send(RentConfirmed {
                bike_id: self.get_bike_id(msg.request.slot_index),
                pre_auth_cents: 100, 
                timestamp_secs: 0, 
            });// Harcodeo inicial
        } else {
            msg.response.do_send(RentRejected {
                reason: "Slot not available".to_string(),
            });
        }
    }   
}

impl Handler<RequestMessage<ReturnRequest>> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<ReturnRequest>, _ctx: &mut Self::Context) {
        if let Some(bike_id) = self.lock_bike(msg.request.slot_index) {
            msg.response.do_send(ReturnConfirmed {
                charged_cents: 150, 
                timestamp_secs: 0,
            }); // Harcodeo inicial, se debería calcular el charged_cents basado en la duración del alquiler y la tarifa.
        } else {
            msg.response.do_send(ReturnRejected {
                reason: "Slot not available for return".to_string(),
            });
        }
    }
}