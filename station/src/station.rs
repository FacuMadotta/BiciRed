use actix::prelude::*;
use common::*;
use std::net::TcpStream;
use crate::connection::RequestMessage;


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

    fn is_bike_available(&self, slot_index: usize) -> bool {
        matches!(self.slots.get(slot_index).map(|s| &s.state), Some(SlotState::Occupied { .. }))
    }

    fn unlock_bike(&mut self, slot_index: usize) -> Option<BikeId> {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Occupied { bike_id } = slot.state {
                slot.state = SlotState::Empty; // Marcar el slot como vacío
                return Some(bike_id);
            }
        } 
        None
    }

    fn is_slot_free(&self, slot_index: usize) -> bool {
        matches!(self.slots.get(slot_index).map(|s| &s.state), Some(SlotState::Empty))
    }

    fn return_bike(&mut self, slot_index: usize, bike_id: BikeId) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            slot.state = SlotState::Occupied { bike_id }; // Marcar el slot como ocupado con la bicicleta devuelta
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
        println!("StationActor recibiendo RentRequest para slot {}", msg.request.slot_index);
        if self.station.is_bike_available(msg.request.slot_index) { 
            let bike_id = self.station.unlock_bike(msg.request.slot_index);
            msg.response.do_send(RentConfirmed {
                bike_id: bike_id.expect("Bici debería estar disponible"),
                pre_auth_cents: 100, 
                timestamp_secs: 0, // Harcodeo inicial 
            });
        } else {
            msg.response.do_send(RentRejected {
                reason: "Bici no disponible".to_string(),
            });
        }
    }   
}

impl Handler<RequestMessage<ReturnRequest>> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<ReturnRequest>, _ctx: &mut Self::Context) {
        if self.station.is_slot_free(msg.request.slot_index) {
            self.station.return_bike(msg.request.slot_index, msg.request.bike_id);
            msg.response.do_send(ReturnConfirmed {
                charged_cents: 150, 
                timestamp_secs: 0, // Harcodeo inicial
            });
        } else {
            msg.response.do_send(ReturnRejected {
                reason: "Slot no está libre".to_string(),
            });
        }
    }
}