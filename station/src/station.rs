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
            .map(|i| Slot { index: i, state: SlotState::Empty })
            .collect();
        Self { id, location, slots }
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

impl Handler<RentRequest> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: RentRequest, _ctx: &mut Self::Context) {
        
    }   
}

impl Handler<ReturnRequest> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: ReturnRequest, _ctx: &mut Self::Context) {
        
    }
}