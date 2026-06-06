use actix::prelude::*;
use common::*;
use std::net::TcpStream;
use crate::connection::RequestMessage;
use std::time::{SystemTime, UNIX_EPOCH};


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

    fn reserve_bike(&mut self, slot_index: usize) -> Option<BikeId> {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Occupied { bike_id } = slot.state {
                slot.state = SlotState::Reserved; // Marcar el slot como reservado antes de commit
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
    payment_service: TcpStream,
}

impl StationActor {
    pub fn new(station: Station, central_server: TcpStream, payment_service: TcpStream) -> Self {
        Self { station, central_server, payment_service }
    }

    fn generate_rental_id(&self, bike_id: BikeId, user_id: UserId) -> String {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => format!("{}-{}-{}", bike_id, user_id, n.as_secs()),
            Err(_) => format!("{}-{}-{}", bike_id, user_id, 0), // fallback en caso de error
        }
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
            let bike_id = self.station.reserve_bike(msg.request.slot_index);
            let rental_id = self.generate_rental_id(bike_id.expect("Bici debería estar disponible"), msg.request.user_id);
            let prepare_msg = PreparePayment {
                card_token: msg.request.card_token.clone(),
                amount_cents: 100, // harcodeo inicial
                transaction_id: rental_id,
            };

            self.payment_service.write(prepare_msg.serialize().as_bytes()).expect("Error enviando mensaje a Payment Service");
            msg.response.do_send(prepare_msg); // Enviar el mensaje de prepare al ConnectionActor para que lo reenvíe al cliente (app)

            // No va aca
            msg.response.do_send(RentConfirmed {
                bike_id: bike_id.expect("Bici debería estar disponible"),
                pre_auth_cents: 100, 
                timestamp_secs: 0, // Harcodeo inicial 
                rental_id: rental_id.clone(),
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

// Conexion con central server 
