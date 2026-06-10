use crate::connection::ConnectionActor;
use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::net::TcpStream;
use std::sync::mpsc::Sender;
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
    pub fn new(id: StationId, location: Location, num_slots: usize, num_bikes: usize) -> Self {
        let slots = (0..num_slots)
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
            .collect();
        Self {
            id,
            location,
            slots,
        }
    }

    fn is_bike_available(&self, slot_index: usize) -> bool {
        matches!(
            self.slots.get(slot_index).map(|s| &s.state),
            Some(SlotState::Occupied { .. })
        )
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

    fn confirm_reservation(&mut self, slot_index: usize) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            if let SlotState::Reserved = slot.state {
                slot.state = SlotState::Empty; // Marcar el slot como vacío después de commit
            }
        }
    }

    fn is_slot_free(&self, slot_index: usize) -> bool {
        matches!(
            self.slots.get(slot_index).map(|s| &s.state),
            Some(SlotState::Empty)
        )
    }

    fn return_bike(&mut self, slot_index: usize, bike_id: BikeId) {
        if let Some(slot) = self.slots.get_mut(slot_index) {
            slot.state = SlotState::Occupied { bike_id }; // Marcar el slot como ocupado con la bicicleta devuelta
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub slot_index: usize,
    pub bike_id: BikeId,
    pub client_addr: Addr<ConnectionActor>, // Para saber a qué ConnectionActor responderle al final
    pub payment_voted_commit: bool,
    pub central_voted_commit: bool,
}

pub struct StationActor {
    station: Station,
    central_server: TcpStream,
    payment_service: Sender<String>,
    pending_transactions: HashMap<String, TransactionState>,
}

impl StationActor {
    pub fn new(
        station: Station,
        central_server: TcpStream,
        payment_service: Sender<String>,
    ) -> Self {
        Self {
            station,
            central_server,
            payment_service,
            pending_transactions: HashMap::new(),
        }
    }

    fn generate_rental_id(&self, bike_id: BikeId, user_id: UserId) -> String {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => format!("{}-{}-{}", bike_id, user_id, n.as_secs()),
            Err(_) => format!("{}-{}-{}", bike_id, user_id, 0), // fallback en caso de error
        }
    }

    fn send_msg_to_payment(&mut self, msg: String) {
        if let Err(_e) = self.payment_service.send(msg) {
            // Manejar caso payment offline
        }
    }

    fn check_transaction_state(&mut self, transaction_id: &str) {
        if self
            .pending_transactions
            .get(transaction_id)
            .map(|tx| tx.payment_voted_commit && tx.central_voted_commit)
            .unwrap_or(false)
        {
            // Si ambos votaron commit, confirmamos el alquiler
            if let Some(tx) = self.pending_transactions.remove(transaction_id) {
                self.station.confirm_reservation(tx.slot_index);
                tx.client_addr.do_send(RentConfirmed {
                    bike_id: tx.bike_id,
                    pre_auth_cents: 100, // Harcodeo inicial
                    timestamp_secs: 0,   // Harcodeo inicial
                    rental_id: transaction_id.to_string(),
                });

                let commit_msg = CommitPayment {
                    transaction_id: transaction_id.to_string(),
                };

                let msg_serialized = commit_msg.serialize();
                self.send_msg_to_payment(msg_serialized);
            }
        }
    }
}

impl Actor for StationActor {
    type Context = Context<Self>;
}

impl Handler<RequestMessage<RentRequest, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<RentRequest, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "StationActor recibiendo RentRequest para slot {}",
            msg.request.slot_index
        );
        if self.station.is_bike_available(msg.request.slot_index) {
            let bike_id = self.station.reserve_bike(msg.request.slot_index);
            let rental_id = self.generate_rental_id(
                bike_id.expect("Bici debería estar disponible"),
                msg.request.user_id,
            );
            let prepare_msg = PreparePayment {
                card_token: msg.request.card_token.clone(),
                amount_cents: 100, // harcodeo inicial
                transaction_id: rental_id.clone(),
            };

            self.pending_transactions.insert(
                prepare_msg.transaction_id.clone(),
                TransactionState {
                    slot_index: msg.request.slot_index,
                    bike_id: bike_id.unwrap(),
                    client_addr: msg.response.clone(),
                    payment_voted_commit: false,
                    central_voted_commit: false,
                },
            );

            // Enviar mensaje de prepare a payment
            let msg_serialized = prepare_msg.serialize();
            self.send_msg_to_payment(msg_serialized);

            msg.response.do_send(Prepare {
                transaction_id: rental_id,
            });
        } else {
            msg.response.do_send(RentRejected {
                reason: "Bici no disponible".to_string(),
            });
        }
    }
}

impl Handler<RequestMessage<ReturnRequest, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<ReturnRequest, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let slot = msg.request.slot_index;
        let bike_id = msg.request.bike_id;
        let rental_id = msg.request.rental_id.clone();

        if self.station.is_slot_free(slot) {
            self.station.return_bike(slot, bike_id);
            let capture_msg = CapturePayment {
                transaction_id: rental_id,
            };

            let payment_msg = capture_msg.serialize();
            self.send_msg_to_payment(payment_msg);

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

// Mensaje para 2PC
// Mensajes que recibe de la App
impl Handler<RequestMessage<VoteCommit, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<VoteCommit, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        self.pending_transactions
            .entry(msg.request.transaction_id.clone())
            .and_modify(|tx| tx.central_voted_commit = true);
        self.check_transaction_state(&msg.request.transaction_id);
    }
}

impl Handler<RequestMessage<VoteAbort, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<VoteAbort, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let rollback_msg = RollbackPayment {
            transaction_id: msg.request.transaction_id.clone(),
        };

        let rollback_msg = rollback_msg.serialize();
        self.send_msg_to_payment(rollback_msg);

        self.pending_transactions
            .remove(&msg.request.transaction_id);
    }
}

// Mensajes que recibe de Payment
impl Handler<VoteCommit> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: VoteCommit, _ctx: &mut Self::Context) {
        self.pending_transactions
            .entry(msg.transaction_id.clone())
            .and_modify(|tx| tx.payment_voted_commit = true);
        self.check_transaction_state(&msg.transaction_id);
    }
}

impl Handler<VoteAbort> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: VoteAbort, _ctx: &mut Self::Context) {
        let return_msg = RentRejected {
            reason: "Pago rechazado".to_string(),
        };

        if let Some(tx) = self.pending_transactions.get(&msg.transaction_id) {
            tx.client_addr.do_send(return_msg);
        }

        self.pending_transactions.remove(&msg.transaction_id);
    }
}

// Conexion con central server
