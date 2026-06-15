use actix::prelude::*;
use common::*;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actors::ConnectionActor;
use crate::domain::*;

use super::messages::{
    CentralServerConnected, CentralServerDisconnected, PaymentServiceDisconnected,
};
use super::persistence::{
    flush_pending_file, get_charges_filename, get_rents_filename, parse_charge_record,
    parse_rent_record, save_pending_charge, save_pending_rent,
};
use super::{PendingCharge, PendingValidation, StationActor, TransactionState};

impl Handler<RequestMessage<RentRequest, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<RentRequest, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        if !self.station.is_bike_available(msg.request.slot_index) {
            msg.response.do_send(RentRejected {
                reason: "Bici no disponible".to_string(),
            });
            return;
        }

        let validation_msg = ValidateUser {
            user_id: msg.request.user_id,
        }
        .serialize();

        if let Some(ref sender) = self.central_server {
            if sender.send(validation_msg).is_ok() {
                self.pending_validations.insert(
                    msg.request.user_id,
                    PendingValidation {
                        msg,
                        started_at: SystemTime::now(),
                    },
                );
                return;
            }
            self.central_server = None;
        }

        // Sin servidor central: procesar directamente en modo offline
        self.process_rent_request(msg, SystemTime::now());
    }
}

impl Handler<UserValidationResult> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: UserValidationResult, _ctx: &mut Self::Context) {
        let validation = self.pending_validations.remove(&msg.user_id);

        if !msg.is_valid {
            if let Some(v) = validation {
                v.msg.response.do_send(RentRejected {
                    reason: msg.reason.unwrap_or_default(),
                });
            }
            return;
        }

        if let Some(v) = validation {
            self.process_rent_request(v.msg, v.started_at);
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
        if !self.station.is_slot_free(msg.request.slot_index) {
            msg.response.do_send(ReturnRejected {
                reason: "Slot ocupado".to_string(),
            });
            return;
        }

        let amount_cents = self.station.calculate_amount(
            msg.request.started_at_secs,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        let capture_msg = CapturePayment {
            transaction_id: msg.request.rental_id.clone(),
            amount_cents,
        }
        .serialize();

        if self.send_to_payment(capture_msg).is_ok() {
            self.pending_charges.insert(
                msg.request.rental_id.clone(),
                PendingCharge {
                    msg,
                    started_at: SystemTime::now(),
                },
            );
        } else {
            save_pending_charge(
                self.station.id,
                &msg.request.rental_id,
                amount_cents,
                msg.request.bike_id,
            );
            self.process_return_request(msg, amount_cents, SystemTime::now());
        }
    }
}

impl Handler<PaymentResult> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: PaymentResult, _ctx: &mut Self::Context) {
        let charge = self.pending_charges.remove(&msg.transaction_id);

        if let Some(pending_charge) = charge {
            if msg.success {
                self.process_return_request(
                    pending_charge.msg,
                    msg.amount_cents,
                    pending_charge.started_at,
                );
            } else {
                pending_charge.msg.response.do_send(ReturnRejected {
                    reason: "Pago fallido".to_string(),
                });
            }
        }
    }
}

impl Handler<RequestMessage<VoteCommit, ConnectionActor>> for StationActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<VoteCommit, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        self.pending_transactions
            .entry(msg.request.transaction_id.clone())
            .and_modify(|tx| tx.app_voted_commit = true);
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
        let _ = self.send_to_payment(
            RollbackPayment {
                transaction_id: msg.request.transaction_id.clone(),
            }
            .serialize(),
        );

        if let Some(tx) = self
            .pending_transactions
            .remove(&msg.request.transaction_id)
        {
            self.station.cancel_reservation(tx.slot_index, tx.bike_id);
            self.station.save_inventory();
        }
    }
}

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
        if let Some(tx) = self.pending_transactions.remove(&msg.transaction_id) {
            tx.client_addr.do_send(RentRejected {
                reason: "Pago rechazado".to_string(),
            });
            self.station.cancel_reservation(tx.slot_index, tx.bike_id);
            self.station.save_inventory();
        }
    }
}

impl Handler<ReservationRejected> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: ReservationRejected, _ctx: &mut Self::Context) {
        if let Some(user_id) = self.client_id_from_rental(&msg.transaction_id) {
            if let Some(ref sender) = self.central_server {
                let _ = sender.send(
                    UserBanned {
                        user_id,
                        reason: msg.reason.clone(),
                    }
                    .serialize(),
                );
            }

            if msg.reason == "Fondos insuficientes para captura" {
                // Caso en el que la tarjeta no tiene saldo, se asume fraude, se banea y se envia mensaje de ReturnRejected al cliente para que no quede esperando indefinidamente.
                // Ademas, debemos devolver la bici al inventario, en caso contrario la bici nunca podra ser devuelta y el usuario quedaria baneado indefinidamente.
                if let Some(message) = self.pending_charges.remove(&msg.transaction_id) {
                    message.msg.response.do_send(ReturnRejected {
                        reason: RETURN_REJECTED_FRAUD_REASON.to_string(),
                    });

                    self.station
                        .return_bike(message.msg.request.slot_index, message.msg.request.bike_id);
                    self.station.save_inventory();
                }
            }
        }
    }
}

impl Handler<CentralServerConnected> for StationActor {
    type Result = ();

    fn handle(&mut self, msg: CentralServerConnected, _ctx: &mut Self::Context) {
        self.central_server = Some(msg.sender);
        self.process_batch_updates();
    }
}

impl Handler<CentralServerDisconnected> for StationActor {
    type Result = ();

    fn handle(&mut self, _msg: CentralServerDisconnected, _ctx: &mut Self::Context) {
        self.central_server = None;
    }
}

impl Handler<PaymentServiceDisconnected> for StationActor {
    type Result = ();

    fn handle(&mut self, _msg: PaymentServiceDisconnected, _ctx: &mut Self::Context) {
        self.payment_service = None;
    }
}

impl StationActor {
    pub fn check_transaction_state(&mut self, transaction_id: &str) {
        let both_voted = self
            .pending_transactions
            .get(transaction_id)
            .map(|tx| tx.payment_voted_commit && tx.app_voted_commit)
            .unwrap_or(false);

        if !both_voted {
            return;
        }

        if let Some(tx) = self.pending_transactions.remove(transaction_id) {
            self.station.confirm_reservation(tx.slot_index);
            self.station.save_inventory();

            if let Some(user_id) = self.client_id_from_rental(transaction_id) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                self.active_rentals.insert(user_id, now);
            }

            tx.client_addr.do_send(RentConfirmed {
                bike_id: tx.bike_id,
                pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                timestamp_secs: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Error de reloj del sistema")
                    .as_secs(),
                rental_id: transaction_id.to_string(),
            });

            let _ = self.send_to_payment(
                CommitPayment {
                    transaction_id: transaction_id.to_string(),
                }
                .serialize(),
            );

            self.sync_with_central();
        }
    }

    pub fn process_rent_request(
        &mut self,
        msg: RequestMessage<RentRequest, ConnectionActor>,
        time: SystemTime,
    ) {
        let bike_id = self.station.reserve_bike(msg.request.slot_index).unwrap();
        self.station.save_inventory();

        let rental_id = self.generate_rental_id(bike_id, msg.request.user_id);
        let prepare_msg = PreparePayment {
            card_token: msg.request.card_token.clone(),
            amount_cents: PRE_AUTH_AMOUNT_CENTS,
            transaction_id: rental_id.clone(),
        }
        .serialize();

        match self.send_to_payment(prepare_msg) {
            Ok(_) => {
                // Modo online: esperar votos del 2PC
                self.pending_transactions.insert(
                    rental_id.clone(),
                    TransactionState {
                        slot_index: msg.request.slot_index,
                        bike_id,
                        client_addr: msg.response.clone(),
                        payment_voted_commit: false,
                        app_voted_commit: false,
                        started_at: time,
                    },
                );
                msg.response.do_send(Prepare {
                    transaction_id: rental_id,
                });
            }
            Err(_) => {
                // Modo offline: confirmar directamente y persistir para sync posterior
                self.station.confirm_reservation(msg.request.slot_index);
                self.station.save_inventory();
                self.sync_with_central();

                save_pending_rent(
                    self.station.id,
                    &rental_id,
                    msg.request.user_id,
                    bike_id,
                    &msg.request.card_token,
                );

                self.active_rentals.insert(
                    msg.request.user_id,
                    time.duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                );

                msg.response.do_send(RentConfirmed {
                    bike_id,
                    pre_auth_cents: PRE_AUTH_AMOUNT_CENTS,
                    timestamp_secs: time
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    rental_id,
                });
            }
        }
    }

    pub fn process_return_request(
        &mut self,
        msg: RequestMessage<ReturnRequest, ConnectionActor>,
        amount_cents: u32,
        time: SystemTime,
    ) {
        self.station
            .return_bike(msg.request.slot_index, msg.request.bike_id);
        self.station.save_inventory();

        if let Some(user_id) = self.client_id_from_rental(&msg.request.rental_id) {
            self.active_rentals.remove(&user_id);
        }

        msg.response.do_send(ReturnConfirmed {
            charged_cents: amount_cents,
            timestamp_secs: time
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        });

        self.sync_with_central();
    }

    pub fn process_batch_updates(&mut self) {
        self.process_pending_rents();
        self.process_pending_charges();
    }

    fn process_pending_rents(&mut self) {
        let filename = get_rents_filename(self.station.id);
        let mut lines_to_keep = Vec::<String>::new();

        if let Ok(contents) = std::fs::read_to_string(&filename) {
            for line in contents.lines() {
                let Some(record) = parse_rent_record(line) else {
                    continue;
                };

                let payment_msg = ReservePayment {
                    card_token: record.card_token,
                    amount_cents: PRE_AUTH_AMOUNT_CENTS,
                    transaction_id: record.rental_id.clone(),
                }
                .serialize();
                let payment_ok = self.send_to_payment(payment_msg).is_ok();

                let central_msg = OfflineRent {
                    rental_id: record.rental_id.clone(),
                    bike_id: record.bike_id,
                    user_id: record.user_id,
                }
                .serialize();
                let central_ok = self
                    .central_server
                    .as_ref()
                    .map(|s| s.send(central_msg).is_ok())
                    .unwrap_or(false);

                if payment_ok && central_ok {
                    println!(
                        "[BATCH-SYNC] Alquiler offline {} sincronizado exitosamente.",
                        record.rental_id
                    );
                } else {
                    println!(
                        "[BATCH-SYNC] Fallo parcial o total al sincronizar {}. Se reintentará.",
                        record.rental_id
                    );
                    lines_to_keep.push(line.to_string());
                }
            }
            flush_pending_file(&filename, &lines_to_keep);
        }
    }

    fn process_pending_charges(&mut self) {
        let filename = get_charges_filename(self.station.id);
        let mut lines_to_keep = Vec::<String>::new();

        if let Ok(contents) = std::fs::read_to_string(&filename) {
            for line in contents.lines() {
                let Some(record) = parse_charge_record(line) else {
                    continue;
                };

                let payment_msg = CapturePayment {
                    transaction_id: record.rental_id.clone(),
                    amount_cents: record.amount_cents,
                }
                .serialize();
                let payment_ok = self.send_to_payment(payment_msg).is_ok();

                let central_msg = ReturnRent {
                    rental_id: record.rental_id.clone(),
                    bike_id: record.bike_id,
                }
                .serialize();
                let central_ok = self
                    .central_server
                    .as_ref()
                    .map(|s| s.send(central_msg).is_ok())
                    .unwrap_or(false);

                if payment_ok && central_ok {
                    println!(
                        "[BATCH-SYNC] Devolución offline {} sincronizada exitosamente.",
                        record.rental_id
                    );
                } else {
                    println!(
                        "[BATCH-SYNC] Fallo al sincronizar cobro/devolución de {}. Se reintentará.",
                        record.rental_id
                    );
                    lines_to_keep.push(line.to_string());
                }
            }
            flush_pending_file(&filename, &lines_to_keep);
        }
    }

    pub fn abort_expired_transactions(&mut self) {
        let now = SystemTime::now();
        let expired: Vec<String> = self
            .pending_transactions
            .iter()
            .filter_map(|(tx_id, tx)| {
                let elapsed = now
                    .duration_since(tx.started_at)
                    .unwrap_or_default()
                    .as_secs();
                if elapsed > TIMEOUT_SECS {
                    Some(tx_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for tx_id in expired {
            if let Some(tx) = self.pending_transactions.remove(&tx_id) {
                let _ = self.send_to_payment(
                    RollbackPayment {
                        transaction_id: tx_id.clone(),
                    }
                    .serialize(),
                );
                self.station.cancel_reservation(tx.slot_index, tx.bike_id);
                self.station.save_inventory();
                tx.client_addr.do_send(RentRejected {
                    reason: "Timeout".to_string(),
                });
            }
        }
    }

    pub fn check_unreturned_bikes(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let overdue_users: Vec<UserId> = self
            .active_rentals
            .iter()
            .filter(|(_, &start)| now.saturating_sub(start) > TIME_TO_RETURN)
            .map(|(&user_id, _)| user_id)
            .collect();

        for user_id in overdue_users {
            let ban_msg = UserBanned {
                user_id,
                reason: "Bicicleta robada / no devuelta a tiempo".to_string(),
            }
            .serialize();

            if let Some(ref sender) = self.central_server {
                let _ = sender.send(ban_msg);
            }
            self.active_rentals.remove(&user_id);
        }
    }
}
