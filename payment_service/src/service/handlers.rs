use actix::prelude::*;
use common::*;
use std::time::Instant;

use crate::actors::ConnectionActor;

use super::models::{Transaction, TransactionStatus};
use super::persistence::{save_cards, save_transaction};
use super::PaymentServiceActor;

impl Handler<RequestMessage<PreparePayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<PreparePayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let tx_id = &msg.request.transaction_id;
        println!("[BANK] PreparePayment para transaction_id {}", tx_id);

        // Si ya existe la transacción, responder según su estado actual
        if let Some(existing) = self.transactions.get(tx_id) {
            match existing.status {
                TransactionStatus::PreAuthorized | TransactionStatus::Committed => {
                    msg.response.do_send(VoteCommit::new(tx_id.clone()));
                }
                _ => {
                    msg.response.do_send(VoteAbort::new(tx_id.clone()));
                }
            }
            return;
        }

        // Nueva transacción: intentar retener fondos
        if self.debit_card(&msg.request.card_token, msg.request.amount_cents) {
            self.transactions.insert(
                tx_id.clone(),
                Transaction {
                    card_token: msg.request.card_token.clone(),
                    amount_cents: msg.request.amount_cents,
                    status: TransactionStatus::PreAuthorized,
                    timestamp: Instant::now(),
                },
            );
            save_transaction(&self.transactions, tx_id, &TransactionStatus::PreAuthorized);
            msg.response.do_send(VoteCommit::new(tx_id.clone()));
        } else {
            msg.response.do_send(VoteAbort::new(tx_id.clone()));
        }
    }
}

impl Handler<RequestMessage<CommitPayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<CommitPayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let tx_id = &msg.request.transaction_id;
        println!("[BANK] CommitPayment para transaction_id {}", tx_id);

        if let Some(tx) = self.transactions.get_mut(tx_id) {
            if tx.status == TransactionStatus::PreAuthorized {
                tx.status = TransactionStatus::Committed;
            }
        }
        save_transaction(&self.transactions, tx_id, &TransactionStatus::Committed);
    }
}

impl Handler<RequestMessage<RollbackPayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<RollbackPayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let tx_id = &msg.request.transaction_id;
        println!("[BANK] RollbackPayment para transaction_id {}", tx_id);

        if let Some(tx) = self.transactions.get_mut(tx_id) {
            if tx.status == TransactionStatus::PreAuthorized {
                tx.status = TransactionStatus::RolledBack;
                if let Some(balance) = self.cards.get_mut(&tx.card_token) {
                    *balance += tx.amount_cents;
                    save_cards(&self.csv_path, &self.cards);
                }
            }
        }
        save_transaction(&self.transactions, tx_id, &TransactionStatus::RolledBack);
    }
}

impl Handler<RequestMessage<CapturePayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<CapturePayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let tx_id = &msg.request.transaction_id;
        println!("[BANK] CapturePayment para transaction_id {}", tx_id);

        let Some(tx) = self.transactions.get(tx_id) else {
            msg.response.do_send(PaymentResult {
                transaction_id: tx_id.clone(),
                success: false,
                amount_cents: msg.request.amount_cents,
            });
            return;
        };

        let card_token = tx.card_token.clone();
        let status = tx.status.clone();
        let amount_cents = msg.request.amount_cents;

        if status == TransactionStatus::Committed && self.debit_card(&card_token, amount_cents) {
            if let Some(tx) = self.transactions.get_mut(tx_id) {
                tx.status = TransactionStatus::Captured;
            }
            save_transaction(&self.transactions, tx_id, &TransactionStatus::Captured);
            msg.response.do_send(PaymentResult {
                transaction_id: tx_id.clone(),
                success: true,
                amount_cents,
            });
        } else {
            msg.response.do_send(ReservationRejected {
                transaction_id: tx_id.clone(),
                reason: "Fondos insuficientes para captura".to_string(),
            });
        }
    }
}

impl Handler<RequestMessage<ReservePayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<ReservePayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        let tx_id = &msg.request.transaction_id;
        println!("[BANK] ReservePayment para transaction_id {}", tx_id);

        self.transactions.insert(
            tx_id.clone(),
            Transaction {
                card_token: msg.request.card_token.clone(),
                amount_cents: msg.request.amount_cents,
                status: TransactionStatus::Committed,
                timestamp: Instant::now(),
            },
        );
        save_transaction(&self.transactions, tx_id, &TransactionStatus::Committed);

        if !self.debit_card(&msg.request.card_token, msg.request.amount_cents) {
            msg.response.do_send(ReservationRejected {
                transaction_id: tx_id.clone(),
                reason: "Fondos insuficientes".to_string(),
            });
        }
    }
}
