use crate::connection::ConnectionActor;
use crate::HashMap;
use actix::prelude::*;
use common::*;
use std::time::Instant;

#[derive(PartialEq, Debug, Clone)]
pub enum TransactionStatus {
    PreAuthorized, // Estado inicial
    Commited,      // Transaccion aceptada, pero no se cobra aun
    Captured,      // Transaccion capturada, cobro finalizado
    RolledBack,    // Transaccion cancelada, se reintegran fondos
}

pub struct Transaction {
    pub card_token: String,
    pub amount_cents: u32,
    pub status: TransactionStatus,
    pub timestamp: Instant,
}

pub struct PaymentServiceActor {
    pub transactions: HashMap<String, Transaction>,
    pub cards: HashMap<String, u32>, // Mapa de card_token a saldo disponible en pesos
    pub csv_path: String,
}

impl PaymentServiceActor {
    pub fn new(cards: HashMap<String, u32>, csv_path: String) -> Self {
        PaymentServiceActor {
            transactions: HashMap::new(),
            cards,
            csv_path,
        }
    }

    fn save_cards(&self) {
        let mut contenido = String::new();
        for (token, saldo) in &self.cards {
            contenido.push_str(&format!("{},{}\n", token, saldo));
        }
        let _ = std::fs::write(&self.csv_path, contenido);
    }

    fn take_money(&mut self, card_token: &str, amount: u32) -> bool {
        if let Some(saldo) = self.cards.get_mut(card_token) {
            if *saldo >= amount {
                *saldo -= amount;
                self.save_cards();
                return true;
            }
        }
        return false;
    }

    fn cleanup_stuck_transactions(&mut self) {
        let mut changes = false;
        let now = Instant::now();

        for (id, tx) in self.transactions.iter_mut() {
            if tx.status == TransactionStatus::PreAuthorized
                && now.duration_since(tx.timestamp).as_secs() > 30
            {
                println!(
                    "\n[BANK] Detectada transacción atascada {}. Estación desconectada.",
                    id
                );
                println!(
                    "[BANK] Haciendo Auto-Rollback: Devolviendo ${} a la tarjeta {}",
                    tx.amount_cents, tx.card_token
                );

                tx.status = TransactionStatus::RolledBack;
                if let Some(saldo) = self.cards.get_mut(&tx.card_token) {
                    *saldo += tx.amount_cents;
                    changes = true;
                }
            }
        }

        if changes {
            self.save_cards();
        }
    }
}

impl Actor for PaymentServiceActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("[BANK] Iniciando monitor de transacciones...");
        ctx.run_interval(std::time::Duration::from_secs(10), |act, _ctx| {
            act.cleanup_stuck_transactions();
        });
    }
}

// Handlers de mensajes
impl Handler<RequestMessage<PreparePayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<PreparePayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "[BANK] Recibiendo PreparePayment para transaction_id {}",
            msg.request.transaction_id
        );
        if self.transactions.contains_key(&msg.request.transaction_id) {
            if let Some(transaction) = self.transactions.get(&msg.request.transaction_id) {
                match transaction.status {
                    TransactionStatus::PreAuthorized | TransactionStatus::Commited => {
                        msg.response
                            .do_send(VoteCommit::new(msg.request.transaction_id));
                        // Si ya estaba preautorizada o commited, se puede votar commit de nuevo
                    }
                    _ => {
                        msg.response
                            .do_send(VoteAbort::new(msg.request.transaction_id));
                    }
                }
            }
            return;
        }

        let card_token = &msg.request.card_token;
        let amount = msg.request.amount_cents;

        if self.take_money(card_token, amount) {
            self.transactions.insert(
                msg.request.transaction_id.clone(),
                Transaction {
                    card_token: card_token.clone(),
                    amount_cents: amount,
                    status: TransactionStatus::PreAuthorized,
                    timestamp: Instant::now(),
                },
            );
            msg.response
                .do_send(VoteCommit::new(msg.request.transaction_id));
            return;
        }
        msg.response
            .do_send(VoteAbort::new(msg.request.transaction_id));
    }
}

impl Handler<RequestMessage<CommitPayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<CommitPayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "[BANK] Recibiendo CommitPayment para transaction_id {}",
            msg.request.transaction_id
        );
        if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
            if transaction.status == TransactionStatus::PreAuthorized {
                transaction.status = TransactionStatus::Commited;
            }
        }
    }
}

impl Handler<RequestMessage<RollbackPayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<RollbackPayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "[BANK] Recibiendo RollbackPayment para transaction_id {}",
            msg.request.transaction_id
        );
        if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
            if transaction.status == TransactionStatus::PreAuthorized {
                transaction.status = TransactionStatus::RolledBack;
                if let Some(saldo) = self.cards.get_mut(&transaction.card_token) {
                    *saldo += transaction.amount_cents; // Reintegrar el monto al saldo de la tarjeta
                    self.save_cards();
                }
            }
        }
    }
}

impl Handler<RequestMessage<CapturePayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<CapturePayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "[BANK] Recibiendo CapturePayment para transaction_id {}",
            msg.request.transaction_id
        );
        let (card_token, amount_cents, status) =
            if let Some(transaction) = self.transactions.get(&msg.request.transaction_id) {
                (
                    transaction.card_token.clone(),
                    msg.request.amount_cents,
                    transaction.status.clone(),
                )
            } else {
                return;
            };

        if status == TransactionStatus::Commited && self.take_money(&card_token, amount_cents) {
            if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
                transaction.status = TransactionStatus::Captured;
            }
            return;
        }

        msg.response.do_send(ReservationRejected {
            transaction_id: msg.request.transaction_id.clone(),
            reason: "Fondos insuficientes para captura".to_string(),
        });
    }
}

impl Handler<RequestMessage<ReservePayment, ConnectionActor>> for PaymentServiceActor {
    type Result = ();

    fn handle(
        &mut self,
        msg: RequestMessage<ReservePayment, ConnectionActor>,
        _ctx: &mut Self::Context,
    ) {
        println!(
            "[BANK] Recibiendo ReservePayment para transaction_id {}",
            msg.request.transaction_id
        );
        self.transactions.insert(
            msg.request.transaction_id.clone(),
            Transaction {
                card_token: msg.request.card_token.clone(),
                amount_cents: msg.request.amount_cents,
                status: TransactionStatus::Captured,
                timestamp: Instant::now(),
            },
        );
        if !self.take_money(&msg.request.card_token, msg.request.amount_cents) {
            msg.response.do_send(ReservationRejected {
                transaction_id: msg.request.transaction_id.clone(),
                reason: "Fondos insuficientes".to_string(),
            });
        }
    }
}
