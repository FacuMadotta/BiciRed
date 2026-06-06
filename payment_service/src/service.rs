use actix::prelude::*;
use std::io::{Read, Write};
use common::*;

pub enum TransactionStatus {
    PreAuthorized, // Estado inicial
    Commited, // Transaccion aceptada, pero no se cobra aun
    Captured, // Transaccion capturada, cobro finalizado
    RolledBack, // Transaccion cancelada, se reintegran fondos
}

pub struct Transaction {
    pub card_token: String,
    pub amount_cents: u32,
    pub status: TransactionStatus,
}

pub struct PaymentServiceActor {
    pub transactions: HashMap<u64, Transaction>,
    pub cards: HashMap<String, u32>, // Mapa de card_token a saldo disponible en pesos
}

impl PaymentServiceActor {
    pub fn new() -> Self {
        let mut cards = HashMap::new();
        cards.insert("VISA".to_string(), 100000); 
        cards.insert("MASTERCARD".to_string(), 50000);
        cards.insert("AMEX".to_string(), 100); // Test tarjeta sin fondos

        PaymentServiceActor {
            transactions: HashMap::new(),
            cards,
        }
    }
}

impl Actor for PaymentServiceActor {
    type Context = Context<Self>;
}

// Handlers de mensajes
impl Handler<RequestMessage<PreparePayment>> for PaymentServiceActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<PreparePayment>, _ctx: &mut Self::Context) {
        println!("[BANK] Recibiendo PreparePayment para transaction_id {}", msg.request.transaction_id);
        if self.transactions.contains_key(&msg.request.transaction_id) {
            if let Some(transaction) = self.transactions.get(&msg.request.transaction_id) {
                match transaction.status {
                    TransactionStatus::PreAuthorized | TransactionStatus::Commited => {
                        msg.response.do_send(VoteCommit::new(msg.request.transaction_id)); // Si ya estaba preautorizada o commited, se puede votar commit de nuevo
                    }
                    _ => {
                        msg.response.do_send(VoteAbort::new(msg.request.transaction_id));
                    }
                }
            }
            return;
        }
        
        let card_token = &msg.request.card_token;
        let amount = msg.request.amount_cents;

        if let Some(saldo) = self.cards.get_mut(card_token) {
            if *saldo >= amount {
                *saldo -= amount; 
                self.transactions.insert(msg.request.transaction_id, Transaction {
                    card_token: card_token.clone(),
                    amount_cents: amount,
                    status: TransactionStatus::PreAuthorized,
                });
                msg.response.do_send(VoteCommit::new(msg.request.transaction_id));
                return;
            } 
        } 
        msg.response.do_send(VoteAbort::new(msg.request.transaction_id));
    }
}

impl Handler<RequestMessage<CommitPayment>> for PaymentServiceActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<CommitPayment>, _ctx: &mut Self::Context) {
        println!("[BANK] Recibiendo CommitPayment para transaction_id {}", msg.request.transaction_id);
        if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
            if transaction.status == TransactionStatus::PreAuthorized {
                transaction.status = TransactionStatus::Commited;
            }
        }
    }
}

impl Handler<RequestMessage<RollbackPayment>> for PaymentServiceActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<RollbackPayment>, _ctx: &mut Self::Context) {
        println!("[BANK] Recibiendo RollbackPayment para transaction_id {}", msg.request.transaction_id);
        if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
            if transaction.status == TransactionStatus::PreAuthorized {
                transaction.status = TransactionStatus::RolledBack;
                if let Some(saldo) = self.cards.get_mut(&transaction.card_token) {
                    *saldo += transaction.amount_cents; // Reintegrar el monto al saldo de la tarjeta
                }
            }
        }
    }
}

impl Handler<RequestMessage<CapturePayment>> for PaymentServiceActor {
    type Result = ();

    fn handle(&mut self, msg: RequestMessage<CapturePayment>, _ctx: &mut Self::Context) {
        println!("[BANK] Recibiendo CapturePayment para transaction_id {}", msg.request.transaction_id);
        if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
            if transaction.status == TransactionStatus::Commited {
                transaction.status = TransactionStatus::Captured;
            }
        }
    }
}
    
    
