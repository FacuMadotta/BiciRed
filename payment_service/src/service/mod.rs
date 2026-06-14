pub mod handlers;
pub mod models;
pub mod persistence;

use actix::prelude::*;
use std::collections::HashMap;
use std::time::Instant;

use models::{Transaction, TransactionStatus};
use persistence::{load_transactions, save_cards};


pub struct PaymentServiceActor {
    pub(crate) transactions: HashMap<String, Transaction>,
    pub(crate) cards: HashMap<String, u32>,
    pub(crate) csv_path: String,
}

impl PaymentServiceActor {
    pub fn new(cards: HashMap<String, u32>, csv_path: String) -> Self {
        PaymentServiceActor {
            transactions: load_transactions(),
            cards,
            csv_path,
        }
    }

    pub(crate) fn debit_card(&mut self, card_token: &str, amount: u32) -> bool {
        if let Some(balance) = self.cards.get_mut(card_token) {
            if *balance >= amount {
                *balance -= amount;
                save_cards(&self.csv_path, &self.cards);
                return true;
            }
        }
        false
    }

    fn cleanup_stuck_transactions(&mut self) {
        let now = Instant::now();
        let mut refunds_applied = false;

        for (id, tx) in self.transactions.iter_mut() {
            let is_stuck = tx.status == TransactionStatus::PreAuthorized
                && now.duration_since(tx.timestamp).as_secs() > 30;

            if is_stuck {
                println!(
                    "[BANK] Transacción atascada {}. Haciendo Auto-Rollback: devolviendo {} cents a {}.",
                    id, tx.amount_cents, tx.card_token
                );
                tx.status = TransactionStatus::RolledBack;
                if let Some(balance) = self.cards.get_mut(&tx.card_token) {
                    *balance += tx.amount_cents;
                    refunds_applied = true;
                }
            }
        }

        if refunds_applied {
            save_cards(&self.csv_path, &self.cards);
        }
    }
}

impl Actor for PaymentServiceActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        println!("[BANK] Iniciando monitor de transacciones atascadas...");
        ctx.run_interval(std::time::Duration::from_secs(10), |act, _ctx| {
            act.cleanup_stuck_transactions();
        });
    }
}
