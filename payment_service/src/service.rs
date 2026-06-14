use crate::connection::ConnectionActor;
use crate::HashMap;
use actix::prelude::*;
use common::*;
use std::time::Instant;
use std::io::Write;

const TRANSACTION_FILE: &str = "payment_transactions.csv";

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
            transactions: Self::load_transactions(),
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

    fn save_transaction(&self, transaction_id: &str, status: &TransactionStatus) {
        let mut contenido = String::new();
        contenido.push_str(&format!(
            "{},{},{},{},{}\n",
            transaction_id,
            self.transactions[transaction_id].card_token,
            self.transactions[transaction_id].amount_cents,
            match status {
                TransactionStatus::PreAuthorized => 0,
                TransactionStatus::Commited => 1,
                TransactionStatus::Captured => 2,
                TransactionStatus::RolledBack => 3,
            },
            Instant::now()
                .duration_since(self.transactions[transaction_id].timestamp)
                .as_secs()
        ));

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(TRANSACTION_FILE)
            .expect("No se pudo abrir el archivo de transacciones");

        file.write_all(contenido.as_bytes())
            .expect("No se pudo escribir en el archivo de transacciones");
    }

    fn load_transactions() -> HashMap<String, Transaction> {
        let mut transactions = HashMap::new();
        if let Ok(contenido) = std::fs::read_to_string(TRANSACTION_FILE) {
            for linea in contenido.lines() {
                let partes: Vec<&str> = linea.split(',').collect();
                if partes.len() == 5 {
                    let id = partes[0].to_string();
                    let card_token = partes[1].to_string();
                    if let Ok(amount_cents) = partes[2].parse::<u32>() {
                        if let Ok(status_num) = partes[3].parse::<u8>() {
                            if let Ok(elapsed_secs) = partes[4].parse::<u64>() {
                                let status = match status_num {
                                    0 => TransactionStatus::PreAuthorized,
                                    1 => TransactionStatus::Commited,
                                    2 => TransactionStatus::Captured,
                                    3 => TransactionStatus::RolledBack,
                                    _ => continue,
                                };
                                transactions.insert(
                                    id.clone(),
                                    Transaction {
                                        card_token,
                                        amount_cents,
                                        status,
                                        timestamp: Instant::now() - std::time::Duration::from_secs(elapsed_secs),
                                    },
                                );
                            }
                        }
                    }
                }
            }
        }
        transactions
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

            self.save_transaction(&msg.request.transaction_id, &TransactionStatus::PreAuthorized);

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
        self.save_transaction(&msg.request.transaction_id, &TransactionStatus::Commited);
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
        self.save_transaction(&msg.request.transaction_id, &TransactionStatus::RolledBack);
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
                msg.response.do_send(PaymentResult {
                    transaction_id: msg.request.transaction_id.clone(),
                    success: false,
                    amount_cents: msg.request.amount_cents,
                });
                return;
            };

        if status == TransactionStatus::Commited && self.take_money(&card_token, amount_cents) {
            if let Some(transaction) = self.transactions.get_mut(&msg.request.transaction_id) {
                transaction.status = TransactionStatus::Captured;
            }
            msg.response.do_send(PaymentResult {
                transaction_id: msg.request.transaction_id.clone(),
                success: true,
                amount_cents: msg.request.amount_cents,
            });

            self.save_transaction(&msg.request.transaction_id, &TransactionStatus::Captured);

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
                status: TransactionStatus::Commited, 
                timestamp: Instant::now(),
            },
        );

        self.save_transaction(&msg.request.transaction_id, &TransactionStatus::Commited);

        if !self.take_money(&msg.request.card_token, msg.request.amount_cents) {
            msg.response.do_send(ReservationRejected {
                transaction_id: msg.request.transaction_id.clone(),
                reason: "Fondos insuficientes".to_string(),
            });
        }
    }
}
