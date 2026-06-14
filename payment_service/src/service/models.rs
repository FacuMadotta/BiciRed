use std::time::Instant;

#[derive(PartialEq, Debug, Clone)]
pub enum TransactionStatus {
    PreAuthorized,
    Committed,
    Captured,
    RolledBack,
}

pub struct Transaction {
    pub card_token: String,
    pub amount_cents: u32,
    pub status: TransactionStatus,
    pub timestamp: Instant,
}
