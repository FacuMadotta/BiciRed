use std::collections::HashMap;
use std::io::Write;
use std::time::Instant;

use super::models::{Transaction, TransactionStatus};

const TRANSACTION_FILE: &str = "payment_transactions.json";

pub fn save_transaction(
    transactions: &HashMap<String, Transaction>,
    transaction_id: &str,
    status: &TransactionStatus,
) {
    let Some(tx) = transactions.get(transaction_id) else {
        return;
    };

    let status_code = match status {
        TransactionStatus::PreAuthorized => 0u8,
        TransactionStatus::Committed => 1,
        TransactionStatus::Captured => 2,
        TransactionStatus::RolledBack => 3,
    };

    let elapsed_secs = Instant::now()
        .duration_since(tx.timestamp)
        .as_secs();

    let line = format!(
        "transaction_id:{},card_token:{},amount_cents:{},status_code:{},elapsed_secs:{}\n",
        transaction_id, tx.card_token, tx.amount_cents, status_code, elapsed_secs
    );

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(TRANSACTION_FILE)
    {
        Ok(mut file) => {
            file.write_all(line.as_bytes())
                .unwrap_or_else(|e| eprintln!("[BANK] Error escribiendo transacción: {}", e));
        }
        Err(e) => {
            eprintln!("[BANK] No se pudo abrir el archivo de transacciones: {}", e);
        }
    }
}

pub fn save_cards(csv_path: &str, cards: &HashMap<String, u32>) {
    let content: String = cards
        .iter()
        .map(|(token, saldo)| format!("{},{}\n", token, saldo))
        .collect();

    std::fs::write(csv_path, content)
        .unwrap_or_else(|e| eprintln!("[BANK] Error guardando tarjetas: {}", e));
}

pub fn load_transactions() -> HashMap<String, Transaction> {
    let mut transactions = HashMap::new();

    let Ok(content) = std::fs::read_to_string(TRANSACTION_FILE) else {
        return transactions;
    };

    for line in content.lines() {
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() != 5 {
            continue;
        }

        let Some(tx_id_part) = parts[0].split(':').nth(1) else { continue; };
        let transaction_id = tx_id_part.trim().to_string();

        let Some(card_token_part) = parts[1].split(':').nth(1) else { continue; };
        let card_token = card_token_part.trim().to_string();

        let Some(amount_part) = parts[2].split(':').nth(1) else { continue; };
        let Ok(amount_cents) = amount_part.trim().parse::<u32>() else { continue; };

        let Some(status_part) = parts[3].split(':').nth(1) else { continue; };
        let Ok(status_code) = status_part.trim().parse::<u8>() else { continue; };

        let Some(elapsed_part) = parts[4].split(':').nth(1) else { continue; };
        let Ok(elapsed_secs) = elapsed_part.trim().parse::<u64>() else { continue; };

        let timestamp = Instant::now() - std::time::Duration::from_secs(elapsed_secs);

        let status = match status_code {
            0 => TransactionStatus::PreAuthorized,
            1 => TransactionStatus::Committed,
            2 => TransactionStatus::Captured,
            3 => TransactionStatus::RolledBack,
            _ => continue,
        };

        transactions.insert(
            transaction_id,
            Transaction {
                card_token,
                amount_cents,
                status,
                timestamp,
            },
        );
    }

    transactions
}
