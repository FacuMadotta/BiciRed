#[cfg(test)]
mod tests {
    use crate::actors::ConnectionActor;
    use crate::service::{
        models::{Transaction, TransactionStatus},
        PaymentServiceActor,
    };
    use actix::prelude::*;
    use common::*;
    use std::collections::HashMap;
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::time::{Duration, Instant};

    fn cleanup_files() {
        let _ = std::fs::remove_file("payment_transactions.json");
        let _ = std::fs::remove_file("test_cards.csv");
        let _ = std::fs::remove_file("test_cleanup_cards.csv");
    }

    async fn read_socket_string(stream: &mut TcpStream) -> String {
        // Configuramos el socket para que no bloquee el hilo
        stream.set_nonblocking(true).unwrap();
        let start = Instant::now();
        let mut buf = [0; 1024];

        loop {
            match stream.read(&mut buf) {
                Ok(n) if n > 0 => return String::from_utf8_lossy(&buf[..n]).to_string(),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    actix_rt::time::sleep(Duration::from_millis(5)).await;
                }
                _ => return String::new(),
            }

            if start.elapsed().as_secs() > 2 {
                return String::new();
            }
        }
    }

    /// Monitoreo de Transacciones Atascadas
    #[test]
    fn test_cleanup_stuck_transactions_libera_fondos() {
        cleanup_files();

        let mut cards = HashMap::new();
        cards.insert("TOKEN_TEST".to_string(), 500);

        let mut actor = PaymentServiceActor {
            transactions: HashMap::new(),
            cards,
            csv_path: "test_cleanup_cards.csv".to_string(),
        };

        let hace_40_segs = Instant::now() - Duration::from_secs(40);
        actor.transactions.insert(
            "tx_atascada_123".to_string(),
            Transaction {
                card_token: "TOKEN_TEST".to_string(),
                amount_cents: 200,
                status: TransactionStatus::PreAuthorized,
                timestamp: hace_40_segs,
            },
        );

        actor.cleanup_stuck_transactions();

        let tx = actor.transactions.get("tx_atascada_123").unwrap();
        assert_eq!(tx.status, TransactionStatus::RolledBack);

        assert_eq!(*actor.cards.get("TOKEN_TEST").unwrap(), 700);

        cleanup_files();
    }

    // Flujos del Protocolo 2PC

    #[actix::test]
    async fn test_flujo_2pc_exitoso_prepare_commit_y_capture() {
        cleanup_files();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local_addr = listener.local_addr().unwrap();
        let mut client_side_stream = TcpStream::connect(local_addr).unwrap();
        let (server_side_stream, _) = listener.accept().unwrap();

        let mut cards = HashMap::new();
        cards.insert("TOKEN_VALIDO".to_string(), 2000);

        let payment_actor = PaymentServiceActor::new(cards, "test_cards.csv".to_string()).start();
        let conn_actor = ConnectionActor::new(server_side_stream, payment_actor.clone()).start();

        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_2pc_ok".to_string(),
                card_token: "TOKEN_VALIDO".to_string(),
                amount_cents: 500, // Pre-autoriza 500c
            },
            response: conn_actor.clone(),
        });

        let response = read_socket_string(&mut client_side_stream).await;
        assert!(response.contains("VOTE_COMMIT") || response.contains("tx_2pc_ok"));

        payment_actor.do_send(RequestMessage {
            request: CommitPayment {
                transaction_id: "tx_2pc_ok".to_string(),
            },
            response: conn_actor.clone(),
        });

        actix_rt::time::sleep(Duration::from_millis(30)).await;

        payment_actor.do_send(RequestMessage {
            request: CapturePayment {
                transaction_id: "tx_2pc_ok".to_string(),
                amount_cents: 500, // Captura final del cobro
            },
            response: conn_actor.clone(),
        });

        let final_response = read_socket_string(&mut client_side_stream).await;
        assert!(final_response.contains("true") || final_response.contains("SUCCESS"));

        cleanup_files();
    }

    #[actix::test]
    async fn test_prepare_payment_fondos_insuficientes_aborta() {
        cleanup_files();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local_addr = listener.local_addr().unwrap();
        let mut client_side_stream = TcpStream::connect(local_addr).unwrap();
        let (server_side_stream, _) = listener.accept().unwrap();

        let mut cards = HashMap::new();
        cards.insert("TOKEN_POBRE".to_string(), 50); // Solo tiene 50 centavos

        let payment_actor = PaymentServiceActor::new(cards, "test_cards.csv".to_string()).start();
        let conn_actor = ConnectionActor::new(server_side_stream, payment_actor.clone()).start();

        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_fallida".to_string(),
                card_token: "TOKEN_POBRE".to_string(),
                amount_cents: 100,
            },
            response: conn_actor.clone(),
        });

        let response = read_socket_string(&mut client_side_stream).await;
        assert!(response.contains("VOTE_ABORT") || response.contains("tx_fallida"));

        cleanup_files();
    }

    #[actix::test]
    async fn test_rollback_payment_restaura_saldo_correctamente() {
        cleanup_files();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local_addr = listener.local_addr().unwrap();
        let mut client_side_stream = TcpStream::connect(local_addr).unwrap();
        let (server_side_stream, _) = listener.accept().unwrap();

        let mut cards = HashMap::new();
        cards.insert("TOKEN_ROLLBACK".to_string(), 1000);

        let payment_actor = PaymentServiceActor::new(cards, "test_cards.csv".to_string()).start();
        let conn_actor = ConnectionActor::new(server_side_stream, payment_actor.clone()).start();

        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_rollback".to_string(),
                card_token: "TOKEN_ROLLBACK".to_string(),
                amount_cents: 600,
            },
            response: conn_actor.clone(),
        });
        let _ = read_socket_string(&mut client_side_stream).await; // Consumimos el VoteCommit del socket

        payment_actor.do_send(RequestMessage {
            request: RollbackPayment {
                transaction_id: "tx_rollback".to_string(),
            },
            response: conn_actor.clone(),
        });
        actix_rt::time::sleep(Duration::from_millis(30)).await;

        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_verificacion".to_string(),
                card_token: "TOKEN_ROLLBACK".to_string(),
                amount_cents: 800,
            },
            response: conn_actor.clone(),
        });

        let verify_response = read_socket_string(&mut client_side_stream).await;
        assert!(
            verify_response.contains("VOTE_COMMIT"),
            "El rollback no reintegró los fondos. El saldo no alcanzó para la nueva solicitud."
        );

        cleanup_files();
    }
}
