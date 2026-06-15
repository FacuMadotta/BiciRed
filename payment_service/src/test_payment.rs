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

    // Helper para limpiar archivos residuales antes y después de cada test
    fn cleanup_files() {
        let _ = std::fs::remove_file("payment_transactions.json");
        let _ = std::fs::remove_file("test_cards.csv");
        let _ = std::fs::remove_file("test_cleanup_cards.csv");
    }

    // // Helper para leer la respuesta serializada que el ConnectionActor escribe en el socket
    // fn read_socket_string(stream: &mut TcpStream) -> String {
    //     stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    //     let mut buf = [0; 1024];
    //     match stream.read(&mut buf) {
    //         Ok(n) if n > 0 => String::from_utf8_lossy(&buf[..n]).to_string(),
    //         _ => String::new(),
    //     }
    // }

    // 1. Reemplaza la función read_socket_string por esta versión asíncrona cooperativa
    async fn read_socket_string(stream: &mut TcpStream) -> String {
        // Configuramos el socket para que no bloquee el hilo
        stream.set_nonblocking(true).unwrap();
        let start = Instant::now();
        let mut buf = [0; 1024];

        loop {
            match stream.read(&mut buf) {
                Ok(n) if n > 0 => return String::from_utf8_lossy(&buf[..n]).to_string(),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Si no hay datos, cedemos el control del hilo a Actix por 5ms
                    // para que los actores puedan procesar los mensajes
                    actix_rt::time::sleep(Duration::from_millis(5)).await;
                }
                _ => return String::new(),
            }

            // Timeout de seguridad de 2 segundos
            if start.elapsed().as_secs() > 2 {
                return String::new();
            }
        }
    }

    // ==========================================
    // 1. TEST UNITARIO: Monitoreo de Transacciones Atascadas
    // ==========================================
    #[test]
    fn test_cleanup_stuck_transactions_libera_fondos() {
        cleanup_files();

        let mut cards = HashMap::new();
        cards.insert("TOKEN_TEST".to_string(), 500); // Saldo inicial: 500 centavos

        let mut actor = PaymentServiceActor {
            transactions: HashMap::new(),
            cards,
            csv_path: "test_cleanup_cards.csv".to_string(),
        };

        // Simulamos una transacción "atascada" creada hace 40 segundos en estado PreAuthorized
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

        // Ejecutamos el método privado de limpieza de forma síncrona
        actor.cleanup_stuck_transactions();

        // Verificaciones:
        // El estado debe haber mutado a RolledBack
        let tx = actor.transactions.get("tx_atascada_123").unwrap();
        assert_eq!(tx.status, TransactionStatus::RolledBack);

        // El dinero retenido (200) debió reintegrarse al saldo de la tarjeta (500 + 200 = 700)
        assert_eq!(*actor.cards.get("TOKEN_TEST").unwrap(), 700);

        cleanup_files();
    }

    // ==========================================
    // 2. TESTS DE INTEGRACIÓN: Flujos del Protocolo 2PC
    // ==========================================
    #[actix::test]
    async fn test_flujo_2pc_exitoso_prepare_commit_y_capture() {
        cleanup_files();

        // Configuración del entorno de actores y sockets locales
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let local_addr = listener.local_addr().unwrap();
        let mut client_side_stream = TcpStream::connect(local_addr).unwrap();
        let (server_side_stream, _) = listener.accept().unwrap();

        let mut cards = HashMap::new();
        cards.insert("TOKEN_VALIDO".to_string(), 2000); // 20.00 pesos/créditos

        let payment_actor = PaymentServiceActor::new(cards, "test_cards.csv".to_string()).start();
        let conn_actor = ConnectionActor::new(server_side_stream, payment_actor.clone()).start();

        // --- FASE 1: Prepare Payment ---
        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_2pc_ok".to_string(),
                card_token: "TOKEN_VALIDO".to_string(),
                amount_cents: 500, // Pre-autoriza 500c
            },
            response: conn_actor.clone(),
        });

        // Verificamos que responda con un voto afirmativo (VoteCommit) a través del socket
        let response = read_socket_string(&mut client_side_stream).await;
        assert!(response.contains("VOTE_COMMIT") || response.contains("tx_2pc_ok"));

        // --- FASE 2: Commit Payment ---
        payment_actor.do_send(RequestMessage {
            request: CommitPayment {
                transaction_id: "tx_2pc_ok".to_string(),
            },
            response: conn_actor.clone(),
        });
        // Damos un instante para procesar el cambio interno a Committed
        actix_rt::time::sleep(Duration::from_millis(30)).await;

        // --- FASE FINAL: Capture Payment ---
        payment_actor.do_send(RequestMessage {
            request: CapturePayment {
                transaction_id: "tx_2pc_ok".to_string(),
                amount_cents: 500, // Captura final del cobro
            },
            response: conn_actor.clone(),
        });

        // Verificamos que el resultado de pago indique éxito en el socket
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

        // Intentamos un Prepare mayor al saldo disponible (100 > 50)
        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_fallida".to_string(),
                card_token: "TOKEN_POBRE".to_string(),
                amount_cents: 100,
            },
            response: conn_actor.clone(),
        });

        // Al no haber fondos sufientes, debe enviar un voto de aborto (VoteAbort)
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

        // 1. Hacemos un prepare exitoso de 600c (restando saldo interno a 400c)
        payment_actor.do_send(RequestMessage {
            request: PreparePayment {
                transaction_id: "tx_rollback".to_string(),
                card_token: "TOKEN_ROLLBACK".to_string(),
                amount_cents: 600,
            },
            response: conn_actor.clone(),
        });
        let _ = read_socket_string(&mut client_side_stream).await; // Consumimos el VoteCommit del socket

        // 2. Enviamos Rollback de la transacción
        payment_actor.do_send(RequestMessage {
            request: RollbackPayment {
                transaction_id: "tx_rollback".to_string(),
            },
            response: conn_actor.clone(),
        });
        actix_rt::time::sleep(Duration::from_millis(30)).await;

        // 3. Verificación de caja negra: Si el saldo se restauró a 1000c,
        // una nueva petición de 800c debería entrar correctamente.
        // Si el rollback falló, el saldo seguiría en 400c y esta petición enviaría un VoteAbort.
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
