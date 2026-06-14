use crate::client::AppClient;
use crate::models::ActiveRental;
use std::fs;
use std::net::TcpListener;

#[test]
fn test_handle_not_replica_cambia_servidor_activo() {
    let mut client = AppClient::new(999, vec!["127.0.0.1:8000".to_string()]);
    let mock_response = "NOT_REPLICA|127.0.0.1:8002";

    let result = client.handle_central_response(mock_response);

    assert!(!result, "Debería devolver false para forzar reintento");
    assert_eq!(client.active_server_addr, "127.0.0.1:8002");
}

#[test]
fn test_handle_ban_notification_bloquea_cliente() {
    let mut client = AppClient::new(999, vec!["127.0.0.1:8000".to_string()]);
    let mock_response = "BAN_NOTIFICATION|{\"reason\": \"Bici no devuelta\"}\n";

    let result = client.handle_central_response(mock_response);

    assert!(result);
    assert!(client.is_blocked);
}

#[test]
fn test_rent_station_falla_rapido_si_ya_tiene_bici() {
    let mut client = AppClient::new(999, vec!["127.0.0.1:8000".to_string()]);
    client.current_rental = Some(ActiveRental {
        bike_id: 1,
        started_at_secs: 1000,
        pre_auth_cents: 100,
        station_id: 0,
    });

    client.rent_station("999.999.999.999:9999", 1, "TOKEN123");
    assert!(
        client.actual_rental_id.is_none(),
        "No debió iniciar el trámite de alquiler"
    );
}

#[test]
fn test_rent_station_falla_rapido_si_esta_baneado() {
    let mut client = AppClient::new(999, vec!["127.0.0.1:8000".to_string()]);
    client.is_blocked = true;

    client.rent_station("999.999.999.999:9999", 1, "TOKEN123");

    assert!(
        client.actual_rental_id.is_none(),
        "No debió iniciar el alquiler estando baneado"
    );
}

#[test]
fn test_persistencia_de_estado_en_disco() {
    let user_id = 8888;
    let file_path = format!("rental_state_{}.json", user_id);

    let _ = fs::remove_file(&file_path);

    let mut client = AppClient::new(user_id, vec!["127.0.0.1:8000".to_string()]);
    client.current_rental = Some(ActiveRental {
        bike_id: 42,
        started_at_secs: 123456789,
        pre_auth_cents: 100,
        station_id: 0,
    });

    client.save_rental_state();

    let client_restaurado = AppClient::new(user_id, vec!["127.0.0.1:8000".to_string()]);

    assert!(
        client_restaurado.current_rental.is_some(),
        "Debería haber levantado el JSON"
    );
    match &client_restaurado.current_rental {
        Some(rental) => assert_eq!(rental.bike_id, 42),
        None => panic!("Debería haber levantado el JSON"),
    }
    let existe_archivo_1 = std::fs::metadata(&file_path).is_ok();
    assert!(
        existe_archivo_1,
        "El archivo {} fue creado correctamente",
        file_path
    );
    client.clear_rental_state();
    let existe_archivo = std::fs::metadata(&file_path).is_ok();
    assert!(
        !existe_archivo,
        "El archivo {} no fue eliminado correctamente",
        file_path
    );
}

#[test]
fn test_rent_station_flujo_2pc_exitoso() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0; 1024];

        let _ = stream.read(&mut buf).unwrap();
        stream.write_all(b"PREPARE|tx-abc-123\n").unwrap();

        let _ = stream.read(&mut buf).unwrap();

        let confirm_msg = b"RENT_CONFIRMED|7|100|12345|tx-abc-123\n";
        stream.write_all(confirm_msg).unwrap();
    });

    let test_user_id = 701;
    let file_path = format!("rental_state_{}.json", test_user_id);
    let _ = std::fs::remove_file(&file_path);

    let mut client = AppClient::new(test_user_id, vec!["127.0.0.1:8000".to_string()]);
    client.rent_station(&addr, 3, "TOKEN_VALIDO");

    assert_eq!(client.actual_rental_id, Some("tx-abc-123".to_string()));
    assert!(client.current_rental.is_some());

    match &client.current_rental {
        Some(rental) => assert_eq!(rental.bike_id, 7),
        None => panic!("El alquiler no se guardó"),
    }

    let _ = std::fs::remove_file(&file_path);
}

#[test]
fn test_rent_station_flujo_2pc_caida_en_fase2() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0; 1024];

        let _ = stream.read(&mut buf).unwrap();

        stream.write_all(b"PREPARE|tx-caida-99\n").unwrap();

        let _ = stream.read(&mut buf).unwrap();
        drop(stream);
    });

    let test_user_id = 702;
    let file_path = format!("rental_state_{}.json", test_user_id);
    let _ = std::fs::remove_file(&file_path);

    let mut client = AppClient::new(test_user_id, vec!["127.0.0.1:8000".to_string()]);
    client.rent_station(&addr, 3, "TOKEN_VALIDO");

    assert!(client.current_rental.is_none());
    let _ = std::fs::remove_file(&file_path);
}

#[test]
fn test_return_station_falla_rapido_si_no_tiene_bici() {
    let mut client = AppClient::new(999, vec!["127.0.0.1:8000".to_string()]);
    client.current_rental = None;
    client.return_station("999.999.999.999:9999", 1);

    assert!(client.current_rental.is_none());
}

#[test]
fn test_rent_station_rechazo_en_fase_1() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0; 1024];
        let _ = stream.read(&mut buf).unwrap();

        let reject_msg = b"RENT_REJECTED|{\"reason\": \"Bici no disponible\"}\n";
        stream.write_all(reject_msg).unwrap();
    });

    let test_user_id = 703;
    let file_path = format!("rental_state_{}.json", test_user_id);
    let _ = fs::remove_file(&file_path);

    let mut client = AppClient::new(test_user_id, vec!["127.0.0.1:8000".to_string()]);
    client.rent_station(&addr, 3, "TOKEN_VALIDO");

    assert!(client.actual_rental_id.is_none());
    assert!(client.current_rental.is_none());
}

#[test]
fn test_return_station_flujo_exitoso() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    std::thread::spawn(move || {
        use std::io::{Read, Write};
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = [0; 1024];
        let _ = stream.read(&mut buf).unwrap();

        let confirm_msg = b"RETURN_CONFIRMED|150|123456789\n";
        stream.write_all(confirm_msg).unwrap();
    });

    let test_user_id = 704;
    let file_path = format!("rental_state_{}.json", test_user_id);
    let _ = fs::remove_file(&file_path);

    let mut client = AppClient::new(test_user_id, vec!["127.0.0.1:8000".to_string()]);

    client.current_rental = Some(crate::models::ActiveRental {
        bike_id: 10,
        started_at_secs: 1000000,
        pre_auth_cents: 100,
        station_id: 0,
    });
    client.actual_rental_id = Some("tx-devolucion-1".to_string());
    client.save_rental_state();

    client.return_station(&addr, 5);

    assert!(client.current_rental.is_none());

    let existe_archivo = std::fs::metadata(&file_path).is_ok();
    assert!(
        !existe_archivo,
        "El archivo no se borró tras la devolución exitosa"
    );
}
