use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix::prelude::*;
use common::*;

use crate::actors::station::messages::*;
use crate::actors::station::persistence::{
    build_charge_json, build_rent_json, flush_pending_file, get_charges_filename,
    get_rents_filename, parse_charge_record, parse_rent_record, save_pending_charge,
    save_pending_rent,
};
use crate::actors::station::TransactionState;
use crate::actors::{ConnectionActor, StationActor};
use crate::domain::*;

fn cleanup_station_files(station_id: StationId) {
    let _ = std::fs::remove_file(format!("{}{}.json", OFFLINE_PREFIX, station_id));
    let _ = std::fs::remove_file(get_rents_filename(station_id));
    let _ = std::fs::remove_file(get_charges_filename(station_id));
}

fn new_station(id: StationId, num_slots: usize, num_bikes: usize) -> Station {
    cleanup_station_files(id);
    Station::new(id, Location { x: 0.0, y: 0.0 }, num_slots, num_bikes)
}

fn build_station_actor(station: Station, payment_ip: &str) -> StationActor {
    StationActor::new(
        station,
        vec!["127.0.0.1:1".to_string()],
        "127.0.0.1:0".to_string(),
        payment_ip.to_string(),
    )
}

#[test]
fn test_nueva_estacion_inicializa_slots_ocupados_y_vacios() {
    let id = 9001;
    let station = new_station(id, 5, 3);

    assert_eq!(station.slots.len(), 5);
    for i in 0..3 {
        assert!(
            station.is_bike_available(i),
            "Slot {} debería tener bici",
            i
        );
    }
    for i in 3..5 {
        assert!(station.is_slot_free(i), "Slot {} debería estar vacío", i);
    }

    cleanup_station_files(id);
}

#[test]
fn test_reserve_bike_ocupado_devuelve_bike_id_y_cambia_estado() {
    let id = 9002;
    let mut station = new_station(id, 5, 3);

    let bike_id = station.reserve_bike(0);
    assert_eq!(bike_id, Some(0));

    assert!(
        !station.is_bike_available(0),
        "El slot reservado no debe contar como disponible"
    );
    assert!(
        !station.is_slot_free(0),
        "El slot reservado no debe estar libre"
    );

    cleanup_station_files(id);
}

#[test]
fn test_reserve_bike_en_slot_vacio_devuelve_none() {
    let id = 9003;
    let mut station = new_station(id, 5, 3);

    let result = station.reserve_bike(4);
    assert_eq!(result, None);

    cleanup_station_files(id);
}

#[test]
fn test_reserve_bike_fuera_de_rango_devuelve_none() {
    let id = 9004;
    let mut station = new_station(id, 2, 2);

    let result = station.reserve_bike(99);
    assert_eq!(result, None);

    cleanup_station_files(id);
}

#[test]
fn test_confirm_reservation_libera_slot_a_empty() {
    let id = 9005;
    let mut station = new_station(id, 5, 3);

    station.reserve_bike(0);
    station.confirm_reservation(0);

    assert!(
        station.is_slot_free(0),
        "El slot confirmado debe quedar Empty"
    );
    assert!(!station.is_bike_available(0));

    cleanup_station_files(id);
}

#[test]
fn test_confirm_reservation_sobre_slot_no_reservado_no_hace_nada() {
    let id = 9006;
    let mut station = new_station(id, 5, 3);

    station.confirm_reservation(0);
    assert!(
        station.is_bike_available(0),
        "Confirmar un slot no reservado no debe alterarlo"
    );

    cleanup_station_files(id);
}

#[test]
fn test_return_bike_ocupa_slot_con_bici() {
    let id = 9007;
    let mut station = new_station(id, 5, 3);

    station.return_bike(3, 42);
    assert!(
        station.is_bike_available(3),
        "El slot debe quedar Occupied tras devolver bici"
    );
    assert!(!station.is_slot_free(3));

    cleanup_station_files(id);
}

#[test]
fn test_cancel_reservation_revierte_a_occupied() {
    let id = 9008;
    let mut station = new_station(id, 5, 3);

    let bike_id = station.reserve_bike(0).unwrap();
    station.cancel_reservation(0, bike_id);

    assert!(
        station.is_bike_available(0),
        "Cancelar la reserva debe devolver el slot a Occupied"
    );

    cleanup_station_files(id);
}

#[test]
fn test_cancel_reservation_sobre_slot_no_reservado_no_hace_nada() {
    let id = 9009;
    let mut station = new_station(id, 5, 3);

    station.cancel_reservation(3, 99);
    assert!(
        station.is_slot_free(3),
        "Cancelar sobre un slot vacío no debe alterarlo"
    );

    cleanup_station_files(id);
}

#[test]
fn test_calculate_amount_redondea_hacia_arriba_por_minuto() {
    let id = 9010;
    let station = new_station(id, 5, 3);

    let amount = station.calculate_amount(0, 90);
    assert_eq!(amount, 2 * AMOUNT_PER_MINUTE_CENTS);

    let amount2 = station.calculate_amount(0, 60);
    assert_eq!(amount2, AMOUNT_PER_MINUTE_CENTS);

    let amount3 = station.calculate_amount(0, 1);
    assert_eq!(amount3, AMOUNT_PER_MINUTE_CENTS);

    let amount4 = station.calculate_amount(10, 10);
    assert_eq!(amount4, 0);

    cleanup_station_files(id);
}

#[test]
fn test_calculate_amount_con_end_menor_que_start_satura_a_cero() {
    let id = 9011;
    let station = new_station(id, 5, 3);

    let amount = station.calculate_amount(100, 50);
    assert_eq!(amount, 0);

    cleanup_station_files(id);
}

#[test]
fn test_save_inventory_y_recarga_desde_disco() {
    let id = 9012;
    let mut station = new_station(id, 4, 2);

    station.reserve_bike(0);
    station.confirm_reservation(0);
    station.save_inventory();

    let reloaded = Station::new(id, Location { x: 0.0, y: 0.0 }, 4, 2);

    assert!(
        reloaded.is_slot_free(0),
        "El estado guardado (slot 0 vacío) debió persistir"
    );
    assert!(
        reloaded.is_bike_available(1),
        "El slot 1 (sin modificar) debe seguir Occupied"
    );

    cleanup_station_files(id);
}

#[test]
fn test_reconectar_revierte_slots_reserved_a_occupied() {
    let id = 9013;
    let mut station = new_station(id, 4, 2);

    let bike_id = station.reserve_bike(0).unwrap();
    station.save_inventory();
    let _ = bike_id;

    let reloaded = Station::new(id, Location { x: 0.0, y: 0.0 }, 4, 2);
    assert!(
        reloaded.is_bike_available(0),
        "Una reserva pendiente debe revertirse a Occupied al reiniciar"
    );

    cleanup_station_files(id);
}

#[test]
fn test_station_new_sin_archivo_previo_usa_defaults() {
    let id = 9014;
    cleanup_station_files(id);

    let station = Station::new(id, Location { x: 1.0, y: 2.0 }, 3, 1);

    assert_eq!(station.slots.len(), 3);
    assert!(station.is_bike_available(0));
    assert!(station.is_slot_free(1));
    assert!(station.is_slot_free(2));

    cleanup_station_files(id);
}

#[test]
fn test_station_new_con_json_corrupto_usa_defaults() {
    let id = 9015;
    cleanup_station_files(id);

    let filename = format!("{}{}.json", OFFLINE_PREFIX, id);
    std::fs::write(&filename, "esto no es json valido {{{").unwrap();

    let station = Station::new(id, Location { x: 0.0, y: 0.0 }, 3, 2);

    assert_eq!(station.slots.len(), 3);
    assert!(station.is_bike_available(0));
    assert!(station.is_bike_available(1));
    assert!(station.is_slot_free(2));

    cleanup_station_files(id);
}

#[test]
fn test_build_rent_json_y_parse_rent_record_roundtrip() {
    let json = build_rent_json("rent-1", 42, 7, "tok_abc");
    let record = parse_rent_record(json.trim()).expect("Debió parsear el registro de alquiler");

    assert_eq!(record.rental_id, "rent-1");
    assert_eq!(record.user_id, 42);
    assert_eq!(record.bike_id, 7);
    assert_eq!(record.card_token, "tok_abc");
}

#[test]
fn test_build_charge_json_y_parse_charge_record_roundtrip() {
    let json = build_charge_json("rent-2", 150, 8);
    let record = parse_charge_record(json.trim()).expect("Debió parsear el registro de cobro");

    assert_eq!(record.rental_id, "rent-2");
    assert_eq!(record.amount_cents, 150);
    assert_eq!(record.bike_id, 8);
}

#[test]
fn test_parse_rent_record_linea_vacia_devuelve_none() {
    assert!(parse_rent_record("").is_none());
    assert!(parse_rent_record("   ").is_none());
}

#[test]
fn test_parse_charge_record_linea_invalida_devuelve_none() {
    assert!(parse_charge_record("no es json").is_none());
    assert!(parse_charge_record("{\"foo\":1}").is_none());
}

#[test]
fn test_save_pending_rent_y_charge_persisten_en_disco() {
    let id = 9020;
    cleanup_station_files(id);

    save_pending_rent(id, "rent-A", 1, 10, "tok1");
    save_pending_rent(id, "rent-B", 2, 11, "tok2");

    let filename = get_rents_filename(id);
    let content = std::fs::read_to_string(&filename).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2);

    let rec1 = parse_rent_record(lines[0]).unwrap();
    assert_eq!(rec1.rental_id, "rent-A");
    let rec2 = parse_rent_record(lines[1]).unwrap();
    assert_eq!(rec2.rental_id, "rent-B");

    save_pending_charge(id, "charge-A", 100, 10);
    let charges_filename = get_charges_filename(id);
    let charges_content = std::fs::read_to_string(&charges_filename).unwrap();
    assert_eq!(charges_content.lines().count(), 1);

    cleanup_station_files(id);
}

#[test]
fn test_flush_pending_file_vacio_elimina_archivo() {
    let id = 9021;
    cleanup_station_files(id);

    save_pending_rent(id, "rent-X", 1, 1, "tok");
    let filename = get_rents_filename(id);
    assert!(std::fs::metadata(&filename).is_ok());

    flush_pending_file(&filename, &[]);
    assert!(
        !std::fs::metadata(&filename).is_ok(),
        "El archivo debió eliminarse al quedar vacío"
    );

    cleanup_station_files(id);
}

#[test]
fn test_flush_pending_file_con_lineas_sobrescribe_contenido() {
    let id = 9022;
    cleanup_station_files(id);

    save_pending_rent(id, "rent-A", 1, 1, "tokA");
    save_pending_rent(id, "rent-B", 2, 2, "tokB");
    let filename = get_rents_filename(id);

    let content = std::fs::read_to_string(&filename).unwrap();
    let lines: Vec<String> = content.lines().map(String::from).collect();

    // Mantenemos solo la primera línea (simulando que rent-B se sincronizó)
    flush_pending_file(&filename, &[lines[0].clone()]);

    let new_content = std::fs::read_to_string(&filename).unwrap();
    assert_eq!(new_content.lines().count(), 1);
    let rec = parse_rent_record(new_content.lines().next().unwrap()).unwrap();
    assert_eq!(rec.rental_id, "rent-A");

    cleanup_station_files(id);
}

#[test]
fn test_station_status_serialize_deserialize_roundtrip() {
    let status = StationStatus {
        station_id: 7,
        location: Location { x: 1.5, y: -2.5 },
        available_bikes: 3,
        free_slots: 2,
        updated_at_secs: 123456,
        station_addr: "127.0.0.1:9000".to_string(),
        slots_occupied: "0,1,2".to_string(),
        slots_frees: "3,4".to_string(),
    };

    let serialized = status.serialize();
    let deserialized = StationStatus::deserialize(&serialized);

    assert_eq!(deserialized.station_id, 7);
    assert_eq!(deserialized.location.x, 1.5);
    assert_eq!(deserialized.location.y, -2.5);
}

#[test]
fn test_station_update_serialize_deserialize_roundtrip() {
    let status = StationStatus {
        station_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        available_bikes: 5,
        free_slots: 5,
        updated_at_secs: 100,
        station_addr: "addr".to_string(),
        slots_occupied: "0".to_string(),
        slots_frees: "1".to_string(),
    };
    let update = StationUpdate { station: status };

    let serialized = update.serialize();
    assert!(serialized.starts_with("STATION_UPDATE|"));

    let deserialized = StationUpdate::deserialize(&serialized);
    assert_eq!(deserialized.station.station_id, 1);
}

#[test]
fn test_nearby_response_serialize_deserialize_roundtrip_vacio() {
    let response = NearbyResponse { stations: vec![] };
    let serialized = response.serialize();
    assert_eq!(serialized, "NEARBY_RESPONSE|0|");
}

#[test]
fn test_rent_request_deserialize() {
    let input = "RENT_REQUEST|5|2|tok_123";
    let req = RentRequest::deserialize(input);
    assert_eq!(req.user_id, 5);
    assert_eq!(req.slot_index, 2);
    assert_eq!(req.card_token, "tok_123");
}

#[test]
fn test_return_request_serialize_deserialize_roundtrip() {
    let req = ReturnRequest {
        user_id: 5,
        bike_id: 9,
        slot_index: 3,
        started_at_secs: 1000,
        rental_id: "9-5-12345".to_string(),
    };
    let serialized = req.serialize();
    let deserialized = ReturnRequest::deserialize(&serialized);

    assert_eq!(deserialized.user_id, 5);
    assert_eq!(deserialized.rental_id, "9-5-12345");
}

async fn spawn_connection_actor() -> (TcpStream, Addr<ConnectionActor>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let client_stream = TcpStream::connect(addr).unwrap();
    let (server_stream, _) = listener.accept().unwrap();

    const DUMMY_STATION_ID: StationId = 999999;
    cleanup_station_files(DUMMY_STATION_ID);
    let dummy_station = Station::new(DUMMY_STATION_ID, Location { x: 0.0, y: 0.0 }, 1, 0);
    let dummy_actor = StationActor::new(
        dummy_station,
        vec!["127.0.0.1:1".to_string()],
        "127.0.0.1:0".to_string(),
        "127.0.0.1:1".to_string(),
    );
    let dummy_addr = dummy_actor.start();

    let conn_addr = ConnectionActor::new(server_stream, dummy_addr).start();

    (client_stream, conn_addr)
}

async fn read_socket_response_async(stream: &TcpStream) -> String {
    let mut stream_clone = stream.try_clone().unwrap();
    stream_clone.set_nonblocking(true).unwrap();
    let start = std::time::Instant::now();
    let mut buf = [0u8; 4096];

    loop {
        match stream_clone.read(&mut buf) {
            Ok(0) => return String::new(),
            Ok(n) => return String::from_utf8_lossy(&buf[..n]).to_string(),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if start.elapsed() >= Duration::from_secs(2) {
                    return String::new();
                }
                actix_rt::time::sleep(Duration::from_millis(10)).await;
            }
            Err(_) => return String::new(),
        }
    }
}

async fn recv_channel_async(rx: &mpsc::Receiver<String>) -> Option<String> {
    let start = std::time::Instant::now();
    loop {
        match rx.try_recv() {
            Ok(msg) => return Some(msg),
            Err(mpsc::TryRecvError::Empty) => {
                if start.elapsed() >= Duration::from_secs(2) {
                    return None;
                }
                actix_rt::time::sleep(Duration::from_millis(10)).await;
            }
            Err(_) => return None,
        }
    }
}

async fn assert_no_response_async(stream: &TcpStream) {
    let mut stream_clone = stream.try_clone().unwrap();
    stream_clone.set_nonblocking(true).unwrap();
    actix_rt::time::sleep(Duration::from_millis(300)).await;
    let mut buf = [0u8; 4096];
    match stream_clone.read(&mut buf) {
        Ok(0) => {}
        Ok(n) => panic!(
            "Se recibió respuesta inesperada: {}",
            String::from_utf8_lossy(&buf[..n])
        ),
        Err(e) => assert!(e.kind() == std::io::ErrorKind::WouldBlock),
    }
}

#[actix::test]
async fn test_rent_request_slot_no_disponible_rechaza() {
    let id = 9100;
    let station = new_station(id, 4, 0);
    let actor = build_station_actor(station, "127.0.0.1:1");
    let addr = actor.start();

    let (listener, conn_addr) = spawn_connection_actor().await;

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RENT_REJECTED|"));
    assert!(response.contains("Bici no disponible"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_rent_request_offline_confirma_directo_y_persiste() {
    let id = 9101;
    let station = new_station(id, 4, 2);
    let actor = build_station_actor(station, "127.0.0.1:1");
    let addr = actor.start();

    let (listener, conn_addr) = spawn_connection_actor().await;

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 5,
            slot_index: 0,
            card_token: "tok_offline".to_string(),
        },
        response: conn_addr.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(
        response.starts_with("RENT_CONFIRMED|"),
        "En modo offline debe confirmar directamente"
    );

    let filename = get_rents_filename(id);
    actix_rt::time::sleep(Duration::from_millis(50)).await;
    let content = std::fs::read_to_string(&filename).unwrap_or_default();
    assert!(
        content.contains("\"user_id\":5"),
        "Debió guardar el alquiler offline en disco"
    );

    cleanup_station_files(id);
}

#[actix::test]
async fn test_user_validation_result_invalido_rechaza_rent_pendiente() {
    let id = 9102;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (listener, conn_addr) = spawn_connection_actor().await;
    let (tx, _rx) = mpsc::channel::<String>();
    actor_struct.central_server = Some(tx);

    let addr = actor_struct.start();

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 7,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    actix_rt::time::sleep(Duration::from_millis(50)).await;

    addr.do_send(UserValidationResult {
        user_id: 7,
        is_valid: false,
        reason: Some("Usuario baneado".to_string()),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RENT_REJECTED|"));
    assert!(response.contains("Usuario baneado"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_user_validation_result_valido_procesa_rent_pendiente() {
    let id = 9103;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (listener, conn_addr) = spawn_connection_actor().await;
    let (tx, _rx) = mpsc::channel::<String>();
    actor_struct.central_server = Some(tx);

    let addr = actor_struct.start();

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 8,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    actix_rt::time::sleep(Duration::from_millis(50)).await;

    addr.do_send(UserValidationResult {
        user_id: 8,
        is_valid: true,
        reason: None,
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RENT_CONFIRMED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_rent_request_online_envia_prepare_y_espera_votos() {
    let id = 9110;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let payment_msg = recv_channel_async(&payment_rx).await.unwrap();
    assert!(payment_msg.starts_with("PREPARE_PAYMENT|"));

    let response = read_socket_response_async(&listener).await;
    assert!(
        response.starts_with("PREPARE|"),
        "Debió enviar PREPARE al cliente en modo online"
    );

    cleanup_station_files(id);
}

#[actix::test]
async fn test_vote_commit_de_ambas_partes_confirma_rent() {
    let id = 9111;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let _ = recv_channel_async(&payment_rx).await.unwrap();
    let prepare_response = read_socket_response_async(&listener).await;
    let tx_id = prepare_response
        .trim()
        .split('|')
        .nth(1)
        .unwrap()
        .to_string();

    addr.do_send(VoteCommit {
        transaction_id: tx_id.clone(),
    });
    addr.do_send(RequestMessage {
        request: VoteCommit {
            transaction_id: tx_id.clone(),
        },
        response: conn_addr.clone(),
    });

    let commit_msg = recv_channel_async(&payment_rx).await.unwrap();
    assert!(commit_msg.starts_with("COMMIT_PAYMENT|"));
    assert!(commit_msg.contains(&tx_id));

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RENT_CONFIRMED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_vote_abort_del_payment_rechaza_y_libera_slot() {
    let id = 9112;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let _ = recv_channel_async(&payment_rx).await.unwrap();
    let prepare_response = read_socket_response_async(&listener).await;
    let tx_id = prepare_response
        .trim()
        .split('|')
        .nth(1)
        .unwrap()
        .to_string();

    addr.do_send(VoteAbort {
        transaction_id: tx_id.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RENT_REJECTED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_request_vote_abort_de_app_envia_rollback_y_cancela_reserva() {
    let id = 9113;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let _ = recv_channel_async(&payment_rx).await.unwrap();
    let prepare_response = read_socket_response_async(&listener).await;
    let tx_id = prepare_response
        .trim()
        .split('|')
        .nth(1)
        .unwrap()
        .to_string();

    addr.do_send(RequestMessage {
        request: VoteAbort {
            transaction_id: tx_id.clone(),
        },
        response: conn_addr.clone(),
    });

    let rollback_msg = recv_channel_async(&payment_rx).await.unwrap();
    assert!(rollback_msg.starts_with("ROLLBACK_PAYMENT|"));
    assert!(rollback_msg.contains(&tx_id));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_return_request_slot_ocupado_rechaza() {
    let id = 9120;
    let station = new_station(id, 4, 2);
    let actor = build_station_actor(station, "127.0.0.1:1");
    let addr = actor.start();

    let (listener, conn_addr) = spawn_connection_actor().await;

    addr.do_send(RequestMessage {
        request: ReturnRequest {
            user_id: 1,
            bike_id: 0,
            slot_index: 0,
            started_at_secs: 0,
            rental_id: "0-1-1000".to_string(),
        },
        response: conn_addr.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RETURN_REJECTED|"));
    assert!(response.contains("Slot ocupado"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_return_request_offline_confirma_directo_y_persiste_charge() {
    let id = 9121;
    let station = new_station(id, 4, 2);
    let actor = build_station_actor(station, "127.0.0.1:1");
    let addr = actor.start();

    let (listener, conn_addr) = spawn_connection_actor().await;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    addr.do_send(RequestMessage {
        request: ReturnRequest {
            user_id: 1,
            bike_id: 0,
            slot_index: 2,
            started_at_secs: now.saturating_sub(60),
            rental_id: "0-1-1000".to_string(),
        },
        response: conn_addr.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RETURN_CONFIRMED|"));

    let filename = get_charges_filename(id);
    actix_rt::time::sleep(Duration::from_millis(50)).await;
    let content = std::fs::read_to_string(&filename).unwrap_or_default();
    assert!(content.contains("\"bike_id\":0"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_return_request_online_espera_payment_result() {
    let id = 9122;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    addr.do_send(RequestMessage {
        request: ReturnRequest {
            user_id: 1,
            bike_id: 0,
            slot_index: 2,
            started_at_secs: now.saturating_sub(60),
            rental_id: "0-1-1000".to_string(),
        },
        response: conn_addr.clone(),
    });

    let capture_msg = recv_channel_async(&payment_rx).await.unwrap();
    assert!(capture_msg.starts_with("CAPTURE_PAYMENT|"));

    assert_no_response_async(&listener).await;

    addr.do_send(PaymentResult {
        transaction_id: "0-1-1000".to_string(),
        success: true,
        amount_cents: 50,
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RETURN_CONFIRMED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_payment_result_fallido_rechaza_devolucion() {
    let id = 9123;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    addr.do_send(RequestMessage {
        request: ReturnRequest {
            user_id: 1,
            bike_id: 0,
            slot_index: 2,
            started_at_secs: now.saturating_sub(60),
            rental_id: "0-1-2000".to_string(),
        },
        response: conn_addr.clone(),
    });

    let _ = recv_channel_async(&payment_rx).await.unwrap();

    addr.do_send(PaymentResult {
        transaction_id: "0-1-2000".to_string(),
        success: false,
        amount_cents: 0,
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RETURN_REJECTED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_reservation_rejected_por_fraude_rechaza_return_y_devuelve_bici() {
    let id = 9130;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);
    let (central_tx, central_rx) = mpsc::channel::<String>();
    actor_struct.central_server = Some(central_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rental_id = "0-42-3000".to_string();
    addr.do_send(RequestMessage {
        request: ReturnRequest {
            user_id: 42,
            bike_id: 0,
            slot_index: 2,
            started_at_secs: now.saturating_sub(60),
            rental_id: rental_id.clone(),
        },
        response: conn_addr.clone(),
    });

    let _ = recv_channel_async(&payment_rx).await.unwrap();

    addr.do_send(ReservationRejected {
        transaction_id: rental_id.clone(),
        reason: RETURN_REJECTED_FRAUD_REASON.to_string(),
    });

    let ban_msg = recv_channel_async(&central_rx).await.unwrap();
    assert!(ban_msg.starts_with("USER_BANNED|42|"));

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RETURN_REJECTED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_reservation_rejected_sin_motivo_fraude_no_responde_al_cliente() {
    let id = 9131;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);
    let (central_tx, central_rx) = mpsc::channel::<String>();
    actor_struct.central_server = Some(central_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;
    let addr = actor_struct.start();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rental_id = "0-43-3001".to_string();
    addr.do_send(RequestMessage {
        request: ReturnRequest {
            user_id: 43,
            bike_id: 0,
            slot_index: 2,
            started_at_secs: now.saturating_sub(60),
            rental_id: rental_id.clone(),
        },
        response: conn_addr.clone(),
    });

    let _ = recv_channel_async(&payment_rx).await.unwrap();

    addr.do_send(ReservationRejected {
        transaction_id: rental_id.clone(),
        reason: "Otro motivo".to_string(),
    });

    let ban_msg = recv_channel_async(&central_rx).await.unwrap();
    assert!(ban_msg.starts_with("USER_BANNED|43|"));

    assert_no_response_async(&listener).await;

    cleanup_station_files(id);
}

#[actix::test]
async fn test_central_server_connected_setea_sender() {
    let id = 9140;
    let station = new_station(id, 4, 2);
    let actor = build_station_actor(station, "127.0.0.1:1");
    let addr = actor.start();

    let (tx, rx) = mpsc::channel::<String>();
    addr.do_send(CentralServerConnected { sender: tx });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    let result = rx.try_recv();
    assert!(
        result.is_err(),
        "Sin pendientes, no debió enviar mensajes al central"
    );

    cleanup_station_files(id);
}

#[actix::test]
async fn test_central_server_disconnected_limpia_sender() {
    let id = 9141;
    let station = new_station(id, 4, 2);
    let actor = build_station_actor(station, "127.0.0.1:1");
    let addr = actor.start();

    let (tx, _rx) = mpsc::channel::<String>();
    addr.do_send(CentralServerConnected { sender: tx });
    actix_rt::time::sleep(Duration::from_millis(20)).await;

    addr.do_send(CentralServerDisconnected);
    actix_rt::time::sleep(Duration::from_millis(20)).await;

    let (listener, conn_addr) = spawn_connection_actor().await;
    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(
        response.starts_with("RENT_CONFIRMED|"),
        "Tras desconexión, debe operar offline"
    );

    cleanup_station_files(id);
}

#[actix::test]
async fn test_payment_service_disconnected_limpia_sender() {
    let id = 9142;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (tx, _rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(tx);
    let addr = actor_struct.start();

    addr.do_send(PaymentServiceDisconnected);
    actix_rt::time::sleep(Duration::from_millis(20)).await;

    let (listener, conn_addr) = spawn_connection_actor().await;
    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 2,
            slot_index: 0,
            card_token: "tok".to_string(),
        },
        response: conn_addr.clone(),
    });

    let response = read_socket_response_async(&listener).await;
    assert!(response.starts_with("RENT_CONFIRMED|"));

    cleanup_station_files(id);
}

#[actix::test]
async fn test_process_batch_updates_sincroniza_pendientes_exitosamente() {
    let id = 9150;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    save_pending_rent(id, "1-1-1000", 1, 1, "tok1");
    save_pending_charge(id, "0-1-2000", 50, 0);

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);
    let (central_tx, central_rx) = mpsc::channel::<String>();
    actor_struct.central_server = Some(central_tx);

    actor_struct.process_batch_updates();

    let msg1 = recv_channel_async(&payment_rx).await.unwrap();
    assert!(msg1.starts_with("RESERVE_PAYMENT|") || msg1.starts_with("CAPTURE_PAYMENT|"));

    let central_msg1 = recv_channel_async(&central_rx).await.unwrap();
    assert!(central_msg1.starts_with("OFFLINE_RENT|") || central_msg1.starts_with("RETURN_RENT|"));

    let rents_file = get_rents_filename(id);
    let charges_file = get_charges_filename(id);
    assert!(
        !std::fs::metadata(&rents_file).is_ok()
            || std::fs::read_to_string(&rents_file)
                .unwrap()
                .trim()
                .is_empty()
    );
    assert!(
        !std::fs::metadata(&charges_file).is_ok()
            || std::fs::read_to_string(&charges_file)
                .unwrap()
                .trim()
                .is_empty()
    );

    cleanup_station_files(id);
}

#[actix::test]
async fn test_process_batch_updates_sin_conexiones_mantiene_pendientes() {
    let id = 9151;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    save_pending_rent(id, "1-1-1000", 1, 1, "tok1");

    actor_struct.process_batch_updates();

    let rents_file = get_rents_filename(id);
    let content = std::fs::read_to_string(&rents_file).unwrap();
    assert!(
        content.contains("1-1-1000"),
        "El registro pendiente debe mantenerse"
    );

    cleanup_station_files(id);
}

#[actix::test]
async fn test_abort_expired_transactions_revierte_y_notifica_timeout() {
    let id = 9152;
    let station = new_station(id, 4, 2);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);

    let (listener, conn_addr) = spawn_connection_actor().await;

    let bike_id = actor_struct.station.reserve_bike(0).unwrap();

    actor_struct.pending_transactions.insert(
        "0-1-old".to_string(),
        TransactionState {
            slot_index: 0,
            bike_id,
            client_addr: conn_addr.clone(),
            payment_voted_commit: false,
            app_voted_commit: false,
            started_at: SystemTime::now() - Duration::from_secs(TIMEOUT_SECS + 5),
        },
    );

    actor_struct.abort_expired_transactions();

    let rollback_msg = recv_channel_async(&payment_rx).await.unwrap();
    assert!(rollback_msg.starts_with("ROLLBACK_PAYMENT|"));

    assert!(
        actor_struct.station.is_bike_available(0),
        "El slot debió revertirse a Occupied"
    );
    assert!(!actor_struct.pending_transactions.contains_key("0-1-old"));

    let _ = listener;
    cleanup_station_files(id);
}

#[actix::test]
async fn test_concurrencia_race_condition_dos_usuarios_piden_mismo_slot() {
    let id = 9999;
    let station = new_station(id, 4, 4);
    let mut actor_struct = build_station_actor(station, "127.0.0.1:1");

    let (payment_tx, _payment_rx) = mpsc::channel::<String>();
    actor_struct.payment_service = Some(payment_tx);
    let addr = actor_struct.start();

    let (listener_user_1, conn_addr_1) = spawn_connection_actor().await;
    let (listener_user_2, conn_addr_2) = spawn_connection_actor().await;

    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 1,
            slot_index: 0,
            card_token: "tok1".to_string(),
        },
        response: conn_addr_1.clone(),
    });
    addr.do_send(RequestMessage {
        request: RentRequest {
            user_id: 2,
            slot_index: 0,
            card_token: "tok2".to_string(),
        },
        response: conn_addr_2.clone(),
    });

    let response_1 = read_socket_response_async(&listener_user_1).await;
    let response_2 = read_socket_response_async(&listener_user_2).await;

    let user_1_gano = response_1.starts_with("PREPARE|");
    let user_2_gano = response_2.starts_with("PREPARE|");

    let user_1_perdio = response_1.starts_with("RENT_REJECTED|");
    let user_2_perdio = response_2.starts_with("RENT_REJECTED|");

    assert!(
        user_1_gano ^ user_2_gano,
        "Data Race prevenida: Solo un usuario pudo iniciar el 2PC sobre el slot"
    );
    assert!(
        user_1_perdio ^ user_2_perdio,
        "Data Race prevenida: El usuario que perdió fue rechazado por slot reservado"
    );

    cleanup_station_files(id);
}
