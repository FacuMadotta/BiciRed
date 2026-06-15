use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

use crate::actors::{CentralServerActor, ConnectionActor, ElectorActor};
use crate::messages_actors::*;

async fn setup_test_env(
    server_id: ServerId,
    is_leader: bool,
) -> (
    Addr<CentralServerActor>,
    Addr<ElectorActor>,
    TcpStream,
    Addr<ConnectionActor>,
) {
    let file_path = format!("banned_users_{}.json", server_id);
    let _ = std::fs::remove_file(&file_path);

    let peer_addrs = HashMap::new();
    let central = CentralServerActor::new(server_id, peer_addrs).start();
    let elector = ElectorActor::new(server_id, central.clone()).start();

    central.do_send(RegisterElectionActor {
        elector_addr: elector.clone(),
    });

    let leader_id = if is_leader { server_id } else { 999 };
    elector.do_send(LeaderAnnouncementMessage { leader_id });

    central.do_send(RoleUpdateMessage {
        is_leader,
        leader_id: Some(leader_id),
    });

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let client_stream = TcpStream::connect(addr).unwrap();
    let (server_stream, _) = listener.accept().unwrap();

    let conn =
        ConnectionActor::new_incoming(server_id, server_stream, central.clone(), elector.clone())
            .start();

    actix_rt::time::sleep(Duration::from_millis(50)).await;

    (central, elector, client_stream, conn)
}

fn dummy_station(id: StationId, x: f64, y: f64) -> StationStatus {
    StationStatus {
        station_id: id,
        location: Location { x, y },
        available_bikes: 10,
        free_slots: 10,
        updated_at_secs: 0,
        station_addr: "127.0.0.1:9000".to_string(),
        slots_occupied: "".to_string(),
        slots_frees: "".to_string(),
    }
}

async fn read_response(stream: &mut TcpStream, timeout: Duration) -> String {
    stream.set_nonblocking(true).unwrap();
    let start = std::time::Instant::now();
    let mut buf = [0u8; 4096];

    loop {
        match stream.read(&mut buf) {
            Ok(0) => {
                return String::new();
            }
            Ok(n) => {
                return String::from_utf8_lossy(&buf[..n]).to_string();
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                if start.elapsed() >= timeout {
                    return String::new();
                }
                actix_rt::time::sleep(Duration::from_millis(10)).await;
            }
            Err(_) => return String::new(),
        }
    }
}

async fn drain_stream(stream: &mut TcpStream) {
    stream.set_nonblocking(true).unwrap();
    actix_rt::time::sleep(Duration::from_millis(50)).await;
    let mut buf = [0u8; 4096];
    while let Ok(n) = stream.read(&mut buf) {
        if n == 0 {
            break;
        }
    }
}

#[actix::test]
async fn test_replica_rechaza_actualizacion_estacion() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(101, false).await;

    central.do_send(StationUpdateMessage {
        station: dummy_station(1, 0.0, 0.0),
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.starts_with("NOT_LEADER|"),
        "Debería rebotar con NOT_LEADER"
    );
}

#[actix::test]
async fn test_lider_acepta_actualizacion_y_sigue_operativo() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(106, true).await;

    drain_stream(&mut client_stream).await;

    central.do_send(StationUpdateMessage {
        station: dummy_station(42, 1.0, 1.0),
        response_addr: conn.clone(),
    });

    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(ValidateUserMessage {
        user_id: 1,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(response.contains("true") || response.contains("VALID"));
}

#[actix::test]
async fn test_lider_valida_usuario_bloqueado() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(102, true).await;
    let target_user = 555;

    central.do_send(UserBanned {
        user_id: target_user,
        reason: "Robo de bicicleta".to_string(),
    });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(ValidateUserMessage {
        user_id: target_user,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("false") || response.contains("INVALID"),
        "El usuario baneado debió ser rechazado"
    );
    assert!(
        response.contains("Robo de bicicleta"),
        "Debe incluir la razón del bloqueo"
    );
}

#[actix::test]
async fn test_lider_valida_usuario_no_bloqueado() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(107, true).await;

    central.do_send(ValidateUserMessage {
        user_id: 12345,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("true") || response.contains("VALID"),
        "El usuario no baneado debe ser válido"
    );
}

#[actix::test]
async fn test_persistencia_usuarios_baneados_sobrevive_apagon() {
    let server_id = 103;
    let file_path = format!("banned_users_{}.json", server_id);
    let _ = std::fs::remove_file(&file_path);

    let (central, _elector, _client_stream, _conn) = setup_test_env(server_id, true).await;
    central.do_send(UserBanned {
        user_id: 888,
        reason: "Test Persistencia".to_string(),
    });

    actix_rt::time::sleep(Duration::from_millis(100)).await;

    let central_restaurado = CentralServerActor::new(server_id, HashMap::new());

    assert!(
        central_restaurado.users_banned.contains_key(&888),
        "No recuperó el usuario del disco"
    );
    assert_eq!(
        central_restaurado.users_banned.get(&888).unwrap(),
        "Test Persistencia"
    );

    let _ = std::fs::remove_file(&file_path);
}

#[actix::test]
async fn test_persistencia_multiples_usuarios_y_actualizacion() {
    let server_id = 108;
    let file_path = format!("banned_users_{}.json", server_id);
    let _ = std::fs::remove_file(&file_path);

    let (central, _elector, _client_stream, _conn) = setup_test_env(server_id, true).await;
    central.do_send(UserBanned {
        user_id: 1,
        reason: "Razon A".to_string(),
    });
    central.do_send(UserBanned {
        user_id: 2,
        reason: "Razon B".to_string(),
    });
    central.do_send(UserBanned {
        user_id: 1,
        reason: "Razon A actualizada".to_string(),
    });

    actix_rt::time::sleep(Duration::from_millis(100)).await;

    let central_restaurado = CentralServerActor::new(server_id, HashMap::new());

    assert_eq!(central_restaurado.users_banned.len(), 2);
    assert_eq!(
        central_restaurado.users_banned.get(&1).unwrap(),
        "Razon A actualizada"
    );
    assert_eq!(central_restaurado.users_banned.get(&2).unwrap(), "Razon B");

    let _ = std::fs::remove_file(&file_path);
}

#[actix::test]
async fn test_replica_responde_nearby_query() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(104, false).await;

    let mut dummy_table = HashMap::new();
    dummy_table.insert(700, dummy_station(700, 10.0, 10.0));

    central.do_send(ReplicaSyncMessage {
        station_table: dummy_table,
        banned_users: HashMap::new(),
    });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 111,
        location: Location { x: 9.0, y: 9.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("NEARBY_RESPONSE"),
        "La réplica debió procesar la consulta"
    );
    assert!(response.contains("700"), "Debió incluir la estación 700");
}

#[actix::test]
async fn test_replica_nearby_query_filtra_por_radio() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(109, false).await;

    let mut dummy_table = HashMap::new();
    dummy_table.insert(801, dummy_station(801, 0.0, 0.0));
    dummy_table.insert(802, dummy_station(802, 100.0, 100.0));

    central.do_send(ReplicaSyncMessage {
        station_table: dummy_table,
        banned_users: HashMap::new(),
    });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("801"),
        "Debió incluir la estación cercana"
    );
    assert!(
        !response.contains("802"),
        "No debió incluir la estación lejana"
    );
}

#[actix::test]
async fn test_replica_nearby_query_usuario_baneado_recibe_notificacion() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(110, false).await;

    let mut banned = HashMap::new();
    banned.insert(999, "Vandalismo".to_string());

    central.do_send(ReplicaSyncMessage {
        station_table: HashMap::new(),
        banned_users: banned,
    });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 999,
        location: Location { x: 0.0, y: 0.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("Vandalismo"),
        "Debió notificar al usuario baneado"
    );
    assert!(
        !response.contains("NEARBY_RESPONSE"),
        "No debió responder con la lista de estaciones"
    );
}

#[actix::test]
async fn test_lider_rechaza_nearby_query() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(111, true).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.starts_with("NOT_REPLICA|"),
        "El líder debió rebotar la consulta NEARBY"
    );
}

#[actix::test]
async fn test_bully_responde_ack_a_nodo_menor() {
    use std::io::Write;
    let (_central, _elector, mut peer_stream, _conn) = setup_test_env(100, false).await;

    peer_stream.write_all(b"ELECTION|50\n").unwrap();

    let response = read_response(&mut peer_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("ELECTION_ACK"),
        "El nodo mayor debió responder ELECTION_ACK"
    );
}

#[actix::test]
async fn test_bully_acepta_coordinador_mayor() {
    use std::io::Write;
    let (central, _elector, mut peer_stream, conn) = setup_test_env(100, true).await;

    peer_stream.write_all(b"COORDINATOR|250\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    central.do_send(StationUpdateMessage {
        station: dummy_station(1, 0.0, 0.0),
        response_addr: conn,
    });

    let response = read_response(&mut peer_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("NOT_LEADER"),
        "Debió acatar al COORDINATOR y rebotar como Réplica"
    );
}

#[actix::test]
async fn test_election_ack_es_procesado_sin_error() {
    use std::io::Write;
    let (_central, _elector, mut peer_stream, _conn) = setup_test_env(120, false).await;

    peer_stream.write_all(b"ELECTION_ACK\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    peer_stream.write_all(b"ACK\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(50)).await;
}

#[actix::test]
async fn test_peer_disconnected_de_lider_dispara_eleccion() {
    use std::io::Write;
    let (_central, elector, mut peer_stream, _conn) = setup_test_env(130, false).await;

    peer_stream.write_all(b"HELLO|200\n").unwrap();

    drain_stream(&mut peer_stream).await;

    elector.do_send(PeerDisconnectedMessage { server_id: 999 });

    actix_rt::time::sleep(Duration::from_millis(100)).await;

    let response = read_response(&mut peer_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("ELECTION|130"),
        "Debió iniciar la elección notificando al peer de mayor ID"
    );
}

#[actix::test]
async fn test_init_election_sin_peers_mayores_autoproclama_lider() {
    use std::io::Write;
    let (_central, elector, mut peer_stream, _conn) = setup_test_env(150, false).await;

    peer_stream.write_all(b"HELLO|10\n").unwrap();

    drain_stream(&mut peer_stream).await;

    elector.do_send(PeerDisconnectedMessage { server_id: 999 });

    let response = read_response(&mut peer_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("COORDINATOR|150"),
        "Debió autoproclamarse líder y notificar COORDINATOR"
    );
}

#[actix::test]
async fn test_leader_election_message_de_nodo_menor_responde_ack_e_inicia_eleccion() {
    use std::io::Write;
    let (_central, elector, mut peer_stream, _conn) = setup_test_env(160, false).await;

    peer_stream.write_all(b"HELLO|50\n").unwrap();

    drain_stream(&mut peer_stream).await;

    elector.do_send(LeaderElectionMessage { server_id: 50 });

    let response = read_response(&mut peer_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("ELECTION_ACK"),
        "Debió responder ELECTION_ACK al nodo de menor ID"
    );
}

#[actix::test]
async fn test_leader_announcement_actualiza_estado_de_central() {
    let (central, elector, mut client_stream, conn) = setup_test_env(170, false).await;

    elector.do_send(LeaderAnnouncementMessage { leader_id: 5 });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(StationUpdateMessage {
        station: dummy_station(1, 0.0, 0.0),
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(
        response.starts_with("NOT_LEADER|"),
        "Debe seguir rechazando como réplica con nuevo leader_id"
    );
}

#[actix::test]
async fn test_lider_broadcast_replica_sync_al_conectar_peer() {
    use std::io::Write;
    let (_central, _elector, mut peer_stream, _conn) = setup_test_env(100, true).await;

    peer_stream.write_all(b"HELLO|50\n").unwrap();

    let response = read_response(&mut peer_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("REPLICA_SYNC|"),
        "Debió mandar el REPLICA_SYNC tras el HELLO"
    );
}

#[actix::test]
async fn test_replica_procesa_y_guarda_sync_del_lider() {
    use std::io::Write;
    let (central, _elector, mut client_stream, conn) = setup_test_env(105, false).await;

    let payload_sync = "REPLICA_SYNC|705#10.0#10.0#5#15#12345#127.0.0.1:9000#0,1#2,3|888,Robo\n";
    client_stream.write_all(payload_sync.as_bytes()).unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 111,
        location: Location { x: 9.0, y: 9.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("705"),
        "La Réplica no guardó la estación sincronizada."
    );
    assert!(
        response.contains("5"),
        "La Réplica no parseó bien las bicis."
    );
}

#[actix::test]
async fn test_replica_sync_con_multiples_estaciones() {
    use std::io::Write;
    let (central, _elector, mut client_stream, conn) = setup_test_env(112, false).await;

    let payload_sync = "REPLICA_SYNC|601#0.0#0.0#3#7#100#127.0.0.1:9001#a#b;602#50.0#50.0#1#1#200#127.0.0.1:9002#c#d|\n";
    client_stream.write_all(payload_sync.as_bytes()).unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(response.contains("601"), "Debió guardar la estación 601");
    assert!(
        !response.contains("602"),
        "La estación 602 está fuera del radio"
    );
}

#[actix::test]
async fn test_replica_sync_con_multiples_usuarios_baneados() {
    use std::io::Write;
    let (central, _elector, mut client_stream, conn) = setup_test_env(113, false).await;

    let payload_sync = "REPLICA_SYNC||1,Razon1;2,Razon2\n";
    client_stream.write_all(payload_sync.as_bytes()).unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    central.do_send(ValidateUserMessage {
        user_id: 2,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.contains("false") || response.contains("INVALID"),
        "El usuario 2 debió quedar baneado"
    );
    assert!(
        response.contains("Razon2"),
        "Debió incluir la razón del usuario 2"
    );
}

#[actix::test]
async fn test_lider_rechaza_nearby_tras_recibir_replica_sync_entrante() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(114, true).await;

    drain_stream(&mut client_stream).await;

    let mut fake_table = HashMap::new();
    fake_table.insert(999, dummy_station(999, 999.0, 999.0));
    central.do_send(ReplicaSyncMessage {
        station_table: fake_table,
        banned_users: HashMap::new(),
    });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });
    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        response.starts_with("NOT_REPLICA|"),
        "El líder sigue rechazando NEARBY"
    );
}

#[actix::test]
async fn test_ping_actualiza_timestamp_de_estacion() {
    use std::io::Write;
    let (central, _elector, mut client_stream, conn) = setup_test_env(115, false).await;

    let mut dummy_table = HashMap::new();
    let mut st = dummy_station(500, 1.1, 1.1);
    st.updated_at_secs = 0;
    dummy_table.insert(500, st);

    central.do_send(ReplicaSyncMessage {
        station_table: dummy_table,
        banned_users: HashMap::new(),
    });
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    client_stream.write_all(b"PING|500\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 1.1, y: 1.1 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(response.contains("500"), "Debió encontrar la estación 500");
    assert!(
        !response.contains("\"updated_at_secs\":0") && !response.contains("\"updated_at_secs\": 0"),
        "El timestamp debió actualizarse a la hora actual"
    );
}

#[actix::test]
async fn test_ping_estacion_inexistente_no_crashea() {
    use std::io::Write;
    let (_central, _elector, mut client_stream, _conn) = setup_test_env(116, false).await;

    client_stream.write_all(b"PING|9999\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    client_stream
        .write_all(b"NEARBY_QUERY|1|0.0|0.0|5.0\n")
        .unwrap();
    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(response.contains("NEARBY_RESPONSE") || response.starts_with("NOT_REPLICA"));
}

#[actix::test]
async fn test_mensaje_no_reconocido_no_crashea_actor() {
    use std::io::Write;
    let (_central, _elector, mut client_stream, _conn) = setup_test_env(117, false).await;

    client_stream
        .write_all(b"MENSAJE_DESCONOCIDO|abc|def\n")
        .unwrap();
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    client_stream
        .write_all(b"NEARBY_QUERY|1|0.0|0.0|5.0\n")
        .unwrap();
    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(response.contains("NEARBY_RESPONSE") || response.starts_with("NOT_REPLICA"));
}

#[actix::test]
async fn test_replica_sync_vacio_no_crashea() {
    use std::io::Write;
    let (central, _elector, mut client_stream, conn) = setup_test_env(118, false).await;

    client_stream.write_all(b"REPLICA_SYNC||\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        radius: 5.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("NEARBY_RESPONSE"),
        "Debió responder igual aunque la tabla esté vacía"
    );
}

#[actix::test]
async fn test_replica_sync_con_estacion_malformada_se_ignora() {
    use std::io::Write;
    let (central, _elector, mut client_stream, conn) = setup_test_env(122, false).await;

    let payload_sync = "REPLICA_SYNC|901#0.0#0.0#3#7#100#127.0.0.1:9001#a;902#5.0#5.0#1#1#200#127.0.0.1:9002#c#d|\n";
    client_stream.write_all(payload_sync.as_bytes()).unwrap();
    actix_rt::time::sleep(Duration::from_millis(100)).await;

    central.do_send(NearbyStationsRequestMessage {
        user_id: 1,
        location: Location { x: 0.0, y: 0.0 },
        radius: 10.0,
        response_addr: conn.clone(),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;

    assert!(
        !response.contains("901"),
        "La estación malformada no debió guardarse"
    );
    assert!(
        response.contains("902"),
        "La estación bien formada debió guardarse"
    );
}

#[actix::test]
async fn test_nearby_query_con_parametros_invalidos_usa_defaults() {
    use std::io::Write;
    let (_central, _elector, mut client_stream, _conn) = setup_test_env(123, false).await;

    client_stream
        .write_all(b"NEARBY_QUERY|1|abc|def|xyz\n")
        .unwrap();

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("NEARBY_RESPONSE"),
        "Debió responder usando los valores por defecto"
    );
}

#[actix::test]
async fn test_conexion_cerrada_no_crashea_actores() {
    use std::io::Write;
    let (_central, elector, mut client_stream, _conn) = setup_test_env(124, false).await;

    client_stream.write_all(b"HELLO|200\n").unwrap();
    actix_rt::time::sleep(Duration::from_millis(50)).await;

    drop(client_stream);
    actix_rt::time::sleep(Duration::from_millis(150)).await;

    elector.do_send(RemovePeerMessage { server_id: 999 });
    actix_rt::time::sleep(Duration::from_millis(50)).await;
}

#[actix::test]
async fn test_remove_peer_message_no_panica_con_peer_inexistente() {
    let (central, _elector, _client_stream, _conn) = setup_test_env(125, true).await;

    central.do_send(RemovePeerMessage { server_id: 555 });
    actix_rt::time::sleep(Duration::from_millis(50)).await;
}

#[actix::test]
async fn test_register_peer_connection_message_lider_hace_broadcast() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(126, true).await;

    drain_stream(&mut client_stream).await;

    central.do_send(RegisterPeerConnectionMessage {
        server_id: 300,
        connection_addr: conn.clone(),
        peer_addr: Some("127.0.0.1:9999".to_string()),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("REPLICA_SYNC|"),
        "El líder debió enviar REPLICA_SYNC al registrar un nuevo peer"
    );
}

#[actix::test]
async fn test_update_station_timestamp_sin_estacion_no_falla() {
    let (central, _elector, _client_stream, _conn) = setup_test_env(127, true).await;

    central.do_send(UpdateStationTimestamp { station_id: 9999 });
    actix_rt::time::sleep(Duration::from_millis(50)).await;
}

#[actix::test]
async fn test_role_update_message_a_lider_dispara_sync_inicial() {
    let (central, _elector, mut client_stream, conn) = setup_test_env(128, false).await;

    drain_stream(&mut client_stream).await;

    central.do_send(RegisterPeerConnectionMessage {
        server_id: 400,
        connection_addr: conn.clone(),
        peer_addr: None,
    });

    drain_stream(&mut client_stream).await;

    central.do_send(RoleUpdateMessage {
        is_leader: true,
        leader_id: Some(128),
    });

    let response = read_response(&mut client_stream, Duration::from_secs(2)).await;
    assert!(
        response.contains("REPLICA_SYNC|"),
        "Al asumir liderazgo debe sincronizar el clúster"
    );
}
