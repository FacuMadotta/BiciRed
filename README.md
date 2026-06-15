[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/KujF6lFv)

# 🚲 BiciRed — Alquiler de Bicicletas

El sistema modela una red de estaciones de bicicletas distribuidas por la ciudad. Cada componente corre como un proceso independiente; y la comunicación entre procesos es a través de stream sockets (TCP), usando mensajes de texto delimitados por `|`.

Dentro de cada proceso se aplica el **modelo de actores** (mediante librería `actix`): cada subsistema es un thread independiente que sólo se comunica a través de mensajes tipados. Por lo que, no hay memoria compartida entre actores.

### Integrantes

| Nombre y Apellido | Padrón |
| :--- | :--- |
| Facundo Madotta | 112180 | 
| Fabricio Batastini | 111828 |
| Manuel Peñalva | 111696 |

---

## Tabla de contenidos

- [Arquitectura de procesos](#arquitectura-de-procesos)
- [Entidades principales](#entidades-principales)
  - [Station](#station)
  - [CentralServer](#centralserver)
  - [App](#app)
  - [PaymentService](#paymentservice)
- [Flujos principales](#flujos-principales)
- [Manejo de errores y caídas](#manejo-de-errores-y-caídas)
- [Operación offline](#operación-offline)
- [Algoritmos de concurrencia distribuida](#algoritmos-de-concurrencia-distribuida)
  - [Elección de líder — Bully](#elección-de-líder--bully)
  - [Transacciones de alquiler — 2PC](#transacciones-de-alquiler--2pc)
- [Guía de Ejecución y Comandos](#guía-de-ejecución-y-comandos)
- [Diagramas](#diagramas)

---

## Arquitectura de procesos

```
[APP] ──────────────────► [STATION]
[APP] ──────────────────► [CENTRALSERVER]
[STATION] ──────────────► [CENTRALSERVER]
[STATION] ──────────────► [PAYMENTSERVICE]
[CENTRALSERVER]─────────► [PAYMENTSERVICE]
```

| Proceso         | Rol                                                        |
|-----------------|------------------------------------------------------------|
| `Station`       | Gestiona slots físicos, coordina alquileres y devoluciones                        |
| `CentralServer` | Mantiene estado global de todas las estaciones             |
| `App`           | Simula la app móvil del usuario                            |
| `PaymentService`| Actúa como banco; preutoriza, captura y libera montos                        |

Se pueden correr múltiples instalaciones de `CentralServer` simultáneamente, definidas en un archivo `servers.csv`. Una actúa como **líder** (recibe actualizaciones de las Stations y replica el estado a los demás nodos). Las demás **réplicas** (responden consultas de disponibilidad de las Apps). Si el líder cae, los nodos restantes eligen a uno nuevo mediante el [Algoritmo de Bully](#elección-de-líder--bully).

---

## Entidades principales

### Station

Gestiona los slots físicos de una estación. Detecta bicicletas, las bloquea y desbloquea, coordina el cobro de tarifas con el `PaymentService` y reporta su estado al servidor central. **Opera de forma autónoma auqnue pierda conectivdad**.

>**Invariante**: Cada `bike_id` pertenece a lo sumo a un alquiler activo a la vez. Una bicicleta solo puede ser alquilada si fue devuelta anteriormente (slot en estado `Occupied`, no `Empty` ni `Reserved`).

#### Estado interno

```rust
struct Station {
    id: StationId,
    location: Location,
    slots: Vec<Slot>,
    pending_rents: Vec<PendingRent>,
    pending_charges: Vec<PendingCharge>
}

struct Slot {
    index: usize,
    state: SlotState,
}

enum SlotState {
    Empty,
    Occupied { bike_id: BikeId },
    Reserved { rental_id: String },
}

struct PendingRentRecord {
    rental_id: String,
    card_token: String,
    bike_id: BikeId,
    user_id: UserId,
}

struct PendingChargeRecord {
    rental_id: String,
    amount_cents: u32,
    bike_id: BikeId,
}
```

### Identificación de alquileres - `rental_id`

Cada viaje se identifica con un `rental_id` único generado por la Station al inicial el alquiler:

```
rental_id = bike_id + user_id + timestamp_secs
```

Esta combinación garantiza unicidad sin depender de sincronización distribuida: una misma bicicleta no puede ser alquilada dos veces por el mismo usuario en el mismo instante. El `rental_id` viaja en todos los mensajes relacionados al viaje (`Prepare`, `VOTE_COMMIT`, `RentConfirmed`, `ReturnRequest`, etc) y es usado por el `PaymentService` para garantizar independencia: si se recibe dos veces la misma operación para el mismo `rental_id`, no la repite.

#### Arquitectura interna (threads)

| Thread            | Responsabilidad                                                                 |
|-------------------|---------------------------------------------------------------------------------|
| `Acceptor`        | Escucha nuevas conexiones TCP entrantes (de la App); spawnea un `ConnectionActor` por conexión   |
| `ConnectionActor`         | Traduce mensajes TCP (texto delimitado por `|`) a mensajes `actix` para el `StationActor`; uno por conexión          |
| `StationActor`   | Único dueño de `Vec<Slot>`; coordina el 2PC de alquiler y el flujo de devolución          |

#### Mensajes que recibe

| Mensaje         | Payload                                                  | Reacción                                                                                                      |
|-----------------|------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `RENT_REQUEST`  | `{ user_id, slot_index, card_token }`                       | Si el slot está `Occupied` → genera `rental_id`, lo marca `Reserved` e inicia 2PC con App y PaymentService; si no → `RENT_REJECTED` |
| `RETURN_REQUEST`| `{ user_id, bike_id, slot_index, started_at_secs, rental_id }` | Si el slot está `Empty` → calcula cargo proporcional al tiempo y solicita `CAPTURE_PAYMENT` al PaymentService; si no → `RETURN_REJECTED` |
| `VOTE_COMMIT`   | `{ transaction_id }`                                        | Voto de la App durante la fase de votación del 2PC                                                          |
| `VOTE_ABORT`    | `{ transaction_id }`                                        | Voto negativo de la App; dispara abort y rollback                                                            |
| `NOT_LEADER`    | `{ leader_addr }`                                           | Actualiza la dirección del líder conocida y reintenta el `STATION_UPDATE` pendiente     

#### Mensajes que envía

| Mensaje           | Destino              | Payload                                                                  |
|-------------------|----------------------|----------------------------------------------------------------------------|
| `PREPARE`         | App (TCP)            | `{ transaction_id }` — pide a la App confirmar que sigue activa y sin reserva |
| `RENT_CONFIRMED`  | App (TCP)            | `{ bike_id, pre_auth_cents, timestamp_secs, rental_id }`                  |
| `RENT_REJECTED`   | App (TCP)            | `{ reason }`                                                              |
| `RETURN_CONFIRMED`| App (TCP)            | `{ charged_cents, timestamp_secs }`                                       |
| `RETURN_REJECTED` | App (TCP)            | `{ reason }` — incluye el caso de fraude (ver `RETURN_REJECTED_FRAUD_REASON`) |
| `STATION_UPDATE`  | CentralServer (TCP)  | `StationStatus { station_id, location, available_bikes, free_slots, updated_at_secs, station_addr, slots_occupied, slots_frees }` |
| `PING`            | CentralServer (TCP)  | `{ station_id }` — heartbeat liviano para evitar que el líder elimine la estación por inactividad |
| `PREPARE_PAYMENT` | PaymentService (TCP) | `{ transaction_id, amount_cents, card_token }` — preautoriza el monto de seguridad |
| `COMMIT_PAYMENT`  | PaymentService (TCP) | `{ transaction_id }` — confirma la preautorización tras el commit del 2PC |
| `ROLLBACK_PAYMENT`| PaymentService (TCP) | `{ transaction_id }` — cancela la preautorización si el 2PC aborta        |
| `CAPTURE_PAYMENT` | PaymentService (TCP) | `{ transaction_id, amount_cents }` — cobra el monto final al devolver     |
| `USER_BANNED`     | CentralServer (TCP)  | `{ user_id, reason }` — informa al líder que un usuario debe ser baneado (cobro de devolución rechazado) |


#### Protocolo de transporte

TCP (stream sockets) con la App, con el CentralServer y PaymentService. Mensajes de texto delimitados por `|`, un mensaje por línea (terminados en `\n`)

#### Casos de interés

- **Feliz (alquiler):** ver [Flujo 1 - 2PC](#flujo-1--alquiler-caso-feliz)
- **Feliz (devolución online):** App envía `RETURN_REQUEST` → Station calcula cargo → envía `CAPTURE_PAYMENT` al PaymentService → responde `RETURN_CONFIRMED`.
- **Devolución offline:** Station guarda en `pending_charges_{id}.json`, libera al usuario respondiendo `RETURN_CONFIRMED`, reintenta el cobro al recuperar red.
- **App muere durante 2PC:** timeout en el socket → la estación aborta, envía `ROLLBACK_PAYMENT` al PaymentService, slot vuelve a `Occupied`.
- **Pago rechazado en Prepare:** PaymentService responde `VOTE_ABORT` → Station envía rollback implícito a la App (no se confirma el alquiler), slot vuelve a `Occupied`.
- **Cobro rechazado al devolver:** PaymentService responde `RESERVATION_REJECTED` → Station responde `RETURN_REJECTED { reason: RETURN_REJECTED_FRAUD_REASON }` y envía `USER_BANNED` al CentralServer líder. La bici queda devuelta físicamente (no penaliza al sistema), pero el usuario queda baneado.
- **Sin conectividad con CentralServer:** opera localmente, guarda en `pending_rents_{id}.json` / `pending_charges_{id}.json`, reintenta `STATION_UPDATE` periódicamente.

#### Manejo de devoluciones

```mermaid
graph TD
    A[Usuario inserta la bici en el slot] --> B[Station calcula tiempo transcurrido y costo proporcional]
    B --> C{¿PaymentService ONLINE o OFFLINE?}
    
    C -->|PaymentService ONLINE| D[Escenario Online]
    C -->|OFFLINE| E[Escenario Offline]

    subgraph Escenario Online
        D --> D1[1. Station solicita 'Capture' al PaymentService usando el rental_id]
        D1 --> D2["2. PaymentService cobra el monto; si falla, responde RESERVATION_REJECTED"]
        D2 --> D3["3a. Éxito: RETURN_CONFIRMED a la App"]
        D2 --> D4["3b. Rechazo: RETURN_REJECTED (fraude) + USER_BANNED al CentralServer"]
    end

    subgraph Escenario Offline
        E --> E1[1. Station guarda el cobro en pending_charges.json]
        E1 --> E2[2. Libera al usuario en la App usando RETURN_CONFIRMED]
        E2 --> E3[3. Reintenta CAPTURE_PAYMENT cuando vuelve a la red]
    end
```

Si previamente retiro una bicicleta en una estacion offline y al enviar Capture es rechazado el pago, entra en la lista negra del central server

---

### CentralServer

Mantiene una vista actualizada del estado de todas las estaciones, responde consultas de disponibilidad y administra la lista de usuarios baneados. Recibe actualizaciones periódicas de las estaciones sin bloquearlas.

Si dos nodos reciben un `STATION_UPDATE` con información contradictoria, gana la entrada más reciente (se reemplaza directamente porque solo el líder procesa escrituras).

```rust
struct CentralServerActor {
    server_id: ServerId,
    is_leader: bool,
    leader_id: Option<ServerId>,
    station_table: HashMap<StationId, StationStatus>,
    peers: HashMap<ServerId, Addr<ConnectionActor>>,
    elector_addr: Option<Addr<ElectorActor>>,
    peer_addrs: HashMap<ServerId, String>,
    users_banned: HashMap<UserId, String>,
}

struct StationStatus {
    station_id: StationId,
    location: Location,
    available_bikes: u8,
    free_slots: u8,
    updated_at_secs: u64,
    station_addr: String,
    slots_occupied: String,
    slots_frees: String,
}
```

#### Arquitectura interna (threads)
 
| Actor              | Responsabilidad                                                                                              |
|--------------------|------------------------------------------------------------------------------------------------------------|
| `Acceptor`         | Escucha el puerto TCP propio; spawnea un `ConnectionActor` (entrante) por cada conexión nueva (Station, App o peer) |
| `SpawnerActor`     | Recibe las conexiones entrantes del `Acceptor` y crea el `ConnectionActor` correspondiente                  |
| `ConnectionActor`  | Uno por conexión TCP (entrante o saliente); parsea mensajes y los traduce a mensajes `actix` para `CentralServerActor` y `ElectorActor` |
| `CentralServerActor` | Único dueño de `station_table` y `users_banned`; responde `NEARBY_QUERY`, aplica `STATION_UPDATE`, ejecuta garbage collection de estaciones inactivas |
| `ElectorActor`     | Implementa el Algoritmo de Bully: detecta caída del líder, gestiona elecciones y anuncia coordinador        |

#### Mensajes que recibe

| Mensaje          | Payload                                                  | Reacción                                                                                       |
|------------------|--------------------------------------------------------|--------------------------------------------------------------------------------------------------|
| `STATION_UPDATE` | `StationStatus` serializado                              | Si es líder: actualiza `station_table` y dispara `REPLICA_SYNC`; si es réplica: responde `NOT_LEADER` |
| `NEARBY_QUERY`   | `{ user_id, x, y, radius }`                              | Si es líder: responde `NOT_REPLICA` (delega la carga); si es réplica: filtra por distancia euclídea y responde `NEARBY_RESPONSE`, o `BAN_NOTIFICATION` si el usuario está baneado |
| `VALIDATE_USER`  | `{ user_id }`                                            | Responde `USER_VALIDATION_RESULT` indicando si el usuario está en `users_banned`               |
| `USER_BANNED`    | `{ user_id, reason }`                                    | Agrega el usuario a `users_banned`, persiste en disco y dispara `REPLICA_SYNC`                |
| `REPLICA_SYNC`   | `{ station_table, banned_users }`                        | Si no es líder: reemplaza `station_table` y `users_banned` locales con los recibidos           |
| `PING`           | `{ station_id }`                                         | Actualiza `updated_at_secs` de esa estación (heartbeat liviano, evita el GC)                    |
| `HELLO`          | `{ server_id }`                                          | Registra la conexión entrante de un peer en `peers` y en el `ElectorActor`                      |
| `ELECTION`       | `{ candidate_id }`                                       | El `ElectorActor` participa del Algoritmo de Bully                                              |
| `ELECTION_ACK` / `ACK` | `{}`                                                | El `ElectorActor` marca que un nodo de mayor ID está vivo y se retira de la elección actual     |
| `COORDINATOR`    | `{ leader_id }`                                          | El `ElectorActor` actualiza `leader_id`, ajusta `is_leader` y propaga `ROLE_UPDATE` al `CentralServerActor` |

#### Mensajes que envía

| Mensaje           | Destino                  | Payload                                                              |
|-------------------|--------------------------|------------------------------------------------------------------------|
| `NEARBY_RESPONSE` | App                      | `Vec<StationStatus>` (estaciones dentro del radio)                    |
| `BAN_NOTIFICATION`| App                      | `{ reason }`                                                          |
| `USER_VALIDATION_RESULT` | Station / App      | `{ user_id, is_valid, reason }`                                       |
| `NOT_LEADER`      | Station                  | `{ leader_addr }` — redirige el `STATION_UPDATE` al líder actual      |
| `NOT_REPLICA`     | App                      | `{ replica_addr }` — redirige el `NEARBY_QUERY` a una réplica         |
| `REPLICA_SYNC`    | Otros CentralServer (réplicas) | `{ station_table, banned_users }`                                |
| `HELLO`           | Otros CentralServer (peers) | `{ server_id }` — handshake al establecer conexión saliente        |
| `ELECTION`        | Peers con ID mayor       | `{ candidate_id }`                                                    |
| `ELECTION_ACK`    | Peer que inició la elección | `{}` — "yo tengo ID mayor, abortá tu elección"                     |
| `COORDINATOR`     | Todos los peers          | `{ leader_id }`                                                       |

### Limpieza de estaciones

El líder ejecuta cada 15 segundos una limpieza: si una estación no actualizó `updated_at_secs` (vía `STATION_UPDATE` o `PING`) en más de 30 segundos, se elimina de `station_table` y se propaga el cambio vía `REPLICA_SYNC`. Esto minimiza el envío de heartbeats explícitos: el propio `PING` liviano de la Station alcanza para mantenerla viva en la tabla. Si nunca llego, la estación se da como "muerta".

#### Protocolo de transporte

TCP (stream sockets) para toda comunicación.

#### Casos de interés

 
- **Feliz:** Station envía `STATION_UPDATE` al líder → líder actualiza `station_table` y dispara `REPLICA_SYNC` a las réplicas → App consulta una réplica → `NEARBY_RESPONSE`.
- **Station conecta al nodo equivocado:** si no es líder, responde `NOT_LEADER` con la dirección del líder conocida.
- **App conecta al líder:** el líder responde `NOT_REPLICA` con la dirección de una réplica, para no sobrecargar al nodo de escritura con consultas de lectura.
- **Usuario baneado:** `NEARBY_QUERY` de un usuario baneado recibe `BAN_NOTIFICATION` en lugar de `NEARBY_RESPONSE`.
- **Líder cae:** ver [Elección de líder — Bully](#elección-de-líder--bully).

---

### App

Simula la app móvil del usuario. Se conecta directamente a la estación para alquilar/devolver y al servidor central para consultar disponibilidad. Mantiene caché local para operación offline y persiste su alquiler activado en disco.

```rust
struct AppClient {
    user_id: UserId,
    current_rental: Option<ActiveRental>,
    cached_stations: Vec<StationStatus>,
    is_blocked: bool,
    central_servers: Vec<String>,
    active_server_addr: String,
    actual_rental_id: Option<String>,
}

struct ActiveRental {
    bike_id: BikeId,
    started_at_secs: u64,
    pre_auth_cents: u32,
    station_id: StationId,
}
```

`current_rental` se persiste en `rental_state_<user_id>.json` y se restaura al iniciar la App, de forma que un alquiler activo sobrevive a un reinicio del proceso.

#### Mensajes que recibe

| Mensaje           | Origen        | Payload                                                              |
|-------------------|---------------|------------------------------------------------------------------------|
| `RENT_CONFIRMED`  | Station       | `{ bike_id, pre_auth_cents, timestamp_secs, rental_id }`               |
| `RENT_REJECTED`   | Station       | `{ reason }`                                                            |
| `RETURN_CONFIRMED`| Station       | `{ charged_cents, timestamp_secs }`                                     |
| `RETURN_REJECTED` | Station       | `{ reason }` — distingue el caso de fraude (`RETURN_REJECTED_FRAUD_REASON`) |
| `NEARBY_RESPONSE` | CentralServer | `Vec<StationStatus>`                                                    |
| `PREPARE`         | Station       | `{ transaction_id }` — la Station pide confirmar que la App sigue activa y sin reserva |
| `NOT_REPLICA`     | CentralServer | `{ replica_addr }` — redirige a la réplica correcta                     |
| `BAN_NOTIFICATION`| CentralServer | `{ reason }` — informa que el usuario fue baneado                       |

#### Mensajes que envía

| Mensaje          | Destino       | Payload                                                          |
|------------------|---------------|---------------------------------------------------------------------|
| `NEARBY_QUERY`   | CentralServer | `{ user_id, x, y, radius }`                                       |
| `RENT_REQUEST`   | Station       | `{ user_id, slot_index, card_token }`                             |
| `RETURN_REQUEST` | Station       | `{ user_id, bike_id, slot_index, started_at_secs, rental_id }`    |
| `VOTE_COMMIT`    | Station       | `{ transaction_id }` — App activa y sin reserva activa             |

#### Protocolo de transporte

TCP (stream sockets). No escucha ningún puerto, solo incia conexiones. Implementa rotación de servidores: si falla al conectar con `active_server_addr`, rota al siguiente nodo de `central_servers` (hasta `MAX_RETRIES = 5`); sino lo da como servidor OFFLINE.

#### Casos de interés

- **Sin señal consultando disponibilidad:** si tras `MAX_RETRIES` no logra contactar a ningún `CentralServer`, muestra `cached_stations` (última `NEARBY_RESPONSE` recibida).
- **Sin señal alquilando o devolviendo:** la App ya tiene la dirección de la Station en `cached_stations` (campo `station_addr`); conecta directamente sin pasar por el CentralServer.
- **Conexión al nodo líder:** recibe `NOT_REPLICA`, actualiza `active_server_addr` a la réplica indicada y reintenta.
- **Usuario bloqueado:** recibe `BAN_NOTIFICATION`, marca `is_blocked = true` y no permite nuevos alquileres.
- **Reinicio con alquiler activo:** al arrancar, lee `rental_state_<user_id>.json` y restaura `current_rental`.

---

### Payment Service

El proceso PaymentService que funcionara como un banco mediante conexiones TCP ya sea con el server o con las estaciones
- Preautoriza montos (asocia tarjeta, monto preautorizado y rental_id)
- Cobra/Libera: Dependiendo del monto final cobra extra o libera el monto previo sobrante
- Usa el rental_id para evitar cobros extra. Si ya esta en su memoria no cobra de vuelta

```rust
struct PaymentService {
    transactions: HashMap<TransactionId, Transaction>,
    cards: HashMap<CardToken, Balance>,
}

struct Transaction {
    card_token: String,
    amount_cents: u32,
    status: TransactionStatus,
}

enum TransactionStatus {
    PreAuthorized,
    Commited,
    Captured,
    RolledBack,
}
```

#### Mensajes que recibe

| Mensaje | Payload | Reacción |
|----------|----------|-----------|
| `PreparePayment` | `{ transaction_id, card_token, amount_cents }` | Si la tarjeta posee fondos suficientes, reserva el monto, registra la transacción como `PreAuthorized` y responde `VoteCommit`; en caso contrario responde `VoteAbort`. |
| `CommitPayment` | `{ transaction_id }` | Si la transacción estaba `PreAuthorized`, actualiza su estado a `Commited`. |
| `RollbackPayment` | `{ transaction_id }` | Si la transacción estaba `PreAuthorized`, reintegra el monto a la tarjeta y cambia el estado a `RolledBack`. |
| `CapturePayment` | `{ transaction_id }` | Si la transacción estaba `Commited`, marca el cobro como definitivo cambiando el estado a `Captured`. |

#### Mensajes que envía

| Mensaje | Destino | Payload |
|----------|----------|----------|
| `VoteCommit` | Station | `{ transaction_id }` - Indica que la preautorización fue exitosa y la transacción puede continuar. |
| `VoteAbort` | Station | `{ transaction_id }` - Indica que la transacción debe abortarse por falta de fondos o estado inválido. |

#### Protocolo de transporte

TCP (stream sockets) para toda comunicación con las `Stations` y otros procesos del sistema.

#### Casos de interés

- **Preautorización exitosa:** recibe `PreparePayment` -> reserva temporalmente los fondos -> registra la transacción -> responde `VoteCommit`.
- **Fondos insuficientes:** recibe `PreparePayment` -> detecta saldo insuficiente -> responde `VoteAbort`.
- **Commit del alquiler:** recibe `CommitPayment` -> actualiza el estado a `Commited`.
- **Abort de la transacción:** recibe `RollbackPayment` -> reintegra los fondos reservados -> actualiza el estado a `RolledBack`.
- **Captura definitiva del pago:** recibe `CapturePayment` -> actualiza el estado a `Captured`.
- **Reintentos de mensajes:** si recibe nuevamente un `PreparePayment` para una transacción ya registrada en estado `PreAuthorized` o `Commited`, responde nuevamente `VoteCommit`, evitando inconsistencias por retransmisiones.
- **Prevención de cobros duplicados:** el uso de `transaction_id` permite identificar operaciones ya procesadas e impedir que una misma transacción sea ejecutada múltiples veces.

#### Estados de una transacción

```text
PreparePayment
        │
        ▼
PreAuthorized
    ├── CommitPayment ─────► Commited ─── CapturePayment ───► Captured
    │
    └── RollbackPayment ─────────────────────────────────────► RolledBack
```

---

## Flujos principales

### Flujo 1 — Alquiler (caso feliz)

```
App  ──RentRequest──────────────────►  Station
     ◄── [2PC interno: Prepare / Vote / Commit] ──►  Actor de Pago
App  ◄──RentConfirmed────────────────  Station
                      Station  ──StationStatus──►  CentralServer líder
                                       líder  ──ReplicaSync──►  Réplicas
```

### Flujo 2 — Devolución (caso feliz)

```
App  ──ReturnRequest────────────────►  Station
App  ◄──ReturnConfirmed──────────────  Station
                      Station  ──StationStatus──►  CentralServer líder
                                       líder  ──ReplicaSync──►  Réplicas
```

### Flujo 3 — Consulta de estaciones cercanas

```
App  ──NearbyQuery──►  CentralServer
App  ◄──NearbyResponse──  CentralServer
```

### Flujo 4 — Caída del líder y reconexión
 
```
CentralServer líder deja de enviar Heartbeat
CentralServer réplica_2 detecta timeout
réplica_2  ──Election(id=2)──►  réplica_3
réplica_2  ──Election(id=2)──►  réplica_4

réplica_3  ──Ok──────────────►  réplica_2   (R3 cancela la elección de R2)
réplica_4  ──Ok──────────────►  réplica_2   (R4 cancela la elección de R2)

réplica_3  ──Election(id=3)──►  réplica_4   (R3 inicia su propia elección)
réplica_4  ──Ok──────────────►  réplica_3   (R4 cancela la elección de R3)

réplica_4  ──Election(id=4)──►  (nadie con ID mayor responde)
réplica_4 se proclama líder
réplica_4  ──Coordinator(id=4, addr)──►  réplica_2
réplica_4  ──Coordinator(id=4, addr)──►  réplica_3

Station intenta reportar estado usando la última IP conocida:
Station  ──StationUpdate──►  réplica_2  (ex líder u otro peer al azar)
Station  ◄──NotLeader(leader_addr=réplica_4)──  réplica_2
Station  ──StationUpdate──►  réplica_4  (actualización exitosa)
 
App intenta buscar bicicletas en un nodo que resultó ser el nuevo líder:
App  ──NearbyQuery──►  réplica_4
App  ◄──NotReplica(replica_addr=réplica_2)──  réplica_4
App  ──NearbyQuery──►  réplica_2  (consulta exitosa)
```
 
---

## Manejo de errores y caídas

| Escenario                  | Comportamiento                                                                                       |
|----------------------------|------------------------------------------------------------------------------------------------------|
| **Crash de la App**        | La estación tiene timeout en el socket; si la App no confirma el retiro, el slot se libera automáticamente luego de `x` segundos (configurable) |
| **Caída del CentralServer**| Las estaciones siguen funcionando. Los usuarios no pueden buscar nuevas estaciones pero pueden interactuar con las que ya conocen |
| **Fallo en pago**          | La transacción se marca como `pending` para ser reintentada más adelante                             |
| **Muerte de Proceso Station** | La información actual se guardará en disco para mantener el último estado antes de la caída, para una posterior recuperación

---

## Operación offline

### App sin señal — consulta de disponibilidad

Usa `cached_stations` (última `NearbyResponse` recibida).

### Pérdida de conexión con CentralServer (App o Station)

- **Alquiler**: se permite si la App ya tiene la IP de la estación en caché. La estación guarda el evento localmente y actualiza su estado.
- **Sincronización**: cuando la conexión se recupera, la estación envía un `BatchUpdate` al servidor para regularizar los estados.

---

## Algoritmos de concurrencia distribuida

### Elección de líder — Bully

Para evitar que `CentralServer` sea un único punto de falla, se mantiene una cantidad de réplicas constantes del servidor ejecutándose de forma independiente.

**Detección de caída:** el líder envía un `Hearbeat` periódico a todas las réplicas. Si una réplica no recibe el hearbeat tras varios intervalos, inicia la elección.

**Algoritmo**:
1. El nodo que detecta la caída envía `Election` a todos los nodos con ID mayor.
2. Si nadie responde -> se proclama líder, anuncia `Coordinator` con su dirección a todos.
3. Si alguien responde `Ok` -> ese nodo toma el control y repite el proceso hacia arriba.
4. El nodo con el ID más alto que esté activo siempre gana.

**Reconexión:** al recibir `Coordinator`, cada réplica actualiza su `leader_addr`. Por el lado de los clientes (Station o App), mantienen localmente una lista de direcciones IP de los nodos del servidor. Si fallan al conectarse con un nodo, intentan con el siguiente de su lista. Si logran conectarse pero el nodo no tiene el rol adecuado para procesar el mensaje, el servidor responderá con `NotLeader` o `NotReplica`, indicándole al cliente la dirección IP correcta a la cual debe reconectarse.


### Persistencia y Recuperación del Estado ante Caída del Líder

Durante la operación normal, el `CentralServer` líder replica de manera asíncrona la tabla global de estados (`station_table`) hacia las réplicas mediante el mensaje `ReplicaSync`. No obstante, si el líder sufre un crash inesperado, existe una pequeña ventana de tiempo en la cual los cambios de estado o transacciones recientes de las estaciones podrían no haber sido replicados, generando una pérdida potencial de información en los nodos restantes.

Para solucionar esta inconsistencia tras la ejecución del algoritmo de Bully y la proclamación de un nuevo líder mediante el mensaje `Coordinator`, se implementa un **Protocolo de Reconciliación Activa**. El nuevo líder no asume que su copia local de la base de datos refleja la realidad de la ciudad; en su lugar, reconstruye el estado del sistema consultando directamente a las fuentes de la verdad: las estaciones distribuidas.

#### Protocolo de Reconciliación Paso a Paso:

1. **Detección y Elección:** Al caer el líder viejo, las réplicas ejecutan la elección por Bully. El nodo ganador se proclama enviando el mensaje `Coordinator(id, addr)` a sus peers.
2. **Broadcast de Solicitud de Estado (`StateRequest`):** Inmediatamente después de asumir, el nuevo líder abre conexiones TCP y envía un mensaje de broadcast de tipo `StateRequest` a todas las direcciones de `Stations` conocidas en su configuración inicial o historial persistido.
3. **Actualización de Redirección:** Al recibir este mensaje, las estaciones actualizan localmente la dirección IP del líder actual, asegurando que los futuros mensajes `StationUpdate` no sean rechazados.
4. **Respuesta de Sincronización Activa:** Cada estación recopila su estado físico real actual y lee sus archivos de persistencia local (`pending_rents.json` y `pending_charges.json`). Luego, responde al nuevo líder con un payload consolidado que incluye:
   * El estado actual e invariante de cada uno de sus slots físicos.
   * Los alquileres locales generados en modo offline que aún no habían sido reportados.
   * Las devoluciones locales pendientes de procesamiento o sincronización diferida.
5. **Consolidación en el Servidor:** El nuevo líder procesa y consolida todas las respuestas de las estaciones. Una vez que la base de datos centralizada vuelve a reflejar fielmente el estado real de la red, el líder reanuda el envío periódico de `Heartbeats` y habilita la sincronización asíncrona hacia las réplicas (`ReplicaSync`), garantizando la consistencia eventual de todo el sistema distribuido.


**Roles:**

```
Station_1  ──StationUpdate──►  CentralServer_líder
Station_2  ──StationUpdate──►  CentralServer_líder
                               CentralServer_líder  ──ReplicaSync──►  CentralServer_2
                               CentralServer_líder  ──ReplicaSync──►  CentralServer_3

App_1  ──NearbyQuery──►  CentralServer_2  (réplica)
App_2  ──NearbyQuery──►  CentralServer_3  (réplica)
```
Ver detalle en [Diagrama UML](#diagrama-de-elección-de-líder-bully)

---

### Transacciones de alquiler — 2PC

Para garantizar el correcto flujo en el alquiler de las bicicletas, previniendo inconsistencias como el doble alquiler de una bicicleta, alquileres sin fondos autorizados, o múltiples retiros simultáneos por parte del mismo cliente, se recurre a un protocolo de compromiso en dos fases (2PC).

**Identificador Único Global (`rental_id`)**: Al iniciarse la transacción, la estación genera un identificador único definido bajo la regla: `String rental_id = bike_id + user_id + timestamp`. Al componerse de estas variables concurrentes, posee unicidad global absoluta y elimina la dependencia de un servidor centralizado de sincronización.

| Rol           | Participante               |
|---------------|----------------------------|
| Coordinador   | `Station Actor`  |
| Participante 1| App del usuario  |
| Participante 2| Proceso `PaymentService` |

**Fase 1 — Prepare:**
La estación muta el estado del slot físico a `ReservedForRent` y genera el `rental_id`. Acto seguido, envía de forma paralela un mensaje de `Prepare` a:
- La **App**: Valida que la terminal del usuario continúe en línea y no retenga ninguna otra transacción o reserva activa en memoria.
- El **PaymentService**: Comprueba los fondos de la tarjeta del cliente y efectúa de manera formal la preautorización del monto de seguridad asociado al `rental_id`.

**Fase 2 — Fase de Voto y Commit / Abort:**
- **Voto Positivo**: Si la App y el `PaymentService` retornan en sus respuestas el flag `Vote_Commit` (lo que certifica retención exitosa de fondos y usuario habilitado), la transacción avanza.
- **Voto Negativo o Timeout**: Si cualquiera de las partes emite un `Vote_Abort` o el socket de comunicación experimenta un timeout (ej. desconexión abrupta de la App), la estación aborta. Revierte de forma inmediata el slot a `Occupied` y envía las alertas de rollback pertinentes: si el problema provino de la App, se le ordena al `PaymentService` desreservar el monto; si el fallo fue del banco, se le instruye rollback a la App para limpiar su memoria.
- **Commit Definitivo**: Tras el consenso exitoso, la estación despacha el mensaje definitivo de `Commit` a ambos procesos. Se destraba físicamente el slot de la bicicleta, la App asienta el registro en `ActiveRental`, y la estación notifica en paralelo al `CentralServer` líder que la bicicleta ha sido retirada.

---
**Caso offline**

Station (Coordinador) genera el rental_id e intenta abrir un socket TCP con el PaymentService para solicitar la preautorización. Al no obtener respuesta, la estación detecta que está offline.
Modo Optimista: la estación no aborta la transacción. Decide asumir el riesgo y degrada el 2PC, eliminando temporalmente al PaymentService de la votación en tiempo real.
Guarda de forma inmediata y síncrona los datos del alquiler (rental_id, user_id, card_token, bike_id y timestamp) en un archivo local llamado pending_rents.json
Responde RentConfirmed a la App enviándole el rental_id generado

BatchUpdate: En cuanto detecta que la conexión TCP con el exterior se ha restablecido, lee el archivo pending_rents.json y envía en lote todas las preautorizaciones pendientes al PaymentService
Envía un StationStatus consolidado al CentralServer líder para informarle qué bicicletas se retiraron mientras estuvo desconectada

---

**Post-commit:**
- Si está **online**: envía `StationStatus` inmediatamente al líder del `CentralServer`.
- Si está **offline**: retiene el evento y lo incluye en el `BatchUpdate` al recuperar conectividad.

---

## Guía de Ejecución y Comandos

### Requisitos Previos
Tener instalado el *toolchain* oficial de Rust (`cargo` y `rustc` en versión estable).

### 1. Compilación del Proyecto
Para compilar todos los binarios que componen el ecosistema BiciRed de forma centralizada, desde la raíz del repositorio ejecutar:
```bash
cargo build --release
```

### 2. Ejecución del Servidor Central (Clúster de Servidores)
El ejecutable requiere pasarle por argumento el ID único de nodo, la dirección IP/puerto local donde escuchará, y el archivo de ruteo CSV que define los miembros de la red:
```bash
# Firma: cargo run --bin central_server <server_id> <ip_servidor> <servers.csv>
# Ejemplo para levantar un clúster local de 3 nodos independientes:
cargo run --bin central_server 1 127.0.0.1:8000 servers.csv
cargo run --bin central_server 2 127.0.0.1:8001 servers.csv
cargo run --bin central_server 3 127.0.0.1:8002 servers.csv
```

### 3. Ejecución del banco (Payment Service)
Para iniciar el servicio de cobros simulado; se requiere pasarle la dirección IP/puerto local donde escuchará, y el archivo de ruteo CSV donde se definen las tarjetas (Token y salgo)
```bash
# Firma: cargo run --bin payment_service <ip_banco> tarjetas.csv
# Ejemplo:
cargo run --bin payment_service 127.0.0.1:8080 tarjetas.csv
```

### 4. Ejecución de las estaciones
Para iniciar el servicio de estaciones; se requiere pasarle por argumento el ID único de la estación, los archivos CSV donde se definen los servidores y estaciones (para obteners coordenas, slots, etc) y la dirección IP/puerto local donde escucha el banco.
```bash
# Firma: cargo run --bin station <id> servers.csv stations.csv <ip_banco>
# Ejemplo:
cargo run --bin station 1 servers.csv stations.csv 127.0.0.1:8080
```

### 5. Ejecución del banco (Payment Service)
Para iniciar la aplicación del cliente, se requiere pasarle por argumento el ID único del cliente (como si fuese el nomnbre de usuario), y el archivo CSV donde se definen los servidores.

```bash
# Firma: cargo run --bin app <id> servers.csv
# Ejemplo:
cargo run --bin app 1 servers.csv
```

### 6. Ejecución de Tests
Para ejecutar los tests armados.

```bash
cargo test
```

---

## Diagramas

### Diagrama de clases

![Diagrama de clases](doc/class_diagram.png)

### Diagrama de threads

![Diagrama de threads](doc/thread_diagram.png)

### Diagrama de flujo alquiler

![Diagrama de flujo](doc/flow_diagram.png)

### Diagrama de elección de líder (Bully)

![Diagrama de elección de líder](doc/alg_bully.png)

### Diagrama de Recuperacion de Informacion Post Bully

```mermaid
sequenceDiagram
    participant LV as Lider Viejo
    participant NL as Nuevo Lider
    participant ST as Estaciones
    participant RE as Replicas

    Note over LV: Crash del proceso
    Note over RE,NL: Ejecucion del algoritmo Bully

    NL->>RE: Coordinator(id, addr)

    Note over NL: Asume como nuevo lider

    NL->>ST: StateRequest

    Note over ST: Actualizan IP del nuevo lider
    Note over ST: Leen slots fisicos y archivos JSON

    ST->>NL: Estado actual + operaciones locales

    Note over NL: Reconstruccion del estado global

    NL->>RE: ReplicaSync
```

