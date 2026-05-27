[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/KujF6lFv)

# 🚲 BiciRed — Alquiler de Bicicletas

El sistema modela una red de estaciones de bicicletas distribuidas por la ciudad. Cada componente corre como un proceso independiente; y la comunicación entre procesos es a través de sockets.

Dentro de cada proceso se aplica el modelo de actores: cada subsistema es un thread independiente que sólo se comunica a través de canales (usando la librería de Rust mpsc) tipados. Por lo que, no hay memoria compartida entre actores.

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
- [Flujos principales](#flujos-principales)
- [Manejo de errores y caídas](#manejo-de-errores-y-caídas)
- [Operación offline](#operación-offline)
- [Algoritmos de concurrencia distribuida](#algoritmos-de-concurrencia-distribuida)
  - [Elección de líder — Bully](#elección-de-líder--bully)
  - [Transacciones de alquiler — 2PC](#transacciones-de-alquiler--2pc)
- [Diagramas](#diagramas)

---

## Arquitectura de procesos

```
[APP] ──────────────────► [STATION]
[APP] ──────────────────► [CENTRALSERVER]
[STATION] ──────────────► [CENTRALSERVER]
```

| Proceso         | Rol                                                        |
|-----------------|------------------------------------------------------------|
| `Station`       | Gestiona slots físicos, cobra pagos                        |
| `CentralServer` | Mantiene estado global de todas las estaciones             |
| `App`           | Simula la app móvil del usuario                            |

Se pueden correr múltiples instalaciones de `CentralServer` simultáneamente. Una actúa como **líder** (recibe actualizaciones de las Stations y replica el estado a los demás nodos). Las demás **réplicas** (responden consultas de disponibilidad de las Apps). Si el líder cae, los nodos restantes eligen a uno nuevo mediante el [Algoritmo de Bully](#elección-de-líder--bully).

---

## Entidades principales

### Station

Gestiona los slots físicos de una estación. Detecta bicicletas, las bloquea y desbloquea, cobra tarifas y reporta su estado al servidor central. **Opera de forma autónoma aunque pierda conectividad.**

> **Invariante:** Una estación no debe permitir más de un alquiler simultáneo sobre el mismo `slot_id`.

#### Estado interno

```rust
struct Station {
    id: StationId,
    location: Location,
    slots: Vec<Slot>,
}

struct Slot {
    index: usize,
    state: SlotState,
}

enum SlotState {
    Empty,
    Occupied { bike_id: BikeId },
    Reserved,
}
```

#### Arquitectura interna (threads)

| Thread            | Responsabilidad                                                                 |
|-------------------|---------------------------------------------------------------------------------|
| `Acceptor`        | Escucha nuevas conexiones TCP desde la App; spawnea un `Handler` por conexión   |
| `Handler`         | Traduce mensajes TCP a mensajes `mpsc` entre la App y el Station Actor          |
| `Station Actor`   | Único dueño de `Vec<Slot>`; único que modifica el estado de los slots           |
| `Actor de pago`    | Simula el proceso de pago de forma asíncrona para no bloquear la estación       |

#### Mensajes que recibe

| Mensaje         | Payload                                    | Reacción                                                                                      |
|-----------------|--------------------------------------------|-----------------------------------------------------------------------------------------------|
| `RentRequest`   | `{ user_id, slot_index, card_token }`      | Si slot ocupado → desbloquea bicicleta, responde `RentConfirmed`; si no → `RentRejected`      |
| `ReturnRequest` | `{ user_id, bike_id, slot_index, started_at }` | Si slot vacío → bloquea bicicleta, calcula cargo proporcional al tiempo, responde `ReturnConfirmed`; si no → `ReturnRejected` |
| `NotLeader` | CentralServer | `{ leader_addr: SocketAddr }` - Actualiza su dirección interna localmente del líder y reintenta enviar su `StationUpdate` a la nueva IP. |

#### Mensajes que envía

| Mensaje          | Destino         | Payload                                              |
|------------------|-----------------|------------------------------------------------------|
| `RentConfirmed`  | App             | `{ bike_id, pre_auth_cents, timestamp_secs }`        |
| `RentRejected`   | App             | `{ reason: String }`                                 |
| `ReturnConfirmed`| App             | `{ charged_cents, timestamp_secs }`                  |
| `ReturnRejected` | App             | `{ reason: String }`                                 |
| `StationStatus`  | CentralServer   | `{ station_id, location, available_bikes, free_slots, timestamp_secs }` |
| `Prepare`  | App   | `{}` - Verifica que la App sigue activa y sin reserva activa |
| `PreparePayment`  | Actor de Pago | `{ card_token }` |
| `Commit`  | Actor de Pago   | `{}` - confirma que el cobro una vez que ambos votan `Vote_Commit` |


#### Protocolo de transporte

TCP (stream sockets) con la App y con el CentralServer. `mpsc` internamenmte entre threads.

#### Casos de interés

- **Feliz (alquiler):** ver [Flujo 1 - 2PC](#flujo-1--alquiler-caso-feliz)
- **Feliz (devolución):** App envía `ReturnRequest` -> `Station Actor` bloquea slot, calcula cargo -> responde `ReturnConfirmed`
- **App muere durante 2PC:** Timeout en el socket -> transacción abortada, slot vuelve a `Occupied`.
- **Pago rechazado:** `Actor de Pago` responde `Vote_Abort` -> `Station Actor` aborta -> `RentRejected`.
- **Sin conectividad con CentralServer:** opera localmente, guarda la información recibida en un archivo local (CSV o JSON), y la envía al recuperar la red.

---

### CentralServer

Mantiene una vista actualizada del estado de todas las estaciones y responde consultas de disponibilidad. Recibe actualizaciones periódicas de las estaciones sin bloquearlas.

```rust
struct CentralServer {
    id: ServerId,
    leader_id: Option<ServerId>,
    station_table: HashMap<StationId, StationStatus>,
    peers: Vec<(ServerId, SocketAddr)>,
}

struct StationStatus {
    station_id: StationId,
    location: Location,
    available_bikes: u8,
    free_slots: u8,
    updated_at: Instant,
}
```

#### Arquitectura interna (threads)
 
| Thread | Responsabilidad |
|---|---|
| `Acceptor` | Escucha el puerto TCP. Por cada conexión entrante (de una Station o de una App) spawnea un Handler. |
| `Handler` (uno por conexión) | Maneja la comunicación con el cliente conectado. Traduce mensajes TCP a mensajes mpsc para el Server Actor. |
| `Server Actor` | Único dueño de `HashMap<StationId, StationStatus>`. Actualiza entradas y responde consultas.|
| `ElectionActor` | Detecta timeout de la vida del líder e inicia la elección por Bully. Procesa mensajes `Election` y `Coordinator`. |

#### Mensajes que recibe

| Mensaje         | Payload                                         | Reacción                                                              |
|-----------------|-------------------------------------------------|-----------------------------------------------------------------------|
| `StationUpdate` | `{ station_id, location, bikes, slots, ts }`    | Actualiza entrada en `station_table` si el timestamp es más reciente  |
| `NearbyQuery`   | `{ location, radius_km }`                       | Filtra tabla por distancia, responde `NearbyResponse`                 |
| `ReplicaSync` | `{ station_table }` | Reemplaza tabla local con la del líder (solo réplicas) |
| `Heartbeat` | `{}` | Confirma que el líder sigue activo; resetea el timeout |
| `Election` | `{ candidate_id }` | Participa en el Algoritmo de Bully |
| `Coordinator` | `{ leader_id }` | Actualiza `leader_id` local; el emisor se proclama líder |

#### Mensajes que envía

| Mensaje          | Destino | Payload                                                         |
|------------------|---------|-----------------------------------------------------------------|
| `NearbyResponse` | App     | `Vec<StationSummary { id, location, available_bikes, free_slots }>` |
| `ReplicaSync` | Otros CentralServer | `{ station_table }` |
| `Heartbeat` | Réplicas (solo líder) | `{}` |
| `Election` | Nodos con ID mayor | `{ candidate_id }` |
| `Ok` | Nodo que inició elección | `{}` — "yo tomo el control" |
| `Coordinator` | Todos los nodos | `{ leader_id }` |
| `NotLeader` | Station | `{ leader_addr: SocketAddr }` - Rechaza un `StationUpdate` (porque este nodo es réplica) y redirige a la Station al líder actual. |
| `NotReplica` | App | `{ replica_addr: SocketAddr }` - Rechaza un `NearbyQuery` (porque este nodo es líder y delega la carga) y redirige a la App hacia una réplica. |

#### Protocolo de transporte

TCP (stream sockets) para toda comunicación.

#### Casos de interés

- **Feliz:** Station envía `StationUpdate` al líder -> líder actualiza tabla y envía `ReplicaSync` -> App consulta réplica -> `NearbyResponse`
- **Líder cae:** réplica detecta timeout de `Heartbeat` -> inicia elección Bully -> nuevo líder anuncia `Coordinator` con su dirección

---

### App

Simula la app móvil del usuario. Se conecta directamente a la estación para alquilar/devolver y al servidor central para consultar disponibilidad. Mantiene caché local para operación offline.

```rust
struct App {
    user_id: UserId,
    current_rental: Option<ActiveRental>,
    cached_stations: Vec<StationSummary>,
}

struct ActiveRental {
    bike_id: BikeId,
    started_at_secs: u64,
    pre_auth_cents: u32,
    retired_at_station: StationId,
}
```

#### Mensajes que recibe

| Mensaje          | Origen        | Payload                                                         |
|------------------|---------------|-----------------------------------------------------------------|
| `RentConfirmed`  | Station       | `{ bike_id, pre_auth_cents, timestamp_secs }`                   |
| `RentRejected`   | Station       | `{ reason: String }`                                            |
| `ReturnConfirmed`| Station       | `{ charged_cents, timestamp_secs }`                             |
| `ReturnRejected` | Station       | `{ reason: String }`                                            |
| `NearbyResponse` | CentralServer | `Vec<StationSummary { id, location, available_bikes, free_slots }>` |
| `Prepare` | Station | `{}` - Station verifica que la App sigue activa y sin reserva |
| `NotReplica` | CentralServer | `{ replica_addr: SocketAddr }` - Actualiza la IP del servidor en uso localmente y reintenta el `NearbyQuery` a la nueva dirección recibida. |

#### Mensajes que envía

| Mensaje         | Destino       | Payload                                          |
|-----------------|---------------|--------------------------------------------------|
| `NearbyQuery`   | CentralServer | `{ location, radius_km }`                        |
| `RentRequest`   | Station       | `{ user_id, slot_index, card_token }`            |
| `ReturnRequest` | Station       | `{ user_id, bike_id, slot_index, started_at }`   |
| `Vote_Commit` | Station | `{}` - App activa y sin reserva activa |
| `Vote_Abort` | Station | `{}` - App con reserva activa o error |

#### Protocolo de transporte

TCP (stream sockets). No escucha ningún puerto, solo incia conexiones.

#### Casos de interés

- **Sin señal consultado disponibilidad:** usa `cached_stations` (última `NearbyResponse` recibida).
- **Sin señal alquilando o devolviendo:** la App ya tiene la IP de la Station guardada en `cached_stations`; conecta directamente sin pasar por el CentralServer.
- **Líder caído:** reintenta con el siguiente peer conocido de su lidta hasta encontrar uno activo.

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

Para garantizar el flujo en el alquiler de las bicicleta, evitando inconsistencias locales y distribuidas, en el que se alquile dos veces la misma bicicleta, se alquile una bicicleta sin pagar o un usuario alquile dos bicicletas al mismo tiempo, se recurrirá al uso de transacciones.
El algoritmo de commit en dos fases permitirá que una bicicleta se libere sólamente si el pago fue autorizado


| Rol           | Participante               |
|---------------|----------------------------|
| Coordinador   | `Station Actor`            |
| Participante 1| App del usuario            |
| Participante 2| Hilo de pago               |

**Fase 1 — Prepare:** la estación envía `Prepare` a ambos participantes:
- Al **actor de pago**: verifica fondos y pre-autoriza el token de tarjeta.
- A la **App**: verifica que sigue activa y sin reserva en curso.

**Fase 2 — Commit / Abort:**

| Condición                                    | Resultado              |
|----------------------------------------------|------------------------|
| Pago aprobado + App responde `Vote_Commit`    | `Commit` → reserva guardada |
| Pago rechazado                               | `Abort`                |
| App no responde (caída)                      | `Abort`                |
| App tiene reserva activa                     | `Abort`                |

**Post-commit:**
- Si está **online**: envía `StationStatus` inmediatamente al líder del `CentralServer`.
- Si está **offline**: retiene el evento y lo incluye en el `BatchUpdate` al recuperar conectividad.

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

