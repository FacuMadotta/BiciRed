identificacion alquileres
introducir un identificador único global para el viaje/alquiler: el rental_id
String rental_id = bike_id + user_id + timestamp. De esta manera será único y no deberá depender de sincronización para conseguir la unicidad

Pagos independientes
Se crea el proceso PaymentService que funcionaría como un banco mediante conexiones TCP ya sea con el server o con las estaciones
- Preautoriza montos (asocia tarjeta, monto preautorizado y rental_id)
- Cobra/Libera: Dependiendo del monto final cobra extra o libera el monto previo sobrante
- Usa el rental_id para evitar cobros extra. Si ya esta en su memoria no cobra de vuelta

2PC
Station (actuando como Coordinador) cambia el estado del slot a ReservedForRent y genera un rental_id. Luego envía un mensaje Prepare en paralelo a:
- La App: Para validar que el usuario sigue conectado y no tiene otro alquiler activo.
- El PaymentService: Para validar fondos y realizar la preautorización del monto de seguridad.
Fase de voto:
Si la App responde Vote_Commit y el PaymentService responde Vote_Commit (fondos retenidos con éxito), la estación avanza.
Si alguno responde Vote_Abort o hay un timeout (ej. la App se desconectó), la estación aborta, libera el slot a Occupied y cancela la transacción. Si el problema es de app le envia rollback al PaymentService que desreserve el monto. Si el problema es en el payment, le envia a la app rollback para que no establezca ninguna reserva en memoria

Commit: La Station envía el mensaje definitivo de Commit a ambos procesos. Entrega la bicicleta, la App guarda su ActiveRental, y la estación notifica al CentralServer líder que la bicicleta ya no está en el slot (guarda el mensaje para despues si esta offline).

Caso offline
Station (Coordinador) genera el rental_id e intenta abrir un socket TCP con el PaymentService para solicitar la preautorización. Al no obtener respuesta, la estación detecta que está offline.
Modo Optimista: la estación no aborta la transacción. Decide asumir el riesgo y degrada el 2PC, eliminando temporalmente al PaymentService de la votación en tiempo real.
Guarda de forma inmediata y síncrona los datos del alquiler (rental_id, user_id, card_token, bike_id y timestamp) en un archivo local llamado pending_rents.json
Responde RentConfirmed a la App enviándole el rental_id generado

BatchUpdate: En cuanto detecta que la conexión TCP con el exterior se ha restablecido, lee el archivo pending_rents.json y envía en lote todas las preautorizaciones pendientes al PaymentService
Envía un StationStatus consolidado al CentralServer líder para informarle qué bicicletas se retiraron mientras estuvo desconectada

Devolucion
Usuario inserta la bici en el slot 
         │
         ▼
Station calcula tiempo transcurrido y costo proporcional
         │
   ┌─────┴────────────────────────────────────────┐
   │                                              │
   ▼ (¿Está ONLINE con PaymentService?)           ▼ (¿Está OFFLINE?)
[ Escenario Online ]                           [ Escenario Offline ]
1. Station solicita "Capture" al              1. Station guarda el cobro en
   PaymentService usando el `rental_id`.          `pending_charges.json`.
2. El pago se procesa inmediatamente.          2. Libera al usuario en la App.
3. Se notifica éxito a la App.                 3. Un hilo interno (`BatchUpdate`)
                                                  reintenta el cobro cuando vuelve la red.

Si previamente retiro una bicicleta en una estacion offline y al enviar Capture es rechazado el pago, entra en la lista negra del central server

Bicicletas Robadas
El central server detectará el robo mediante una verificación periódica de los alquileres activos. Si un rental_id pasa un umbral de tiempo (constante STOLEN_TIME) el servidor marcará el viaje con estado Stolen.
El Central server envia un mensaje al Payment Service para cobrar el monto base.
El usuario es agregado a un listado que no le permitirá recibir actualizaciones del server ni retirar bicicletas

Heartbeats
Se podrian minimizar la cantidad de heartbeats, en lugar de hacerlo constantemente, cuando se supere un umbral de tiempo sin transmitir actualizaciones de estado. Disminuirá la cantidad de mensajes, sin embargo no encontramos forma de conocer el estado sin su envío.

Recuperación del lider
crashea el Líder Viejo -> Elección Bully -> nuevo lider
[Nuevo Líder] ── Establecimiento de conexion y solicitud de actualización ──► [Todas las Stations]
[Stations] ── Estado actual + Alquileres locales + Devoluciones que no pudieron enviar ──► [Nuevo Líder]

Al asumir el rol a través del mensaje Coordinator, el nuevo líder envía un mensaje de broadcast de tipo StateRequest. Asi las estaciones conocen al nuevo lider y este se informa de lo que necesita.


Cambios en estructuras:

struct CentralServer {
    id: ServerId,
    leader_id: Option<ServerId>,
    station_table: HashMap<StationId, StationStatus>,
    blacklist: HashSet<UserId>, // Control global de morosos/robos
    peers: Vec<(ServerId, SocketAddr)>,
}

struct App {
    user_id: UserId,
    current_rental: Option<ActiveRental>,
    cached_stations: Vec<StationSummary>,
    is_blocked: bool, // Bloquea alquileres si el servidor lo indica
}