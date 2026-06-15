pub mod handlers;
pub mod messages;
pub mod persistence;
pub mod sync;

use actix::prelude::*;
use common::*;
use std::collections::HashMap;
use std::sync::mpsc::Sender;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::actors::ConnectionActor;
use crate::domain::*;

#[derive(Debug, Clone)]
pub struct TransactionState {
    pub slot_index: usize,
    pub bike_id: BikeId,
    pub client_addr: Addr<ConnectionActor>,
    pub payment_voted_commit: bool,
    pub app_voted_commit: bool,
    pub started_at: SystemTime,
}

pub struct PendingValidation {
    pub msg: RequestMessage<RentRequest, ConnectionActor>,
    pub started_at: SystemTime,
}

pub struct PendingCharge {
    pub msg: RequestMessage<ReturnRequest, ConnectionActor>,
    pub started_at: SystemTime,
}

pub struct StationActor {
    pub(crate) station: Station,
    pub(crate) central_server: Option<Sender<String>>,
    pub(crate) payment_service: Option<Sender<String>>,
    pub(crate) pending_transactions: HashMap<String, TransactionState>,
    pub(crate) server_addrs: Vec<String>,
    pub(crate) my_ip: String,
    pub(crate) payment_ip: String,
    pub(crate) pending_validations: HashMap<UserId, PendingValidation>,
    pub(crate) pending_charges: HashMap<String, PendingCharge>, // rental_id -> PendingCharge
    pub(crate) active_rentals: HashMap<UserId, u64>,
}

impl StationActor {
    pub fn new(
        station: Station,
        server_addrs: Vec<String>,
        my_ip: String,
        payment_ip: String,
    ) -> Self {
        Self {
            station,
            central_server: None,
            payment_service: None,
            pending_transactions: HashMap::new(),
            server_addrs,
            my_ip,
            payment_ip,
            pending_validations: HashMap::new(),
            pending_charges: HashMap::new(),
            active_rentals: HashMap::new(),
        }
    }

    pub(crate) fn generate_rental_id(&self, bike_id: BikeId, user_id: UserId) -> String {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        format!("{}-{}-{}", bike_id, user_id, ts)
    }

    pub(crate) fn client_id_from_rental(&self, rental_id: &str) -> Option<UserId> {
        rental_id.split('-').nth(1)?.parse().ok()
    }
}

impl Actor for StationActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.start_central_connection(ctx);
        self.try_reconnect_payment(ctx);

        ctx.run_interval(std::time::Duration::from_secs(5), |act, ctx| {
            act.try_reconnect_payment(ctx);
            act.process_batch_updates();
            act.abort_expired_transactions();
            act.check_unreturned_bikes();
            if let Some(ref sender) = act.central_server {
                let _ = sender.send(format!("PING|{}\n", act.station.id));
            }
        });
    }
}
