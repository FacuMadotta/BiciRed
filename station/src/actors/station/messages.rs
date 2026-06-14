use actix::prelude::*;

#[derive(Message)]
#[rtype(result = "()")]
pub struct CentralServerConnected {
    pub sender: std::sync::mpsc::Sender<String>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CentralServerDisconnected;

#[derive(Message)]
#[rtype(result = "()")]
pub struct PaymentServiceDisconnected;
