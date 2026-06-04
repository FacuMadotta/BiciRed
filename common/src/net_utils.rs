use actix::prelude::*;
use std::net::TcpStream;
use std::io::{self, Read};

pub struct SocketStream(pub TcpStream);

impl futures::Stream for SocketStream {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(mut self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let mut buf = [0; 1024];
        match self.0.read(&mut buf) {
            Ok(0) => std::task::Poll::Ready(None), // Conexión cerrada
            Ok(n) => std::task::Poll::Ready(Some(Ok(buf[..n].to_vec()))), // Lectura de mensaje
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => std::task::Poll::Pending, // No hay datos disponibles, esperar
            Err(e) => std::task::Poll::Ready(Some(Err(e))), // Error de lectura
        }
    }
}