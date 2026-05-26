use std::io::{self, Write, Read};
use std::net::TcpStream;


fn query_central() {

    // Ordeno servers conocidos de mas cercana a mas lejana

    for i in 0..len(servers) {
        // let addr = servers[i].address;
        match TcpStream::connect(addr) {
        Ok(mut s) => {

            // Envio NearByQuery con Location y radio

            let mut buf = [0u8; 1024];
            match s.read(&mut buf) {
                Ok(n) if n > 0 => {
                    // Mostrar estaciones cercanas
                    break;
                },
                _ => println!("No response from central"),
            }
        }
        Err(e) => println!("Failed to connect: {}", e),
    }
    }
    
}

fn rent_station(addr: &str) {
    match TcpStream::connect(addr) {
        Ok(mut s) => {

            // Enviar datos de alquiler

            let mut buf = [0u8; 1024];
            match s.read(&mut buf) {
                Ok(n) if n > 0 => {
                    // if rent confirmed, mostrar datos de alquiler
                    // Agregamos la bicicleta al usuario

                    // if rent rejected, mostrar motivo
                },
                _ => println!("No response from station"),
            }
        }
        Err(e) => println!("Failed to connect: {}", e),
    }
}

fn return_station(addr: &str) {
    match TcpStream::connect(addr) {
        Ok(mut s) => {

            // Enviar datos de devolución

            let mut buf = [0u8; 1024];
            match s.read(&mut buf) {
                Ok(n) if n > 0 => {
                    // if return confirmed, mostrar datos de devolución
                    // eliminamos la bicicleta del usuario

                    // if return rejected, mostrar motivo
                },
                _ => println!("No response from station"),
            }
        }
        Err(e) => println!("Failed to connect: {}", e),
    }
}

fn main() {
    let mut input = String::new();
    loop {
        println!("Select: 1) Near stations 2) Rent 3) Return Bike 4) Quit");
        let _ = io::stdout().flush();
        input.clear();
        if io::stdin().read_line(&mut input).is_err() { 
            println!("Error reading input");
            break; 
        }
        match input.trim() {
            "1" => query_central(),
            "2" => rent_station("127.0.0.1:9000"),
            "3" => return_station("127.0.0.1:9000"),
            "4" => { println!("Session ended"); break; }
            other => println!("Unknown: {}", other),
        }
    }
}
