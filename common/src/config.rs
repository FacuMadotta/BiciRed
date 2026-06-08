use crate::entities::{Location, StationId, StationStatus};
use crate::entities::{ServerId, ServerNode};
use std::fs::File;
use std::io::{self, BufRead, BufReader};

pub fn load_servers_csv(filepath: &str) -> io::Result<Vec<ServerNode>> {
    let file = File::open(filepath)?;
    let reader = BufReader::new(file);
    let mut servers = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let trimmed_line = line.trim();

        if trimmed_line.is_empty() || trimmed_line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = trimmed_line.split(',').collect();

        if parts.len() == 2 {
            if let Ok(id) = parts[0].trim().parse::<ServerId>() {
                let addr = parts[1].trim().to_string();
                servers.push(ServerNode { id, addr });
            } else {
                eprintln!(
                    "[WARNING] Ignorando línea por ID inválido: {}",
                    trimmed_line
                );
            }
        } else {
            eprintln!(
                "[WARNING] Ignorando línea por formato incorrecto (se esperaba ID, IP:Puerto): {}",
                trimmed_line
            );
        }
    }

    Ok(servers)
}

pub fn load_stations_csv(filepath: &str) -> io::Result<Vec<StationStatus>> {
    let file = File::open(filepath)?;
    let reader = BufReader::new(file);
    let mut stations = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() || line.starts_with('#') {
            continue;
        }

        let parts: Vec<&str> = line.split(',').collect();

        if parts.len() == 6 {
            let id = parts[0].trim().parse::<StationId>().unwrap_or(0);
            let slots = parts[2].trim().parse::<u8>().unwrap_or(0);
            let bikes = parts[3].trim().parse::<u8>().unwrap_or(0);
            let x = parts[4].trim().parse::<f64>().unwrap_or(0.0);
            let y = parts[5].trim().parse::<f64>().unwrap_or(0.0);

            stations.push(StationStatus {
                station_id: id,
                location: Location { x, y },
                available_bikes: bikes,
                free_slots: slots,
                updated_at_secs: 0,
            });
        }
    }
    Ok(stations)
}
