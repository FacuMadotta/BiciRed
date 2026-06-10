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

pub fn load_stations_csv(
    filepath: &str,
    target_id: StationId,
) -> Option<(String, usize, usize, Location)> {
    let content = std::fs::read_to_string(filepath).ok()?;

    for line in content.lines() {
        let clean_line = line.trim();
        if clean_line.starts_with('#') || clean_line.is_empty() {
            continue;
        }

        let parts: Vec<&str> = line.split(',').collect();

        if parts.len() >= 6 {
            if let Ok(id) = parts[0].parse::<StationId>() {
                if id == target_id {
                    let ip = parts[1].to_string();
                    let slots = parts[2].trim().parse().unwrap_or(10);
                    let bikes = parts[3].trim().parse().unwrap_or(5);
                    let x: f64 = parts[4].trim().parse().unwrap_or(0.0);
                    let y: f64 = parts[5].trim().parse().unwrap_or(0.0);
                    return Some((ip, slots, bikes, Location { x, y }));
                }
            }
        }
    }
    None
}
