use std::fs::File;
use std::io::{self, BufRead, BufReader};
use crate::entities::{ServerId, ServerNode};

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
                eprintln!("[WARNING] Ignorando línea por ID inválido: {}", trimmed_line);
            }
        } else {
            eprintln!("[WARNING] Ignorando línea por formato incorrecto (se esperaba ID, IP:Puerto): {}", trimmed_line);
        }
    }

    Ok(servers)
}