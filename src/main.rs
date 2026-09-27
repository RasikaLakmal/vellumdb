use std::env;
use std::io::{self, Write};

use vellumdb::Db;

fn main() {
    let wal_path = env::args().nth(1).unwrap_or_else(|| "vellum.wal".to_string());
    let mut db = match Db::open(&wal_path) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("failed to open {wal_path}: {e}");
            std::process::exit(1);
        }
    };
    let stdin = io::stdin();

    println!("vellumdb 0.1.0 (wal: {wal_path})");
    println!("commands: put <key> <value> | get <key> | delete <key> | exit");

    loop {
        print!("vellum> ");
        io::stdout().flush().ok();

        let mut line = String::new();
        if stdin.read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let mut parts = line.splitn(3, ' ');
        let cmd = parts.next().unwrap_or("");

        match cmd {
            "put" => {
                let key = parts.next();
                let value = parts.next();
                match (key, value) {
                    (Some(key), Some(value)) => {
                        match db.put(key.as_bytes().to_vec(), value.as_bytes().to_vec()) {
                            Ok(()) => println!("ok"),
                            Err(e) => println!("error: {e}"),
                        }
                    }
                    _ => println!("usage: put <key> <value>"),
                }
            }
            "get" => match parts.next() {
                Some(key) => match db.get(key.as_bytes()) {
                    Some(value) => println!("{}", String::from_utf8_lossy(value)),
                    None => println!("(not found)"),
                },
                None => println!("usage: get <key>"),
            },
            "delete" => match parts.next() {
                Some(key) => match db.delete(key.as_bytes()) {
                    Ok(true) => println!("ok"),
                    Ok(false) => println!("(not found)"),
                    Err(e) => println!("error: {e}"),
                },
                None => println!("usage: delete <key>"),
            },
            "exit" | "quit" => break,
            other => println!("unknown command: {other}"),
        }
    }
}
