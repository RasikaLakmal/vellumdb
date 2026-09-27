use std::env;
use std::io::{self, Write};

use vellumdb::Db;

fn main() {
    let dir = env::args().nth(1).unwrap_or_else(|| "vellum-data".to_string());
    let mut db = match Db::open(&dir) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("failed to open {dir}: {e}");
            std::process::exit(1);
        }
    };
    let stdin = io::stdin();

    println!("vellumdb 0.1.0 (dir: {dir}, {} sstables on disk)", db.sstable_count());
    println!("commands: put <key> <value> | get <key> | delete <key> | scan | flush | exit");

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
                    Ok(Some(value)) => println!("{}", String::from_utf8_lossy(&value)),
                    Ok(None) => println!("(not found)"),
                    Err(e) => println!("error: {e}"),
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
            "scan" => {
                for (key, value) in db.iter() {
                    println!(
                        "{}={}",
                        String::from_utf8_lossy(key),
                        String::from_utf8_lossy(value)
                    );
                }
            }
            "flush" => match db.flush() {
                Ok(()) => println!("ok ({} sstables on disk)", db.sstable_count()),
                Err(e) => println!("error: {e}"),
            },
            "exit" | "quit" => break,
            other => println!("unknown command: {other}"),
        }
    }
}
