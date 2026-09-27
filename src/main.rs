use std::io::{self, Write};

use vellumdb::Db;

fn main() {
    let mut db = Db::new();
    let stdin = io::stdin();

    println!("vellumdb 0.1.0 (in-memory, no persistence yet)");
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
                        db.put(key.as_bytes().to_vec(), value.as_bytes().to_vec());
                        println!("ok");
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
                Some(key) => {
                    if db.delete(key.as_bytes()) {
                        println!("ok");
                    } else {
                        println!("(not found)");
                    }
                }
                None => println!("usage: delete <key>"),
            },
            "exit" | "quit" => break,
            other => println!("unknown command: {other}"),
        }
    }
}
