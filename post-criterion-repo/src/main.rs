#[macro_use]
extern crate serde_json;
extern crate chrono;
extern crate futures;
extern crate hyper;
extern crate tokio;
use std::io::Read;
use std::time::SystemTime;
use std::{env, fs, path, process};

use futures::{future, Future, Stream};
use hyper::{Body, Client, Request};
use tokio::runtime::current_thread;

fn usage() -> ! {
    println!("Usage: post-criterion <host> <dir-name>");
    process::exit(0)
}

fn main() {
    let mut runtime = current_thread::Runtime::new().unwrap();

    let mut args = env::args().skip(1);
    let host = args.next().unwrap_or_else(|| usage());
    let dir_name = args.next().unwrap_or_else(|| usage());

    let ts = chrono::DateTime::<chrono::Local>::from(SystemTime::now());
    let ts = format!("{}", ts.format("%Y-%m-%d %H:%M:%S"));

    for entry in fs::read_dir(&dir_name).unwrap() {
        let e = entry.unwrap();
        if !e.file_type().unwrap().is_dir() {
            continue;
        }
        let file_name = e.file_name().to_str().unwrap().to_owned();
        if file_name == "report" {
            continue;
        }
        let method = file_name;

        let method_dir = path::PathBuf::from(&dir_name).join(&method);
        for entry in fs::read_dir(&method_dir).unwrap() {
            let e = entry.unwrap();
            if !e.file_type().unwrap().is_dir() {
                continue;
            }
            let file_name = e.file_name().to_str().unwrap().to_owned();
            if file_name == "report" {
                continue;
            }
            let args = file_name.replace("+", "\"");

            let estimates_file = method_dir
                .join(&file_name)
                .join("new")
                .join("estimates.json");

            let mut estimates = String::new();
            if fs::File::open(&estimates_file)
                .and_then(|mut f| f.read_to_string(&mut estimates))
                .is_ok()
            {
                let j = json!({
                    "method": method,
                    "args": args,
                    "estimates": estimates,
                    "ts": ts,
                });

                let request = Request::builder()
                    .method("POST")
                    .uri(&format!("http://{}/put-result/", host))
                    .body(Body::from(j.to_string().into_bytes()))
                    .unwrap();

                let f = Client::new()
                    .request(request)
                    .and_then(|resp| {
                        let status = resp.status();
                        resp.into_body()
                            .fold(Vec::new(), |mut v, c| {
                                v.extend(c);
                                future::ok::<_, hyper::Error>(v)
                            })
                            .map(move |payload| {
                                if status.is_success() {
                                    println!("post -> ({})", status);
                                } else {
                                    let body = String::from_utf8(payload).unwrap();
                                    eprintln!("post -> ({}, {:?})", status, body);
                                }
                            })
                    })
                    .map_err(|e: hyper::Error| eprintln!("post error: {:?}", e));

                runtime.block_on(f).unwrap();
            }
        }
    }
}
