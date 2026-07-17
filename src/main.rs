use std::{
    fs,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
    vec,
};

use multithreaded_kv_store::StoreEngine;
use multithreaded_kv_store::proto::{
    AvailableOperationsOnKv, DeleteKv, ReadKv, Wal, WriteKv, available_operations_on_kv::Op,
};

use tokio;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    start_tcp_server("127.0.0.1:7878");

    // Declaration
    let wal = Wal {
        sq_no: 1,
        operation: Some(AvailableOperationsOnKv {
            op: Some(Op::WriteOp(WriteKv {
                key: "name".to_string(),
                value: "Amey".to_string(),
            })),
        }),
    };

    //Access
    if let Op::WriteOp(write) = wal.operation.unwrap().op.unwrap() {
        println!("key: {}\tvalue: {}", write.key, write.value);
    }
}

///
/// ## start_tcp_server
/// start_tcp_server function starts server
///
/// # Example
/// ```
/// start_tcp_server("127.0.0.1:7878", 4);
/// ```
fn start_tcp_server(tcp_server_address: &str) {
    let listener = TcpListener::bind(tcp_server_address).expect("Network Binding Failed!");

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => panic!("TCPStream lost or maybe connection lost \n{e}"),
        };

        let mut store_engine_hub: Arc<Mutex<Vec<StoreEngine>>> = Arc::new(Mutex::new(vec![]));

        let seh_clone = Arc::clone(&mut store_engine_hub);
        tokio::spawn(async move {
            handle_request(stream, seh_clone).await;
        });
    }
}

async fn handle_request(mut stream: TcpStream, store_engine_hub: Arc<Mutex<Vec<StoreEngine>>>) {
    let buf_reader = BufReader::new(&stream);

    let request_line = buf_reader
        .lines()
        .next()
        .expect("buf_reader is empty. Found None")
        .expect("buf_reader to request_line conversion failed!");

    println!("Request: {request_line:#?}");
    let response = match get_response(request_line, store_engine_hub).await {
        Some(r) => r,
        None => "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {length} \r\n\r\n Internal Server Error Occured. Please Try again later".to_string()
    };

    stream
        .write_all(response.as_bytes())
        .expect("Sending response failed");
}

async fn get_response(
    request_line: String,
    store_engine_hub: Arc<Mutex<Vec<StoreEngine>>>,
) -> Option<String> {
    let (status_line, filename) = match &request_line[..] {
        "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "pages/home.html"),
        "GET /create-kv HTTP/1.1" => {
            let mut store_engine_hub = match store_engine_hub.lock() {
                Ok(seh) => seh,
                Err(e) => return None,
            };

            let _ = store_engine_hub.push_mut(StoreEngine::connect("my_kv"));
            println!("{store_engine_hub:#?}");

            ("HTTP/1.1 200 OK", "")
        }
        "GET /sleep HTTP/1.1" => {
            tokio::time::sleep(Duration::from_secs(5)).await;

            ("HTTP/1.1 200 OK", "pages/sleep.html")
        }
        _ => ("HTTP/1.1 404 OK", "pages/404.html"),
    };

    let response: String;
    if filename.is_empty() {
        response = format!(
            "{status_line}\r\n\
     \r\n"
        );
    } else {
        println!("Reading {filename} file");
        let content = fs::read_to_string(filename).expect("file reading failed");

        let length = content.len();

        response = format!("{status_line}\r\nContent-Length: {length} \r\n\r\n {content}");
    }

    Some(response)
}
