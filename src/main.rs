use std::{
    fs, io::{BufRead, BufReader, ErrorKind, Write}, net::{TcpListener, TcpStream}, sync::{Arc, LazyLock, Mutex}, thread, time::Duration, vec,
};

use multithreaded_kv_store::StoreEngine;

use tokio;

// GLOBALS
static INTERNAL_SERVER_ERROR_500:LazyLock<String> = LazyLock::new(|| {
    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {length} \r\n\r\n Internal Server Error Occured. Please Try again later".to_string()
});


#[tokio::main(flavor = "multi_thread")]
async fn main() {
    start_tcp_server("127.0.0.1:7878");
}

/// start_tcp_server function starts server
///
/// ## Example
/// ```
/// start_tcp_server("127.0.0.1:7878", 4);
/// ```
/// ## Panics
/// If TCP Listener is not working then the program panics
fn start_tcp_server(tcp_server_address: &str) {
    let listener = match TcpListener::bind(tcp_server_address) {
        Ok(tcp_listner) => tcp_listner,
        Err(e) => panic!("Network Binding Failed!\n{e}"),
    };

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

async fn handle_request(stream: TcpStream, store_engine_hub: Arc<Mutex<Vec<StoreEngine>>>) {
    let buf_reader = BufReader::new(&stream);



    match buf_reader.lines().next() {
        Some(Ok(request_line)) => {
            println!("Request: {request_line:#?}");
            let response = match get_response(request_line, store_engine_hub).await {
                Some(r) => r,
                None => INTERNAL_SERVER_ERROR_500.clone()
            };
            send_response(stream, response);
        }
        Some(Err(e)) => {
            eprintln!("Error occured while reading the user request through buffer reader\n{e}");
            send_response(stream, INTERNAL_SERVER_ERROR_500.clone());
        }
        None => {
            eprintln!("Client closed the connection without sending a request.");
        }
    }
}

/// Sends response to client through TcpStream
fn send_response(mut stream: TcpStream, response: String) {
    if let Err(e) = stream.write_all(response.as_bytes()) {
        eprintln!("Sending response to the client failed\n{e}");
    }
}

async fn get_response(
    request_line: String,
    store_engine_hub: Arc<Mutex<Vec<StoreEngine>>>,
) -> Option<String> {
    let (status_line, filename, msg) = match &request_line[..] {

        // "/" route
        "GET / HTTP/1.1" => ("HTTP/1.1 200 OK", "pages/home.html", ""),

        // "/create-kv" route
        "GET /create-kv HTTP/1.1" => {
            let mut store_engine_hub = match store_engine_hub.lock() {
                Ok(seh) => seh,
                Err(e) => return None,
            };

            if let Ok(kv_store) = StoreEngine::connect("my_kv") {
                let _ = store_engine_hub.push_mut(kv_store);
                println!("{store_engine_hub:#?}");

                ("HTTP/1.1 200 OK", "", "my_kv Created Successfully")
            } else {
                ("HTTP/1.1 200 OK", "", "Failed to create my_kv")
            }
        },

        "GET /write HTTP/1.1" => {
            ("", "", "")
        }

        // "/sleep" route
        "GET /sleep HTTP/1.1" => {
            tokio::time::sleep(Duration::from_secs(5)).await;

            ("HTTP/1.1 200 OK", "pages/sleep.html", "")
        }

        // DEFAULT route
        _ => ("HTTP/1.1 404 OK", "pages/404.html", ""),


    };

    let response: String;
    if filename.is_empty() {
        if msg.is_empty() {
            response = format!(
                "{status_line}\r\n\
     \r\n"
            );
        } else {
            let length = msg.len();

            response = format!("{status_line}\r\nContent-Length: {length} \r\n\r\n {msg}");
        }
    } else {
        println!("Reading {filename} file");
        let content = match fs::read_to_string(filename){
            Ok(content) => content,
            Err(e) if e.kind() == ErrorKind::NotFound=> {
                match fs::read_to_string("pages/404.html"){ //If that page not found then show '404 Not Found' page 
                    Ok(content) => content,
                    Err(e) => {
                        return Some(INTERNAL_SERVER_ERROR_500.clone());
                    }
                }
            }
            Err(e) => {
                return Some(INTERNAL_SERVER_ERROR_500.clone());
            }
        };

        let length = content.len();

        response = format!("{status_line}\r\nContent-Length: {length} \r\n\r\n {content}");
    }

    Some(response)
}
