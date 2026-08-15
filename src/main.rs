mod api;

use std::sync::Arc;

// Internal Modules
use multithreaded_kv_store::{AppState};
use crate::api::start_axum_tcp_server;

// External Modules
use tokio;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    
    let app_state = Arc::new(AppState::new());

    if let Err(e) = start_axum_tcp_server("127.0.0.1:7878", app_state, None).await {
        eprintln!("{e}");
    }
    
}