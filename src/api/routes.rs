use axum::{Router, routing::{get}};
use handlers::*;
use multithreaded_kv_store::AppState;

use super::handlers;

pub fn create_app() -> Router<AppState> {
    Router::new()
    .route("/", get(health_check))
    .route("/home", get(get_home))
    .route("/create_kv", get(create_kv))
}