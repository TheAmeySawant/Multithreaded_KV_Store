use std::sync::Arc;

use axum::{Router, routing::{get, post}};
use handlers::*;
use multithreaded_kv_store::AppState;

use super::handlers;

pub fn create_app() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(health_check))
        .route("/home", get(get_home))
        .route("/create_kv/{kv_store_name}", get(create_kv))
        .route("/{kv_store_name}/read/{key}", get(read_value))
        .route("/{kv_store_name}/delete/{key}", get(delete_value))
        .route("/write", post(write_value))
        .route("/pages/{kv_store_name}", get(fetch_initial_page_from_kv_store))
        .route("/pages/{kv_store_name}/{offset}/{limit}", get(fetch_page_kv_store))
}
