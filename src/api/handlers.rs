use std::sync::LazyLock;
use axum::{Json, extract::State, response::{Html, IntoResponse}};
use multithreaded_kv_store::{AppState};
use serde_json::json;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

// GLOBALS
static INTERNAL_SERVER_ERROR_500:LazyLock<String> = LazyLock::new(|| {
    "<!DOCTYPE html>
<html lang=\"en\">
<head>
    <meta charset=\"UTF-8\">
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">
    <title>Not Found</title>
</head>
<body>
    <h1>404 Error</h1>
</body>
</html>".to_string()
});

pub async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "message": "Server is running"
    }))
}

#[axum::debug_handler]
pub async fn create_kv(State(app_state): State<AppState>) -> impl IntoResponse {
    println!("/create_kv called!");
    let stores_map = app_state.get_stores_map_clone();

    let mut stores_map_guard = stores_map.lock().unwrap();

    if let Ok(()) = stores_map_guard.insert("my_kv") {
        println!("{stores_map_guard:#?}");

        Json(json!({
            "status": "ok",
            "message": "Server is running"
        }))
    } else {
        Json(json!({
            "status": "ok",
            "message": "Server is running"
        }))
    }
}

pub async fn get_home() -> impl IntoResponse {
    let mut file = File::open("pages/home.html").await.unwrap();
    let mut contents = String::new();
    match file.read_to_string(&mut contents).await{
        Ok(_) => {Html(contents)},
        Err(_) => {
            Html(INTERNAL_SERVER_ERROR_500.clone())
        }
    }
    
}