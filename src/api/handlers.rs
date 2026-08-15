use axum::{
    Form, Json,
    extract::{Path, State},
    response::{Html, IntoResponse},
};
use multithreaded_kv_store::{AppState, StoreEngine};
use serde::Deserialize;
use serde_json::json;
use std::sync::{Arc, LazyLock};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

#[derive(Deserialize, Debug)]
pub struct NewKV {
    pub key: String,
    pub value: String,
    pub kv_store_name: String,
}

// GLOBALS
static INTERNAL_SERVER_ERROR_500: LazyLock<String> = LazyLock::new(|| {
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
</html>"
        .to_string()
});

pub async fn health_check() -> impl IntoResponse {
    Json(json!({
        "status": "ok",
        "message": "Server is running"
    }))
}

pub async fn get_home() -> impl IntoResponse {
    let mut file = File::open("pages/home.html").await.unwrap();
    let mut contents = String::new();
    match file.read_to_string(&mut contents).await {
        Ok(_) => Html(contents),
        Err(_) => Html(INTERNAL_SERVER_ERROR_500.clone()),
    }
}

#[axum::debug_handler]
pub async fn create_kv(
    State(app_state): State<Arc<AppState>>,
    Path(kv_store_name): Path<String>,
) -> impl IntoResponse {
    println!("/create_kv called!");
    let stores_map = app_state.get_stores_map_clone();

    match StoreEngine::connect(&kv_store_name, 65000) {
        Ok(store_engine) => {
            let mut stores_map_guard = stores_map.write().await;

            stores_map_guard.insert(&kv_store_name, store_engine);

            println!("{stores_map_guard:#?}");

            Json(json!({
                "status": "ok",
                "message": "Server is running"
            }))
        }
        Err(e) => Json(json!({
            "status": "ok",
            "message": "Server is running",
            "error": e
        })),
    }
}

#[axum::debug_handler]
pub async fn read_value(
    State(app_state): State<Arc<AppState>>,
    Path((kv_store_name, key)): Path<(String, String)>,
) -> impl IntoResponse {
    println!("/{kv_store_name}/read/{key} CALLED");
    let stores_map = app_state.get_stores_map_clone();

    let kv_store = {
        let stores_map_guard = stores_map.read().await;
        stores_map_guard.get(&kv_store_name).clone()
    };

    match kv_store {
        Some(kv_store) => match kv_store.read(key).await {
            Some(kv_output) => Json(json!({
                "status": "ok",
                "message": "opearation was successful",
                "kv_store_name": kv_store_name,
                "operation_status": true,
                "kv_output": {
                    "key": kv_output.key,
                    "value": kv_output.value
                }
            })),
            None => Json(json!({
                "status": "ok",
                "message": "No such key exists in this KvStore",
                "kv_store_name": kv_store_name,
                "operation_status": false,
                "kv_output": {
                    "key": "",
                    "value": ""
                }
            })),
        },
        None => Json(json!({
            "status": "ok",
            "message": "There ain't any such KvStore",
            "kv_store_name": kv_store_name,
            "operation_status": false,
            "kv_output": {
                "key": "",
                "value": ""
            }
        })),
    }
}

#[axum::debug_handler]
pub async fn delete_value(
    State(app_state): State<Arc<AppState>>,
    Path((kv_store_name, key)): Path<(String, String)>,
) -> impl IntoResponse {
    println!("/{kv_store_name}/delete/{key} CALLED");
    let stores_map = app_state.get_stores_map_clone();

    let kv_store = {
        let stores_map_guard = stores_map.read().await;
        stores_map_guard.get(&kv_store_name).clone()
    };

    match kv_store {
        Some(kv_store) => match kv_store.delete(key).await {
            Ok(something) => match something {
                Some(kv_output) => Json(json!({
                    "status": "ok",
                    "message": "opearation was successful",
                    "kv_store_name": kv_store_name,
                    "operation_status": true,
                    "kv_output": {
                        "key": kv_output.key,
                        "value": kv_output.value
                    }
                })),
                None => Json(json!({
                    "status": "ok",
                    "message": "No such key exists in this KvStore",
                    "kv_store_name": kv_store_name,
                    "operation_status": false,
                    "kv_output": {
                        "key": "",
                        "value": ""
                    }
                })),
            },
            Err(e) => Json(json!({
                "status": "ok",
                "message": "Failed to Delete, Try Again Later",
                "kv_store_name": kv_store_name,
                "operation_status": false,
                "error": e,
                "kv_output": {
                    "key": "",
                    "value": ""
                }
            })),
        },
        None => Json(json!({
            "status": "ok",
            "message": "There ain't any such KvStore",
            "kv_store_name": kv_store_name,
            "operation_status": false,
            "kv_output": {
                "key": "",
                "value": ""
            }
        })),
    }
}

#[axum::debug_handler]
pub async fn write_value(
    State(app_state): State<Arc<AppState>>,
    Form(payload): Form<NewKV>,
) -> impl IntoResponse {
    println!("/write CALLED\nPayload: {:#?}", payload);

    let stores_map = app_state.get_stores_map_clone();

    let kv_store = {
        let stores_map_guard = stores_map.read().await;
        stores_map_guard.get(&payload.kv_store_name).clone()
    };
    match kv_store {
        Some(kv_store) => match kv_store.write(payload.key, payload.value).await {
            Ok(something) => match something {
                Some(kv_ouput) => Json(json!({
                    "status": "ok",
                    "message": "opearation was successful",
                    "kv_store_name": payload.kv_store_name,
                    "operation_status": true,
                    "kv_output": {
                        "key": kv_ouput.key,
                        "value": kv_ouput.value
                    }
                })),
                None => Json(json!({
                    "status": "ok",
                    "message": "Failed to Write, Try Again Later",
                    "kv_store_name": payload.kv_store_name,
                    "operation_status": false,
                    "kv_output": {
                        "key": "",
                        "value": ""
                    }
                })),
            },
            Err(e) => Json(json!({
                "status": "ok",
                "message": "Failed to Write, Try Again Later",
                "kv_store_name": payload.kv_store_name,
                "error": e,
                "operation_status": false,
                "kv_output": {
                    "key": "",
                    "value": ""
                }
            })),
        },
        None => Json(json!({
            "status": "ok",
            "message": "There ain't any such KvStore",
            "kv_store_name": payload.kv_store_name,
            "operation_status": false,
            "kv_output": {
                "key": "",
                "value": ""
            }
        })),
    }
}

pub async fn fetch_full_kv_store(
    State(app_state): State<Arc<AppState>>,
    Path(kv_store_name): Path<String>,
) -> impl IntoResponse {
    let stores_map = app_state.get_stores_map_clone();

    let kv_store = {
        let stores_map_guard = stores_map.read().await;
        stores_map_guard.get(&kv_store_name).clone()
    };

    println!("{kv_store:#?}");

    match kv_store {
        Some(kv_store) => {
            let kv_output = kv_store.fetch_all().await;

            Json(json!({
                "status": "ok",
                "message": "opearation was successful",
                "kv_store_name": kv_store_name,
                "operation_status": true,
                "kv_output": kv_output
            }))
        }
        None => Json(json!({
            "status": "ok",
            "message": "There ain't any such KvStore",
            "kv_store_name": kv_store_name,
            "operation_status": false,
            "kv_output": "Not Found"
        })),
    }
}
