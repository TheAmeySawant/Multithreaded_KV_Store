use multithreaded_kv_store::{AppState, get_err};
use tokio::net::TcpListener;

mod handlers;
mod middleware;
mod routes;

use routes::create_app;

/// Starts Axum-TCP Server
/// 
/// When *access_tracker_interval_in_seconds* is **None**, then 120 seconds (2 mins) is set by default.
/// This time interval is used to call access_tracker_update which calculates avg_access_count per *access_tracker_interval_in_seconds*
pub async fn start_axum_tcp_server(addr: &str, app_state: AppState, access_tracker_interval_in_seconds: Option<u64>) -> Result<(), String> {
    let server_listener = TcpListener::bind(addr)
        .await
        .expect("failed to bind tcp listener");

    // here, None means default time will be used
    app_state.spawn_access_tracker(access_tracker_interval_in_seconds);

    let app = create_app().with_state(app_state);

    if let Err(e) = axum::serve(server_listener, app).await {
        Err(get_err(format!(
            "Error Occured while starting the Axum Server.\n{e}"
        )))
    } else {
        Ok(())
    }
}
