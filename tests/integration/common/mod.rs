use multithreaded_kv_store::AppState;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
};
use tokio::sync::{Mutex, MutexGuard};

static APP_STATE: OnceLock<Arc<AppState>> = OnceLock::new();
static TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static STORE_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) fn app_state() -> Arc<AppState> {
    Arc::clone(APP_STATE.get_or_init(|| Arc::new(AppState::new())))
}

pub(crate) async fn serial_guard() -> MutexGuard<'static, ()> {
    TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

pub(crate) fn unique_store_name(prefix: &str) -> String {
    let id = STORE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("test_{prefix}_{}_{}", std::process::id(), id)
}

pub(crate) async fn create_store(
    state: &AppState,
    name: &str,
    snapshot_window_size: u64,
) -> Result<(), String> {
    state.connect_store(name, snapshot_window_size).await
}

pub(crate) async fn cleanup_store(state: &AppState, name: &str) {
    if state.store_exists(name).await {
        state
            .delete_store(name)
            .await
            .expect("test cleanup: delete_store failed");
    }

    state.cleanup_deleted_folder().await;
}