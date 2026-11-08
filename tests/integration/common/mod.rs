use multithreaded_kv_store::AppState;
use std::{
    collections::HashMap,
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

/// Writes `count` entries: key `k{i}` -> value `v{i}`.
pub(crate) async fn seed_store(state: &AppState, name: &str, count: usize) {
    for i in 0..count {
        state
            .write_in_store(name, format!("k{i}"), format!("v{i}"))
            .await
            .expect("seed write failed");
    }
}

/// The map that `seed_store(.., count)` should produce.
pub(crate) fn expected_map(count: usize) -> HashMap<String, String> {
    (0..count)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect()
}

/// Walks every page with `page_size` (1..=100) and merges the results.
///
/// Fails the test if a key shows up on two pages. Only call this once writes
/// have stopped: HashMap order can change when the map is mutated, so pages
/// fetched during writes are not guaranteed to partition the store.
pub(crate) async fn fetch_all_paged(
    state: &AppState,
    name: &str,
    page_size: usize,
) -> HashMap<String, String> {
    assert!(
        (1..=100).contains(&page_size),
        "page_size must be within 1..=100"
    );

    let mut merged = HashMap::new();
    let mut offset = 0usize;

    loop {
        let (page, total) = state
            .fetch_page_from_store(name, offset, page_size)
            .await
            .expect("fetch_page_from_store failed");

        assert!(page.len() <= page_size, "page exceeded limit");

        for (k, v) in page {
            assert!(
                merged.insert(k.clone(), v).is_none(),
                "key {k} appeared on two pages"
            );
        }

        offset += page_size;
        if offset >= total {
            break;
        }
    }

    merged
}

/// Replacement for the removed `fetch_all`: the full state via 100-entry pages.
pub(crate) async fn fetch_all(state: &AppState, name: &str) -> HashMap<String, String> {
    fetch_all_paged(state, name, 100).await
}