use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use multithreaded_kv_store::StoreEngine;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates a unique store name for each test.
///
/// This prevents different tests from accidentally sharing
/// the same on-disk WAL/snapshot files.
pub fn unique_store_name(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System clock is before UNIX_EPOCH")
        .as_nanos();

    let id = COUNTER.fetch_add(1, Ordering::Relaxed);

    format!("test_{prefix}_{now}_{id}")
}

/// Removes the on-disk state created by StoreEngine::connect().
///
/// StoreEngine stores its data under:
/// projects/<store_name>/
pub fn cleanup_store(store_name: &str) {
    let path = Path::new("projects").join(store_name);

    if path.exists() {
        fs::remove_dir_all(path)
            .expect("Failed to clean up test store");
    }
}

/// Creates a fresh StoreEngine specifically for testing.
///
/// Returns:
///     (store_name, StoreEngine)
///
/// The store name is returned so that the caller can clean it up
/// after the test.
pub fn connect_test_store(
    prefix: &str,
    snapshot_window_size: u64,
) -> (String, std::sync::Arc<StoreEngine>) {
    let name = unique_store_name(prefix);

    let engine = StoreEngine::connect(
        &name,
        snapshot_window_size,
    )
    .expect("Failed to create test StoreEngine");

    (name, engine)
}