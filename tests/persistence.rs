mod common;

use common::{
    cleanup_store,
    connect_test_store,
};

use std::{
    fs,
    path::Path,
    time::Duration,
};

use multithreaded_kv_store::StoreEngine;

#[tokio::test]
async fn wal_recovers_state_after_reconnect() {
    let (name, store) =
        connect_test_store("wal_recovery", 100_000);

    // Write some data.
    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("city".into(), "Mumbai".into())
        .await
        .unwrap();

    store
        .write("language".into(), "Rust".into())
        .await
        .unwrap();

    // Update an existing key.
    store
        .write("city".into(), "Pune".into())
        .await
        .unwrap();

    // Delete a key.
    store
        .delete("language".into())
        .await
        .unwrap();

    // Drop the StoreEngine without graceful shutdown.
    //
    // This intentionally avoids creating a snapshot. The next
    // StoreEngine instance must reconstruct the state from WAL.
    drop(store);

    // Reconnect to the same persistent store.
    let recovered = StoreEngine::connect(
        &name,
        100_000,
    )
    .expect("Failed to reconnect to store");

    // Verify recovered state.
    let name_value = recovered
        .read("name".into())
        .await
        .unwrap();

    assert_eq!(name_value.key, "name");
    assert_eq!(name_value.value, "Amey");

    let city_value = recovered
        .read("city".into())
        .await
        .unwrap();

    assert_eq!(city_value.key, "city");
    assert_eq!(city_value.value, "Pune");

    // Deleted key must remain deleted after recovery.
    assert!(
        recovered
            .read("language".into())
            .await
            .is_none()
    );

    let all = recovered.fetch_all().await;

    assert_eq!(all.len(), 2);
    assert_eq!(all["name"], "Amey");
    assert_eq!(all["city"], "Pune");

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn graceful_shutdown_creates_snapshot() {
    let (name, store) =
        connect_test_store("snapshot_creation", 100_000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("city".into(), "Mumbai".into())
        .await
        .unwrap();

    // graceful_shutdown() should create the snapshot.
    store
        .graceful_shutdown()
        .await
        .unwrap();

    let snapshot_path = Path::new("projects")
        .join(&name)
        .join("snapshot")
        .join("snapshot.bin");

    assert!(
        snapshot_path.exists(),
        "snapshot.bin should exist after graceful_shutdown()"
    );

    cleanup_store(&name);
}

#[tokio::test]
async fn snapshot_recovers_store_state() {
    let (name, store) =
        connect_test_store("snapshot_recovery", 100_000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("city".into(), "Mumbai".into())
        .await
        .unwrap();

    store
        .write("language".into(), "Rust".into())
        .await
        .unwrap();

    // Force creation of snapshot.
    store
        .graceful_shutdown()
        .await
        .unwrap();

    drop(store);

    // Reconnect. The store should recover from snapshot.bin.
    let recovered = StoreEngine::connect(
        &name,
        100_000,
    )
    .expect("Failed to recover store");

    let all = recovered.fetch_all().await;

    assert_eq!(all.len(), 3);

    assert_eq!(
        all.get("name"),
        Some(&"Amey".to_string())
    );

    assert_eq!(
        all.get("city"),
        Some(&"Mumbai".to_string())
    );

    assert_eq!(
        all.get("language"),
        Some(&"Rust".to_string())
    );

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn deleted_keys_remain_deleted_after_recovery() {
    let (name, store) =
        connect_test_store("delete_recovery", 100_000);

    store
        .write("keep".into(), "yes".into())
        .await
        .unwrap();

    store
        .write("delete_me".into(), "temporary".into())
        .await
        .unwrap();

    store
        .delete("delete_me".into())
        .await
        .unwrap();

    // Do not create a snapshot.
    drop(store);

    let recovered = StoreEngine::connect(
        &name,
        100_000,
    )
    .expect("Failed to recover store");

    assert!(
        recovered
            .read("delete_me".into())
            .await
            .is_none()
    );

    let keep = recovered
        .read("keep".into())
        .await
        .unwrap();

    assert_eq!(keep.value, "yes");

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn updated_values_are_recovered_from_wal() {
    let (name, store) =
        connect_test_store("update_recovery", 100_000);

    store
        .write("counter".into(), "1".into())
        .await
        .unwrap();

    store
        .write("counter".into(), "2".into())
        .await
        .unwrap();

    store
        .write("counter".into(), "3".into())
        .await
        .unwrap();

    // Drop without snapshot.
    drop(store);

    let recovered = StoreEngine::connect(
        &name,
        100_000,
    )
    .expect("Failed to recover store");

    let result = recovered
        .read("counter".into())
        .await
        .unwrap();

    assert_eq!(result.value, "3");

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn snapshot_followed_by_wal_operations_recovers_latest_state() {
    let (name, store) =
        connect_test_store("snapshot_wal", 100_000);

    // Create the initial state.
    store
        .write("a".into(), "before".into())
        .await
        .unwrap();

    store
        .write("b".into(), "before".into())
        .await
        .unwrap();

    // Explicitly create a snapshot.
    store
        .graceful_shutdown()
        .await
        .unwrap();

    // IMPORTANT:
    //
    // graceful_shutdown() does not make the StoreEngine unusable.
    // It writes the snapshot and truncates WAL, but the StoreEngine
    // can still be used afterward.
    store
        .write("a".into(), "after".into())
        .await
        .unwrap();

    store
        .write("c".into(), "new".into())
        .await
        .unwrap();

    // Drop without another graceful shutdown.
    //
    // On reconnect:
    //
    // snapshot:
    //     a = before
    //     b = before
    //
    // WAL:
    //     a = after
    //     c = new
    //
    // Final expected state:
    //     a = after
    //     b = before
    //     c = new
    drop(store);

    let recovered = StoreEngine::connect(
        &name,
        100_000,
    )
    .expect("Failed to recover store");

    let all = recovered.fetch_all().await;

    assert_eq!(all.len(), 3);

    assert_eq!(
        all.get("a"),
        Some(&"after".to_string())
    );

    assert_eq!(
        all.get("b"),
        Some(&"before".to_string())
    );

    assert_eq!(
        all.get("c"),
        Some(&"new".to_string())
    );

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn snapshot_window_triggers_snapshot() {
    let (name, store) =
        connect_test_store("snapshot_threshold", 2);

    store
        .write("a".into(), "1".into())
        .await
        .unwrap();

    store
        .write("b".into(), "2".into())
        .await
        .unwrap();

    store
        .write("c".into(), "3".into())
        .await
        .unwrap();

    // Snapshot creation happens in a background Tokio task after
    // the threshold notification. Give that task time to execute.
    tokio::time::sleep(Duration::from_millis(100))
        .await;

    let snapshot_path = Path::new("projects")
        .join(&name)
        .join("snapshot")
        .join("snapshot.bin");

    // The threshold should eventually cause snapshot creation.
    assert!(
        snapshot_path.exists(),
        "Snapshot should have been created after crossing the snapshot window"
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn wal_file_exists_after_store_creation() {
    let (name, store) =
        connect_test_store("wal_creation", 1000);

    let wal_path = Path::new("projects")
        .join(&name)
        .join("wal")
        .join("wal.log");

    assert!(
        wal_path.exists(),
        "wal.log should exist after StoreEngine::connect()"
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn graceful_shutdown_preserves_data_for_future_recovery() {
    let (name, store) =
        connect_test_store("shutdown_recovery", 100_000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("project".into(), "KV Store".into())
        .await
        .unwrap();

    // Graceful shutdown creates the durable snapshot.
    store
        .graceful_shutdown()
        .await
        .unwrap();

    drop(store);

    // Reconnect after shutdown.
    let recovered = StoreEngine::connect(
        &name,
        100_000,
    )
    .expect("Failed to reconnect after graceful shutdown");

    let all = recovered.fetch_all().await;

    assert_eq!(all.len(), 2);
    assert_eq!(all["name"], "Amey");
    assert_eq!(all["project"], "KV Store");

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn cleanup_store_removes_test_directory() {
    let (name, store) =
        connect_test_store("cleanup", 1000);

    let store_path = Path::new("projects").join(&name);

    assert!(
        store_path.exists(),
        "Test store directory should exist"
    );

    store.graceful_shutdown().await.unwrap();
    drop(store);

    cleanup_store(&name);

    assert!(
        !fs::exists(&store_path).unwrap(),
        "Test store directory should have been removed"
    );
}