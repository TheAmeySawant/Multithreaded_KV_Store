mod common;

use common::{
    cleanup_store,
    connect_test_store,
};

use std::sync::Arc;

use multithreaded_kv_store::StoreEngine;

#[tokio::test]
async fn multiple_store_engines_are_isolated() {
    let (name_a, store_a) =
        connect_test_store("isolation_a", 1000);

    let (name_b, store_b) =
        connect_test_store("isolation_b", 1000);

    store_a
        .write("shared_key".into(), "A".into())
        .await
        .unwrap();

    store_b
        .write("shared_key".into(), "B".into())
        .await
        .unwrap();

    let value_a = store_a
        .read("shared_key".into())
        .await
        .unwrap();

    let value_b = store_b
        .read("shared_key".into())
        .await
        .unwrap();

    assert_eq!(value_a.value, "A");
    assert_eq!(value_b.value, "B");

    let all_a = store_a.fetch_all().await;
    let all_b = store_b.fetch_all().await;

    assert_eq!(all_a.len(), 1);
    assert_eq!(all_b.len(), 1);

    assert_eq!(all_a["shared_key"], "A");
    assert_eq!(all_b["shared_key"], "B");

    store_a
        .graceful_shutdown()
        .await
        .unwrap();

    store_b
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name_a);
    cleanup_store(&name_b);
}

#[tokio::test]
async fn cloned_arc_can_be_used_from_multiple_tasks() {
    let (name, store) =
        connect_test_store("arc_clone", 1000);

    let store_a = Arc::clone(&store);
    let store_b = Arc::clone(&store);

    let task_a = tokio::spawn(async move {
        store_a
            .write("key_a".into(), "value_a".into())
            .await
            .unwrap();
    });

    let task_b = tokio::spawn(async move {
        store_b
            .write("key_b".into(), "value_b".into())
            .await
            .unwrap();
    });

    task_a.await.unwrap();
    task_b.await.unwrap();

    let all = store.fetch_all().await;

    assert_eq!(all.len(), 2);

    assert_eq!(
        all.get("key_a"),
        Some(&"value_a".to_string())
    );

    assert_eq!(
        all.get("key_b"),
        Some(&"value_b".to_string())
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn reconnecting_to_existing_store_preserves_state() {
    let (name, store) =
        connect_test_store("reconnect", 1000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("city".into(), "Mumbai".into())
        .await
        .unwrap();

    drop(store);

    let recovered = StoreEngine::connect(
        &name,
        1000,
    )
    .expect("Failed to reconnect");

    let name_value = recovered
        .read("name".into())
        .await
        .unwrap();

    let city_value = recovered
        .read("city".into())
        .await
        .unwrap();

    assert_eq!(name_value.value, "Amey");
    assert_eq!(city_value.value, "Mumbai");

    recovered
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn graceful_shutdown_can_be_called_multiple_times() {
    let (name, store) =
        connect_test_store("multiple_shutdowns", 1000);

    store
        .write("key".into(), "value".into())
        .await
        .unwrap();

    store
        .graceful_shutdown()
        .await
        .unwrap();

    // Second call should not corrupt state.
    store
        .graceful_shutdown()
        .await
        .unwrap();

    let value = store
        .read("key".into())
        .await
        .unwrap();

    assert_eq!(value.value, "value");

    cleanup_store(&name);
}

#[tokio::test]
async fn store_starts_empty() {
    let (name, store) =
        connect_test_store("empty_store", 1000);

    let all = store.fetch_all().await;

    assert!(
        all.is_empty(),
        "Newly created store should be empty"
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn multiple_store_instances_can_run_simultaneously() {
    let (name_a, store_a) =
        connect_test_store("simultaneous_a", 1000);

    let (name_b, store_b) =
        connect_test_store("simultaneous_b", 1000);

    let (name_c, store_c) =
        connect_test_store("simultaneous_c", 1000);

    store_a
        .write("key".into(), "A".into())
        .await
        .unwrap();

    store_b
        .write("key".into(), "B".into())
        .await
        .unwrap();

    store_c
        .write("key".into(), "C".into())
        .await
        .unwrap();

    assert_eq!(
        store_a
            .read("key".into())
            .await
            .unwrap()
            .value,
        "A"
    );

    assert_eq!(
        store_b
            .read("key".into())
            .await
            .unwrap()
            .value,
        "B"
    );

    assert_eq!(
        store_c
            .read("key".into())
            .await
            .unwrap()
            .value,
        "C"
    );

    store_a
        .graceful_shutdown()
        .await
        .unwrap();

    store_b
        .graceful_shutdown()
        .await
        .unwrap();

    store_c
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name_a);
    cleanup_store(&name_b);
    cleanup_store(&name_c);
}

#[tokio::test]
async fn store_remains_usable_after_snapshot_creation() {
    let (name, store) =
        connect_test_store("post_snapshot_usage", 1000);

    store
        .write("before".into(), "snapshot".into())
        .await
        .unwrap();

    store
        .graceful_shutdown()
        .await
        .unwrap();

    // Store should still accept operations after snapshot creation.
    store
        .write("after".into(), "snapshot".into())
        .await
        .unwrap();

    let before = store
        .read("before".into())
        .await
        .unwrap();

    let after = store
        .read("after".into())
        .await
        .unwrap();

    assert_eq!(before.value, "snapshot");
    assert_eq!(after.value, "snapshot");

    cleanup_store(&name);
}