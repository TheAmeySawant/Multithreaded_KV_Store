use crate::common::{
    app_state, cleanup_store, create_store, expected_map, fetch_all, fetch_all_paged, seed_store,
    serial_guard, unique_store_name,
};
use std::{fs, path::Path};

fn wal_path(name: &str) -> String {
    format!("projects/{name}/wal/wal.log")
}

fn snapshot_path(name: &str) -> String {
    format!("projects/{name}/snapshot/snapshot.bin")
}

#[tokio::test]
async fn restart_recovers_latest_state_from_wal() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("wal_recovery");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    state
        .write_in_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    state
        .delete_from_store(&name, "a".into())
        .await
        .unwrap();

    // Reconnect through AppState. Since the snapshot window is huge,
    // recovery should occur from WAL.
    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    assert!(
        state
            .read_from_store(&name, "a".into())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        state
            .read_from_store(&name, "b".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "2"
    );

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn writes_after_recovery_are_recovered_again() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("wal_recovery_twice");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    // First recovery
    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    // The sequence number must continue after recovery
    state
        .write_in_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    // Second recovery
    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    let all = fetch_all(&state, &name).await;

    assert_eq!(all.len(), 2);
    assert_eq!(all.get("a"), Some(&"1".to_string()));
    assert_eq!(all.get("b"), Some(&"2".to_string()));

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn graceful_shutdown_creates_snapshot_and_truncates_wal() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("graceful_shutdown");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    state
        .write_in_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    state.graceful_shutdown().await.unwrap();

    assert!(Path::new(&snapshot_path(&name)).exists());
    assert_eq!(fs::metadata(wal_path(&name)).unwrap().len(), 0);

    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    let all = fetch_all(&state, &name).await;

    assert_eq!(all.get("a"), Some(&"1".to_string()));
    assert_eq!(all.get("b"), Some(&"2".to_string()));

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn wal_crc_corruption_is_rejected_during_recovery() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("wal_crc");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name, "key".into(), "value".into())
        .await
        .unwrap();

    let path = wal_path(&name);

    let mut bytes = fs::read(&path).unwrap();

    let last = bytes
        .last_mut()
        .expect("WAL should not be empty");

    *last ^= 0xFF;

    fs::write(&path, bytes).unwrap();

    let result = state.connect_store(&name, 1_000_000).await;

    assert!(result.is_err());

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn snapshot_recovery_works_with_empty_wal() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("snapshot_recovery");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    seed_store(&state, &name, 10).await;

    state.graceful_shutdown().await.unwrap();

    assert!(Path::new(&snapshot_path(&name)).exists());
    assert_eq!(fs::metadata(wal_path(&name)).unwrap().len(), 0);

    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    let all = fetch_all(&state, &name).await;

    assert_eq!(all, expected_map(10));

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn paged_fetch_after_recovery_covers_multiple_pages() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("recovery_pages");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    // 150 entries = more than one maximum-size page (100)
    seed_store(&state, &name, 150).await;

    state.graceful_shutdown().await.unwrap();

    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    let (first, total) = state
        .fetch_page_from_store(&name, 0, 100)
        .await
        .unwrap();

    assert_eq!(total, 150);
    assert_eq!(first.len(), 100);

    let all = fetch_all_paged(&state, &name, 40).await;
    assert_eq!(all, expected_map(150));

    cleanup_store(&state, &name).await;
}