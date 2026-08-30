use crate::common::{
    app_state, cleanup_store, create_store, serial_guard, unique_store_name,
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
        .write_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    state
        .write_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    state.delete_key(&name, "a".into()).await.unwrap();

    // Reconnect through AppState. Since the snapshot window is huge,
    // recovery should occur from WAL.
    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    assert!(
        state
            .read_store(&name, "a".into())
            .await
            .unwrap()
            .is_none()
    );

    assert_eq!(
        state
            .read_store(&name, "b".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "2"
    );

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
        .write_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    state
        .write_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    state.graceful_shutdown().await.unwrap();

    assert!(Path::new(&snapshot_path(&name)).exists());
    assert_eq!(fs::metadata(wal_path(&name)).unwrap().len(), 0);

    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    let all = state.fetch_all_store(&name).await.unwrap();

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
        .write_store(&name, "key".into(), "value".into())
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

    for i in 0..10 {
        state
            .write_store(&name, format!("k{i}"), format!("v{i}"))
            .await
            .unwrap();
    }

    state.graceful_shutdown().await.unwrap();

    assert!(Path::new(&snapshot_path(&name)).exists());
    assert_eq!(fs::metadata(wal_path(&name)).unwrap().len(), 0);

    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    let all = state.fetch_all_store(&name).await.unwrap();

    let expected: std::collections::HashMap<_, _> = (0..10)
        .map(|i| (format!("k{i}"), format!("v{i}")))
        .collect();

    assert_eq!(all, expected);

    cleanup_store(&state, &name).await;
}