use crate::common::{
    app_state, cleanup_store, create_store, expected_map, fetch_all, fetch_all_paged, seed_store,
    serial_guard, unique_store_name,
};
use std::{fs, path::Path};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn snapshot_window_triggers_background_snapshot() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("snapshot_window");

    // Initial next_seq = 1 + window_size.
    // With window = 2, the third write makes next_seq > threshold.
    create_store(&state, &name, 2).await.unwrap();

    state
        .write_in_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    state
        .write_in_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    state
        .write_in_store(&name, "c".into(), "3".into())
        .await
        .unwrap();

    let snapshot_path = format!("projects/{name}/snapshot/snapshot.bin");

    let wal_path = format!("projects/{name}/wal/wal.log");

    let mut snapshot_seen = false;

    for _ in 0..40 {
        if Path::new(&snapshot_path).exists()
            && fs::metadata(&wal_path).unwrap().len() == 0
        {
            snapshot_seen = true;
            break;
        }

        sleep(Duration::from_millis(50)).await;
    }

    assert!(
        snapshot_seen,
        "snapshot tracker did not persist the snapshot"
    );

    let all = fetch_all(&state, &name).await;

    assert_eq!(all.get("a"), Some(&"1".to_string()));
    assert_eq!(all.get("b"), Some(&"2".to_string()));
    assert_eq!(all.get("c"), Some(&"3".to_string()));

    // Paging must serve the same data once the snapshot has run,
    // including with a page size smaller than the store.
    let paged = fetch_all_paged(&state, &name, 2).await;
    assert_eq!(paged, all);

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn pages_are_complete_after_snapshot_and_reconnect() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("snapshot_pages");

    // Huge window: no background snapshot, only the explicit one below.
    create_store(&state, &name, 1_000_000).await.unwrap();

    seed_store(&state, &name, 10).await;

    state.graceful_shutdown().await.unwrap();

    state
        .connect_store(&name, 1_000_000)
        .await
        .unwrap();

    assert_eq!(fetch_all_paged(&state, &name, 3).await, expected_map(10));

    let (_, total) = state
        .fetch_page_from_store(&name, 0, 1)
        .await
        .unwrap();

    assert_eq!(total, 10);

    cleanup_store(&state, &name).await;
}