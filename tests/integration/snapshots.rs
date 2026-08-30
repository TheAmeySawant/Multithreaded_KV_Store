use crate::common::{
    app_state, cleanup_store, create_store, serial_guard, unique_store_name,
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
        .write_store(&name, "a".into(), "1".into())
        .await
        .unwrap();

    state
        .write_store(&name, "b".into(), "2".into())
        .await
        .unwrap();

    state
        .write_store(&name, "c".into(), "3".into())
        .await
        .unwrap();

    let snapshot_path =
        format!("projects/{name}/snapshot/snapshot.bin");

    let wal_path =
        format!("projects/{name}/wal/wal.log");

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

    let all = state.fetch_all_store(&name).await.unwrap();

    assert_eq!(all.get("a"), Some(&"1".to_string()));
    assert_eq!(all.get("b"), Some(&"2".to_string()));
    assert_eq!(all.get("c"), Some(&"3".to_string()));

    cleanup_store(&state, &name).await;
}