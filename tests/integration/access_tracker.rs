use crate::common::{cleanup_store, create_store, serial_guard, unique_store_name};
use multithreaded_kv_store::AppState;
use std::fs;
use tokio::time::{sleep, Duration};

// These tests use a PRIVATE AppState instead of the shared `app_state()`:
// the tracker evicts every idle store in its map, so on the shared state it
// could remove stores that belong to other tests.

#[tokio::test]
async fn access_tracker_evicts_an_idle_store() {
    let _guard = serial_guard().await;
    let state = AppState::new();
    let name = unique_store_name("access_idle");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    assert!(state.store_exists(&name).await);

    state.spawn_access_tracker(Some(10));

    // tokio::time::interval ticks immediately on its first tick.
    sleep(Duration::from_millis(100)).await;

    assert!(!state.store_exists(&name).await);

    state.stop_access_tracker();

    // Eviction only drops the in-memory engine; the files stay on disk.
    let _ = fs::remove_dir_all(format!("projects/{name}"));
}

#[tokio::test]
async fn accessed_store_is_expected_to_survive_tracker_interval() {
    let _guard = serial_guard().await;
    let state = AppState::new();
    let name = unique_store_name("access_used");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    // _execute_operation increments current_access_count.
    state
        .execute_query(&name, "Write key = value")
        .await
        .unwrap();

    state.spawn_access_tracker(Some(10));

    sleep(Duration::from_millis(100)).await;

    // Intended behavior: an accessed store should not be evicted.
    //
    // With the current source, the first accessed interval performs
    //
    //     (current_access_count - avg_access_per_minute) / intervals_count
    //
    // while intervals_count is still 0. That divides by zero and panics the
    // tracker task while it holds the store's access_tracker mutex, which
    // poisons it. This test is a regression test for that defect.
    assert!(
        state.store_exists(&name).await,
        "an accessed store should survive the first tracker interval"
    );

    // A follow-up operation locks the same mutex. If the tracker task
    // panicked while holding it, this panics on the poisoned lock.
    let follow_up_state = state.clone();
    let follow_up_name = name.clone();

    let follow_up = tokio::spawn(async move {
        follow_up_state
            .execute_query(&follow_up_name, "Read key")
            .await
    })
    .await;

    assert!(
        follow_up.is_ok(),
        "the tracker task must not poison the store's access_tracker mutex"
    );

    state.stop_access_tracker();
    cleanup_store(&state, &name).await;
}