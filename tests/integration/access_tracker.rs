use crate::common::{
    app_state, cleanup_store, create_store, serial_guard, unique_store_name,
};
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn access_tracker_evicts_an_idle_store() {
    let _guard = serial_guard().await;
    let state = app_state();
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
    state.cleanup_deleted_folder().await;
}

#[tokio::test]
async fn accessed_store_is_expected_to_survive_tracker_interval() {
    let _guard = serial_guard().await;
    let state = app_state();
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
    // With the current source, the first accessed interval performs:
    //
    //     current_access_count / intervals_count
    //
    // while intervals_count is still 0, which can panic the background
    // task. This test is therefore a regression test for that defect.
    assert!(
        state.store_exists(&name).await,
        "an accessed store should survive the first tracker interval"
    );

    state.stop_access_tracker();
    cleanup_store(&state, &name).await;
}