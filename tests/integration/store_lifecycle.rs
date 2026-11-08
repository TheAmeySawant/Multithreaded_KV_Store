use crate::common::{
    app_state, cleanup_store, create_store, serial_guard, unique_store_name,
};
use std::{fs, path::Path};

#[tokio::test]
async fn create_store_registers_it_in_app_state() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("lifecycle_create");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    assert!(state.store_exists(&name).await);

    cleanup_store(&state, &name).await;

    assert!(!state.store_exists(&name).await);
}

#[tokio::test]
async fn deleting_one_store_does_not_affect_another() {
    let _guard = serial_guard().await;
    let state = app_state();

    let name_a = unique_store_name("delete_isolation_a");
    let name_b = unique_store_name("delete_isolation_b");

    create_store(&state, &name_a, 1_000_000)
        .await
        .unwrap();

    create_store(&state, &name_b, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name_a, "key".into(), "A".into())
        .await
        .unwrap();

    state
        .write_in_store(&name_b, "key".into(), "B".into())
        .await
        .unwrap();

    assert!(state.delete_store(&name_a).await.unwrap());

    assert!(!state.store_exists(&name_a).await);
    assert!(state.store_exists(&name_b).await);

    assert_eq!(
        state
            .read_from_store(&name_b, "key".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "B"
    );

    state.cleanup_deleted_folder().await;

    cleanup_store(&state, &name_b).await;
}

#[tokio::test]
async fn deleting_missing_store_is_a_noop_and_returns_false() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("delete_missing");

    assert!(!state.delete_store(&name).await.unwrap());
    assert!(!state.store_exists(&name).await);
}

#[tokio::test]
async fn delete_moves_store_directory_out_of_projects() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("delete_move");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name, "key".into(), "value".into())
        .await
        .unwrap();

    assert!(Path::new(&format!("projects/{name}")).exists());

    assert!(state.delete_store(&name).await.unwrap());

    assert!(
        !Path::new(&format!("projects/{name}")).exists()
    );

    let deleted_entries = fs::read_dir("deleted").unwrap();

    let moved = deleted_entries.flatten().any(|entry| {
        entry
            .file_name()
            .to_string_lossy()
            .starts_with(&name)
    });

    assert!(
        moved,
        "deleted store directory was not found"
    );

    state.cleanup_deleted_folder().await;
}

#[tokio::test]
async fn deleted_store_can_no_longer_be_read_written_or_paged() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("delete_then_use");

    create_store(&state, &name, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name, "key".into(), "value".into())
        .await
        .unwrap();

    assert!(state.delete_store(&name).await.unwrap());

    assert!(
        state
            .read_from_store(&name, "key".into())
            .await
            .is_err()
    );

    assert!(
        state
            .write_in_store(&name, "key".into(), "value".into())
            .await
            .is_err()
    );

    assert!(
        state
            .fetch_page_from_store(&name, 0, 10)
            .await
            .is_err()
    );

    state.cleanup_deleted_folder().await;
}

#[tokio::test]
async fn graceful_shutdown_persists_all_active_stores() {
    let _guard = serial_guard().await;
    let state = app_state();

    let name_a = unique_store_name("shutdown_a");
    let name_b = unique_store_name("shutdown_b");

    create_store(&state, &name_a, 1_000_000)
        .await
        .unwrap();

    create_store(&state, &name_b, 1_000_000)
        .await
        .unwrap();

    state
        .write_in_store(&name_a, "k".into(), "A".into())
        .await
        .unwrap();

    state
        .write_in_store(&name_b, "k".into(), "B".into())
        .await
        .unwrap();

    state.graceful_shutdown().await.unwrap();

    state
        .connect_store(&name_a, 1_000_000)
        .await
        .unwrap();

    state
        .connect_store(&name_b, 1_000_000)
        .await
        .unwrap();

    assert_eq!(
        state
            .read_from_store(&name_a, "k".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "A"
    );

    assert_eq!(
        state
            .read_from_store(&name_b, "k".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "B"
    );

    cleanup_store(&state, &name_a).await;
    cleanup_store(&state, &name_b).await;
}