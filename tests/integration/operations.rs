use crate::common::{
    app_state, cleanup_store, create_store, serial_guard, unique_store_name,
};

#[tokio::test]
async fn write_read_and_update_existing_key() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("basic_ops");

    create_store(&state, &name, 1_000_000).await.unwrap();

    let first = state
        .write_store(&name, "name".into(), "Amey".into())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(first.key, "name");
    assert_eq!(first.value, "Amey");

    let read = state
        .read_store(&name, "name".into())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read.value, "Amey");

    let updated = state
        .write_store(&name, "name".into(), "Sawant".into())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(updated.value, "Sawant");

    let read = state
        .read_store(&name, "name".into())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read.value, "Sawant");

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn delete_existing_and_non_existing_keys() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("delete");

    create_store(&state, &name, 1_000_000).await.unwrap();

    state
        .write_store(&name, "k".into(), "v".into())
        .await
        .unwrap();

    let deleted = state
        .delete_key(&name, "k".into())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(deleted.key, "k");
    assert_eq!(deleted.value, "v");

    assert!(
        state
            .read_store(&name, "k".into())
            .await
            .unwrap()
            .is_none()
    );

    assert!(
        state
            .delete_key(&name, "k".into())
            .await
            .unwrap()
            .is_none()
    );

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn fetch_all_returns_complete_current_state() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("fetch_all");

    create_store(&state, &name, 1_000_000).await.unwrap();

    for (key, value) in [("a", "1"), ("b", "2"), ("c", "3")] {
        state
            .write_store(&name, key.into(), value.into())
            .await
            .unwrap();
    }

    state.delete_key(&name, "b".into()).await.unwrap();

    let all = state.fetch_all_store(&name).await.unwrap();

    assert_eq!(all.len(), 2);
    assert_eq!(all.get("a"), Some(&"1".to_string()));
    assert_eq!(all.get("c"), Some(&"3".to_string()));
    assert!(!all.contains_key("b"));

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn keys_and_values_are_case_sensitive() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("case");

    create_store(&state, &name, 1_000_000).await.unwrap();

    state
        .write_store(&name, "name".into(), "Amey".into())
        .await
        .unwrap();

    state
        .write_store(&name, "Name".into(), "SAWANT".into())
        .await
        .unwrap();

    assert_eq!(
        state
            .read_store(&name, "name".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "Amey"
    );

    assert_eq!(
        state
            .read_store(&name, "Name".into())
            .await
            .unwrap()
            .unwrap()
            .value,
        "SAWANT"
    );

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn textual_operation_parser_is_case_insensitive_for_keywords() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("parser_case");

    create_store(&state, &name, 1_000_000).await.unwrap();

    state
        .execute_query(&name, "wRiTe key = value")
        .await
        .unwrap();

    let output = state
        .execute_query(&name, "READ key")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(output.value, "value");

    let output = state
        .execute_query(&name, "DeLeTe key")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(output.value, "value");

    assert!(
        state
            .execute_query(&name, "read key")
            .await
            .unwrap()
            .is_none()
    );

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn invalid_textual_operations_are_rejected() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("parser_invalid");

    create_store(&state, &name, 1_000_000).await.unwrap();

    for query in [
        "",
        "Unknown key",
        "Write key value",
        "Write key != value",
        "Read",
        "Read key extra",
        "Delete",
        "Delete key extra",
    ] {
        assert!(
            state.execute_query(&name, query).await.is_err(),
            "query: {query}"
        );
    }

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn documented_fetch_all_query_is_currently_rejected_by_parser() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("parser_fetch_all");

    create_store(&state, &name, 1_000_000).await.unwrap();

    // The source documentation describes `Fetch All`, but
    // _prepare_operation only implements Write, Read and Delete.
    assert!(
        state
            .execute_query(&name, "Fetch All")
            .await
            .is_err()
    );

    cleanup_store(&state, &name).await;
}