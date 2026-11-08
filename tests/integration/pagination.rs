//! Tests for `AppState::fetch_page_from_store`.
//!
//! Contract :
//! - `offset` must be any positive value, including 0 (the first page)
//! - `limit` must be within 1..=100
//! - returns `(page, total_entries_in_store)`
//!
//! HashMap iteration order is arbitrary, so no test assumes WHICH keys land
//! on which page. They check sizes, absence of duplicates, and that the pages
//! together equal the full state.

use crate::common::{
    app_state, cleanup_store, create_store, expected_map, fetch_all_paged, seed_store,
    serial_guard, unique_store_name,
};
use std::collections::HashMap;

#[tokio::test]
async fn pages_partition_the_store() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_partition");
    create_store(&state, &name, 1_000_000).await.unwrap();
    seed_store(&state, &name, 25).await;

    let (p0, total) = state.fetch_page_from_store(&name, 0, 10).await.unwrap();
    let (p1, _) = state.fetch_page_from_store(&name, 10, 10).await.unwrap();
    let (p2, _) = state.fetch_page_from_store(&name, 20, 10).await.unwrap();

    assert_eq!(total, 25);
    assert_eq!((p0.len(), p1.len(), p2.len()), (10, 10, 5));

    let mut merged = HashMap::new();
    for page in [p0, p1, p2] {
        for (k, v) in page {
            assert!(merged.insert(k, v).is_none(), "duplicate across pages");
        }
    }
    assert_eq!(merged, expected_map(25));

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn offset_zero_is_valid_and_offset_past_end_is_empty() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_offsets");
    create_store(&state, &name, 1_000_000).await.unwrap();
    seed_store(&state, &name, 25).await;

    assert!(
        state.fetch_page_from_store(&name, 0, 10).await.is_ok(),
        "offset 0 is the first page and must be accepted"
    );

    for offset in [25, 26, 1_000_000] {
        let (page, total) = state
            .fetch_page_from_store(&name, offset, 10)
            .await
            .unwrap();

        assert!(page.is_empty(), "offset {offset} should be empty");
        assert_eq!(total, 25);
    }

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn limit_must_be_between_1_and_100() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_limits");
    create_store(&state, &name, 1_000_000).await.unwrap();
    seed_store(&state, &name, 120).await;

    assert!(state.fetch_page_from_store(&name, 0, 0).await.is_err());
    assert!(state.fetch_page_from_store(&name, 0, 101).await.is_err());
    assert!(state.fetch_page_from_store(&name, 0, 1_000).await.is_err());

    let (one, _) = state.fetch_page_from_store(&name, 0, 1).await.unwrap();
    let (hundred, total) = state.fetch_page_from_store(&name, 0, 100).await.unwrap();

    assert_eq!(one.len(), 1);
    assert_eq!(hundred.len(), 100);
    assert_eq!(total, 120);

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn limit_larger_than_remaining_entries_returns_the_rest() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_big_limit");
    create_store(&state, &name, 1_000_000).await.unwrap();
    seed_store(&state, &name, 25).await;

    let (page, total) = state.fetch_page_from_store(&name, 0, 100).await.unwrap();
    assert_eq!(total, 25);
    assert_eq!(page, expected_map(25));

    let (tail, _) = state.fetch_page_from_store(&name, 20, 100).await.unwrap();
    assert_eq!(tail.len(), 5);

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn empty_store_has_no_entries() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_empty");
    create_store(&state, &name, 1_000_000).await.unwrap();

    let (page, total) = state.fetch_page_from_store(&name, 0, 10).await.unwrap();
    assert!(page.is_empty());
    assert_eq!(total, 0);

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn missing_store_is_an_error() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_missing");

    assert!(state.fetch_page_from_store(&name, 0, 10).await.is_err());
}

#[tokio::test]
async fn pages_reflect_deletes_and_updates() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_mutations");
    create_store(&state, &name, 1_000_000).await.unwrap();
    seed_store(&state, &name, 10).await;

    state
        .delete_from_store(&name, "k3".into())
        .await
        .unwrap();

    state
        .write_in_store(&name, "k5".into(), "updated".into())
        .await
        .unwrap();

    let all = fetch_all_paged(&state, &name, 4).await;

    assert_eq!(all.len(), 9);
    assert!(!all.contains_key("k3"));
    assert_eq!(all.get("k5"), Some(&"updated".to_string()));

    let (_, total) = state.fetch_page_from_store(&name, 0, 1).await.unwrap();
    assert_eq!(total, 9);

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn different_page_sizes_reconstruct_the_same_state() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("pages_sizes");
    create_store(&state, &name, 1_000_000).await.unwrap();
    seed_store(&state, &name, 37).await;

    for page_size in [1, 7, 10, 37, 100] {
        let all = fetch_all_paged(&state, &name, page_size).await;
        assert_eq!(all, expected_map(37), "page_size {page_size}");
    }

    cleanup_store(&state, &name).await;
}