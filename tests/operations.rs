mod common;

use common::{
    cleanup_store,
    connect_test_store,
};

#[tokio::test]
async fn write_and_read_work() {
    let (name, store) =
        connect_test_store("write_read", 1000);

    // Write a key-value pair.
    let output = store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    // write() should return the written key-value pair.
    let output = output.unwrap();

    assert_eq!(output.key, "name");
    assert_eq!(output.value, "Amey");

    // Read the same key.
    let output = store
        .read("name".into())
        .await
        .unwrap();

    assert_eq!(output.key, "name");
    assert_eq!(output.value, "Amey");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn read_returns_none_for_missing_key() {
    let (name, store) =
        connect_test_store("read_missing", 1000);

    let output = store
        .read("does_not_exist".into())
        .await;

    assert!(output.is_none());

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn write_overwrites_existing_value() {
    let (name, store) =
        connect_test_store("overwrite", 1000);

    // First write.
    store
        .write("name".into(), "first".into())
        .await
        .unwrap();

    // Second write to the same key.
    store
        .write("name".into(), "second".into())
        .await
        .unwrap();

    // The latest value should be returned.
    let result = store
        .read("name".into())
        .await
        .unwrap();

    assert_eq!(result.key, "name");
    assert_eq!(result.value, "second");

    // There should still be only one key.
    let all = store.fetch_all().await;

    assert_eq!(all.len(), 1);
    assert_eq!(all["name"], "second");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn keys_are_case_sensitive() {
    let (name, store) =
        connect_test_store("key_case", 1000);

    store
        .write("name".into(), "lower".into())
        .await
        .unwrap();

    store
        .write("Name".into(), "upper".into())
        .await
        .unwrap();

    let lower = store
        .read("name".into())
        .await
        .unwrap();

    let upper = store
        .read("Name".into())
        .await
        .unwrap();

    assert_eq!(lower.key, "name");
    assert_eq!(lower.value, "lower");

    assert_eq!(upper.key, "Name");
    assert_eq!(upper.value, "upper");

    // Both keys should exist independently.
    let all = store.fetch_all().await;

    assert_eq!(all.len(), 2);
    assert_eq!(all["name"], "lower");
    assert_eq!(all["Name"], "upper");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn fetch_all_returns_all_key_value_pairs() {
    let (name, store) =
        connect_test_store("fetch_all", 1000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("city".into(), "Mumbai".into())
        .await
        .unwrap();

    store
        .write("language".into(), "Rust".into())
        .await
        .unwrap();

    let all = store.fetch_all().await;

    assert_eq!(all.len(), 3);

    assert_eq!(all["name"], "Amey");
    assert_eq!(all["city"], "Mumbai");
    assert_eq!(all["language"], "Rust");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn fetch_all_returns_empty_map_for_empty_store() {
    let (name, store) =
        connect_test_store("fetch_all_empty", 1000);

    let all = store.fetch_all().await;

    assert!(all.is_empty());

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn delete_existing_key_returns_deleted_value() {
    let (name, store) =
        connect_test_store("delete_existing", 1000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    let deleted = store
        .delete("name".into())
        .await
        .unwrap();

    let deleted = deleted.unwrap();

    // delete() should return the key-value pair that was removed.
    assert_eq!(deleted.key, "name");
    assert_eq!(deleted.value, "Amey");

    // The key should no longer exist.
    assert!(
        store
            .read("name".into())
            .await
            .is_none()
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn delete_missing_key_returns_none() {
    let (name, store) =
        connect_test_store("delete_missing", 1000);

    let deleted = store
        .delete("does_not_exist".into())
        .await
        .unwrap();

    assert!(deleted.is_none());

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn delete_does_not_affect_other_keys() {
    let (name, store) =
        connect_test_store("delete_isolation", 1000);

    store
        .write("name".into(), "Amey".into())
        .await
        .unwrap();

    store
        .write("city".into(), "Mumbai".into())
        .await
        .unwrap();

    store
        .delete("name".into())
        .await
        .unwrap();

    // Deleted key should be gone.
    assert!(
        store
            .read("name".into())
            .await
            .is_none()
    );

    // Other key should remain untouched.
    let city = store
        .read("city".into())
        .await
        .unwrap();

    assert_eq!(city.key, "city");
    assert_eq!(city.value, "Mumbai");

    let all = store.fetch_all().await;

    assert_eq!(all.len(), 1);
    assert_eq!(all["city"], "Mumbai");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn write_empty_key_and_value() {
    let (name, store) =
        connect_test_store("empty_strings", 1000);

    let output = store
        .write("".into(), "".into())
        .await
        .unwrap()
        .unwrap();

    assert_eq!(output.key, "");
    assert_eq!(output.value, "");

    let result = store
        .read("".into())
        .await
        .unwrap();

    assert_eq!(result.key, "");
    assert_eq!(result.value, "");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test]
async fn unicode_keys_and_values_work() {
    let (name, store) =
        connect_test_store("unicode", 1000);

    store
        .write("नाव".into(), "अमेय".into())
        .await
        .unwrap();

    store
        .write("城市".into(), "Mumbai".into())
        .await
        .unwrap();

    let name_result = store
        .read("नाव".into())
        .await
        .unwrap();

    assert_eq!(name_result.key, "नाव");
    assert_eq!(name_result.value, "अमेय");

    let city_result = store
        .read("城市".into())
        .await
        .unwrap();

    assert_eq!(city_result.key, "城市");
    assert_eq!(city_result.value, "Mumbai");

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}