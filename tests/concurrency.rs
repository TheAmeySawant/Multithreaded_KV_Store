mod common;

use common::{
    cleanup_store,
    connect_test_store,
};

use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_are_all_preserved() {
    let (name, store) =
        connect_test_store("concurrent_writes", 100_000);

    let number_of_tasks = 16;
    let writes_per_task = 100;

    let mut handles = Vec::new();

    for task_id in 0..number_of_tasks {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            for i in 0..writes_per_task {
                let key = format!("key_{task_id}_{i}");
                let value = format!("value_{task_id}_{i}");

                let result = store
                    .write(key.clone(), value.clone())
                    .await;

                assert!(
                    result.is_ok(),
                    "Write failed for key: {key}"
                );

                let output = result.unwrap().unwrap();

                assert_eq!(output.key, key);
                assert_eq!(output.value, value);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let all = store.fetch_all().await;

    let expected_number_of_keys =
        number_of_tasks * writes_per_task;

    assert_eq!(
        all.len(),
        expected_number_of_keys
    );

    for task_id in 0..number_of_tasks {
        for i in 0..writes_per_task {
            let key = format!("key_{task_id}_{i}");
            let expected_value =
                format!("value_{task_id}_{i}");

            assert_eq!(
                all.get(&key),
                Some(&expected_value),
                "Incorrect value for key: {key}"
            );
        }
    }

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reads_are_safe() {
    let (name, store) =
        connect_test_store("concurrent_reads", 100_000);

    let number_of_keys = 100;

    // Prepare data before starting concurrent readers.
    for i in 0..number_of_keys {
        let key = format!("key_{i}");
        let value = format!("value_{i}");

        store
            .write(key, value)
            .await
            .unwrap();
    }

    let number_of_readers = 16;
    let reads_per_reader = 100;

    let mut handles = Vec::new();

    for reader_id in 0..number_of_readers {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            for i in 0..reads_per_reader {
                let key_id = i % number_of_keys;

                let key = format!("key_{key_id}");
                let expected_value =
                    format!("value_{key_id}");

                let output = store
                    .read(key.clone())
                    .await;

                assert!(
                    output.is_some(),
                    "Reader {reader_id} could not find key: {key}"
                );

                let output = output.unwrap();

                assert_eq!(output.key, key);
                assert_eq!(output.value, expected_value);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let all = store.fetch_all().await;

    assert_eq!(
        all.len(),
        number_of_keys
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_reads_and_writes_are_safe() {
    let (name, store) =
        connect_test_store("concurrent_read_write", 100_000);

    let number_of_keys = 100;

    // Create the initial state.
    for i in 0..number_of_keys {
        let key = format!("key_{i}");
        let value = format!("initial_{i}");

        store
            .write(key, value)
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Spawn readers.
    for reader_id in 0..8 {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            for i in 0..500 {
                let key_id = i % number_of_keys;
                let key = format!("key_{key_id}");

                let result = store
                    .read(key.clone())
                    .await;

                assert!(
                    result.is_some(),
                    "Reader {reader_id} observed missing key: {key}"
                );
            }
        });

        handles.push(handle);
    }

    // Spawn writers.
    for writer_id in 0..8 {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key_id =
                    (writer_id * 13 + i) % number_of_keys;

                let key = format!("key_{key_id}");
                let value =
                    format!("writer_{writer_id}_value_{i}");

                store
                    .write(key, value)
                    .await
                    .unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Every original key must still exist.
    let all = store.fetch_all().await;

    assert_eq!(
        all.len(),
        number_of_keys
    );

    for i in 0..number_of_keys {
        let key = format!("key_{i}");

        assert!(
            all.contains_key(&key),
            "Key disappeared during concurrent operations: {key}"
        );
    }

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_to_same_key_produce_one_valid_final_value() {
    let (name, store) =
        connect_test_store("same_key_writes", 100_000);

    let number_of_writers = 32;

    let mut handles = Vec::new();

    for writer_id in 0..number_of_writers {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            let value =
                format!("value_from_writer_{writer_id}");

            store
                .write("shared_key".into(), value.clone())
                .await
                .unwrap();

            value
        });

        handles.push(handle);
    }

    let mut values_written = Vec::new();

    for handle in handles {
        let value = handle.await.unwrap();

        values_written.push(value);
    }

    let final_value = store
        .read("shared_key".into())
        .await
        .unwrap()
        .value;

    // The final value must be one of the values actually written.
    assert!(
        values_written.contains(&final_value),
        "Final value was not produced by any writer: {final_value}"
    );

    // There must still be exactly one key.
    let all = store.fetch_all().await;

    assert_eq!(all.len(), 1);
    assert_eq!(
        all.get("shared_key"),
        Some(&final_value)
    );

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_writes_and_deletes_are_safe() {
    let (name, store) =
        connect_test_store("concurrent_write_delete", 100_000);

    let number_of_keys = 100;

    // Initial state.
    for i in 0..number_of_keys {
        let key = format!("key_{i}");

        store
            .write(key, "initial".into())
            .await
            .unwrap();
    }

    let mut handles = Vec::new();

    // Writers repeatedly update keys.
    for writer_id in 0..8 {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key_id =
                    (writer_id * 17 + i) % number_of_keys;

                let key = format!("key_{key_id}");
                let value =
                    format!("writer_{writer_id}_{i}");

                store
                    .write(key, value)
                    .await
                    .unwrap();
            }
        });

        handles.push(handle);
    }

    // Deleters remove keys concurrently.
    for deleter_id in 0..8 {
        let store = Arc::clone(&store);

        let handle = tokio::spawn(async move {
            for i in 0..100 {
                let key_id =
                    (deleter_id * 19 + i) % number_of_keys;

                let key = format!("key_{key_id}");

                store
                    .delete(key)
                    .await
                    .unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // The important property here is that the store remains internally
    // consistent and no operation causes a panic or corruption.
    let all = store.fetch_all().await;

    assert!(
        all.len() <= number_of_keys,
        "Store contains more keys than were possible"
    );

    for (key, value) in &all {
        assert!(
            key.starts_with("key_"),
            "Unexpected key: {key}"
        );

        assert!(
            !value.is_empty(),
            "Value for {key} should not be empty"
        );
    }

    store
        .graceful_shutdown()
        .await
        .unwrap();

    cleanup_store(&name);
}