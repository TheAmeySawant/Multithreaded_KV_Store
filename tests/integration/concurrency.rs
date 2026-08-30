use crate::common::{
    app_state, cleanup_store, create_store, serial_guard, unique_store_name,
};
use tokio::task::JoinSet;

#[tokio::test]
async fn concurrent_writes_to_distinct_keys_are_all_durable_in_memory() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("concurrent_writes");

    create_store(&state, &name, 1_000_000).await.unwrap();

    let workers = 20usize;
    let writes_per_worker = 25usize;
    let mut tasks = JoinSet::new();

    for worker in 0..workers {
        let state = state.clone();
        let name = name.clone();

        tasks.spawn(async move {
            for i in 0..writes_per_worker {
                let key = format!("w{worker}_k{i}");
                let value = format!("value-{worker}-{i}");

                state
                    .write_store(&name, key, value)
                    .await
                    .expect("concurrent write failed");
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let all = state.fetch_all_store(&name).await.unwrap();

    assert_eq!(all.len(), workers * writes_per_worker);

    for worker in 0..workers {
        for i in 0..writes_per_worker {
            let key = format!("w{worker}_k{i}");
            let value = format!("value-{worker}-{i}");

            assert_eq!(all.get(&key), Some(&value));
        }
    }

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn concurrent_mixed_writes_and_reads_preserve_consistent_values() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("concurrent_mixed");

    create_store(&state, &name, 1_000_000).await.unwrap();

    for i in 0..100u32 {
        state
            .write_store(&name, format!("k{i}"), format!("v{i}"))
            .await
            .unwrap();
    }

    let mut tasks = JoinSet::new();

    for worker in 0..20u32 {
        let state = state.clone();
        let name = name.clone();

        tasks.spawn(async move {
            let start = worker * 5;

            for i in start..start + 5 {
                let result = state
                    .read_store(&name, format!("k{i}"))
                    .await
                    .expect("read failed")
                    .expect("expected key to exist");

                assert_eq!(result.value, format!("v{i}"));
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let all = state.fetch_all_store(&name).await.unwrap();

    assert_eq!(all.len(), 100);

    cleanup_store(&state, &name).await;
}

#[tokio::test]
async fn concurrent_updates_of_same_key_end_in_one_complete_value() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("concurrent_same_key");

    create_store(&state, &name, 1_000_000).await.unwrap();

    let mut tasks = JoinSet::new();

    for worker in 0..100u32 {
        let state = state.clone();
        let name = name.clone();

        tasks.spawn(async move {
            state
                .write_store(
                    &name,
                    "shared".into(),
                    format!("value-{worker}"),
                )
                .await
                .expect("write failed");
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let all = state.fetch_all_store(&name).await.unwrap();

    assert_eq!(all.len(), 1);

    let final_value = all.get("shared").unwrap();

    assert!(final_value.starts_with("value-"));

    cleanup_store(&state, &name).await;
}