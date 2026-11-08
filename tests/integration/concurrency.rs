use crate::common::{
    app_state, cleanup_store, create_store, fetch_all, fetch_all_paged, serial_guard,
    unique_store_name,
};
use tokio::{
    task::JoinSet,
    time::{timeout, Duration},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
                    .write_in_store(&name, key, value)
                    .await
                    .expect("concurrent write failed");
            }
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let all = fetch_all(&state, &name).await;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_mixed_writes_and_reads_preserve_consistent_values() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("concurrent_mixed");

    create_store(&state, &name, 1_000_000).await.unwrap();

    for i in 0..100u32 {
        state
            .write_in_store(&name, format!("k{i}"), format!("v{i}"))
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
                    .read_from_store(&name, format!("k{i}"))
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

    let all = fetch_all(&state, &name).await;

    assert_eq!(all.len(), 100);

    cleanup_store(&state, &name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
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
                .write_in_store(&name, "shared".into(), format!("value-{worker}"))
                .await
                .expect("write failed");
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.unwrap();
    }

    let all = fetch_all(&state, &name).await;

    assert_eq!(all.len(), 1);

    let final_value = all.get("shared").unwrap();

    assert!(final_value.starts_with("value-"));

    cleanup_store(&state, &name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_page_reads_during_writes_stay_consistent() {
    let _guard = serial_guard().await;
    let state = app_state();
    let name = unique_store_name("concurrent_pages");

    create_store(&state, &name, 1_000_000).await.unwrap();

    let mut tasks = JoinSet::new();

    // 4 writers x 125 keys. Every value is derived from its key ("k7" -> "v7").
    for worker in 0..4usize {
        let state = state.clone();
        let name = name.clone();

        tasks.spawn(async move {
            for i in (worker * 125)..((worker + 1) * 125) {
                state
                    .write_in_store(&name, format!("k{i}"), format!("v{i}"))
                    .await
                    .expect("write failed");
            }
        });
    }

    // 4 readers paging while the writes are in flight.
    for reader in 0..4usize {
        let state = state.clone();
        let name = name.clone();

        tasks.spawn(async move {
            for iter in 0..100usize {
                let offset = (reader * 31 + iter * 7) % 600;

                let (page, total) = state
                    .fetch_page_from_store(&name, offset, 50)
                    .await
                    .expect("page fetch failed");

                assert!(page.len() <= 50, "page exceeded limit");
                assert!(total <= 500, "total can never exceed the keys being written");

                // No torn entries: a value always matches its key.
                for (k, v) in &page {
                    assert_eq!(v, &format!("v{}", &k[1..]), "inconsistent pair {k}={v}");
                }
            }
        });
    }

    timeout(Duration::from_secs(60), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .expect("tasks timed out: possible lock starvation or deadlock");

    // Writes have stopped, so paging must now reproduce the exact final state.
    let paged = fetch_all_paged(&state, &name, 64).await;

    assert_eq!(paged.len(), 500);

    for i in 0..500usize {
        assert_eq!(paged.get(&format!("k{i}")), Some(&format!("v{i}")));
    }

    cleanup_store(&state, &name).await;
}