mod bench_common;

use std::sync::{Arc, Weak};

use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, BillingMode, KeySchemaElement, KeyType, OnDemandThroughput,
    ScalarAttributeType, Select, WarmThroughput,
};
use aws_types::region::Region;
use bench_common::{
    BENCH_RECORD_COUNTS, BENCH_WORKER_COUNTS, BenchKeyStruct, BenchTestStruct, bench_iter,
    build_indexed_batch, generate_test_data, // used by generate_dynamodb_test_data
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use dbsp::circuit::tokio::TOKIO;
use dbsp_adapters::Encoder;
use dbsp_adapters::SerBatch;
use dbsp_adapters::integrated::{
    DynamoDBOutputEndpoint, dynamodb_bench_stats, reset_dynamodb_bench_stats,
};
use feldera_adapterlib::transport::OutputBatchType;
use feldera_types::transport::dynamodb::{DynamoDBWriteMode, DynamoDBWriterConfig};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// DynamoDB-specific helpers
// ---------------------------------------------------------------------------

fn dynamodb_endpoint_url() -> Option<String> {
    std::env::var("DYNAMODB_ENDPOINT")
        .ok()
        .filter(|endpoint| !endpoint.trim().is_empty())
}

fn dynamodb_region() -> String {
    std::env::var("DYNAMODB_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

fn dynamodb_client(endpoint_url: Option<&str>) -> Client {
    let mut config_builder =
        aws_sdk_dynamodb::Config::builder().region(Region::new(dynamodb_region()));

    if let Some(endpoint_url) = endpoint_url {
        let credentials =
            aws_sdk_dynamodb::config::Credentials::new("dummy", "dummy", None, None, "bench");
        config_builder = config_builder
            .endpoint_url(endpoint_url)
            .credentials_provider(credentials);
    } else {
        let provider = TOKIO.block_on(async {
            aws_config::default_provider::credentials::default_provider().await
        });
        config_builder = config_builder.credentials_provider(provider);
    }

    Client::from_conf(config_builder.build())
}

fn env_i64(name: &str) -> Option<i64> {
    std::env::var(name).ok().and_then(|value| {
        value
            .parse::<i64>()
            .map_err(|error| {
                eprintln!("Ignoring invalid {name}={value:?}: {error}");
                error
            })
            .ok()
    })
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|value| {
        value
            .parse::<usize>()
            .map_err(|error| {
                eprintln!("Ignoring invalid {name}={value:?}: {error}");
                error
            })
            .ok()
    })
}

fn warm_throughput_from_env() -> Option<WarmThroughput> {
    let read_units = env_i64("DYNAMODB_WARM_READ_UNITS");
    let write_units = env_i64("DYNAMODB_WARM_WRITE_UNITS");

    if read_units.is_none() && write_units.is_none() {
        return None;
    }

    Some(
        WarmThroughput::builder()
            .set_read_units_per_second(read_units)
            .set_write_units_per_second(write_units)
            .build(),
    )
}

fn on_demand_throughput_from_env() -> Option<OnDemandThroughput> {
    let read_units = env_i64("DYNAMODB_MAX_READ_REQUEST_UNITS");
    let write_units = env_i64("DYNAMODB_MAX_WRITE_REQUEST_UNITS");

    if read_units.is_none() && write_units.is_none() {
        return None;
    }

    Some(
        OnDemandThroughput::builder()
            .set_max_read_request_units(read_units)
            .set_max_write_request_units(write_units)
            .build(),
    )
}

fn wait_for_table(client: &Client, table: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
    loop {
        let active = TOKIO
            .block_on(client.describe_table().table_name(table).send())
            .ok()
            .is_some_and(|output| {
                output
                    .table()
                    .and_then(|table| table.table_status())
                    .is_some_and(|status| status.as_str() == "ACTIVE")
            });

        if active {
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for DynamoDB benchmark table {table}"
        );
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

fn wait_for_table_deleted(client: &Client, table: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    loop {
        let missing = TOKIO
            .block_on(client.describe_table().table_name(table).send())
            .is_err();

        if missing {
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for DynamoDB benchmark table {table} to be deleted"
        );
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

fn create_bench_table(client: &Client, table: &str) {
    // Attempt delete unconditionally so that tables left in DELETING state
    // from a previous run don't cause create to fail with ResourceInUseException.
    let _ = TOKIO.block_on(client.delete_table().table_name(table).send());
    wait_for_table_deleted(client, table);

    let mut create = client
        .create_table()
        .table_name(table)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::N)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest);

    if let Some(warm_throughput) = warm_throughput_from_env() {
        create = create.warm_throughput(warm_throughput);
    }
    if let Some(on_demand_throughput) = on_demand_throughput_from_env() {
        create = create.on_demand_throughput(on_demand_throughput);
    }

    TOKIO
        .block_on(create.send())
        .expect("failed to create DynamoDB benchmark table");
    wait_for_table(client, table);
}

fn update_bench_table_throughput(client: &Client, table: &str) {
    let warm_throughput = warm_throughput_from_env();
    let on_demand_throughput = on_demand_throughput_from_env();

    if warm_throughput.is_none() && on_demand_throughput.is_none() {
        return;
    }

    let mut update = client.update_table().table_name(table);
    if let Some(warm_throughput) = warm_throughput {
        update = update.warm_throughput(warm_throughput);
    }
    if let Some(on_demand_throughput) = on_demand_throughput {
        update = update.on_demand_throughput(on_demand_throughput);
    }

    TOKIO
        .block_on(update.send())
        .expect("failed to update DynamoDB benchmark table throughput");
    wait_for_table(client, table);
}

fn drop_bench_table(client: &Client, table: &str) {
    let _ = TOKIO.block_on(client.delete_table().table_name(table).send());
}

/// Returns the number of items in `table` using a paginated `Scan` with
/// `Select::Count` so no item data is transferred.
fn count_table_items(client: &Client, table: &str) -> usize {
    let mut count = 0i64;
    let mut last_key = None;
    loop {
        let mut req = client.scan().table_name(table).select(Select::Count);
        if let Some(key) = last_key {
            req = req.set_exclusive_start_key(Some(key));
        }
        let resp = TOKIO
            .block_on(req.send())
            .expect("failed to scan DynamoDB benchmark table");
        count += resp.count() as i64;
        last_key = resp.last_evaluated_key;
        if last_key.is_none() {
            break;
        }
    }
    count as usize
}

fn make_config(table: &str, endpoint_url: Option<&str>, threads: usize) -> DynamoDBWriterConfig {
    let use_static_dummy_credentials = endpoint_url.is_some();
    DynamoDBWriterConfig {
        table: table.to_string(),
        region: dynamodb_region(),
        endpoint_url: endpoint_url.map(str::to_string),
        aws_access_key_id: use_static_dummy_credentials.then(|| "dummy".to_string()),
        aws_secret_access_key: use_static_dummy_credentials.then(|| "dummy".to_string()),
        batch_size: None,
        write_mode: DynamoDBWriteMode::Batch,
        max_buffer_size_bytes: 1024 * 1024,
        max_concurrent_requests: 16,
        threads,
        max_retries: 10,
    }
}

fn make_config_with_concurrency(
    table: &str,
    endpoint_url: Option<&str>,
    threads: usize,
    max_concurrent_requests: usize,
) -> DynamoDBWriterConfig {
    DynamoDBWriterConfig {
        max_concurrent_requests,
        ..make_config(table, endpoint_url, threads)
    }
}

fn create_endpoint(config: &DynamoDBWriterConfig) -> DynamoDBOutputEndpoint {
    DynamoDBOutputEndpoint::new(
        Default::default(),
        "bench_endpoint",
        config,
        &Some(BenchKeyStruct::relation_schema()),
        &BenchTestStruct::relation_schema(),
        Weak::new(),
        true,
    )
    .expect("failed to create DynamoDBOutputEndpoint")
}


fn run_endpoint_batch(endpoint: &mut DynamoDBOutputEndpoint, batch: &Arc<dyn SerBatch>) {
    endpoint.consumer().batch_start(0, OutputBatchType::Delta);
    endpoint
        .encode(batch.clone().arc_as_batch_reader())
        .unwrap();
    endpoint.consumer().batch_end();
}

fn bench_endpoint_iter(
    endpoint: &mut DynamoDBOutputEndpoint,
    batch: &Arc<dyn SerBatch>,
    iters: u64,
) -> std::time::Duration {
    bench_iter(iters, || run_endpoint_batch(endpoint, batch), || {})
}

fn report_dynamodb_stats(label: &str, num_records: usize) -> u64 {
    let stats = dynamodb_bench_stats();
    let capacity_per_requested_item = if stats.requested_items == 0 {
        0.0
    } else {
        stats.consumed_capacity_units / stats.requested_items as f64
    };
    let throttle_rate = if stats.requested_items == 0 {
        0.0
    } else {
        stats.throttled_requests as f64 / stats.requested_items as f64 * 100.0
    };
    eprintln!(
        "{label}: requested_items={}, consumed_write_capacity_units={:.3}, capacity_per_item={:.6}, unprocessed_items={}, retries={} (throttle={}, error={}), throttled_requests={} ({throttle_rate:.1}%)",
        stats.requested_items,
        stats.consumed_capacity_units,
        capacity_per_requested_item,
        stats.unprocessed_items,
        stats.retries,
        stats.throttled_requests,
        stats.error_retries,
        stats.throttled_requests,
    );
    if stats.requested_items > 0 && stats.requested_items < num_records as u64 {
        eprintln!(
            "{label}: requested_items is below one full benchmark batch ({num_records}); this usually means the benchmark did not run a complete sample"
        );
    }

    let mut durations = stats.flush_durations_us;
    if !durations.is_empty() {
        durations.sort_unstable();
        let n = durations.len();
        let p50 = durations[n * 50 / 100];
        let p95 = durations[n * 95 / 100];
        let p99 = durations[n * 99 / 100];
        let max = *durations.last().unwrap();
        eprintln!(
            "{label}: flush_duration_ms: p50={:.1}, p95={:.1}, p99={:.1}, max={:.1} (n={n})",
            p50 as f64 / 1000.0,
            p95 as f64 / 1000.0,
            p99 as f64 / 1000.0,
            max as f64 / 1000.0,
        );
    }

    stats.requested_items
}

fn verify_table_once(client: &Client, table: &str, num_records: usize, label: &str) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let count = count_table_items(client, table);
        if count == num_records {
            return;
        }

        assert!(
            std::time::Instant::now() < deadline,
            "{label}: expected {num_records} DynamoDB items in {table}, observed {count}"
        );
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

fn finish_dynamodb_benchmark(client: &Client, table: &str, num_records: usize, label: &str) {
    let requested_items = report_dynamodb_stats(label, num_records);
    if requested_items == 0 {
        eprintln!(
            "{label}: skipping final table verification because Criterion did not execute this filtered benchmark"
        );
        return;
    }
    verify_table_once(client, table, num_records, label);
}

/// Generate test data with IDs spread across the partition key space via a
/// multiplicative hash. This prevents all writes landing on the same DynamoDB
/// partition, which would artificially cap throughput.
fn generate_dynamodb_test_data(num_records: usize) -> Vec<BenchTestStruct> {
    let mut data = generate_test_data(num_records);
    for record in &mut data {
        record.id = record.id.wrapping_mul(2_654_435_761);
    }
    data
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Benchmark DynamoDB encode+flush with 100k records across 1/2/4/8 worker threads.
///
/// Mirrors `bench_postgres_encode` so the two connectors can be compared
/// side-by-side using Criterion's per-worker throughput report.
fn bench_dynamodb_encode_flush(c: &mut Criterion) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    let num_records = 100_000;
    let data = generate_dynamodb_test_data(num_records);
    let batch = build_indexed_batch(&data);
    let run_id = Uuid::new_v4().simple().to_string();

    let mut group = c.benchmark_group("dynamodb_output_encode_flush");
    group.throughput(criterion::Throughput::Elements(num_records as u64));
    group.sample_size(10);

    for workers in BENCH_WORKER_COUNTS {
        let table = format!("{run_id}_workers_{workers}");
        create_bench_table(&client, &table);

        let config = make_config(&table, endpoint_url.as_deref(), workers);
        reset_dynamodb_bench_stats();
        group.bench_with_input(BenchmarkId::new("workers", workers), &workers, |b, _| {
            let mut endpoint = create_endpoint(&config);
            b.iter_custom(|iters| bench_endpoint_iter(&mut endpoint, &batch, iters));
        });
        finish_dynamodb_benchmark(
            &client,
            &table,
            num_records,
            &format!("dynamodb_output_encode_flush/workers/{workers}"),
        );

        drop_bench_table(&client, &table);
    }

    group.finish();
}

/// Benchmark DynamoDB encode+flush while sweeping the number of in-flight
/// DynamoDB write requests per worker.
///
/// This helps distinguish connector under-driving from DynamoDB Local/server
/// saturation. If throughput plateaus as this value increases, the endpoint is
/// the bottleneck.
fn bench_dynamodb_encode_flush_concurrency(c: &mut Criterion) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    let num_records = 100_000;
    let data = generate_dynamodb_test_data(num_records);
    let batch = build_indexed_batch(&data);
    let workers = env_usize("DYNAMODB_BENCH_WORKERS").unwrap_or(8);
    let concurrency_values = env_usize("DYNAMODB_BENCH_CONCURRENCY")
        .map(|value| vec![value])
        .unwrap_or_else(|| vec![1, 2, 4, 8, 16, 32, 64]);
    let run_id = Uuid::new_v4().simple().to_string();

    let mut group = c.benchmark_group("dynamodb_output_encode_flush_concurrency");
    group.throughput(criterion::Throughput::Elements(num_records as u64));
    group.sample_size(10);

    for max_concurrent_requests in concurrency_values {
        let table = format!("{run_id}_concurrency_{max_concurrent_requests}");
        create_bench_table(&client, &table);

        let config = make_config_with_concurrency(
            &table,
            endpoint_url.as_deref(),
            workers,
            max_concurrent_requests,
        );
        reset_dynamodb_bench_stats();
        group.bench_with_input(
            BenchmarkId::new("max_concurrent_requests", max_concurrent_requests),
            &max_concurrent_requests,
            |b, _| {
                let mut endpoint = create_endpoint(&config);
                b.iter_custom(|iters| bench_endpoint_iter(&mut endpoint, &batch, iters));
            },
        );
        finish_dynamodb_benchmark(
            &client,
            &table,
            num_records,
            &format!(
                "dynamodb_output_encode_flush_concurrency/max_concurrent_requests/{max_concurrent_requests}"
            ),
        );

        drop_bench_table(&client, &table);
    }

    group.finish();
}

/// Benchmark a high-parallelism DynamoDB encode+flush stress point.
fn bench_dynamodb_encode_flush_8_workers_64_concurrent(c: &mut Criterion) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    let num_records = 100_000;
    let data = generate_dynamodb_test_data(num_records);
    let batch = build_indexed_batch(&data);
    let workers = 8;
    let max_concurrent_requests = 64;
    let configured_table = std::env::var("DYNAMODB_BENCH_TABLE").ok();
    let generated_table;
    let table = if let Some(ref t) = configured_table {
        t.as_str()
    } else {
        generated_table = format!("{}_8w_64c", Uuid::new_v4().simple());
        generated_table.as_str()
    };

    if configured_table.is_some() {
        wait_for_table(&client, table);
        update_bench_table_throughput(&client, table);
    } else {
        create_bench_table(&client, table);
    }

    let config = make_config_with_concurrency(
        table,
        endpoint_url.as_deref(),
        workers,
        max_concurrent_requests,
    );
    let mut group = c.benchmark_group("dynamodb_output_encode_flush_8_workers_64_concurrent");
    group.throughput(criterion::Throughput::Elements(num_records as u64));
    group.sample_size(10);
    reset_dynamodb_bench_stats();
    group.bench_function("100000_records", |b| {
        let mut endpoint = create_endpoint(&config);
        b.iter_custom(|iters| bench_endpoint_iter(&mut endpoint, &batch, iters));
    });
    group.finish();
    finish_dynamodb_benchmark(
        &client,
        table,
        num_records,
        "dynamodb_output_encode_flush_8_workers_64_concurrent/100000_records",
    );

    if configured_table.is_none() {
        drop_bench_table(&client, table);
    }
}

/// Benchmark DynamoDB encode+flush scaling: 100k/1M/2M records × 1/2/4/8 workers.
///
/// The benchmark IDs include both record count and worker count so Criterion
/// can report throughput per configuration.
fn bench_dynamodb_encode_flush_scaling(c: &mut Criterion) {
    let endpoint_url = dynamodb_endpoint_url();
    let client = dynamodb_client(endpoint_url.as_deref());
    let run_id = Uuid::new_v4().simple().to_string();

    let mut group = c.benchmark_group("dynamodb_output_encode_flush_scaling");
    group.sample_size(10);

    for num_records in BENCH_RECORD_COUNTS {
        let data = generate_dynamodb_test_data(num_records);
        let batch = build_indexed_batch(&data);

        for workers in BENCH_WORKER_COUNTS {
            let table = format!("{run_id}_{num_records}_{workers}");
            create_bench_table(&client, &table);

            let config = make_config(&table, endpoint_url.as_deref(), workers);
            group.throughput(criterion::Throughput::Elements(num_records as u64));
            reset_dynamodb_bench_stats();
            group.bench_with_input(
                BenchmarkId::new(format!("{num_records}_records"), workers),
                &workers,
                |b, _| {
                    let mut endpoint = create_endpoint(&config);
                    b.iter_custom(|iters| bench_endpoint_iter(&mut endpoint, &batch, iters));
                },
            );
            finish_dynamodb_benchmark(
                &client,
                &table,
                num_records,
                &format!("dynamodb_output_encode_flush_scaling/{num_records}_records/{workers}"),
            );

            drop_bench_table(&client, &table);
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_dynamodb_encode_flush,
    bench_dynamodb_encode_flush_concurrency,
    bench_dynamodb_encode_flush_8_workers_64_concurrent,
    bench_dynamodb_encode_flush_scaling,
);
criterion_main!(benches);
