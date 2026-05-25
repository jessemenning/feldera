use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result as AnyResult, anyhow, bail};
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::types::{
    AttributeValue, Delete, DeleteRequest, Put, PutRequest, ReturnConsumedCapacity,
    TransactWriteItem, WriteRequest,
};
use aws_types::region::Region;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::catalog::SplitCursorBuilder;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputBatchType, Step};
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::transport::dynamodb::{DynamoDBWriteMode, DynamoDBWriterConfig};
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use futures_util::future::join_all;
use tracing::{debug, info, info_span, warn};

use crate::ControllerError;
use crate::catalog::{RecordFormat, SerBatchReader, SerCursor};
use crate::controller::{ControllerInner, EndpointId};
use crate::format::{Encoder, OutputConsumer};
use crate::transport::OutputEndpoint;
use crate::util::{IndexedOperationType, indexed_operation_type};

enum WorkerCommand {
    BatchStart,
    Encode(SplitCursorBuilder),
    BatchEnd,
    Shutdown,
}

enum WorkerResult {
    Ok { num_bytes: usize, num_rows: usize },
    Err(anyhow::Error),
}

struct DynamoDBWorker {
    endpoint_name: String,
    table: String,
    write_mode: DynamoDBWriteMode,
    batch_size: usize,
    max_retries: usize,
    max_concurrent_requests: usize,
    client: Client,
    key_schema: Relation,
    value_schema: Relation,
    key_field_names: Vec<String>,
    /// True when every key field name is also present in the value schema.
    /// When true, `val_to_dynamodb_item()` always includes all key fields, so
    /// `item()` can skip the per-record merge check.
    key_fields_in_value: bool,
    pub(crate) pending: Vec<WriteRequest>,
    pending_bytes: usize,
    max_buffer_size_bytes: usize,
    flush_records_threshold: usize,
    records_written: Arc<AtomicU64>,
    retries: Arc<AtomicU64>,
    pending_writes: Vec<tokio::task::JoinHandle<AnyResult<()>>>,
}

#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_CONSUMED_CAPACITY_MILLIS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_REQUESTED_ITEMS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_UNPROCESSED_ITEMS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_RETRIES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_THROTTLED_REQUESTS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_ERROR_RETRIES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "bench-mode")]
static DYNAMODB_BENCH_FLUSH_DURATIONS_US: std::sync::Mutex<Vec<u64>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(feature = "bench-mode")]
#[derive(Debug, Default, Clone)]
pub struct DynamoDBBenchStats {
    pub requested_items: u64,
    pub consumed_capacity_units: f64,
    pub unprocessed_items: u64,
    pub retries: u64,
    pub throttled_requests: u64,
    /// Retries caused by non-throttle errors (e.g. transient service errors).
    pub error_retries: u64,
    /// Per-flush durations in microseconds, one entry per `batch_end` call.
    pub flush_durations_us: Vec<u64>,
}

#[cfg(feature = "bench-mode")]
pub fn reset_dynamodb_bench_stats() {
    DYNAMODB_BENCH_CONSUMED_CAPACITY_MILLIS.store(0, Ordering::Relaxed);
    DYNAMODB_BENCH_REQUESTED_ITEMS.store(0, Ordering::Relaxed);
    DYNAMODB_BENCH_UNPROCESSED_ITEMS.store(0, Ordering::Relaxed);
    DYNAMODB_BENCH_RETRIES.store(0, Ordering::Relaxed);
    DYNAMODB_BENCH_THROTTLED_REQUESTS.store(0, Ordering::Relaxed);
    DYNAMODB_BENCH_ERROR_RETRIES.store(0, Ordering::Relaxed);
    DYNAMODB_BENCH_FLUSH_DURATIONS_US.lock().unwrap().clear();
}

#[cfg(feature = "bench-mode")]
pub fn dynamodb_bench_stats() -> DynamoDBBenchStats {
    DynamoDBBenchStats {
        requested_items: DYNAMODB_BENCH_REQUESTED_ITEMS.load(Ordering::Relaxed),
        consumed_capacity_units: DYNAMODB_BENCH_CONSUMED_CAPACITY_MILLIS.load(Ordering::Relaxed)
            as f64
            / 1000.0,
        unprocessed_items: DYNAMODB_BENCH_UNPROCESSED_ITEMS.load(Ordering::Relaxed),
        retries: DYNAMODB_BENCH_RETRIES.load(Ordering::Relaxed),
        throttled_requests: DYNAMODB_BENCH_THROTTLED_REQUESTS.load(Ordering::Relaxed),
        error_retries: DYNAMODB_BENCH_ERROR_RETRIES.load(Ordering::Relaxed),
        flush_durations_us: DYNAMODB_BENCH_FLUSH_DURATIONS_US.lock().unwrap().clone(),
    }
}

/// Returns true when an SDK error is a DynamoDB throttle (provisioned throughput
/// exceeded, account request-rate limit, or general throttling).
#[cfg(feature = "bench-mode")]
fn is_throttle_error(error: &impl std::fmt::Display) -> bool {
    let s = error.to_string();
    s.contains("ProvisionedThroughputExceededException")
        || s.contains("RequestLimitExceeded")
        || s.contains("ThrottlingException")
}

async fn write_transact_items(
    client: &Client,
    endpoint_name: &str,
    table: &str,
    requests: Vec<WriteRequest>,
    batch_size: usize,
    max_retries: usize,
    max_concurrent_requests: usize,
    retries: Arc<AtomicU64>,
) -> AnyResult<()> {
    let chunks = requests
        .chunks(batch_size)
        .map(|chunk| {
            chunk
                .iter()
                .map(|request| transact_write_item(table, request))
                .collect::<AnyResult<Vec<_>>>()
        })
        .collect::<AnyResult<Vec<_>>>()?;

    futures_util::stream::iter(chunks)
        .map(|transact_items| {
            write_transact_chunk(
                client.clone(),
                endpoint_name.to_string(),
                transact_items,
                max_retries,
                retries.clone(),
            )
        })
        .buffer_unordered(max_concurrent_requests)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(())
}

async fn write_batch_items(
    client: &Client,
    endpoint_name: &str,
    table: &str,
    requests: Vec<WriteRequest>,
    batch_size: usize,
    max_retries: usize,
    max_concurrent_requests: usize,
    retries: Arc<AtomicU64>,
) -> AnyResult<()> {
    let mut remaining = requests;
    let chunks = std::iter::from_fn(|| {
        if remaining.is_empty() {
            return None;
        }
        let n = batch_size.min(remaining.len());
        Some(remaining.drain(..n).collect::<Vec<_>>())
    })
    .collect::<Vec<_>>();

    futures_util::stream::iter(chunks)
        .map(|chunk| {
            write_batch_chunk(
                client.clone(),
                endpoint_name.to_string(),
                table.to_string(),
                chunk,
                max_retries,
                retries.clone(),
            )
        })
        .buffer_unordered(max_concurrent_requests)
        .try_collect::<Vec<_>>()
        .await?;

    Ok(())
}

fn transact_write_item(table: &str, request: &WriteRequest) -> AnyResult<TransactWriteItem> {
    match (request.put_request(), request.delete_request()) {
        (Some(put_request), None) => Ok(TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(table)
                    .set_item(Some(put_request.item().clone()))
                    .build()?,
            )
            .build()),
        (None, Some(delete_request)) => Ok(TransactWriteItem::builder()
            .delete(
                Delete::builder()
                    .table_name(table)
                    .set_key(Some(delete_request.key().clone()))
                    .build()?,
            )
            .build()),
        _ => bail!("expected exactly one put or delete request"),
    }
}

async fn write_transact_chunk(
    client: Client,
    endpoint_name: String,
    transact_items: Vec<TransactWriteItem>,
    max_retries: usize,
    retries: Arc<AtomicU64>,
) -> AnyResult<()> {
    #[cfg(feature = "bench-mode")]
    DYNAMODB_BENCH_REQUESTED_ITEMS.fetch_add(transact_items.len() as u64, Ordering::Relaxed);

    let mut retry = 0usize;
    loop {
        let result = client
            .transact_write_items()
            .set_transact_items(Some(transact_items.clone()))
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .await;

        match result {
            Ok(_output) => {
                #[cfg(feature = "bench-mode")]
                for capacity in _output.consumed_capacity() {
                    DYNAMODB_BENCH_CONSUMED_CAPACITY_MILLIS.fetch_add(
                        ((capacity.capacity_units().unwrap_or_default() * 1000.0).round()) as u64,
                        Ordering::Relaxed,
                    );
                }
                return Ok(());
            }
            Err(error) => {
                if retry >= max_retries {
                    return Err(error).with_context(|| {
                        format!(
                            "dynamodb output connector '{endpoint_name}' failed to commit transaction chunk after {max_retries} retries"
                        )
                    });
                }
                #[cfg(feature = "bench-mode")]
                if is_throttle_error(&error) {
                    DYNAMODB_BENCH_THROTTLED_REQUESTS.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        retry += 1;
        retries.fetch_add(1, Ordering::Relaxed);
        warn!(
            "dynamodb output connector '{endpoint_name}' retrying transaction chunk with {} item(s) ({retry}/{max_retries})",
            transact_items.len(),
        );
        #[cfg(feature = "bench-mode")]
        DYNAMODB_BENCH_RETRIES.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(backoff_delay(retry)).await;
    }
}

async fn write_batch_chunk(
    client: Client,
    endpoint_name: String,
    table: String,
    requests: Vec<WriteRequest>,
    max_retries: usize,
    retries: Arc<AtomicU64>,
) -> AnyResult<()> {
    #[cfg(feature = "bench-mode")]
    DYNAMODB_BENCH_REQUESTED_ITEMS.fetch_add(requests.len() as u64, Ordering::Relaxed);

    let mut request_items = HashMap::from([(table, requests)]);
    let mut retry = 0usize;

    loop {
        let result = client
            .batch_write_item()
            .set_request_items(Some(request_items.clone()))
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .send()
            .await;

        match result {
            Ok(output) => {
                #[cfg(feature = "bench-mode")]
                for capacity in output.consumed_capacity() {
                    DYNAMODB_BENCH_CONSUMED_CAPACITY_MILLIS.fetch_add(
                        ((capacity.capacity_units().unwrap_or_default() * 1000.0).round()) as u64,
                        Ordering::Relaxed,
                    );
                }

                let Some(unprocessed) = output.unprocessed_items() else {
                    return Ok(());
                };
                if unprocessed.is_empty() {
                    return Ok(());
                }

                if retry >= max_retries {
                    let count = unprocessed.values().map(Vec::len).sum::<usize>();
                    bail!(
                        "dynamodb output connector '{endpoint_name}' failed to write {count} unprocessed item(s) after {max_retries} retries"
                    );
                }

                request_items = unprocessed.clone();
                let count = request_items.values().map(Vec::len).sum::<usize>();
                retries.fetch_add(1, Ordering::Relaxed);
                warn!(
                    "dynamodb output connector '{endpoint_name}' retrying {count} unprocessed batch item(s) ({}/{max_retries})",
                    retry + 1,
                );
                #[cfg(feature = "bench-mode")]
                {
                    DYNAMODB_BENCH_UNPROCESSED_ITEMS.fetch_add(count as u64, Ordering::Relaxed);
                    DYNAMODB_BENCH_THROTTLED_REQUESTS.fetch_add(count as u64, Ordering::Relaxed);
                }
            }
            Err(error) => {
                if retry >= max_retries {
                    return Err(error).with_context(|| {
                        format!(
                            "dynamodb output connector '{endpoint_name}' failed to write batch chunk after {max_retries} retries"
                        )
                    });
                }
                retries.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "bench-mode")]
                if is_throttle_error(&error) {
                    DYNAMODB_BENCH_THROTTLED_REQUESTS.fetch_add(1, Ordering::Relaxed);
                } else {
                    DYNAMODB_BENCH_ERROR_RETRIES.fetch_add(1, Ordering::Relaxed);
                }
                warn!(
                    "dynamodb output connector '{endpoint_name}' retrying failed batch chunk ({}/{max_retries}): {error}",
                    retry + 1,
                );
            }
        }

        retry += 1;
        #[cfg(feature = "bench-mode")]
        DYNAMODB_BENCH_RETRIES.fetch_add(1, Ordering::Relaxed);
        tokio::time::sleep(backoff_delay(retry)).await;
    }
}

fn backoff_delay(retry: usize) -> Duration {
    Duration::from_millis(50 * (1u64 << retry.min(8)))
}

impl DynamoDBWorker {
    fn new(
        endpoint_name: &str,
        config: &DynamoDBWriterConfig,
        key_schema: &Relation,
        value_schema: &Relation,
        records_written: Arc<AtomicU64>,
        retries: Arc<AtomicU64>,
    ) -> Self {
        Self::with_client(
            make_client(config),
            endpoint_name,
            config,
            key_schema,
            value_schema,
            records_written,
            retries,
        )
    }

    fn with_client(
        client: Client,
        endpoint_name: &str,
        config: &DynamoDBWriterConfig,
        key_schema: &Relation,
        value_schema: &Relation,
        records_written: Arc<AtomicU64>,
        retries: Arc<AtomicU64>,
    ) -> Self {
        let key_field_names: Vec<String> = key_schema
            .fields
            .iter()
            .map(|field| field.name.name())
            .collect();
        let value_field_names: HashSet<String> = value_schema
            .fields
            .iter()
            .map(|field| field.name.name())
            .collect();
        let key_fields_in_value = key_field_names
            .iter()
            .all(|name| value_field_names.contains(name));
        Self {
            endpoint_name: endpoint_name.to_string(),
            table: config.table.clone(),
            write_mode: config.write_mode,
            batch_size: config.effective_batch_size(),
            max_retries: config.max_retries,
            max_concurrent_requests: config.max_concurrent_requests,
            client,
            key_schema: key_schema.clone(),
            value_schema: value_schema.clone(),
            key_field_names,
            key_fields_in_value,
            pending: Vec::new(),
            pending_bytes: 0,
            max_buffer_size_bytes: config.max_buffer_size_bytes,
            flush_records_threshold: config.effective_batch_size() * config.max_concurrent_requests,
            records_written,
            retries,
            pending_writes: Vec::new(),
        }
    }

    fn view_name(&self) -> &SqlIdentifier {
        &self.value_schema.name
    }

    fn index_name(&self) -> &SqlIdentifier {
        &self.key_schema.name
    }

    fn join_pending_writes(&mut self) -> AnyResult<()> {
        let handles = std::mem::take(&mut self.pending_writes);
        if handles.is_empty() {
            return Ok(());
        }
        let mut errors: Vec<anyhow::Error> = Vec::new();
        for result in TOKIO.block_on(join_all(handles)) {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(anyhow!("write task panicked: {e}")),
            }
        }
        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("DynamoDB writes failed: {msg}");
        }
        Ok(())
    }

    fn batch_start_inner(&mut self) {
        self.pending.clear();
        self.pending_bytes = 0;
    }

    fn batch_end_inner(&mut self) -> AnyResult<(usize, usize)> {
        let (bytes, rows) = self.flush()?;

        let handles = std::mem::take(&mut self.pending_writes);
        if handles.is_empty() {
            return Ok((bytes, rows));
        }

        let results = TOKIO.block_on(join_all(handles));
        let mut errors: Vec<anyhow::Error> = Vec::new();
        for result in results {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => errors.push(e),
                Err(e) => errors.push(anyhow!("write task panicked: {e}")),
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("DynamoDB writes failed: {msg}");
        }

        Ok((bytes, rows))
    }

    fn push_request(&mut self, request: WriteRequest, bytes: usize) -> AnyResult<(usize, usize)> {
        self.pending.push(request);
        self.pending_bytes += bytes;

        if self.pending.len() >= self.flush_records_threshold
            || self.pending_bytes >= self.max_buffer_size_bytes
        {
            self.flush()
        } else {
            Ok((0, 0))
        }
    }

    fn flush(&mut self) -> AnyResult<(usize, usize)> {
        if self.pending.is_empty() {
            return Ok((0, 0));
        }

        let requests = std::mem::replace(
            &mut self.pending,
            Vec::with_capacity(self.flush_records_threshold),
        );
        let rows = requests.len();
        let bytes = std::mem::take(&mut self.pending_bytes);

        {
            let client = self.client.clone();
            let endpoint_name = self.endpoint_name.clone();
            let table = self.table.clone();
            let write_mode = self.write_mode;
            let batch_size = self.batch_size;
            let max_retries = self.max_retries;
            let retries = self.retries.clone();
            let max_concurrent = self.max_concurrent_requests;

            let handle = TOKIO.spawn(async move {
                let start = Instant::now();
                let result = match write_mode {
                    DynamoDBWriteMode::Batch => {
                        write_batch_items(
                            &client,
                            &endpoint_name,
                            &table,
                            requests,
                            batch_size,
                            max_retries,
                            max_concurrent,
                            retries,
                        )
                        .await
                    }
                    DynamoDBWriteMode::Transactional => {
                        write_transact_items(
                            &client,
                            &endpoint_name,
                            &table,
                            requests,
                            batch_size,
                            max_retries,
                            max_concurrent,
                            retries,
                        )
                        .await
                    }
                };
                debug!(
                    endpoint = %endpoint_name,
                    rows,
                    bytes,
                    elapsed_ms = start.elapsed().as_millis(),
                    success = result.is_ok(),
                    "dynamodb: flushed batch",
                );
                result
            });
            self.pending_writes.push(handle);
        }

        self.records_written
            .fetch_add(rows as u64, Ordering::Relaxed);
        Ok((bytes, rows))
    }

    fn encode_cursor(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<(usize, usize)> {
        let mut num_bytes = 0usize;
        let mut num_rows = 0usize;

        while cursor.key_valid() {
            if let Some(op) = indexed_operation_type(self.view_name(), self.index_name(), cursor)? {
                cursor.rewind_vals();
                let (request, bytes) = match op {
                    IndexedOperationType::Insert => {
                        let item = self.item(cursor)?;
                        let bytes = item_size(&item);
                        (
                            WriteRequest::builder()
                                .put_request(PutRequest::builder().set_item(Some(item)).build()?)
                                .build(),
                            bytes,
                        )
                    }
                    IndexedOperationType::Delete => {
                        let key = self.key(cursor)?;
                        let bytes = item_size(&key);
                        (
                            WriteRequest::builder()
                                .delete_request(
                                    DeleteRequest::builder().set_key(Some(key)).build()?,
                                )
                                .build(),
                            bytes,
                        )
                    }
                    IndexedOperationType::Upsert => {
                        if cursor.weight() < 0 {
                            cursor.step_val();
                        }
                        let item = self.item(cursor)?;
                        let bytes = item_size(&item);
                        (
                            WriteRequest::builder()
                                .put_request(PutRequest::builder().set_item(Some(item)).build()?)
                                .build(),
                            bytes,
                        )
                    }
                };

                let (flushed_bytes, flushed_rows) = self.push_request(request, bytes)?;
                num_bytes += flushed_bytes;
                num_rows += flushed_rows;
            }

            cursor.step_key();
        }

        Ok((num_bytes, num_rows))
    }

    fn item(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<HashMap<String, AttributeValue>> {
        let mut item = cursor.val_to_dynamodb_item()?;
        if !self.key_fields_in_value
            && self
                .key_field_names
                .iter()
                .any(|field_name| !item.contains_key(field_name))
        {
            for (name, value) in self.key(cursor)? {
                item.entry(name).or_insert(value);
            }
        }
        Ok(item)
    }

    fn key(&mut self, cursor: &mut dyn SerCursor) -> AnyResult<HashMap<String, AttributeValue>> {
        let key = cursor.key_to_dynamodb_item()?;
        if key.is_empty() {
            bail!("dynamodb output connector requires a non-empty index key");
        }
        Ok(key)
    }

    fn run(
        mut self,
        cmd_rx: crossbeam::channel::Receiver<WorkerCommand>,
        result_tx: crossbeam::channel::Sender<WorkerResult>,
    ) {
        while let Ok(cmd) = cmd_rx.recv() {
            match cmd {
                WorkerCommand::BatchStart => {
                    self.batch_start_inner();
                    let _ = result_tx.send(WorkerResult::Ok {
                        num_bytes: 0,
                        num_rows: 0,
                    });
                }
                WorkerCommand::Encode(cursor_builder) => {
                    let mut cursor = cursor_builder.build();
                    match self.encode_cursor(&mut cursor) {
                        Ok((num_bytes, num_rows)) => {
                            let _ = result_tx.send(WorkerResult::Ok {
                                num_bytes,
                                num_rows,
                            });
                        }
                        Err(e) => {
                            let _ = result_tx.send(WorkerResult::Err(e));
                        }
                    }
                }
                WorkerCommand::BatchEnd => match self.batch_end_inner() {
                    Ok((num_bytes, num_rows)) => {
                        let _ = result_tx.send(WorkerResult::Ok {
                            num_bytes,
                            num_rows,
                        });
                    }
                    Err(e) => {
                        let _ = result_tx.send(WorkerResult::Err(e));
                    }
                },
                WorkerCommand::Shutdown => {
                    if let Err(e) = self.join_pending_writes() {
                        warn!(
                            endpoint = %self.endpoint_name,
                            "dynamodb: write error(s) on shutdown (data may be lost): {e:#}"
                        );
                    }
                    break;
                }
            }
        }
    }
}

struct WorkerHandle {
    cmd_tx: crossbeam::channel::Sender<WorkerCommand>,
    result_rx: crossbeam::channel::Receiver<WorkerResult>,
    thread: Option<std::thread::JoinHandle<()>>,
}

pub struct DynamoDBOutputEndpoint {
    endpoint_id: EndpointId,
    endpoint_name: String,
    config: DynamoDBWriterConfig,
    controller: Weak<ControllerInner>,
    handles: Vec<WorkerHandle>,
    records_written: Arc<AtomicU64>,
    retries: Arc<AtomicU64>,
    num_bytes: usize,
    num_rows: usize,
    // Periodic throughput summary state.
    rows_since_last_log: u64,
    bytes_since_last_log: u64,
    retries_since_last_log: u64,
    batches_since_last_log: u64,
    last_throughput_log: Instant,
}

impl Drop for DynamoDBOutputEndpoint {
    fn drop(&mut self) {
        for handle in &self.handles {
            let _ = handle.cmd_tx.send(WorkerCommand::Shutdown);
        }
        for handle in &mut self.handles {
            if let Some(thread) = handle.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

impl DynamoDBOutputEndpoint {
    pub fn new(
        endpoint_id: EndpointId,
        endpoint_name: &str,
        config: &DynamoDBWriterConfig,
        key_schema: &Option<Relation>,
        value_schema: &Relation,
        controller: Weak<ControllerInner>,
        is_index: bool,
    ) -> Result<Self, ControllerError> {
        config.validate().map_err(|e| {
            ControllerError::invalid_transport_configuration(endpoint_name, &e.to_string())
        })?;

        if !is_index || key_schema.is_none() {
            return Err(ControllerError::not_supported(
                "DynamoDB output connector requires the view to have a unique key. Please specify the `index` property in the connector configuration. For more details, see: https://docs.feldera.com/connectors/unique_keys",
            ));
        }

        let key_schema = key_schema.as_ref().unwrap();
        let records_written = Arc::new(AtomicU64::new(0));
        let retries = Arc::new(AtomicU64::new(0));
        if let Some(controller) = controller.upgrade() {
            controller
                .status
                .register_batch_progress_counter(&endpoint_id, records_written.clone());
        }

        let mut handles = Vec::with_capacity(config.threads);
        for i in 0..config.threads {
            let worker = DynamoDBWorker::new(
                endpoint_name,
                config,
                key_schema,
                value_schema,
                records_written.clone(),
                retries.clone(),
            );

            let (cmd_tx, cmd_rx) = crossbeam::channel::bounded(1);
            let (result_tx, result_rx) = crossbeam::channel::bounded(1);
            let thread_name = format!("dynamodb-output-{endpoint_name}-{i}");
            let thread = std::thread::Builder::new()
                .name(thread_name)
                .spawn(move || worker.run(cmd_rx, result_tx))
                .map_err(|e| {
                    ControllerError::output_transport_error(
                        endpoint_name,
                        true,
                        anyhow!("failed to spawn worker thread: {e}"),
                    )
                })?;

            handles.push(WorkerHandle {
                cmd_tx,
                result_rx,
                thread: Some(thread),
            });
        }

        Ok(Self {
            endpoint_id,
            endpoint_name: endpoint_name.to_string(),
            config: config.clone(),
            controller,
            handles,
            records_written,
            retries,
            num_bytes: 0,
            num_rows: 0,
            rows_since_last_log: 0,
            bytes_since_last_log: 0,
            retries_since_last_log: 0,
            batches_since_last_log: 0,
            last_throughput_log: Instant::now(),
        })
    }

    fn broadcast_and_collect(&mut self, command: WorkerCommand) -> AnyResult<(usize, usize)> {
        for handle in &self.handles {
            handle
                .cmd_tx
                .send(match &command {
                    WorkerCommand::BatchStart => WorkerCommand::BatchStart,
                    WorkerCommand::BatchEnd => WorkerCommand::BatchEnd,
                    WorkerCommand::Shutdown | WorkerCommand::Encode(_) => {
                        unreachable!("broadcast only supports batch lifecycle commands")
                    }
                })
                .map_err(|_| anyhow!("worker thread disconnected"))?;
        }

        let mut num_bytes = 0usize;
        let mut num_rows = 0usize;
        let mut errors = Vec::new();
        for handle in &self.handles {
            match handle.result_rx.recv() {
                Ok(WorkerResult::Ok {
                    num_bytes: bytes,
                    num_rows: rows,
                }) => {
                    num_bytes += bytes;
                    num_rows += rows;
                }
                Ok(WorkerResult::Err(e)) => errors.push(e),
                Err(_) => errors.push(anyhow!("worker thread disconnected")),
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{} DynamoDB worker(s) failed: {msg}", errors.len());
        }

        Ok((num_bytes, num_rows))
    }
}

impl OutputConsumer for DynamoDBOutputEndpoint {
    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) {
        self.records_written.store(0, Ordering::Relaxed);
        self.num_bytes = 0;
        self.num_rows = 0;
        if let Err(error) = self.broadcast_and_collect(WorkerCommand::BatchStart) {
            if let Some(controller) = self.controller.upgrade() {
                controller.output_transport_error(
                    self.endpoint_id,
                    &self.endpoint_name,
                    true,
                    error,
                    None,
                );
            }
        }
    }

    fn push_buffer(&mut self, _buffer: &[u8], _num_records: usize) {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
        _num_records: usize,
    ) {
        unreachable!()
    }

    fn batch_end(&mut self) {
        const THROUGHPUT_LOG_INTERVAL: Duration = Duration::from_secs(60);

        let start = Instant::now();
        match self.broadcast_and_collect(WorkerCommand::BatchEnd) {
            Ok((num_bytes, num_rows)) => {
                self.num_bytes += num_bytes;
                self.num_rows += num_rows;
                let batch_retries = self.retries.swap(0, Ordering::Relaxed);
                #[cfg(feature = "bench-mode")]
                DYNAMODB_BENCH_FLUSH_DURATIONS_US
                    .lock()
                    .unwrap()
                    .push(start.elapsed().as_micros() as u64);

                self.rows_since_last_log += self.num_rows as u64;
                self.bytes_since_last_log += self.num_bytes as u64;
                self.retries_since_last_log += batch_retries;
                self.batches_since_last_log += 1;

                let since_last = self.last_throughput_log.elapsed();
                if since_last >= THROUGHPUT_LOG_INTERVAL {
                    let secs = since_last.as_secs_f64();
                    let avg_retries = if self.batches_since_last_log > 0 {
                        self.retries_since_last_log as f64 / self.batches_since_last_log as f64
                    } else {
                        0.0
                    };
                    info!(
                        "dynamodb throughput: {:.0} rows/sec, {:.0} bytes/sec, \
                         avg {:.2} retries/batch ({} batches over {:.1}s)",
                        self.rows_since_last_log as f64 / secs,
                        self.bytes_since_last_log as f64 / secs,
                        avg_retries,
                        self.batches_since_last_log,
                        secs,
                    );
                    self.rows_since_last_log = 0;
                    self.bytes_since_last_log = 0;
                    self.retries_since_last_log = 0;
                    self.batches_since_last_log = 0;
                    self.last_throughput_log = Instant::now();
                }
            }
            Err(error) => {
                if let Some(controller) = self.controller.upgrade() {
                    controller.output_transport_error(
                        self.endpoint_id,
                        &self.endpoint_name,
                        true,
                        error,
                        None,
                    );
                }
            }
        }

        let num_bytes = std::mem::take(&mut self.num_bytes);
        let num_rows = std::mem::take(&mut self.num_rows);
        self.records_written.store(0, Ordering::Relaxed);
        if let Some(controller) = self.controller.upgrade() {
            controller
                .status
                .output_buffer(self.endpoint_id, num_bytes, num_rows);
        }
    }
}

impl Encoder for DynamoDBOutputEndpoint {
    fn consumer(&mut self) -> &mut dyn OutputConsumer {
        self
    }

    fn encode(&mut self, batch: Arc<dyn SerBatchReader>) -> AnyResult<()> {
        let _span = info_span!(
            "dynamodb_output",
            endpoint = &*self.endpoint_name,
            table = &*self.config.table,
        )
        .entered();

        let num_workers = self.handles.len();
        let mut bounds = batch.keys_factory().default_box();
        batch.partition_keys(num_workers, &mut *bounds);

        let mut workers_dispatched = 0;
        for i in 0..=bounds.len() {
            let Some(cursor_builder) = SplitCursorBuilder::from_bounds(
                batch.clone(),
                &*bounds,
                i,
                RecordFormat::Json(JsonFlavor::Default),
            ) else {
                continue;
            };

            assert!(
                workers_dispatched < num_workers,
                "DynamoDB output connector split batch into more partitions than worker threads"
            );

            self.handles[workers_dispatched]
                .cmd_tx
                .send(WorkerCommand::Encode(cursor_builder))
                .map_err(|_| anyhow!("worker thread disconnected"))?;
            workers_dispatched += 1;
        }

        let mut errors = Vec::new();
        for i in 0..workers_dispatched {
            match self.handles[i].result_rx.recv() {
                Ok(WorkerResult::Ok {
                    num_bytes,
                    num_rows,
                }) => {
                    self.num_bytes += num_bytes;
                    self.num_rows += num_rows;
                }
                Ok(WorkerResult::Err(e)) => errors.push(e),
                Err(_) => errors.push(anyhow!("worker thread disconnected")),
            }
        }

        if !errors.is_empty() {
            let msg = errors
                .iter()
                .map(|e| format!("{e:#}"))
                .collect::<Vec<_>>()
                .join("; ");
            bail!("{} DynamoDB worker(s) failed: {msg}", errors.len());
        }

        Ok(())
    }
}

impl OutputEndpoint for DynamoDBOutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        todo!()
    }

    fn max_buffer_size_bytes(&self) -> usize {
        self.config.max_buffer_size_bytes
    }

    fn push_buffer(&mut self, _buffer: &[u8]) -> AnyResult<()> {
        unreachable!()
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        unreachable!()
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

fn make_client(config: &DynamoDBWriterConfig) -> Client {
    // Disable the SDK's built-in retry logic. Our write_batch_chunk /
    // write_transact_chunk loops own all retry/backoff decisions so that we
    // don't end up with compounding backoff from two independent retry layers.
    let no_retries = aws_sdk_dynamodb::config::retry::RetryConfig::disabled();
    let mut config_builder = aws_sdk_dynamodb::Config::builder()
        .region(Region::new(config.region.clone()))
        .retry_config(no_retries);

    if let Some(endpoint_url) = &config.endpoint_url {
        config_builder = config_builder.endpoint_url(endpoint_url);
    }

    if let (Some(access_key), Some(secret_key)) =
        (&config.aws_access_key_id, &config.aws_secret_access_key)
    {
        let credentials = aws_sdk_dynamodb::config::Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "credential-provider",
        );
        Client::from_conf(config_builder.credentials_provider(credentials).build())
    } else {
        let provider = TOKIO.block_on(async {
            aws_config::default_provider::credentials::default_provider().await
        });
        Client::from_conf(config_builder.credentials_provider(provider).build())
    }
}

#[cfg(test)]
fn json_record_to_item(value: serde_json::Value) -> AnyResult<HashMap<String, AttributeValue>> {
    serde_dynamo::aws_sdk_dynamodb_1::to_item(value).map_err(Into::into)
}

fn item_size(item: &HashMap<String, AttributeValue>) -> usize {
    item.iter()
        .map(|(key, value)| key.len() + attribute_value_size(value))
        .sum()
}

fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) => value.len(),
        AttributeValue::B(value) => value.as_ref().len(),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        AttributeValue::M(values) => item_size(values),
        AttributeValue::L(values) => values.iter().map(attribute_value_size).sum(),
        AttributeValue::Ss(values) | AttributeValue::Ns(values) => {
            values.iter().map(String::len).sum()
        }
        AttributeValue::Bs(values) => values.iter().map(|value| value.as_ref().len()).sum(),
        _ => 0,
    }
}

#[cfg(test)]
fn json_to_attribute_value(value: serde_json::Value) -> AnyResult<AttributeValue> {
    serde_dynamo::aws_sdk_dynamodb_1::to_attribute_value(value).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::thread::sleep;

    use dbsp::OrdIndexedZSet;
    use dbsp::utils::Tup2;
    use feldera_adapterlib::catalog::SerBatch;
    use feldera_adapterlib::transport::OutputBatchType;
    use feldera_macros::IsNone;
    use feldera_sqllib::{
        ByteArray, Date, F32, F64, SqlDecimal, SqlString, Time, Timestamp, Uuid, Variant,
    };
    use feldera_types::program_schema::{ColumnType, Field, SqlIdentifier};
    use feldera_types::{
        deserialize_table_record, deserialize_without_context, serialize_struct,
        serialize_table_record,
    };
    use size_of::SizeOf;

    use crate::static_compile::seroutput::SerBatchImpl;
    use crate::test::TestStruct;

    use super::*;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, BillingMode, KeySchemaElement, KeyType, ScalarAttributeType,
    };

    #[derive(
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
        Clone,
        Hash,
        SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
        IsNone,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct TestRecord {
        id: i32,
        sort: String,
        b: bool,
        i: Option<i64>,
        s: String,
    }

    deserialize_without_context!(TestRecord);

    serialize_struct!(TestRecord()[5]{
        id["id"]: i32,
        sort["sort"]: String,
        b["b"]: bool,
        i["i"]: Option<i64>,
        s["s"]: String
    });

    #[derive(
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
        Clone,
        Hash,
        SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
        IsNone,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct TestKey {
        id: i32,
        sort: String,
    }

    deserialize_without_context!(TestKey);

    serialize_struct!(TestKey()[2]{
        id["id"]: i32,
        sort["sort"]: String
    });

    #[derive(
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Clone,
        Hash,
        SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
        IsNone,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct AllTypesRecord {
        id: i32,
        boolean_: bool,
        tinyint_: i8,
        smallint_: i16,
        int_: i32,
        bigint_: i64,
        decimal_: SqlDecimal<38, 10>,
        float_: F32,
        double_: F64,
        varchar_: SqlString,
        time_: Time,
        date_: Date,
        timestamp_: Timestamp,
        variant_: Variant,
        uuid_: Uuid,
        varbinary_: ByteArray,
        struct_: TestStruct,
        string_array_: Vec<SqlString>,
        struct_array_: Vec<TestStruct>,
        map_: BTreeMap<SqlString, TestStruct>,
        nullable_: Option<i64>,
    }

    serialize_table_record!(AllTypesRecord[21]{
        id["id"]: i32,
        boolean_["boolean_"]: bool,
        tinyint_["tinyint_"]: i8,
        smallint_["smallint_"]: i16,
        int_["int_"]: i32,
        bigint_["bigint_"]: i64,
        decimal_["decimal_"]: SqlDecimal,
        float_["float_"]: F32,
        double_["double_"]: F64,
        varchar_["varchar_"]: SqlString,
        time_["time_"]: Time,
        date_["date_"]: Date,
        timestamp_["timestamp_"]: Timestamp,
        variant_["variant_"]: Variant,
        uuid_["uuid_"]: Uuid,
        varbinary_["varbinary_"]: ByteArray,
        struct_["struct_"]: TestStruct,
        string_array_["string_array_"]: Vec<SqlString>,
        struct_array_["struct_array_"]: Vec<TestStruct>,
        map_["map_"]: BTreeMap<SqlString, TestStruct>,
        nullable_["nullable_"]: Option<i64>
    });

    deserialize_table_record!(AllTypesRecord["AllTypesRecord", Variant, 21] {
        (id, "id", false, i32, |_| None),
        (boolean_, "boolean_", false, bool, |_| None),
        (tinyint_, "tinyint_", false, i8, |_| None),
        (smallint_, "smallint_", false, i16, |_| None),
        (int_, "int_", false, i32, |_| None),
        (bigint_, "bigint_", false, i64, |_| None),
        (decimal_, "decimal_", false, SqlDecimal<38, 10>, |_| None),
        (float_, "float_", false, F32, |_| None),
        (double_, "double_", false, F64, |_| None),
        (varchar_, "varchar_", false, SqlString, |_| None),
        (time_, "time_", false, Time, |_| None),
        (date_, "date_", false, Date, |_| None),
        (timestamp_, "timestamp_", false, Timestamp, |_| None),
        (variant_, "variant_", false, Variant, |_| None),
        (uuid_, "uuid_", false, Uuid, |_| None),
        (varbinary_, "varbinary_", false, ByteArray, |_| None),
        (struct_, "struct_", false, TestStruct, |_| None),
        (string_array_, "string_array_", false, Vec<SqlString>, |_| None),
        (struct_array_, "struct_array_", false, Vec<TestStruct>, |_| None),
        (map_, "map_", false, BTreeMap<SqlString, TestStruct>, |_| None),
        (nullable_, "nullable_", true, Option<i64>, |_| None)
    });

    #[derive(
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        serde::Serialize,
        serde::Deserialize,
        Clone,
        Hash,
        SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
        IsNone,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct AllTypesKey {
        id: i32,
    }

    deserialize_without_context!(AllTypesKey);

    serialize_struct!(AllTypesKey()[1]{
        id["id"]: i32
    });

    fn key_relation() -> Relation {
        Relation {
            name: SqlIdentifier::new("test_idx", false),
            fields: vec![
                Field::new("id".into(), ColumnType::int(false)),
                Field::new("sort".into(), ColumnType::varchar(false)),
            ],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    fn value_relation() -> Relation {
        Relation {
            name: SqlIdentifier::new("test_view", false),
            fields: vec![
                Field::new("id".into(), ColumnType::int(false)),
                Field::new("sort".into(), ColumnType::varchar(false)),
                Field::new("b".into(), ColumnType::boolean(false)),
                Field::new("i".into(), ColumnType::bigint(true)),
                Field::new("s".into(), ColumnType::varchar(false)),
            ],
            materialized: true,
            properties: BTreeMap::new(),
            primary_key: Some(vec!["id".into(), "sort".into()]),
        }
    }

    fn all_types_key_relation() -> Relation {
        Relation {
            name: SqlIdentifier::new("all_types_idx", false),
            fields: vec![Field::new("id".into(), ColumnType::int(false))],
            materialized: false,
            properties: BTreeMap::new(),
            primary_key: None,
        }
    }

    fn all_types_value_relation() -> Relation {
        Relation {
            name: SqlIdentifier::new("all_types_view", false),
            fields: vec![
                Field::new("id".into(), ColumnType::int(false)),
                Field::new("boolean_".into(), ColumnType::boolean(false)),
                Field::new("tinyint_".into(), ColumnType::tinyint(false)),
                Field::new("smallint_".into(), ColumnType::smallint(false)),
                Field::new("int_".into(), ColumnType::int(false)),
                Field::new("bigint_".into(), ColumnType::bigint(false)),
                Field::new("decimal_".into(), ColumnType::decimal(38, 10, false)),
                Field::new("float_".into(), ColumnType::real(false)),
                Field::new("double_".into(), ColumnType::double(false)),
                Field::new("varchar_".into(), ColumnType::varchar(false)),
                Field::new("time_".into(), ColumnType::time(false)),
                Field::new("date_".into(), ColumnType::date(false)),
                Field::new("timestamp_".into(), ColumnType::timestamp(false)),
                Field::new("variant_".into(), ColumnType::variant(false)),
                Field::new("uuid_".into(), ColumnType::uuid(false)),
                Field::new("varbinary_".into(), ColumnType::varbinary(false)),
                Field::new(
                    "struct_".into(),
                    ColumnType::structure(false, &TestStruct::schema()),
                ),
                Field::new(
                    "string_array_".into(),
                    ColumnType::array(false, ColumnType::varchar(false)),
                ),
                Field::new(
                    "struct_array_".into(),
                    ColumnType::array(false, ColumnType::structure(false, &TestStruct::schema())),
                ),
                Field::new(
                    "map_".into(),
                    ColumnType::map(
                        false,
                        ColumnType::varchar(false),
                        ColumnType::structure(false, &TestStruct::schema()),
                    ),
                ),
                Field::new("nullable_".into(), ColumnType::bigint(true)),
            ],
            materialized: true,
            properties: BTreeMap::new(),
            primary_key: Some(vec!["id".into()]),
        }
    }

    fn config(batch_size: usize) -> DynamoDBWriterConfig {
        DynamoDBWriterConfig {
            table: "test_table".to_string(),
            region: "us-east-1".to_string(),
            endpoint_url: None,
            aws_access_key_id: None,
            aws_secret_access_key: None,
            batch_size: Some(batch_size),
            write_mode: DynamoDBWriteMode::Batch,
            max_buffer_size_bytes: 1024 * 1024,
            max_concurrent_requests: 16,
            threads: 1,
            max_retries: 10,
        }
    }

    fn dynamodb_region() -> String {
        std::env::var("DYNAMODB_REGION")
            .or_else(|_| std::env::var("AWS_REGION"))
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".to_string())
    }

    fn endpoint_config(
        table: String,
        endpoint_url: Option<String>,
        threads: usize,
    ) -> DynamoDBWriterConfig {
        let use_static_dummy_credentials = endpoint_url.is_some();
        DynamoDBWriterConfig {
            table,
            region: dynamodb_region(),
            endpoint_url,
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

    /// Creates a client pointed at a non-existent endpoint. Writes will fail,
    /// but encoding tests never await the spawned write tasks.
    fn dummy_client() -> Client {
        make_client(&DynamoDBWriterConfig {
            endpoint_url: Some("http://localhost:0".to_string()),
            aws_access_key_id: Some("dummy".to_string()),
            aws_secret_access_key: Some("dummy".to_string()),
            ..config(25)
        })
    }

    fn worker(batch_size: usize) -> DynamoDBWorker {
        DynamoDBWorker::with_client(
            dummy_client(),
            "",
            &config(batch_size),
            &key_relation(),
            &value_relation(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn worker_with_max_buffer_size(
        batch_size: usize,
        max_buffer_size_bytes: usize,
    ) -> DynamoDBWorker {
        let mut config = config(batch_size);
        config.max_buffer_size_bytes = max_buffer_size_bytes;
        DynamoDBWorker::with_client(
            dummy_client(),
            "",
            &config,
            &key_relation(),
            &value_relation(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn worker_with_config(config: DynamoDBWriterConfig) -> DynamoDBWorker {
        DynamoDBWorker::with_client(
            dummy_client(),
            "",
            &config,
            &key_relation(),
            &value_relation(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn worker_with_counter(
        config: DynamoDBWriterConfig,
        records_written: Arc<AtomicU64>,
    ) -> DynamoDBWorker {
        DynamoDBWorker::with_client(
            dummy_client(),
            "",
            &config,
            &key_relation(),
            &value_relation(),
            records_written,
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn empty_endpoint_with_counter(records_written: Arc<AtomicU64>) -> DynamoDBOutputEndpoint {
        DynamoDBOutputEndpoint {
            endpoint_id: EndpointId::default(),
            endpoint_name: "test_dynamodb_endpoint".to_string(),
            config: config(25),
            controller: Weak::new(),
            handles: Vec::new(),
            records_written,
            retries: Arc::new(AtomicU64::new(0)),
            num_bytes: 0,
            num_rows: 0,
            rows_since_last_log: 0,
            bytes_since_last_log: 0,
            retries_since_last_log: 0,
            batches_since_last_log: 0,
            last_throughput_log: Instant::now(),
        }
    }

    fn build_batch(tuples: Vec<(TestRecord, i64)>) -> Arc<dyn SerBatch> {
        let tuples: Vec<_> = tuples
            .into_iter()
            .map(|(record, weight)| {
                Tup2(
                    Tup2(
                        TestKey {
                            id: record.id,
                            sort: record.sort.clone(),
                        },
                        record,
                    ),
                    weight,
                )
            })
            .collect();
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        Arc::new(SerBatchImpl::<_, TestKey, TestRecord>::new(zset))
    }

    fn build_all_types_batch(tuples: Vec<(AllTypesRecord, i64)>) -> Arc<dyn SerBatch> {
        let tuples: Vec<_> = tuples
            .into_iter()
            .map(|(record, weight)| Tup2(Tup2(AllTypesKey { id: record.id }, record), weight))
            .collect();
        let zset = OrdIndexedZSet::from_tuples((), tuples);
        Arc::new(SerBatchImpl::<_, AllTypesKey, AllTypesRecord>::new(zset))
    }

    fn record(id: i32, sort: &str, i: Option<i64>, s: &str) -> TestRecord {
        TestRecord {
            id,
            sort: sort.to_string(),
            b: id % 2 == 0,
            i,
            s: s.to_string(),
        }
    }

    fn all_types_record() -> AllTypesRecord {
        let mut map = BTreeMap::new();
        map.insert(
            SqlString::from_ref("nested"),
            TestStruct {
                id: 7,
                b: true,
                i: Some(70),
                s: "inside-map".to_string(),
            },
        );

        AllTypesRecord {
            id: 42,
            boolean_: true,
            tinyint_: -8,
            smallint_: 16,
            int_: 32,
            bigint_: 64,
            decimal_: SqlDecimal::<38, 10>::new(12345, 3).unwrap(),
            float_: F32::new(1.5),
            double_: F64::new(2.25),
            varchar_: SqlString::from_ref("hello"),
            time_: Time::from_time(chrono::NaiveTime::from_hms_micro_opt(1, 2, 3, 456).unwrap()),
            date_: Date::from_date(chrono::NaiveDate::from_ymd_opt(2024, 5, 25).unwrap()),
            timestamp_: Timestamp::from_naiveDateTime(
                chrono::NaiveDate::from_ymd_opt(2024, 5, 25)
                    .unwrap()
                    .and_hms_micro_opt(1, 2, 3, 456)
                    .unwrap(),
            ),
            variant_: Variant::String(SqlString::from_ref("variant-value")),
            uuid_: uuid::uuid!("550e8400-e29b-41d4-a716-446655440000").into(),
            varbinary_: ByteArray::from_vec(vec![1, 2, 3, 4]),
            struct_: TestStruct {
                id: 5,
                b: false,
                i: Some(50),
                s: "inside-struct".to_string(),
            },
            string_array_: vec![SqlString::from_ref("a"), SqlString::from_ref("b")],
            struct_array_: vec![TestStruct {
                id: 6,
                b: true,
                i: None,
                s: "inside-array".to_string(),
            }],
            map_: map,
            nullable_: None,
        }
    }

    fn all_types_worker() -> DynamoDBWorker {
        DynamoDBWorker::with_client(
            dummy_client(),
            "",
            &config(25),
            &all_types_key_relation(),
            &all_types_value_relation(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
    }

    fn encode_batch(worker: &mut DynamoDBWorker, batch: &Arc<dyn SerBatch>) {
        let reader = batch.clone().arc_as_batch_reader();
        let mut cursor = reader
            .cursor(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        worker.batch_start_inner();
        worker.encode_cursor(&mut *cursor).unwrap();
        worker.batch_end_inner().unwrap();
    }

    fn stage_batch(worker: &mut DynamoDBWorker, batch: &Arc<dyn SerBatch>) {
        let reader = batch.clone().arc_as_batch_reader();
        let mut cursor = reader
            .cursor(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        worker.batch_start_inner();
        worker.encode_cursor(&mut *cursor).unwrap();
    }

    fn encode_endpoint_batch(endpoint: &mut DynamoDBOutputEndpoint, batch: &Arc<dyn SerBatch>) {
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        endpoint.consumer().batch_end();
    }

    fn attr_s<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
        item.get(field).unwrap().as_s().unwrap()
    }

    fn attr_n<'a>(item: &'a HashMap<String, AttributeValue>, field: &str) -> &'a str {
        item.get(field).unwrap().as_n().unwrap()
    }

    #[test]
    fn encoder_insert_as_put_item() {
        let mut worker = worker(25);
        let batch = build_batch(vec![(record(1, "a", Some(10), "inserted"), 1)]);
        stage_batch(&mut worker, &batch);
        assert_eq!(worker.pending.len(), 1);
        let item = worker.pending[0].put_request().unwrap().item();
        assert_eq!(attr_n(item, "id"), "1");
        assert_eq!(attr_s(item, "sort"), "a");
        assert_eq!(item.get("b").unwrap().as_bool().unwrap(), &false);
        assert_eq!(attr_n(item, "i"), "10");
        assert_eq!(attr_s(item, "s"), "inserted");
    }

    #[test]
    fn encoder_delete_as_delete_item_key_only() {
        let mut worker = worker(25);
        let batch = build_batch(vec![(record(2, "b", Some(20), "deleted"), -1)]);
        stage_batch(&mut worker, &batch);
        assert_eq!(worker.pending.len(), 1);
        assert!(worker.pending[0].put_request().is_none());
        let key = worker.pending[0].delete_request().unwrap().key();
        assert_eq!(key.len(), 2);
        assert_eq!(attr_n(key, "id"), "2");
        assert_eq!(attr_s(key, "sort"), "b");
    }

    #[test]
    fn encoder_upsert_as_put_item_with_new_value() {
        let mut worker = worker(25);
        let batch = build_batch(vec![
            (record(3, "c", Some(30), "old"), -1),
            (record(3, "c", None, "new"), 1),
        ]);
        stage_batch(&mut worker, &batch);
        assert_eq!(worker.pending.len(), 1);
        let item = worker.pending[0].put_request().unwrap().item();
        assert_eq!(attr_n(item, "id"), "3");
        assert_eq!(attr_s(item, "sort"), "c");
        assert_eq!(item.get("i").unwrap().as_null().unwrap(), &true);
        assert_eq!(attr_s(item, "s"), "new");
    }

    #[test]
    fn encoder_stages_until_batch_end() {
        let mut worker = worker(100);
        let batch = build_batch(
            (0..5)
                .map(|id| (record(id, "chunk", Some(id as i64), "r"), 1))
                .collect(),
        );

        stage_batch(&mut worker, &batch);
        // pending holds all records until flush is called
        assert_eq!(worker.pending.len(), 5);

        let pending = worker.pending.clone();
        let (_bytes, rows) = worker.flush().unwrap();
        assert_eq!(rows, 5);

        let mut ids = pending
            .iter()
            .map(|r| {
                r.put_request()
                    .unwrap()
                    .item()
                    .get("id")
                    .unwrap()
                    .as_n()
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn encoder_flushes_when_buffer_limit_is_reached() {
        // max_buffer_size_bytes=1 forces a flush after the first record.
        let mut worker = worker_with_max_buffer_size(25, 1);
        let batch = build_batch(vec![(record(9, "early", Some(90), "flushed"), 1)]);
        stage_batch(&mut worker, &batch);
        // pending is empty because the record was flushed mid-encode
        assert!(worker.pending.is_empty());
        assert_eq!(worker.records_written.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn encoder_flushes_when_record_threshold_is_reached() {
        // threshold = batch_size(2) × max_concurrent_requests(2) = 4
        let mut config = config(2);
        config.max_concurrent_requests = 2;
        config.max_buffer_size_bytes = usize::MAX;
        let mut worker = worker_with_config(config);
        let batch = build_batch(
            (0..4)
                .map(|id| (record(id, "threshold", Some(id as i64), "r"), 1))
                .collect(),
        );
        stage_batch(&mut worker, &batch);
        assert!(worker.pending.is_empty());
        assert_eq!(worker.records_written.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn encoder_empty_batch_single_thread() {
        let mut worker = worker(25);
        encode_batch(&mut worker, &build_batch(Vec::new()));
        assert!(worker.pending.is_empty());
        assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn encoder_empty_batch_multi_thread() {
        let batch = build_batch(Vec::new());
        for _ in 0..4 {
            let mut worker = worker(25);
            encode_batch(&mut worker, &batch);
            assert!(worker.pending.is_empty());
            assert_eq!(worker.records_written.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn encoder_multiple_batches_insert_upsert_delete() {
        let mut worker = worker(25);

        stage_batch(
            &mut worker,
            &build_batch(vec![(record(11, "sequence", Some(110), "insert"), 1)]),
        );
        assert_eq!(worker.pending.len(), 1);
        assert!(worker.pending[0].put_request().is_some());

        stage_batch(
            &mut worker,
            &build_batch(vec![
                (record(11, "sequence", Some(110), "insert"), -1),
                (record(11, "sequence", Some(111), "upsert"), 1),
            ]),
        );
        assert_eq!(worker.pending.len(), 1);
        assert_eq!(
            attr_s(worker.pending[0].put_request().unwrap().item(), "s"),
            "upsert"
        );

        stage_batch(
            &mut worker,
            &build_batch(vec![(record(11, "sequence", Some(111), "upsert"), -1)]),
        );
        assert_eq!(worker.pending.len(), 1);
        assert!(worker.pending[0].delete_request().is_some());
    }

    #[test]
    fn encoder_progress_counter_advances_on_mid_batch_flush() {
        let records_written = Arc::new(AtomicU64::new(0));
        let mut config = config(2);
        config.max_concurrent_requests = 2;
        config.max_buffer_size_bytes = usize::MAX;
        let mut worker = worker_with_counter(config, records_written.clone());
        let batch = build_batch(
            (0..4)
                .map(|id| (record(id, "progress", Some(id as i64), "r"), 1))
                .collect(),
        );
        stage_batch(&mut worker, &batch);
        assert_eq!(records_written.load(Ordering::Relaxed), 4);
    }

    #[test]
    fn encoder_progress_counter_resets_at_batch_start_and_end() {
        let records_written = Arc::new(AtomicU64::new(99));
        let mut endpoint = empty_endpoint_with_counter(records_written.clone());

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        assert_eq!(records_written.load(Ordering::Relaxed), 0);

        records_written.store(88, Ordering::Relaxed);
        endpoint.consumer().batch_end();
        assert_eq!(records_written.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn encoder_all_supported_sql_types() {
        let mut worker = all_types_worker();
        let batch = build_all_types_batch(vec![(all_types_record(), 1)]);
        stage_batch(&mut worker, &batch);
        assert_eq!(worker.pending.len(), 1);
        let item = worker.pending[0].put_request().unwrap().item();

        assert_eq!(attr_n(item, "id"), "42");
        assert_eq!(item.get("boolean_").unwrap().as_bool().unwrap(), &true);
        assert_eq!(attr_n(item, "tinyint_"), "-8");
        assert_eq!(attr_n(item, "smallint_"), "16");
        assert_eq!(attr_n(item, "int_"), "32");
        assert_eq!(attr_n(item, "bigint_"), "64");
        assert_eq!(attr_n(item, "decimal_"), "12.345");
        assert_eq!(attr_n(item, "float_"), "1.5");
        assert_eq!(attr_n(item, "double_"), "2.25");
        assert_eq!(attr_s(item, "varchar_"), "hello");
        assert!(item.get("time_").unwrap().is_s());
        assert!(item.get("date_").unwrap().is_s());
        assert!(item.get("timestamp_").unwrap().is_s());
        assert_eq!(attr_s(item, "variant_"), "variant-value");
        assert_eq!(
            attr_s(item, "uuid_"),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert!(item.get("varbinary_").unwrap().is_l());
        assert!(item.get("struct_").unwrap().is_m());
        assert!(item.get("string_array_").unwrap().is_l());
        assert!(item.get("struct_array_").unwrap().is_l());
        assert!(item.get("map_").unwrap().is_m());
        assert_eq!(item.get("nullable_").unwrap().as_null().unwrap(), &true);
    }

    #[test]
    fn encoder_split_cursor_partitions_encode_disjoint_key_ranges() {
        let batch = build_batch(
            (0..10)
                .map(|id| (record(id, "split", Some(id as i64), "r"), 1))
                .collect(),
        );
        let reader = batch.arc_as_batch_reader();
        let mut bounds = reader.keys_factory().default_box();
        reader.partition_keys(3, &mut *bounds);

        let mut all_requests = Vec::new();
        for i in 0..=bounds.len() {
            let Some(cursor_builder) = SplitCursorBuilder::from_bounds(
                reader.clone(),
                &*bounds,
                i,
                RecordFormat::Json(JsonFlavor::Default),
            ) else {
                continue;
            };
            let mut worker = worker(100);
            let mut cursor = cursor_builder.build();
            worker.batch_start_inner();
            worker.encode_cursor(&mut cursor).unwrap();
            all_requests.extend(worker.pending.iter().cloned());
            worker.batch_end_inner().unwrap();
        }

        let mut ids = all_requests
            .iter()
            .map(|request| {
                request
                    .put_request()
                    .unwrap()
                    .item()
                    .get("id")
                    .unwrap()
                    .as_n()
                    .unwrap()
                    .parse::<i32>()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(ids, (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn encoder_json_to_attribute_value_encodes_nested_values() {
        let item = json_record_to_item(serde_json::json!({
            "n": 1.25,
            "s": "x",
            "b": true,
            "null": null,
            "list": [1, "two"],
            "map": { "nested": false }
        }))
        .unwrap();

        assert_eq!(attr_n(&item, "n"), "1.25");
        assert_eq!(attr_s(&item, "s"), "x");
        assert_eq!(item.get("b").unwrap().as_bool().unwrap(), &true);
        assert_eq!(item.get("null").unwrap().as_null().unwrap(), &true);
        assert!(item.get("list").unwrap().is_l());
        assert!(item.get("map").unwrap().is_m());
    }

    fn dynamodb_endpoint_url() -> Option<String> {
        std::env::var("DYNAMODB_ENDPOINT")
            .ok()
            .filter(|endpoint| !endpoint.trim().is_empty())
    }

    fn dynamodb_client(endpoint_url: Option<&str>) -> Client {
        make_client(&endpoint_config(
            "unused".to_string(),
            endpoint_url.map(str::to_string),
            1,
        ))
    }

    fn wait_for_dynamodb(client: &Client) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if TOKIO
                .block_on(client.list_tables().send())
                .map(|_| ())
                .is_ok()
            {
                return;
            }

            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for DynamoDB endpoint"
            );
            sleep(Duration::from_millis(250));
        }
    }

    fn test_table_name(suffix: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("feldera_dynamodb_{suffix}_{}_{}", std::process::id(), nanos)
    }

    fn create_table(client: &Client, table: &str, sort_key: bool) {
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

        if sort_key {
            create = create
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("sort")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("sort")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                );
        }

        TOKIO.block_on(create.send()).unwrap();
    }

    fn delete_table(client: &Client, table: &str) {
        let _ = TOKIO.block_on(client.delete_table().table_name(table).send());
    }

    fn scan_table(client: &Client, table: &str) -> Vec<HashMap<String, AttributeValue>> {
        let mut rows = TOKIO
            .block_on(client.scan().table_name(table).send())
            .unwrap()
            .items()
            .to_vec();
        rows.sort_by_key(|row| {
            row.get("id")
                .unwrap()
                .as_n()
                .unwrap()
                .parse::<i32>()
                .unwrap()
        });
        rows
    }

    fn dynamodb_endpoint(
        threads: usize,
        table: &str,
        endpoint_url: Option<&str>,
    ) -> DynamoDBOutputEndpoint {
        let config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), threads);
        DynamoDBOutputEndpoint::new(
            EndpointId::default(),
            "test_endpoint",
            &config,
            &Some(key_relation()),
            &value_relation(),
            Weak::new(),
            true,
        )
        .unwrap()
    }

    fn dynamodb_all_types_endpoint(
        table: &str,
        endpoint_url: Option<&str>,
    ) -> DynamoDBOutputEndpoint {
        let config = endpoint_config(table.to_string(), endpoint_url.map(str::to_string), 2);
        DynamoDBOutputEndpoint::new(
            EndpointId::default(),
            "test_endpoint",
            &config,
            &Some(all_types_key_relation()),
            &all_types_value_relation(),
            Weak::new(),
            true,
        )
        .unwrap()
    }

    fn expected_all_types_item() -> HashMap<String, AttributeValue> {
        let mut worker = all_types_worker();
        let batch = build_all_types_batch(vec![(all_types_record(), 1)]);
        stage_batch(&mut worker, &batch);
        let item = worker.pending[0].put_request().unwrap().item().clone();
        worker.batch_end_inner().unwrap();
        item
    }

    #[test]
    fn dynamodb_insert() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("insert");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(2, "b", Some(20), "two"), 1),
            ]),
        );

        let rows = scan_table(&client, &table);
        assert_eq!(rows.len(), 2);
        assert_eq!(attr_n(&rows[0], "id"), "1");
        assert_eq!(attr_s(&rows[0], "s"), "one");
        assert_eq!(attr_n(&rows[1], "id"), "2");
        assert_eq!(attr_s(&rows[1], "s"), "two");
        delete_table(&client, &table);
    }

    #[test]
    fn dynamodb_upsert() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("upsert");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![(record(2, "b", Some(20), "two"), 1)]),
        );
        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![
                (record(2, "b", Some(20), "two"), -1),
                (record(2, "b", None, "two-updated"), 1),
            ]),
        );

        let rows = scan_table(&client, &table);
        assert_eq!(rows.len(), 1);
        assert_eq!(attr_n(&rows[0], "id"), "2");
        assert_eq!(attr_s(&rows[0], "sort"), "b");
        assert_eq!(rows[0].get("i").unwrap().as_null().unwrap(), &true);
        assert_eq!(attr_s(&rows[0], "s"), "two-updated");
        delete_table(&client, &table);
    }

    #[test]
    fn dynamodb_delete() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("delete");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(3, "c", Some(30), "three"), 1),
            ]),
        );
        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![(record(1, "a", Some(10), "one"), -1)]),
        );

        let rows = scan_table(&client, &table);
        assert_eq!(rows.len(), 1);
        assert_eq!(attr_n(&rows[0], "id"), "3");
        assert_eq!(attr_s(&rows[0], "sort"), "c");
        assert_eq!(attr_s(&rows[0], "s"), "three");
        delete_table(&client, &table);
    }

    #[test]
    fn dynamodb_all_supported_sql_types_round_trip() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("all_types");
        create_table(&client, &table, false);
        let mut endpoint = dynamodb_all_types_endpoint(&table, endpoint_url.as_deref());
        let batch = build_all_types_batch(vec![(all_types_record(), 1)]);

        encode_endpoint_batch(&mut endpoint, &batch);

        let rows = scan_table(&client, &table);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], expected_all_types_item());
        delete_table(&client, &table);
    }

    fn records_written(endpoint: &DynamoDBOutputEndpoint) -> u64 {
        endpoint.records_written.load(Ordering::Relaxed)
    }

    fn dynamodb_endpoint_flush_every(
        max_buffer_size_bytes: usize,
        threads: usize,
        table: &str,
        endpoint_url: Option<&str>,
    ) -> DynamoDBOutputEndpoint {
        let mut config =
            endpoint_config(table.to_string(), endpoint_url.map(str::to_string), threads);
        config.max_buffer_size_bytes = max_buffer_size_bytes;
        DynamoDBOutputEndpoint::new(
            EndpointId::default(),
            "test_endpoint",
            &config,
            &Some(key_relation()),
            &value_relation(),
            Weak::new(),
            true,
        )
        .unwrap()
    }

    // Integration test: an empty batch completes without error and writes nothing.
    #[test]
    fn dynamodb_empty_batch() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("empty");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

        encode_endpoint_batch(&mut endpoint, &build_batch(vec![]));

        assert_eq!(scan_table(&client, &table).len(), 0);
        delete_table(&client, &table);
    }

    // Integration test: insert → upsert → delete across multiple successive batches.
    #[test]
    fn dynamodb_multiple_batches() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("multi");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(3, &table, endpoint_url.as_deref());

        // Batch 1: insert two records.
        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![
                (record(1, "a", Some(10), "one"), 1),
                (record(2, "b", Some(20), "two"), 1),
            ]),
        );
        assert_eq!(scan_table(&client, &table).len(), 2);

        // Batch 2: upsert record 1.
        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![
                (record(1, "a", Some(10), "one"), -1),
                (record(1, "a", Some(99), "one-updated"), 1),
            ]),
        );
        let rows = scan_table(&client, &table);
        assert_eq!(rows.len(), 2);
        assert_eq!(attr_n(&rows[0], "i"), "99");
        assert_eq!(attr_s(&rows[0], "s"), "one-updated");

        // Batch 3: delete record 2.
        encode_endpoint_batch(
            &mut endpoint,
            &build_batch(vec![(record(2, "b", Some(20), "two"), -1)]),
        );
        let rows = scan_table(&client, &table);
        assert_eq!(rows.len(), 1);
        assert_eq!(attr_n(&rows[0], "id"), "1");

        delete_table(&client, &table);
    }

    // Progress counter: starts at zero, and resets to zero after every batch.
    fn progress_basic(threads: usize) {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("progress_basic");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(threads, &table, endpoint_url.as_deref());
        let batch = build_batch(
            (0..100i32)
                .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
                .collect(),
        );

        assert_eq!(records_written(&endpoint), 0);
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        assert_eq!(records_written(&endpoint), 0);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();
        endpoint.consumer().batch_end();
        // Counter resets to 0 at the end of every batch.
        assert_eq!(records_written(&endpoint), 0);

        assert_eq!(scan_table(&client, &table).len(), 100);
        delete_table(&client, &table);
    }

    #[test]
    fn dynamodb_progress_counter_single_thread() {
        progress_basic(1);
    }

    #[test]
    fn dynamodb_progress_counter_multi_thread() {
        progress_basic(3);
    }

    // Progress counter: an empty batch leaves the counter at zero throughout.
    #[test]
    fn dynamodb_progress_counter_empty_batch() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("progress_empty");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(build_batch(vec![]).arc_as_batch_reader())
            .unwrap();
        assert_eq!(records_written(&endpoint), 0);
        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);

        delete_table(&client, &table);
    }

    // Progress counter: advances during encode when mid-batch flushes occur.
    fn progress_advances_mid_batch(threads: usize) {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("progress_mid");
        create_table(&client, &table, true);
        // max_buffer_size_bytes=1 forces a flush after every single encoded record.
        let mut endpoint =
            dynamodb_endpoint_flush_every(1, threads, &table, endpoint_url.as_deref());
        let num_records = 50usize;
        let batch = build_batch(
            (0..num_records as i32)
                .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
                .collect(),
        );

        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        endpoint
            .encode(batch.clone().arc_as_batch_reader())
            .unwrap();

        // With per-record flushes, the counter must be > 0 before batch_end.
        let mid_count = records_written(&endpoint);
        assert!(
            mid_count > 0,
            "expected records_written > 0 mid-batch, got {mid_count}"
        );
        assert!(
            mid_count <= num_records as u64,
            "records_written {mid_count} exceeds total {num_records}"
        );

        endpoint.consumer().batch_end();
        assert_eq!(records_written(&endpoint), 0);

        assert_eq!(scan_table(&client, &table).len(), num_records);
        delete_table(&client, &table);
    }

    #[test]
    fn dynamodb_progress_counter_advances_mid_batch_single_thread() {
        progress_advances_mid_batch(1);
    }

    #[test]
    fn dynamodb_progress_counter_advances_mid_batch_multi_thread() {
        progress_advances_mid_batch(3);
    }

    // Progress counter: batch_start resets a stale counter from a prior batch.
    #[test]
    fn dynamodb_progress_counter_batch_start_resets() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("progress_reset");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

        // Simulate a stale counter left over from a previous batch.
        endpoint.records_written.store(99, Ordering::Relaxed);
        endpoint.consumer().batch_start(0, OutputBatchType::Delta);
        assert_eq!(records_written(&endpoint), 0);

        // Close cleanly so the worker does not leak an open state.
        endpoint.consumer().batch_end();
        delete_table(&client, &table);
    }

    // Progress counter: resets to zero between successive batches.
    #[test]
    fn dynamodb_progress_counter_multiple_batches() {
        let endpoint_url = dynamodb_endpoint_url();
        let client = dynamodb_client(endpoint_url.as_deref());
        wait_for_dynamodb(&client);

        let table = test_table_name("progress_multi");
        create_table(&client, &table, true);
        let mut endpoint = dynamodb_endpoint(1, &table, endpoint_url.as_deref());

        let batch1 = build_batch(
            (0..50i32)
                .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
                .collect(),
        );
        encode_endpoint_batch(&mut endpoint, &batch1);
        assert_eq!(records_written(&endpoint), 0);

        let batch2 = build_batch(
            (50..80i32)
                .map(|i| (record(i, "x", Some(i as i64), "r"), 1))
                .collect(),
        );
        encode_endpoint_batch(&mut endpoint, &batch2);
        assert_eq!(records_written(&endpoint), 0);

        assert_eq!(scan_table(&client, &table).len(), 80);
        delete_table(&client, &table);
    }
}
