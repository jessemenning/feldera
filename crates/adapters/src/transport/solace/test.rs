//! Integration tests for the Solace transport, gated on the
//! `solace-integration-test` feature.
//!
//! The tests need a Solace Platform broker with SMF on port 55555 and SEMP v2
//! on port 8080 (admin/admin).  Start one with:
//!
//! ```text
//! docker run -d --name solace-test --shm-size=2g \
//!   -p 55555:55555 -p 8080:8080 \
//!   -e username_admin_globalaccesslevel=admin \
//!   -e username_admin_password=admin \
//!   solace/solace-pubsub-standard:latest
//! ```
//!
//! (or reuse the perf harness compose file under `perf/solace/`).  Point the
//! tests at a non-local broker with `SOLACE_TEST_BROKER=<host>`.  Run with:
//!
//! ```text
//! cargo test -p dbsp_adapters --no-default-features \
//!   --features solace-integration-test --lib transport::solace::test
//! ```
//!
//! Each test provisions its own uniquely-named queue over SEMP and removes it
//! afterwards, so tests can run concurrently against a shared broker.
//!
//! A broker kill/restart mid-stream (full process death rather than an
//! unbind) is not automated here — container control from the test binary is
//! not worth the weight.  [`input_recovers_from_flow_loss`] covers the same
//! connector-side path (flow `DownError` → teardown → periodic rebind) by
//! bouncing the queue's egress instead.

use std::io;
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::{Duration, Instant};

use csv::WriterBuilder;
use serde_json::json;
use tokio::sync::watch;
use tracing::info;

use crate::test::{
    MockDeZSet, TestStruct, init_test_logger, mock_input_pipeline, mock_parser_pipeline,
    wait_for_output_unordered,
};
use crate::transport::input_transport_config_to_endpoint;
use anyhow::Error as AnyError;
use feldera_adapterlib::format::BufferSize;
use feldera_adapterlib::transport::{
    InputConsumer, InputReader, OutputEndpoint, Resume, Watermark,
};
use feldera_types::adapter_stats::ConnectorHealth;
use feldera_types::config::{FtModel, InputEndpointConfig};
use feldera_types::coordination::Completion;
use feldera_types::program_schema::Relation;
use feldera_types::secret_resolver::default_secrets_directory;
use feldera_types::transport::solace::SolaceOutputConfig;
use solace_rs::async_support::{AsyncSession, AsyncSessionBuilder};
use solace_rs::message::{
    DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder,
};
use solace_rs::{Context, SolaceLogLevel};

use super::SolaceOutputEndpoint;
use crate::ParseError;

/// Message VPN used by all tests (the broker default).
const VPN: &str = "default";

/// SMF port on the test broker.
const SMF_PORT: u16 = 55555;

/// SEMP v2 management port on the test broker.
const SEMP_PORT: u16 = 8080;

/// Client credentials, provisioned over SEMP by [`Semp::ensure_client_username`].
const CLIENT_USERNAME: &str = "feldera-test";
const CLIENT_PASSWORD: &str = "feldera-test";

/// Hostname of the test broker; override with `SOLACE_TEST_BROKER`.
fn broker_host() -> String {
    std::env::var("SOLACE_TEST_BROKER").unwrap_or_else(|_| "localhost".to_string())
}

fn probe_port(address: &str) -> bool {
    let Ok(address) = address.parse() else {
        panic!("cannot parse probe address {address}");
    };
    match TcpStream::connect_timeout(&address, Duration::from_secs(1)) {
        Ok(_) => true,
        Err(e) => match e.kind() {
            io::ErrorKind::ConnectionRefused | io::ErrorKind::TimedOut => false,
            _ => panic!("Error probing port {address}: {e:?}"),
        },
    }
}

/// Panics with setup instructions when the broker is not reachable.
fn require_broker() -> String {
    let host = broker_host();
    let smf = format!("{host}:{SMF_PORT}");
    if !probe_port(&smf) {
        panic!(
            "Solace Platform broker not found at {smf}. Start one with \
             'docker run -d --name solace-test --shm-size=2g -p 55555:55555 -p 8080:8080 \
             -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
             solace/solace-pubsub-standard:latest' or set SOLACE_TEST_BROKER."
        );
    }
    host
}

// ---------------------------------------------------------------------------
// SEMP v2 helpers
// ---------------------------------------------------------------------------

/// Thin SEMP v2 client bound to one broker and message VPN.
///
/// Provisioning is idempotent: HTTP 400/409 ("already exists") counts as
/// success so repeated runs against a warm broker work (mirrors the perf
/// harness `perf/solace/solace_perf/broker.py`).
struct Semp {
    client: reqwest::blocking::Client,
    config_base: String,
    monitor_base: String,
    action_base: String,
}

impl Semp {
    fn new(host: &str) -> Self {
        let base = format!("http://{host}:{SEMP_PORT}/SEMP/v2");
        Self {
            client: reqwest::blocking::Client::new(),
            config_base: format!("{base}/config/msgVpns/{VPN}"),
            monitor_base: format!("{base}/monitor/msgVpns/{VPN}"),
            action_base: format!("{base}/action/msgVpns/{VPN}"),
        }
    }

    fn post_config(&self, path: &str, body: serde_json::Value) {
        let resp = self
            .client
            .post(format!("{}/{path}", self.config_base))
            .basic_auth("admin", Some("admin"))
            .json(&body)
            .send()
            .expect("SEMP POST failed to send");
        let status = resp.status().as_u16();
        assert!(
            matches!(status, 200 | 400 | 409),
            "SEMP POST {path} failed: {status} {}",
            resp.text().unwrap_or_default()
        );
    }

    fn patch_config(&self, path: &str, body: serde_json::Value) {
        let resp = self
            .client
            .patch(format!("{}/{path}", self.config_base))
            .basic_auth("admin", Some("admin"))
            .json(&body)
            .send()
            .expect("SEMP PATCH failed to send");
        assert!(
            resp.status().is_success(),
            "SEMP PATCH {path} failed: {} {}",
            resp.status(),
            resp.text().unwrap_or_default()
        );
    }

    fn delete_config(&self, path: &str) {
        let resp = self
            .client
            .delete(format!("{}/{path}", self.config_base))
            .basic_auth("admin", Some("admin"))
            .send()
            .expect("SEMP DELETE failed to send");
        let status = resp.status().as_u16();
        assert!(
            matches!(status, 200 | 400 | 404),
            "SEMP DELETE {path} failed: {status} {}",
            resp.text().unwrap_or_default()
        );
    }

    /// Creates the client username the connector and publisher authenticate as.
    fn ensure_client_username(&self) {
        self.post_config(
            "clientUsernames",
            json!({
                "clientUsername": CLIENT_USERNAME,
                "password": CLIENT_PASSWORD,
                "enabled": true,
                "aclProfileName": "default",
                "clientProfileName": "default",
            }),
        );
    }

    /// Deletes and recreates `queue` so each test starts from an empty spool.
    fn recreate_queue(&self, queue: &str) {
        self.delete_config(&format!("queues/{queue}"));
        self.post_config(
            "queues",
            json!({
                "queueName": queue,
                "accessType": "exclusive",
                "permission": "consume",
                "ingressEnabled": true,
                "egressEnabled": true,
            }),
        );
    }

    /// Subscribes `queue` to `topic` so published topic messages spool to it.
    fn add_queue_subscription(&self, queue: &str, topic: &str) {
        self.post_config(
            &format!("queues/{queue}/subscriptions"),
            json!({ "subscriptionTopic": topic }),
        );
    }

    /// Enables or disables message delivery from `queue` (bounces consumer
    /// flows without deleting the queue).
    fn set_queue_egress(&self, queue: &str, enabled: bool) {
        self.patch_config(
            &format!("queues/{queue}"),
            json!({ "egressEnabled": enabled }),
        );
    }

    /// Force-disconnects one client by its client name (SEMP action API).
    /// Scoped to a single client so concurrent tests sharing the broker are
    /// unaffected.  Succeeds silently if the client is already gone.
    fn disconnect_client(&self, client_name: &str) {
        let resp = self
            .client
            .put(format!(
                "{}/clients/{client_name}/disconnect",
                self.action_base
            ))
            .basic_auth("admin", Some("admin"))
            .json(&json!({}))
            .send()
            .expect("SEMP action PUT failed to send");
        let status = resp.status().as_u16();
        assert!(
            matches!(status, 200 | 400 | 404),
            "SEMP disconnect of {client_name} failed: {status} {}",
            resp.text().unwrap_or_default()
        );
    }

    /// Current queue depth: messages spooled and not yet acknowledged
    /// (`collections.msgs.count` in the SEMP monitor response).
    ///
    /// `spooledMsgCount` is cumulative and never decrements, and the
    /// `lastSpooledMsgId - highestAckedMsgId` difference uses broker-global
    /// message IDs, so it is wrong whenever more than one queue receives
    /// messages.  The `msgs` collection count is the only per-queue depth.
    /// Returns -1 when the queue cannot be read.
    fn queue_backlog(&self, queue: &str) -> i64 {
        let resp = self
            .client
            .get(format!("{}/queues/{queue}", self.monitor_base))
            .basic_auth("admin", Some("admin"))
            .send()
            .expect("SEMP GET failed to send");
        if !resp.status().is_success() {
            return -1;
        }
        let body: serde_json::Value = resp.json().expect("SEMP GET returned invalid JSON");
        body["collections"]["msgs"]["count"].as_i64().unwrap_or(-1)
    }

    /// Polls until the queue backlog equals `expected`.
    fn wait_for_backlog(&self, queue: &str, expected: i64, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut backlog = i64::MIN;
        while Instant::now() < deadline {
            backlog = self.queue_backlog(queue);
            if backlog == expected {
                return;
            }
            sleep(Duration::from_millis(200));
        }
        panic!("queue {queue} backlog is {backlog}, expected {expected} within {timeout:?}");
    }
}

// ---------------------------------------------------------------------------
// Test publisher
// ---------------------------------------------------------------------------

/// Publishes guaranteed messages straight to a queue over its own session.
struct TestPublisher {
    // Dropped after `session` (declaration order): the context must outlive it.
    session: AsyncSession,
    _context: Context,
}

impl TestPublisher {
    fn connect(host: &str) -> Self {
        let context = Context::new(SolaceLogLevel::Warning).expect("publisher context");
        let session = AsyncSessionBuilder::new(&context)
            .host_name(format!("tcp://{host}:{SMF_PORT}"))
            .vpn_name(VPN.to_string())
            .username(CLIENT_USERNAME.to_string())
            .password(CLIENT_PASSWORD.to_string())
            .build()
            .expect("publisher session");
        Self {
            session,
            _context: context,
        }
    }

    /// Publishes each batch as one persistent CSV message to `queue`,
    /// waiting for the broker to acknowledge every message.
    fn send_csv(&self, queue: &str, data: &[Vec<TestStruct>]) {
        for batch in data {
            if batch.is_empty() {
                continue;
            }
            let mut writer = WriterBuilder::new()
                .has_headers(false)
                .from_writer(Vec::with_capacity(batch.len() * 32));
            for val in batch.iter().cloned() {
                writer.serialize(val).unwrap();
            }
            writer.flush().unwrap();
            let bytes = writer.into_inner().unwrap();
            self.send_raw(queue, bytes);
        }
    }

    /// Publishes one persistent message to `queue` and waits for the broker ack.
    fn send_raw(&self, queue: &str, payload: Vec<u8>) {
        let msg = OutboundMessageBuilder::new()
            .destination(MessageDestination::new(DestinationType::Queue, queue).unwrap())
            .delivery_mode(DeliveryMode::Persistent)
            .payload(payload)
            .build()
            .expect("message build");
        let rx = self.session.publish_with_ack(msg).expect("publish");
        rx.blocking_recv()
            .expect("ack channel closed")
            .expect("broker rejected message");
    }
}

// ---------------------------------------------------------------------------
// Shared setup
// ---------------------------------------------------------------------------

struct TestBroker {
    host: String,
    semp: Semp,
    queue: String,
}

impl TestBroker {
    /// Probes the broker and provisions a uniquely-named queue.
    fn provision(test_name: &str) -> Self {
        init_test_logger();
        let host = require_broker();
        let semp = Semp::new(&host);
        semp.ensure_client_username();
        let queue = format!("feldera-test-{test_name}-{}", uuid::Uuid::new_v4());
        semp.recreate_queue(&queue);
        Self { host, semp, queue }
    }

    /// Input-connector config JSON for [`mock_input_pipeline`].
    fn input_pipeline_config(&self) -> serde_json::Value {
        json!({
            "stream": "test_input",
            "transport": {
                "name": "solace_input",
                "config": {
                    "host": self.host,
                    "port": SMF_PORT,
                    "vpn": VPN,
                    "username": CLIENT_USERNAME,
                    "password": CLIENT_PASSWORD,
                    "queue": self.queue,
                    // Fast connector-level retries keep the recovery test short.
                    "retry_interval_secs": 1,
                    "connect_timeout_secs": 5,
                }
            },
            "format": {
                "name": "csv"
            }
        })
    }
}

impl Drop for TestBroker {
    fn drop(&mut self) {
        self.semp.delete_config(&format!("queues/{}", self.queue));
    }
}

fn test_batches(first_id: u32, batches: usize, batch_size: usize) -> Vec<Vec<TestStruct>> {
    (0..batches)
        .map(|b| {
            (0..batch_size)
                .map(|i| TestStruct::for_id(first_id + (b * batch_size + i) as u32))
                .collect()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Watched consumer
// ---------------------------------------------------------------------------

/// An `InputConsumer` whose completion and (optionally) checkpoint frontiers
/// are driven by the test, with capture of the `Resume` values passed to
/// `extended()`.
///
/// `MockInputConsumer` deliberately supplies no watchers, which makes the
/// connector take its at-most-once fallback (ack on hand-off).  This consumer
/// exercises the deferred-ack machinery instead: the connector releases acks
/// only when the test advances the completion frontier — or, when a
/// checkpoint watcher is supplied, the checkpoint frontier.
#[derive(Clone)]
struct WatchedConsumer {
    completion_rx: watch::Receiver<Completion>,
    checkpoint_rx: Option<watch::Receiver<u64>>,
    /// For every `extended()` call: whether it carried a `Resume::Seek`.
    seek_replies: Arc<Mutex<Vec<bool>>>,
}

impl WatchedConsumer {
    /// Returns the consumer plus the senders that drive its frontiers; the
    /// checkpoint sender is `Some` iff `strict`.
    fn new(strict: bool) -> (Self, watch::Sender<Completion>, Option<watch::Sender<u64>>) {
        let (completion_tx, completion_rx) = watch::channel(Completion::default());
        let (checkpoint_tx, checkpoint_rx) = if strict {
            let (tx, rx) = watch::channel(0u64);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let consumer = Self {
            completion_rx,
            checkpoint_rx,
            seek_replies: Arc::new(Mutex::new(Vec::new())),
        };
        (consumer, completion_tx, checkpoint_tx)
    }

    /// Asserts that every `Queue` reply so far carried `Resume::Seek` (the
    /// value the checkpoint builder requires from an at-least-once endpoint).
    fn assert_all_replies_carry_seek(&self) {
        let replies = self.seek_replies.lock().unwrap();
        assert!(!replies.is_empty(), "no Queue replies were captured");
        assert!(
            replies.iter().all(|seek| *seek),
            "every extended() must carry Resume::Seek"
        );
    }
}

impl InputConsumer for WatchedConsumer {
    fn error(&self, fatal: bool, error: AnyError, tag: Option<&'static str>) {
        // Transient errors (reconnects, shutdown races) are expected during
        // these tests; a fatal error is not.
        assert!(!fatal, "fatal transport error (tag={tag:?}): {error}");
        info!("non-fatal transport error (tag={tag:?}): {error}");
    }

    fn eoi(&self) {}

    fn max_batch_size(&self) -> usize {
        usize::MAX
    }

    fn pipeline_fault_tolerance(&self) -> Option<FtModel> {
        if self.checkpoint_rx.is_some() {
            Some(FtModel::AtLeastOnce)
        } else {
            None
        }
    }

    fn parse_errors(&self, _errors: Vec<ParseError>) {}

    fn buffered(&self, _amt: BufferSize) {}

    fn replayed(&self, _num_records: BufferSize, _hash: u64) {}

    fn request_step(&self) {}

    fn extended(&self, _amt: BufferSize, resume: Option<Resume>, _watermarks: Vec<Watermark>) {
        self.seek_replies
            .lock()
            .unwrap()
            .push(matches!(resume, Some(Resume::Seek { .. })));
    }

    fn start_transaction(&self, _label: Option<&str>) {}

    fn commit_transaction(&self) {}

    fn update_connector_health(&self, _health: ConnectorHealth) {}

    fn completion_watcher(&self) -> Option<watch::Receiver<Completion>> {
        Some(self.completion_rx.clone())
    }

    fn checkpoint_watcher(&self) -> Option<watch::Receiver<u64>> {
        self.checkpoint_rx.clone()
    }
}

/// Like [`mock_input_pipeline`], but wires a [`WatchedConsumer`] so the test
/// controls the completion/checkpoint frontiers that release deferred acks.
#[allow(clippy::type_complexity)]
fn watched_input_pipeline(
    broker: &TestBroker,
    strict: bool,
) -> (
    Box<dyn InputReader>,
    MockDeZSet<TestStruct, TestStruct>,
    WatchedConsumer,
    watch::Sender<Completion>,
    Option<watch::Sender<u64>>,
) {
    let config: InputEndpointConfig =
        serde_json::from_value(broker.input_pipeline_config()).unwrap();
    let relation = Relation::empty();

    // Reuse the mock parser/zset plumbing; its consumer is unused.
    let (_mock_consumer, parser, zset) = mock_parser_pipeline::<TestStruct, TestStruct>(
        &relation,
        config.connector_config.format.as_ref().unwrap(),
    )
    .unwrap();

    let (consumer, completion_tx, checkpoint_tx) = WatchedConsumer::new(strict);

    let endpoint = input_transport_config_to_endpoint(
        &config.connector_config.transport,
        "",
        default_secrets_directory(),
    )
    .unwrap()
    .unwrap();
    let reader = endpoint
        .open(Box::new(consumer.clone()), Box::new(parser), relation, None)
        .unwrap();

    (reader, zset, consumer, completion_tx, checkpoint_tx)
}

/// Advances a completion watcher to `steps` completed steps.
fn complete_steps(tx: &watch::Sender<Completion>, steps: u64) {
    tx.send_replace(Completion {
        total_completed_steps: steps,
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Publish → ingest → every message acked (broker backlog drains to zero).
#[test]
fn input_end_to_end_acks_all_messages() {
    let broker = TestBroker::provision("e2e");
    let publisher = TestPublisher::connect(&broker.host);

    // 10 batches of 10 rows: send_csv publishes each batch as one message.
    let data = test_batches(0, 10, 10);
    publisher.send_csv(&broker.queue, &data);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 10, Duration::from_secs(30));

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_json::from_value(broker.input_pipeline_config()).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    endpoint.extend();

    wait_for_output_unordered(&zset, &data, || endpoint.queue(false));

    // At-least-once bookkeeping: once every step is flushed, the connector
    // acks everything and the broker spool drains.
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    endpoint.disconnect();
}

/// A paused endpoint buffers nothing; resuming delivers everything without
/// loss or duplication.
#[test]
fn input_pause_resume_no_loss() {
    let broker = TestBroker::provision("pause");
    let publisher = TestPublisher::connect(&broker.host);

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_json::from_value(broker.input_pipeline_config()).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    endpoint.extend();

    let data = test_batches(0, 5, 10);
    publisher.send_csv(&broker.queue, &data);
    wait_for_output_unordered(&zset, &data, || endpoint.queue(false));
    zset.reset();

    info!("pausing endpoint");
    endpoint.pause();
    sleep(Duration::from_millis(1000));

    let more = test_batches(1000, 5, 10);
    publisher.send_csv(&broker.queue, &more);
    sleep(Duration::from_millis(1000));
    assert_eq!(
        zset.state().flushed.len(),
        0,
        "paused endpoint must not ingest"
    );

    info!("resuming endpoint");
    endpoint.extend();
    wait_for_output_unordered(&zset, &more, || endpoint.queue(false));

    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    endpoint.disconnect();
}

/// Losing the flow mid-stream (queue egress bounced, which unbinds the
/// consumer exactly like a broker restart does) must trigger the connector's
/// reconnect loop: it rebinds within `retry_interval_secs` and resumes
/// ingesting with no duplicate rows.
#[test]
fn input_recovers_from_flow_loss() {
    let broker = TestBroker::provision("recover");
    let publisher = TestPublisher::connect(&broker.host);

    let (endpoint, _consumer, _parser, zset) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_json::from_value(broker.input_pipeline_config()).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    endpoint.extend();

    let data = test_batches(0, 5, 10);
    publisher.send_csv(&broker.queue, &data);
    wait_for_output_unordered(&zset, &data, || endpoint.queue(false));
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    zset.reset();

    info!("bouncing queue egress to kill the flow");
    broker.semp.set_queue_egress(&broker.queue, false);
    // Give the flow-down event time to reach the connector (it polls flow
    // events every 500ms) before restoring egress.
    sleep(Duration::from_millis(1500));
    broker.semp.set_queue_egress(&broker.queue, true);

    // The connector retries every retry_interval_secs (1s in this config)
    // and rebinds; new data flows again, exactly once.
    let more = test_batches(1000, 5, 10);
    publisher.send_csv(&broker.queue, &more);
    wait_for_output_unordered(&zset, &more, || endpoint.queue(false));

    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    endpoint.disconnect();
}

/// Acks are deferred until the completion frontier passes the ingesting
/// step: the broker backlog stays put while the test withholds completion,
/// and drains only after the frontier advances.  This is the wiring test for
/// the connector's core at-least-once mechanism ([`mock_input_pipeline`]'s
/// consumer has no completion watcher, so the other input tests exercise the
/// at-most-once fallback instead).
#[test]
fn input_defers_acks_until_step_completion() {
    let broker = TestBroker::provision("defer");
    let publisher = TestPublisher::connect(&broker.host);

    let data = test_batches(0, 2, 10);
    publisher.send_csv(&broker.queue, &data);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 2, Duration::from_secs(30));

    let (reader, zset, consumer, completion_tx, _) = watched_input_pipeline(&broker, false);
    reader.extend();
    wait_for_output_unordered(&zset, &data, || reader.queue(false));

    // Ingested and flushed, but the ingesting step has not completed: the
    // messages must stay unacked on the broker.
    sleep(Duration::from_secs(2));
    assert_eq!(
        broker.semp.queue_backlog(&broker.queue),
        2,
        "acks must be withheld until step completion"
    );

    // Completing the step releases the acks.
    complete_steps(&completion_tx, 1);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    consumer.assert_all_replies_carry_seek();
    reader.disconnect();
}

/// Regression test for the completed-step drift bug: the controller can
/// complete steps without polling this endpoint (checkpoint-barrier or
/// transaction-commit steps), so the completion count jumps past the
/// endpoint's flush count.  A batch flushed *after* such a jump must not be
/// released by the (already higher) frontier — it must wait for its own
/// step's completion.  The old per-Queue counter got exactly this wrong.
#[test]
fn input_acks_survive_frontier_jumps() {
    let broker = TestBroker::provision("drift");
    let publisher = TestPublisher::connect(&broker.host);

    let (reader, zset, _consumer, completion_tx, _) = watched_input_pipeline(&broker, false);
    reader.extend();

    // Batch 1 flushes while the completion count is 0 and releases at 1.
    let batch1 = test_batches(0, 1, 10);
    publisher.send_csv(&broker.queue, &batch1);
    wait_for_output_unordered(&zset, &batch1, || reader.queue(false));
    zset.reset();

    // Simulate steps that skipped this endpoint: jump the frontier to 5.
    // Batch 1 (keyed 0) drains; nothing else may.
    complete_steps(&completion_tx, 5);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));

    // Batch 2 flushes while the count is already 5, so it is keyed 5 and
    // must stay unacked at frontier 5.
    let batch2 = test_batches(1000, 1, 10);
    publisher.send_csv(&broker.queue, &batch2);
    wait_for_output_unordered(&zset, &batch2, || reader.queue(false));
    sleep(Duration::from_secs(2));
    assert_eq!(
        broker.semp.queue_backlog(&broker.queue),
        1,
        "a batch flushed after a frontier jump must wait for its own step"
    );

    complete_steps(&completion_tx, 6);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    reader.disconnect();
}

/// Fault-tolerant (strict) mode: acks are gated on durable checkpoints, not
/// on in-memory step completion, so data covered only by completed steps
/// stays unacked until a checkpoint covers it.
#[test]
fn input_ft_acks_only_after_checkpoint() {
    let broker = TestBroker::provision("ft-ack");
    let publisher = TestPublisher::connect(&broker.host);

    let data = test_batches(0, 2, 10);
    publisher.send_csv(&broker.queue, &data);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 2, Duration::from_secs(30));

    let (reader, zset, consumer, completion_tx, checkpoint_tx) =
        watched_input_pipeline(&broker, true);
    let checkpoint_tx = checkpoint_tx.expect("strict pipeline supplies a checkpoint sender");
    reader.extend();
    wait_for_output_unordered(&zset, &data, || reader.queue(false));

    // Step completion alone must not release acks in strict mode.
    complete_steps(&completion_tx, 1);
    sleep(Duration::from_secs(2));
    assert_eq!(
        broker.semp.queue_backlog(&broker.queue),
        2,
        "strict mode must hold acks until a checkpoint covers the step"
    );

    // A durable checkpoint covering the step releases them.
    checkpoint_tx.send_replace(1);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    consumer.assert_all_replies_carry_seek();
    reader.disconnect();
}

/// Fault-tolerant crash/resume: only checkpoint-covered messages are acked
/// at shutdown; the uncovered tail stays on the broker and redelivers in
/// full to a fresh endpoint (no loss; duplicates would be permitted but the
/// covered prefix was acked, so none occur here).
#[test]
fn input_ft_resume_after_crash_no_loss() {
    let broker = TestBroker::provision("ft-resume");
    let publisher = TestPublisher::connect(&broker.host);

    let (reader, zset, _consumer, completion_tx, checkpoint_tx) =
        watched_input_pipeline(&broker, true);
    let checkpoint_tx = checkpoint_tx.expect("strict pipeline supplies a checkpoint sender");
    reader.extend();

    // Prefix: ingested, completed, and checkpoint-covered => acked.
    let covered = test_batches(0, 1, 10);
    publisher.send_csv(&broker.queue, &covered);
    wait_for_output_unordered(&zset, &covered, || reader.queue(false));
    complete_steps(&completion_tx, 1);
    checkpoint_tx.send_replace(1);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 0, Duration::from_secs(30));
    zset.reset();

    // Tail: ingested and completed but NOT covered by any checkpoint.
    let uncovered = test_batches(1000, 5, 2);
    publisher.send_csv(&broker.queue, &uncovered);
    wait_for_output_unordered(&zset, &uncovered, || reader.queue(false));
    complete_steps(&completion_tx, 2);

    // "Crash": shut the endpoint down.  Strict close releases only
    // checkpoint-covered acks, so the tail must remain on the broker.
    reader.disconnect();
    broker
        .semp
        .wait_for_backlog(&broker.queue, 5, Duration::from_secs(30));

    // Resume from the checkpoint: a fresh endpoint receives exactly the
    // uncovered tail again.
    let (reader2, _consumer2, _parser2, zset2) = mock_input_pipeline::<TestStruct, TestStruct>(
        serde_json::from_value(broker.input_pipeline_config()).unwrap(),
        Relation::empty(),
    )
    .unwrap();
    reader2.extend();
    wait_for_output_unordered(&zset2, &uncovered, || reader2.queue(false));
    reader2.disconnect();
}

/// The output endpoint survives a session-level failure: after the broker
/// force-disconnects the client (standing in for an outage that exhausts the
/// SDK's reconnect budget), the next publish rebuilds the session and
/// delivery continues.
#[test]
fn output_recovers_from_session_loss() {
    let broker = TestBroker::provision("out-recover");
    let topic = format!("feldera/test/{}", uuid::Uuid::new_v4());
    broker.semp.add_queue_subscription(&broker.queue, &topic);
    let client_name = format!("feldera-out-{}", uuid::Uuid::new_v4().simple());

    let config: SolaceOutputConfig = serde_json::from_value(json!({
        "host": broker.host,
        "port": SMF_PORT,
        "vpn": VPN,
        "username": CLIENT_USERNAME,
        "password": CLIENT_PASSWORD,
        "topic": topic,
        "delivery_mode": "persistent",
        "client_name": client_name,
        // No SDK-level retries: a forced disconnect surfaces as DownError
        // immediately, exercising the connector-level rebuild.
        "reconnect_retries": 0,
    }))
    .unwrap();

    let mut endpoint = SolaceOutputEndpoint::new(config).unwrap();
    endpoint
        .connect(Box::new(|fatal, error, code| {
            // Session loss must surface as non-fatal (the endpoint rebuilds).
            assert!(!fatal, "fatal output error (code={code:?}): {error}");
            info!("non-fatal output error (code={code:?}): {error}");
        }))
        .unwrap();

    endpoint.push_buffer(b"{\"id\":1}").unwrap();
    endpoint.batch_end().unwrap();
    broker
        .semp
        .wait_for_backlog(&broker.queue, 1, Duration::from_secs(30));

    info!("force-disconnecting the output client");
    broker.semp.disconnect_client(&client_name);

    // The DownError propagates to the event drainer asynchronously; retry
    // the publish until the rebuild succeeds.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        match endpoint
            .push_buffer(b"{\"id\":2}")
            .and_then(|()| endpoint.batch_end())
        {
            Ok(()) => break,
            Err(e) if Instant::now() < deadline => {
                info!("publish after disconnect failed (retrying): {e}");
                sleep(Duration::from_millis(500));
            }
            Err(e) => panic!("output did not recover from session loss: {e}"),
        }
    }

    broker
        .semp
        .wait_for_backlog(&broker.queue, 2, Duration::from_secs(30));
}

/// Dedup window against a live broker: records inside the window are
/// conflated to a single message flushed after expiry (mirrors the
/// Materialize sink-dedup scenario: three timed publishes through a window
/// yield exactly two messages).
#[test]
fn output_dedup_window_conflates() {
    let broker = TestBroker::provision("out-dedup");
    let topic = format!("feldera/test/{}", uuid::Uuid::new_v4());
    broker.semp.add_queue_subscription(&broker.queue, &topic);

    const WINDOW_MS: u64 = 2000;
    let config: SolaceOutputConfig = serde_json::from_value(json!({
        "host": broker.host,
        "port": SMF_PORT,
        "vpn": VPN,
        "username": CLIENT_USERNAME,
        "password": CLIENT_PASSWORD,
        "topic": topic,
        "delivery_mode": "persistent",
        "dedup_window_ms": WINDOW_MS,
    }))
    .unwrap();

    let mut endpoint = SolaceOutputEndpoint::new(config).unwrap();
    endpoint
        .connect(Box::new(|fatal, error, code| {
            panic!("async error (fatal={fatal}, code={code:?}): {error}");
        }))
        .unwrap();

    // First record publishes immediately; the next two conflate.
    endpoint.push_buffer(b"{\"id\":1}").unwrap();
    endpoint.push_buffer(b"{\"id\":2}").unwrap();
    endpoint.push_buffer(b"{\"id\":3}").unwrap();
    endpoint.batch_end().unwrap();
    broker
        .semp
        .wait_for_backlog(&broker.queue, 1, Duration::from_secs(30));

    // After the window expires, the next batch boundary flushes exactly one
    // conflated message (the latest value; latest-wins is unit-tested).
    sleep(Duration::from_millis(WINDOW_MS + 500));
    endpoint.batch_end().unwrap();
    broker
        .semp
        .wait_for_backlog(&broker.queue, 2, Duration::from_secs(30));

    // Nothing further flushes: the intermediate record was conflated away.
    sleep(Duration::from_secs(1));
    endpoint.batch_end().unwrap();
    sleep(Duration::from_millis(500));
    assert_eq!(
        broker.semp.queue_backlog(&broker.queue),
        2,
        "conflation must collapse the intermediate record"
    );
}

/// Persistent-mode output: every published record is broker-acknowledged by
/// `batch_end`, and all records arrive on a queue subscribed to the topic.
#[test]
fn output_persistent_publish_is_acknowledged() {
    let broker = TestBroker::provision("output");
    let topic = format!("feldera/test/{}", uuid::Uuid::new_v4());
    broker.semp.add_queue_subscription(&broker.queue, &topic);

    let config: SolaceOutputConfig = serde_json::from_value(json!({
        "host": broker.host,
        "port": SMF_PORT,
        "vpn": VPN,
        "username": CLIENT_USERNAME,
        "password": CLIENT_PASSWORD,
        "topic": topic,
        "delivery_mode": "persistent",
    }))
    .unwrap();

    let mut endpoint = SolaceOutputEndpoint::new(config).unwrap();
    endpoint
        .connect(Box::new(|fatal, error, code| {
            panic!("async error (fatal={fatal}, code={code:?}): {error}");
        }))
        .unwrap();

    const N: i64 = 100;
    for i in 0..N {
        endpoint
            .push_buffer(format!("{{\"id\":{i}}}").as_bytes())
            .unwrap();
    }
    // batch_end drains the windowed publish acks; returning Ok means the
    // broker persisted every message.
    endpoint.batch_end().unwrap();

    broker
        .semp
        .wait_for_backlog(&broker.queue, N, Duration::from_secs(30));
}
