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
use std::thread::sleep;
use std::time::{Duration, Instant};

use csv::WriterBuilder;
use serde_json::json;
use tracing::info;

use crate::test::{
    TestStruct, init_test_logger, mock_input_pipeline, wait_for_output_unordered,
};
use feldera_adapterlib::transport::OutputEndpoint;
use feldera_types::program_schema::Relation;
use feldera_types::transport::solace::SolaceOutputConfig;
use solace_rs::async_support::{AsyncSession, AsyncSessionBuilder};
use solace_rs::message::{
    DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder,
};
use solace_rs::{Context, SolaceLogLevel};

use super::SolaceOutputEndpoint;

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
}

impl Semp {
    fn new(host: &str) -> Self {
        let base = format!("http://{host}:{SEMP_PORT}/SEMP/v2");
        Self {
            client: reqwest::blocking::Client::new(),
            config_base: format!("{base}/config/msgVpns/{VPN}"),
            monitor_base: format!("{base}/monitor/msgVpns/{VPN}"),
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

    /// Deletes and recreates `queue` so message-ID-based backlog arithmetic
    /// starts from a clean slate.
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
        self.patch_config(&format!("queues/{queue}"), json!({ "egressEnabled": enabled }));
    }

    /// Undelivered/unacked queue depth: `lastSpooledMsgId - highestAckedMsgId`.
    ///
    /// `spooledMsgCount` is cumulative and never decrements, so the message-ID
    /// difference is the only correct depth measure.  Returns -1 when the
    /// fields are unavailable (nothing spooled yet).
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
        let data = &body["data"];
        match (data["lastSpooledMsgId"].as_i64(), data["highestAckedMsgId"].as_i64()) {
            (Some(last), Some(acked)) => last - acked,
            _ => -1,
        }
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
// Tests
// ---------------------------------------------------------------------------

/// Publish → ingest → every message acked (broker backlog drains to zero).
#[test]
fn input_end_to_end_acks_all_messages() {
    let broker = TestBroker::provision("e2e");
    let publisher = TestPublisher::connect(&broker.host);

    let data = test_batches(0, 10, 10);
    publisher.send_csv(&broker.queue, &data);
    broker
        .semp
        .wait_for_backlog(&broker.queue, 100, Duration::from_secs(10));

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(
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

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(
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

    let (endpoint, _consumer, _parser, zset) =
        mock_input_pipeline::<TestStruct, TestStruct>(
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
        .wait_for_backlog(&broker.queue, N, Duration::from_secs(10));
}
