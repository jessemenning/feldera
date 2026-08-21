//! Solace Platform input/output transport adapters using the SMF native protocol.
//!
//! # Sidecar alternative (recommended for new deployments)
//!
//! This in-process connector compiles the Solace C SDK into the pipeline
//! binary and therefore requires a custom Feldera build.  A standalone
//! **sidecar** — <https://github.com/jessemenning/feldera-solace-sidecar> —
//! provides the same behavior (durable-queue consumption with deferred acks
//! and RGMID dedup, topic-template publishing with a conflation window) while
//! running beside an **unmodified upstream Feldera image**, bridging data over
//! the pipeline's HTTP API.  Prefer the sidecar unless you specifically need
//! the in-process connector's tighter completion/checkpoint ack coupling; see
//! that repo's `docs/SEMANTICS.md` for the delivery-guarantee differences.
//!
//! # Input (`SolaceInputEndpoint`)
//!
//! Binds to a durable Solace queue via `solace-rs` (Solace C SDK wrapper).
//! Uses `AckMode::Client`: a message is acknowledged to the broker only after
//! the circuit step that ingested it has been fully processed.  The release
//! frontier is the checkpoint watcher in a fault-tolerant pipeline (acks wait
//! for a durable checkpoint covering the step) and the in-memory completion
//! watcher otherwise.  This gives at-least-once delivery — a crash before
//! step completion leaves the message unacked, so the broker redelivers it —
//! with in-session RGMID deduplication of those redeliveries. Acks are drained
//! from a dedicated task branch so they never block the circuit's step loop.
//!
//! The endpoint declares `FtModel::AtLeastOnce`: the durable queue is the
//! resume cursor, so checkpoint/suspend/resume need no seek metadata — on
//! resume the broker redelivers every unacked message.  Redeliveries of
//! messages that were ingested but not yet acked at suspend time appear as
//! duplicates after a resume (the RGMID cache is session-scoped); a future
//! enhancement could persist a bounded RGMID tail in the checkpoint to
//! suppress them.
//!
//! Hierarchical topic decomposition: configure `topic_pattern` (e.g.
//! `"demo/events/{region}/{event_type}"`) and the connector automatically
//! extracts named captures from the Solace message destination and injects
//! them as `ConnectorMetadata` fields — table columns can be populated from
//! topic levels without requiring publishers to embed them in the payload.
//!
//! ## Connection lifecycle
//!
//! The input connector owns a reconnect loop.  Short network blips are
//! handled inside the Solace C SDK (`reconnect_retries` /
//! `reconnect_retry_wait_ms`); when the SDK gives up (session `DownError`,
//! flow `DownError`/`BindFailedError`/`SessionDown`, or a closed message
//! channel), the connector tears the session down and rebuilds it every
//! `retry_interval_secs` until the broker returns.  Authentication,
//! authorization, and unknown-queue failures are fatal and stop the
//! endpoint; ambiguous failures default to retryable.  Unacknowledged
//! in-flight messages are redelivered by the broker on the new flow and
//! dropped by the RGMID cache when already ingested (with
//! `dedup_history_size = 0` a reconnect can therefore duplicate rows).
//!
//! There is no application-level inactivity probe: the Solace C SDK runs
//! protocol keepalives, and session/flow events are the health signal.
//!
//! # Output (`SolaceOutputEndpoint`)
//!
//! Publishes serialized view records to a Solace topic. The destination topic
//! supports `{field}` placeholders resolved from the JSON record, including
//! Feldera's `insert_delete` wrapper format.
//!
//! ## Connection lifecycle
//!
//! The output recovers differently from the input: it is a synchronous
//! endpoint driven per batch, so instead of a background reconnect loop it
//! rebuilds on demand.  When the SDK exhausts its own reconnect budget, the
//! session-event drainer marks the session poisoned and the next publish
//! tears it down and builds a fresh one; batches that raced the failure
//! surface as (non-fatal) transport errors.  `persistent` delivery bounds
//! each batch-boundary ack wait with `ack_timeout_secs`, so a broker that
//! stops acknowledging fails the batch rather than stalling the pipeline.
//!
//! # Feature flag
//!
//! Enable with `--features with-solace`. Requires the Solace C SDK installed
//! on the build host (see `solace-rs` for installation instructions).

use feldera_types::transport::solace::SolaceLogLevel as ConfigLogLevel;
use solace_rs::SolaceLogLevel;

pub mod config;
pub mod input;
pub mod output;

#[cfg(all(test, feature = "solace-integration-test"))]
mod test;

pub use input::SolaceInputEndpoint;
pub use output::SolaceOutputEndpoint;

/// Map the connector's log-level config to the Solace SDK enum, defaulting to
/// `Warning` when unset.
fn solace_log_level(cfg: Option<ConfigLogLevel>) -> SolaceLogLevel {
    match cfg {
        Some(ConfigLogLevel::Critical) => SolaceLogLevel::Critical,
        Some(ConfigLogLevel::Error) => SolaceLogLevel::Error,
        Some(ConfigLogLevel::Warning) | None => SolaceLogLevel::Warning,
        Some(ConfigLogLevel::Notice) => SolaceLogLevel::Notice,
        Some(ConfigLogLevel::Info) => SolaceLogLevel::Info,
        Some(ConfigLogLevel::Debug) => SolaceLogLevel::Debug,
    }
}
