//! Solace Platform input/output transport adapters using the SMF native protocol.
//!
//! # Input (`SolaceInputEndpoint`)
//!
//! Binds to a durable Solace queue via `solace-rs` (Solace C SDK wrapper).
//! Uses `AckMode::Client`: a message is acknowledged to the broker only after
//! the circuit step that ingested it has been fully processed, tracked via
//! `checkpoint_watcher()` when the pipeline is fault-tolerant, otherwise
//! `completion_watcher()`. This gives at-least-once delivery — a crash before
//! step completion leaves the message unacked, so the broker redelivers it —
//! with in-session RGMID deduplication of those redeliveries. Acks are drained
//! from a dedicated task branch so they never block the circuit's step loop.
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
//! # Feature flag
//!
//! Enable with `--features with-solace`. Requires the Solace C SDK installed
//! on the build host (see `solace-rs` for installation instructions).

pub mod config;
pub mod input;
pub mod output;
pub mod output_config;

pub use config::SolaceInputConfig;
pub use input::SolaceInputEndpoint;
pub use output::SolaceOutputEndpoint;
pub use output_config::SolaceOutputConfig;
