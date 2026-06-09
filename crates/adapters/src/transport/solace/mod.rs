//! Solace Platform input/output transport adapters using the SMF native protocol.
//!
//! # Input (`SolaceInputEndpoint`)
//!
//! Binds to a durable Solace queue via `solace-rs` (Solace C SDK wrapper).
//! Uses `AckMode::Client` so messages are held unacked until Feldera confirms the
//! circuit step has completed via `completion_watcher()`, providing at-least-once
//! delivery with in-session RGMID dedup.
//!
//! Hierarchical topic decomposition: configure `topic_pattern` (e.g.
//! `"demo/events/{region}/{event_type}"`) and the connector automatically
//! extracts named captures from the Solace message destination and injects
//! them as `ConnectorMetadata` fields — table columns can be populated from
//! topic levels without requiring publishers to embed them in the payload.
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
