use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

fn default_port() -> u16 {
    55555
}

fn default_vpn() -> String {
    "default".into()
}

fn default_window_size() -> u32 {
    255
}

/// Transport configuration for the Solace Platform SMF input connector.
///
/// Binds to a durable Solace queue over the native SMF protocol (port 55555).
/// Messages are held unacked until Feldera confirms the circuit step has
/// completed (`AckMode::Client` + `completion_watcher()`), providing
/// at-least-once delivery with in-session RGMID deduplication.
///
/// ## Topic decomposition
///
/// Set `topic_pattern` (e.g. `"demo/events/{region}/{event_type}"`) to
/// automatically extract named captures from the Solace message destination
/// and inject them as table columns via `ConnectorMetadata`. Publishers do
/// not need to embed topic levels in the payload.
///
/// ## Example SQL
///
/// ```sql
/// CREATE TABLE events (
///   solace_topic VARCHAR,
///   region       VARCHAR,
///   event_type   VARCHAR,
///   payload      VARCHAR
/// ) WITH (
///   'connector'            = 'solace_input',
///   'host'                 = 'broker.example.com',
///   'username'             = 'feldera',
///   'password'             = 'secret',
///   'queue'                = 'feldera-events-q',
///   'topic_pattern'        = 'demo/events/{region}/{event_type}'
/// );
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct SolaceInputConfig {
    /// Broker hostname (no scheme, no port).
    pub host: String,

    /// SMF port (default: `55555`).
    #[serde(default = "default_port")]
    pub port: u16,

    /// Solace message VPN name (default: `"default"`).
    #[serde(default = "default_vpn")]
    pub vpn: String,

    /// Client username.
    pub username: String,

    /// Client password.
    pub password: String,

    /// Durable queue name to bind to.
    pub queue: String,

    /// Flow window size — how many unacked messages the broker buffers
    /// client-side. Set to at least `target_msg_rate × commit_interval_secs`
    /// to avoid stalling (default: `255`).
    #[serde(default = "default_window_size")]
    pub window_size: u32,

    /// Optional topic decomposition pattern using `{name}` placeholders, e.g.
    /// `"demo/events/{region}/{event_type}"`.
    ///
    /// Each `{name}` segment is matched against the corresponding slash-level
    /// of the Solace message destination topic. Matched values are injected
    /// as `ConnectorMetadata` fields, making them available as table columns
    /// without requiring publishers to embed them in the payload.
    #[serde(default)]
    pub topic_pattern: Option<String>,
}

impl SolaceInputConfig {
    /// Full SMF URL for the Solace C SDK, e.g. `tcp://broker.example.com:55555`.
    pub fn smf_url(&self) -> String {
        format!("tcp://{}:{}", self.host, self.port)
    }
}

/// Transport configuration for the Solace Platform SMF output connector.
///
/// Publishes serialized view records to a Solace topic over SMF.
/// The destination topic supports `{field}` placeholders resolved from the
/// JSON record, including Feldera's `insert_delete` wrapper format.
///
/// ## Example SQL
///
/// ```sql
/// CREATE MATERIALIZED VIEW region_counts
/// WITH (
///   'connector'     = 'solace_output',
///   'host'          = 'broker.example.com',
///   'username'      = 'feldera',
///   'password'      = 'secret',
///   'topic'         = 'demo/results/{region}/{event_type}',
///   'delivery_mode' = 'persistent'
/// ) AS
/// SELECT region, event_type, COUNT(*) AS event_count
/// FROM   events
/// GROUP  BY region, event_type;
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
pub struct SolaceOutputConfig {
    /// Broker hostname (no scheme, no port).
    pub host: String,

    /// SMF port (default: `55555`).
    #[serde(default = "default_port")]
    pub port: u16,

    /// Solace message VPN name (default: `"default"`).
    #[serde(default = "default_vpn")]
    pub vpn: String,

    /// Client username.
    pub username: String,

    /// Client password.
    pub password: String,

    /// Destination topic template. Static topics are used as-is.
    /// Use `{field}` placeholders to derive the topic from the record.
    ///
    /// For Feldera's `insert_delete` format the connector checks under the
    /// `"insert"` or `"delete"` wrapper before falling back to the top level.
    pub topic: String,

    /// Delivery mode (default: `direct`).
    #[serde(default)]
    pub delivery_mode: OutputDeliveryMode,
}

impl SolaceOutputConfig {
    /// Full SMF URL for the Solace C SDK, e.g. `tcp://broker.example.com:55555`.
    pub fn smf_url(&self) -> String {
        format!("tcp://{}:{}", self.host, self.port)
    }
}

/// Solace message delivery mode for the output connector.
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum OutputDeliveryMode {
    /// Fire-and-forget; lowest latency, no broker acknowledgement (default).
    #[default]
    Direct,
    /// Broker-acknowledged; survives broker restarts at the cost of latency.
    Persistent,
}
