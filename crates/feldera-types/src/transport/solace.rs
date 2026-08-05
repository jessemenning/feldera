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

fn default_connect_timeout_secs() -> u64 {
    10
}

fn default_reconnect_retries() -> i64 {
    3
}

fn default_reconnect_retry_wait_ms() -> u64 {
    3000
}

fn default_retry_interval_secs() -> u64 {
    5
}

fn default_dedup_history_size() -> usize {
    100_000
}

/// Maximum flow window size accepted by the Solace C SDK
/// (`FLOW_PROP_WINDOWSIZE` is limited to 1..=255).
const MAX_WINDOW_SIZE: u32 = 255;

/// Upper bound on the RGMID dedup cache to keep its memory footprint sane
/// (each entry costs roughly 150 bytes, so the cap is ~1.5 GB).
const MAX_DEDUP_HISTORY_SIZE: usize = 10_000_000;

/// Solace C SDK log level, mirrored here so the connector configuration does
/// not depend on the `solace-rs` crate.  `None` defaults to `Warning`.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SolaceLogLevel {
    Critical,
    Error,
    Warning,
    Notice,
    Info,
    Debug,
}

/// Validates the connection settings shared by the Solace input and output
/// connectors.
///
/// A free function rather than a struct: the fields are spelled out inline in
/// each config (flat serde keys) for backward compatibility, so only the
/// validation logic is shared.
fn validate_common(
    host: &str,
    connect_timeout_secs: u64,
    reconnect_retries: i64,
    tls: bool,
    ssl_trust_store_dir: &Option<String>,
) -> Result<(), String> {
    if host.trim().is_empty() {
        return Err("host must not be empty".into());
    }
    if connect_timeout_secs == 0 {
        return Err("connect_timeout_secs must be >= 1".into());
    }
    if reconnect_retries < -1 {
        return Err("reconnect_retries must be >= 0, or -1 to retry forever".into());
    }
    if ssl_trust_store_dir.is_some() && !tls {
        return Err("ssl_trust_store_dir requires tls = true".into());
    }
    Ok(())
}

fn scheme(tls: bool) -> &'static str {
    if tls { "tcps" } else { "tcp" }
}

/// Transport configuration for the Solace Platform SMF input connector.
///
/// Binds to a durable Solace queue over the native SMF protocol (port 55555).
/// Messages are acknowledged to the broker only after the circuit step that
/// ingested them has been fully processed — or, in a fault-tolerant pipeline,
/// durably checkpointed — giving at-least-once delivery with in-session RGMID
/// deduplication.  The queue itself is the resume cursor: on restart the
/// broker redelivers every unacknowledged message.
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
///   'topic_pattern'        = 'demo/events/{region}/{event_type}',
///   'include_topic'        = 'true'
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
    /// client-side. Set to at least `target_msg_rate × step_interval_secs`
    /// to avoid stalling (default: `255`).
    #[serde(default = "default_window_size")]
    pub window_size: u32,

    /// Maximum number of unacknowledged messages the broker will deliver
    /// before pausing the flow. `None` uses the broker default.
    #[serde(default)]
    pub max_unacked: Option<i32>,

    /// Optional topic decomposition pattern using `{name}` placeholders, e.g.
    /// `"demo/events/{region}/{event_type}"`.
    ///
    /// Each `{name}` segment is matched against the corresponding slash-level
    /// of the Solace message destination topic. Matched values are injected
    /// as `ConnectorMetadata` fields, making them available as table columns
    /// without requiring publishers to embed them in the payload.
    #[serde(default)]
    pub topic_pattern: Option<String>,

    /// Expose the full destination topic as the `solace_topic` metadata field
    /// (default: `false`).
    ///
    /// Metadata fields are opt-in: extracting them costs allocations on every
    /// message, so enable only the fields a table actually consumes.
    #[serde(default)]
    pub include_topic: bool,

    /// Expose the broker receive timestamp as the `broker_ts` metadata field
    /// (a `TIMESTAMP`; default: `false`).
    #[serde(default)]
    pub include_broker_timestamp: bool,

    /// Number of recently-seen replication-group message IDs retained for
    /// deduplication of broker redeliveries (default: `100_000`; `0` disables
    /// deduplication).
    #[serde(default = "default_dedup_history_size")]
    pub dedup_history_size: usize,

    /// Route completely unparseable payloads to the dead-message queue via a
    /// broker `Rejected` settlement, instead of reporting a parse error and
    /// acking (default: `false`).
    #[serde(default)]
    pub reject_parse_errors: bool,

    // --- connection / resiliency (shared shape with the output config) ---
    /// Use TLS (`tcps://`) instead of plaintext `tcp://` (default: `false`).
    #[serde(default)]
    pub tls: bool,

    /// Directory holding the trusted CA certificates for TLS. Requires `tls`.
    #[serde(default)]
    pub ssl_trust_store_dir: Option<String>,

    /// Client name reported to the broker. `None` lets the SDK generate one.
    #[serde(default)]
    pub client_name: Option<String>,

    /// Timeout for the initial broker connection, in seconds (default: `10`).
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// SDK-level reconnect attempts for transient blips (default: `3`;
    /// `-1` retries forever). Longer outages are handled by the connector's
    /// own retry loop.
    #[serde(default = "default_reconnect_retries")]
    pub reconnect_retries: i64,

    /// Wait between SDK-level reconnect attempts, in milliseconds
    /// (default: `3000`).
    #[serde(default = "default_reconnect_retry_wait_ms")]
    pub reconnect_retry_wait_ms: u64,

    /// Wait between connector-level reconnect attempts after the SDK gives up,
    /// in seconds (default: `5`).
    #[serde(default = "default_retry_interval_secs")]
    pub retry_interval_secs: u64,

    /// Solace SDK log level (default: `warning`).
    #[serde(default)]
    pub log_level: Option<SolaceLogLevel>,
}

/// Sensible defaults mirroring the serde `#[serde(default …)]` attributes, so
/// `SolaceInputConfig::default()` is a valid configuration (deriving `Default`
/// would zero the timeout fields and fail `validate`).
impl Default for SolaceInputConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: default_port(),
            vpn: default_vpn(),
            username: String::new(),
            password: String::new(),
            queue: String::new(),
            window_size: default_window_size(),
            max_unacked: None,
            topic_pattern: None,
            include_topic: false,
            include_broker_timestamp: false,
            dedup_history_size: default_dedup_history_size(),
            reject_parse_errors: false,
            tls: false,
            ssl_trust_store_dir: None,
            client_name: None,
            connect_timeout_secs: default_connect_timeout_secs(),
            reconnect_retries: default_reconnect_retries(),
            reconnect_retry_wait_ms: default_reconnect_retry_wait_ms(),
            retry_interval_secs: default_retry_interval_secs(),
            log_level: None,
        }
    }
}

impl SolaceInputConfig {
    /// Whether any per-message metadata extraction is configured.  The
    /// connector skips metadata construction entirely when this is `false`,
    /// so tables that never call `CONNECTOR_METADATA()` pay nothing for it.
    pub fn metadata_requested(&self) -> bool {
        self.include_topic || self.include_broker_timestamp || self.topic_pattern.is_some()
    }

    /// Full SMF URL for the Solace C SDK, e.g. `tcp://broker.example.com:55555`
    /// (or `tcps://…` when `tls` is set).
    pub fn smf_url(&self) -> String {
        format!("{}://{}:{}", scheme(self.tls), self.host, self.port)
    }

    /// Validate the configuration, returning a human-readable error.
    pub fn validate(&self) -> Result<(), String> {
        if self.window_size == 0 || self.window_size > MAX_WINDOW_SIZE {
            // Enforced here so an out-of-range window is a config error, not
            // a bind failure at runtime (the C SDK caps the flow window).
            return Err(format!("window_size must be in 1..={MAX_WINDOW_SIZE}"));
        }
        if let Some(max_unacked) = self.max_unacked
            && max_unacked != -1
            && max_unacked <= 0
        {
            return Err("max_unacked must be > 0, or -1 for no limit".into());
        }
        if self.dedup_history_size > MAX_DEDUP_HISTORY_SIZE {
            return Err(format!(
                "dedup_history_size must be <= {MAX_DEDUP_HISTORY_SIZE} \
                 (each entry costs roughly 150 bytes of memory)"
            ));
        }
        if self.retry_interval_secs == 0 {
            return Err("retry_interval_secs must be >= 1".into());
        }
        if self.username.trim().is_empty() {
            return Err("username must not be empty".into());
        }
        if self.queue.trim().is_empty() {
            return Err("queue must not be empty".into());
        }
        validate_common(
            &self.host,
            self.connect_timeout_secs,
            self.reconnect_retries,
            self.tls,
            &self.ssl_trust_store_dir,
        )
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

    /// Maximum broker acknowledgements in flight before `batch_end` blocks,
    /// for `persistent` delivery (default: `256`).
    #[serde(default = "default_max_inflight_acks")]
    pub max_inflight_acks: usize,

    /// Maximum time to wait for broker acknowledgements when draining
    /// in-flight `persistent` publishes, in seconds (default: `30`).
    ///
    /// Bounds the blocking wait at each batch boundary.  If the broker does
    /// not acknowledge within this window (broker death, spool over quota),
    /// the batch fails with a transport error instead of stalling the
    /// pipeline indefinitely.
    #[serde(default = "default_ack_timeout_secs")]
    pub ack_timeout_secs: u64,

    /// Per-topic publish throttle with conflation, in milliseconds
    /// (default: `None` — disabled).
    ///
    /// When set, at most one message per resolved topic is published per
    /// window.  Records arriving inside the window are conflated: only the
    /// latest is kept, and it is published at a batch boundary once the
    /// window expires.  Use this to bound the outbound message rate of a
    /// high-churn view (e.g. a per-key aggregate updating hundreds of times
    /// per second) without losing the newest value.
    ///
    /// A record conflated on a stream that then goes quiet is published at
    /// the next batch boundary after its window expires, not immediately on
    /// expiry.
    #[serde(default)]
    pub dedup_window_ms: Option<u64>,

    // --- connection / resiliency (shared shape with the input config) ---
    /// Use TLS (`tcps://`) instead of plaintext `tcp://` (default: `false`).
    #[serde(default)]
    pub tls: bool,

    /// Directory holding the trusted CA certificates for TLS. Requires `tls`.
    #[serde(default)]
    pub ssl_trust_store_dir: Option<String>,

    /// Client name reported to the broker. `None` lets the SDK generate one.
    #[serde(default)]
    pub client_name: Option<String>,

    /// Timeout for the initial broker connection, in seconds (default: `10`).
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// SDK-level reconnect attempts for transient blips (default: `3`;
    /// `-1` retries forever).
    #[serde(default = "default_reconnect_retries")]
    pub reconnect_retries: i64,

    /// Wait between SDK-level reconnect attempts, in milliseconds
    /// (default: `3000`).
    #[serde(default = "default_reconnect_retry_wait_ms")]
    pub reconnect_retry_wait_ms: u64,

    /// Solace SDK log level (default: `warning`).
    #[serde(default)]
    pub log_level: Option<SolaceLogLevel>,
}

fn default_max_inflight_acks() -> usize {
    256
}

fn default_ack_timeout_secs() -> u64 {
    30
}

/// Sensible defaults mirroring the serde attributes (see the input config for
/// the rationale).
impl Default for SolaceOutputConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: default_port(),
            vpn: default_vpn(),
            username: String::new(),
            password: String::new(),
            topic: String::new(),
            delivery_mode: OutputDeliveryMode::default(),
            max_inflight_acks: default_max_inflight_acks(),
            ack_timeout_secs: default_ack_timeout_secs(),
            dedup_window_ms: None,
            tls: false,
            ssl_trust_store_dir: None,
            client_name: None,
            connect_timeout_secs: default_connect_timeout_secs(),
            reconnect_retries: default_reconnect_retries(),
            reconnect_retry_wait_ms: default_reconnect_retry_wait_ms(),
            log_level: None,
        }
    }
}

impl SolaceOutputConfig {
    /// Full SMF URL for the Solace C SDK, e.g. `tcp://broker.example.com:55555`
    /// (or `tcps://…` when `tls` is set).
    pub fn smf_url(&self) -> String {
        format!("{}://{}:{}", scheme(self.tls), self.host, self.port)
    }

    /// Validate the configuration, returning a human-readable error.
    pub fn validate(&self) -> Result<(), String> {
        if self.username.trim().is_empty() {
            return Err("username must not be empty".into());
        }
        if self.topic.trim().is_empty() {
            return Err("topic must not be empty".into());
        }
        if self.ack_timeout_secs == 0 {
            return Err("ack_timeout_secs must be >= 1".into());
        }
        if self.max_inflight_acks == 0 {
            // 0 would silently degrade to one blocking broker round-trip per
            // message — a latency cliff, not a meaningful configuration.
            return Err("max_inflight_acks must be >= 1".into());
        }
        if self.dedup_window_ms == Some(0) {
            return Err("dedup_window_ms must be >= 1 when set; omit it to disable".into());
        }
        validate_common(
            &self.host,
            self.connect_timeout_secs,
            self.reconnect_retries,
            self.tls,
            &self.ssl_trust_store_dir,
        )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn input_cfg(host: &str, port: u16) -> SolaceInputConfig {
        SolaceInputConfig {
            host: host.into(),
            port,
            username: "u".into(),
            password: "p".into(),
            queue: "q".into(),
            window_size: 255,
            ..Default::default()
        }
    }

    fn output_cfg(host: &str, port: u16) -> SolaceOutputConfig {
        SolaceOutputConfig {
            host: host.into(),
            port,
            username: "u".into(),
            password: "p".into(),
            topic: "t".into(),
            ..Default::default()
        }
    }

    // --- smf_url ---

    #[test]
    fn input_smf_url_default_port() {
        assert_eq!(
            input_cfg("broker.example.com", 55555).smf_url(),
            "tcp://broker.example.com:55555"
        );
    }

    #[test]
    fn input_smf_url_custom_port() {
        assert_eq!(
            input_cfg("localhost", 1234).smf_url(),
            "tcp://localhost:1234"
        );
    }

    #[test]
    fn input_smf_url_tls() {
        let mut cfg = input_cfg("broker.example.com", 55443);
        cfg.tls = true;
        assert_eq!(cfg.smf_url(), "tcps://broker.example.com:55443");
    }

    #[test]
    fn output_smf_url() {
        assert_eq!(
            output_cfg("mq.prod.corp", 55555).smf_url(),
            "tcp://mq.prod.corp:55555"
        );
    }

    // --- validate ---

    #[test]
    fn input_validate_accepts_defaults() {
        assert!(input_cfg("h", 55555).validate().is_ok());
    }

    #[test]
    fn input_validate_rejects_zero_window() {
        let mut cfg = input_cfg("h", 55555);
        cfg.window_size = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn input_validate_rejects_empty_queue() {
        let mut cfg = input_cfg("h", 55555);
        cfg.queue = "  ".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn input_validate_trust_store_requires_tls() {
        let mut cfg = input_cfg("h", 55555);
        cfg.ssl_trust_store_dir = Some("/certs".into());
        assert!(
            cfg.validate().is_err(),
            "trust store without tls is invalid"
        );
        cfg.tls = true;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn input_validate_rejects_oversized_window() {
        // The C SDK caps FLOW_PROP_WINDOWSIZE at 255; larger values must be a
        // config error, not a bind failure at runtime.
        let mut cfg = input_cfg("h", 55555);
        cfg.window_size = 256;
        assert!(cfg.validate().is_err());
        cfg.window_size = 255;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn input_validate_max_unacked_bounds() {
        let mut cfg = input_cfg("h", 55555);
        cfg.max_unacked = Some(-1);
        assert!(cfg.validate().is_ok(), "-1 means no limit");
        cfg.max_unacked = Some(100);
        assert!(cfg.validate().is_ok());
        cfg.max_unacked = Some(0);
        assert!(cfg.validate().is_err());
        cfg.max_unacked = Some(-5);
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn input_validate_rejects_oversized_dedup_history() {
        let mut cfg = input_cfg("h", 55555);
        cfg.dedup_history_size = 10_000_001;
        assert!(cfg.validate().is_err());
        cfg.dedup_history_size = 0;
        assert!(cfg.validate().is_ok(), "0 disables dedup");
    }

    #[test]
    fn validate_rejects_bad_reconnect_retries() {
        let mut input = input_cfg("h", 55555);
        input.reconnect_retries = -5;
        assert!(input.validate().is_err(), "-5 is not a retry count");
        input.reconnect_retries = -1;
        assert!(input.validate().is_ok(), "-1 means retry forever");

        let mut output = output_cfg("h", 55555);
        output.reconnect_retries = -5;
        assert!(output.validate().is_err());
    }

    #[test]
    fn output_validate_rejects_empty_topic() {
        let mut cfg = output_cfg("h", 55555);
        cfg.topic = "".into();
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn output_validate_dedup_window_bounds() {
        let mut cfg = output_cfg("h", 55555);
        cfg.dedup_window_ms = Some(0);
        assert!(
            cfg.validate().is_err(),
            "0 ms is not a window; omit to disable"
        );
        cfg.dedup_window_ms = Some(1);
        assert!(cfg.validate().is_ok());
        cfg.dedup_window_ms = None;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn output_validate_rejects_zero_ack_knobs() {
        let mut cfg = output_cfg("h", 55555);
        cfg.ack_timeout_secs = 0;
        assert!(cfg.validate().is_err());

        let mut cfg = output_cfg("h", 55555);
        cfg.max_inflight_acks = 0;
        assert!(
            cfg.validate().is_err(),
            "0 would mean one blocking round-trip per message"
        );
    }

    // --- serde defaults ---

    #[test]
    fn input_config_minimal_json_applies_defaults() {
        let json = r#"{"host":"h","username":"u","password":"p","queue":"q"}"#;
        let cfg: SolaceInputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.port, 55555);
        assert_eq!(cfg.vpn, "default");
        assert_eq!(cfg.window_size, 255);
        assert_eq!(cfg.topic_pattern, None);
        assert_eq!(cfg.dedup_history_size, 100_000);
        assert!(!cfg.reject_parse_errors);
        assert!(!cfg.tls);
        assert_eq!(cfg.connect_timeout_secs, 10);
        assert_eq!(cfg.reconnect_retries, 3);
        assert_eq!(cfg.retry_interval_secs, 5);
        assert_eq!(cfg.max_unacked, None);
        assert_eq!(cfg.log_level, None);
    }

    #[test]
    fn input_config_explicit_values_round_trip() {
        let json = r#"{
            "host":"broker.example.com",
            "port":55003,
            "vpn":"my-vpn",
            "username":"feldera",
            "password":"secret",
            "queue":"feldera-q",
            "window_size":128,
            "topic_pattern":"demo/{region}/{type}",
            "dedup_history_size":50000,
            "tls":true,
            "log_level":"info"
        }"#;
        let cfg: SolaceInputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.host, "broker.example.com");
        assert_eq!(cfg.port, 55003);
        assert_eq!(cfg.vpn, "my-vpn");
        assert_eq!(cfg.window_size, 128);
        assert_eq!(cfg.topic_pattern.as_deref(), Some("demo/{region}/{type}"));
        assert_eq!(cfg.dedup_history_size, 50000);
        assert!(cfg.tls);
        assert_eq!(cfg.log_level, Some(SolaceLogLevel::Info));
        assert_eq!(cfg.smf_url(), "tcps://broker.example.com:55003");
    }

    #[test]
    fn output_config_minimal_json_applies_defaults() {
        let json = r#"{"host":"h","username":"u","password":"p","topic":"t"}"#;
        let cfg: SolaceOutputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.port, 55555);
        assert_eq!(cfg.vpn, "default");
        assert_eq!(cfg.delivery_mode, OutputDeliveryMode::Direct);
        assert_eq!(cfg.max_inflight_acks, 256);
    }

    #[test]
    fn output_config_persistent_delivery_mode() {
        let json = r#"{"host":"h","username":"u","password":"p","topic":"t","delivery_mode":"persistent"}"#;
        let cfg: SolaceOutputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.delivery_mode, OutputDeliveryMode::Persistent);
    }

    #[test]
    fn delivery_mode_default_is_direct() {
        assert_eq!(OutputDeliveryMode::default(), OutputDeliveryMode::Direct);
    }
}
