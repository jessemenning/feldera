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

#[cfg(test)]
mod tests {
    use super::*;

    fn input_cfg(host: &str, port: u16) -> SolaceInputConfig {
        SolaceInputConfig {
            host: host.into(),
            port,
            vpn: "default".into(),
            username: "u".into(),
            password: "p".into(),
            queue: "q".into(),
            window_size: 255,
            topic_pattern: None,
        }
    }

    fn output_cfg(host: &str, port: u16) -> SolaceOutputConfig {
        SolaceOutputConfig {
            host: host.into(),
            port,
            vpn: "default".into(),
            username: "u".into(),
            password: "p".into(),
            topic: "t".into(),
            delivery_mode: OutputDeliveryMode::Direct,
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
        assert_eq!(input_cfg("localhost", 1234).smf_url(), "tcp://localhost:1234");
    }

    #[test]
    fn output_smf_url() {
        assert_eq!(
            output_cfg("mq.prod.corp", 55555).smf_url(),
            "tcp://mq.prod.corp:55555"
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
            "topic_pattern":"demo/{region}/{type}"
        }"#;
        let cfg: SolaceInputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.host, "broker.example.com");
        assert_eq!(cfg.port, 55003);
        assert_eq!(cfg.vpn, "my-vpn");
        assert_eq!(cfg.window_size, 128);
        assert_eq!(cfg.topic_pattern.as_deref(), Some("demo/{region}/{type}"));
        assert_eq!(cfg.smf_url(), "tcp://broker.example.com:55003");
    }

    #[test]
    fn output_config_minimal_json_applies_defaults() {
        let json = r#"{"host":"h","username":"u","password":"p","topic":"t"}"#;
        let cfg: SolaceOutputConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.port, 55555);
        assert_eq!(cfg.vpn, "default");
        assert_eq!(cfg.delivery_mode, OutputDeliveryMode::Direct);
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
