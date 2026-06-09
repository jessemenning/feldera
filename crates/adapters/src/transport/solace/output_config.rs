use serde::Deserialize;

/// Transport configuration for the Solace output connector.
///
/// Declared in the SQL `WITH` clause on a view:
///
/// ```sql
/// CREATE MATERIALIZED VIEW region_counts
/// WITH (
///   'connector' = 'solace_output',
///   'solace.host' = 'localhost',
///   'solace.topic' = 'demo/results/{region}/{event_type}'
/// )
/// AS SELECT ...;
/// ```
#[derive(Clone, Debug, Deserialize)]
pub struct SolaceOutputConfig {
    /// Broker hostname (no scheme, no port).
    pub host: String,

    /// SMF port (default: 55555).
    #[serde(default = "default_port")]
    pub port: u16,

    /// Solace message VPN name.
    #[serde(default = "default_vpn")]
    pub vpn: String,

    /// Client username.
    pub username: String,

    /// Client password.
    pub password: String,

    /// Destination topic template. Static topics are used as-is.
    /// Use `{field}` placeholders to derive the topic from the serialized record.
    ///
    /// Example: `"demo/results/{region}/{event_type}"` — the connector parses
    /// the record as JSON and substitutes `region` and `event_type` field values.
    /// For Feldera's insert_delete output format the lookup checks under the
    /// `"insert"` or `"delete"` wrapper before falling back to top-level.
    pub topic: String,

    /// Delivery mode (default: `direct`).
    #[serde(default)]
    pub delivery_mode: OutputDeliveryMode,
}

#[derive(Clone, Debug, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OutputDeliveryMode {
    #[default]
    Direct,
    Persistent,
}

fn default_port() -> u16 {
    55555
}

fn default_vpn() -> String {
    "default".into()
}

impl SolaceOutputConfig {
    pub fn smf_url(&self) -> String {
        format!("tcp://{}:{}", self.host, self.port)
    }
}
