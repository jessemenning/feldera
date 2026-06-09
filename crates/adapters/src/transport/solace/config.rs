use serde::Deserialize;

/// Transport configuration for the Solace input connector.
///
/// Declared in the SQL `WITH` clause:
///
/// ```sql
/// CREATE TABLE t (...)
/// WITH (
///   'connector' = 'solace_input',
///   'solace.host' = 'localhost',
///   'solace.port' = '55555',
///   'solace.vpn' = 'default',
///   'solace.username' = 'admin',
///   'solace.password' = 'admin',
///   'solace.queue' = 'my-queue'
/// );
/// ```
#[derive(Clone, Debug, Deserialize)]
pub struct SolaceInputConfig {
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

    /// Durable queue name to bind to.
    pub queue: String,

    /// Flow window size: how many unacked messages the broker buffers client-side.
    /// Set to at least `target_msg_rate × commit_interval_secs` to avoid stalling.
    #[serde(default = "default_window_size")]
    pub window_size: u32,

    /// Optional topic decomposition pattern using `{name}` placeholders, e.g.
    /// `"demo/events/{region}/{event_type}"`.
    ///
    /// Each `{name}` segment is matched against the corresponding slash-level of the
    /// Solace message destination topic. Matched values are inserted into
    /// `ConnectorMetadata` under that name, making them available as table columns
    /// without requiring the publisher to embed them in the payload.
    ///
    /// Static segments (no braces) are skipped. Unmatched levels (topic shorter than
    /// pattern) are silently ignored.
    #[serde(default)]
    pub topic_pattern: Option<String>,
}

fn default_port() -> u16 {
    55555
}

fn default_vpn() -> String {
    "default".into()
}

fn default_window_size() -> u32 {
    255
}

impl SolaceInputConfig {
    /// Full SMF URL passed to the Solace C SDK, e.g. `tcp://localhost:55555`.
    pub fn smf_url(&self) -> String {
        format!("tcp://{}:{}", self.host, self.port)
    }
}

/// Extract named captures from a Solace destination topic using a pattern.
///
/// Pattern: `"demo/events/{region}/{event_type}"`
/// Topic:   `"demo/events/us-east/order"`
/// Returns: `[("region", "us-east"), ("event_type", "order")]`
///
/// Segments without braces are static and produce no captures.
pub fn parse_topic_fields<'a>(pattern: &'a str, topic: &'a str) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    for (pat_seg, topic_seg) in pattern.split('/').zip(topic.split('/')) {
        if pat_seg.starts_with('{') && pat_seg.ends_with('}') {
            let name = &pat_seg[1..pat_seg.len() - 1];
            if !name.is_empty() {
                fields.push((name.to_string(), topic_seg.to_string()));
            }
        }
    }
    fields
}
