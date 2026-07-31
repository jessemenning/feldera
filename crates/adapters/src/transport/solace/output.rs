use anyhow::Result as AnyResult;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputBatchType, OutputEndpoint, Step};
use feldera_types::transport::solace::SolaceLogLevel as ConfigLogLevel;
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::message::{DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder};
use solace_rs::{Context, SolaceLogLevel};
use tracing::{info, warn};

use super::output_config::{OutputDeliveryMode, SolaceOutputConfig};

/// Map the connector's log-level config to the Solace SDK enum (default
/// `Warning`).
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

// ---------------------------------------------------------------------------
// Endpoint
// ---------------------------------------------------------------------------

/// Feldera output connector that publishes serialized view records to a Solace topic.
///
/// The destination topic supports `{field}` placeholders resolved from the
/// serialized JSON record. See [`SolaceOutputConfig::topic`] for details.
pub struct SolaceOutputEndpoint {
    config: SolaceOutputConfig,
    conn: Option<SolaceConnection>,
    /// True when the topic has no `{field}` placeholders, so every record
    /// publishes to the same destination.
    static_topic: bool,
}

struct SolaceConnection {
    /// Kept alive so the session's C-SDK context pointer remains valid.
    _context: Context,
    session: solace_rs::async_support::AsyncSession,
}

impl SolaceOutputEndpoint {
    pub fn new(config: SolaceOutputConfig) -> AnyResult<Self> {
        config
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid Solace output config: {e}"))?;
        // A static topic (no `{` placeholders) resolves to the same
        // destination for every record, so validate it once here and fail
        // fast on an invalid topic string rather than per-publish.
        let is_static = !config.topic.contains('{');
        if is_static {
            MessageDestination::new(DestinationType::Topic, config.topic.as_str())
                .map_err(|e| anyhow::anyhow!("invalid topic '{}': {e:?}", config.topic))?;
        }
        Ok(Self {
            config,
            conn: None,
            static_topic: is_static,
        })
    }

    fn delivery_mode(&self) -> DeliveryMode {
        match self.config.delivery_mode {
            OutputDeliveryMode::Direct => DeliveryMode::Direct,
            OutputDeliveryMode::Persistent => DeliveryMode::Persistent,
        }
    }

    fn publish(&self, topic: &str, payload: &[u8]) -> AnyResult<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("output endpoint not connected"))?;

        let dest = MessageDestination::new(DestinationType::Topic, topic)
            .map_err(|e| anyhow::anyhow!("invalid topic '{topic}': {e:?}"))?;

        let msg = OutboundMessageBuilder::new()
            .destination(dest)
            .delivery_mode(self.delivery_mode())
            .payload(payload.to_vec())
            .build()
            .map_err(|e| anyhow::anyhow!("message build: {e:?}"))?;

        conn.session
            .publish(msg)
            .map_err(|e| anyhow::anyhow!("publish to '{topic}': {e:?}"))?;

        Ok(())
    }
}

impl OutputEndpoint for SolaceOutputEndpoint {
    fn connect(&mut self, _async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        // NOTE: async delivery errors (broker rejections, connection-down) are
        // not yet surfaced through `_async_error_callback`.  Draining the
        // session event channel from a sync `OutputEndpoint` requires
        // `AsyncSession::take_event_receiver`, a pending change in the
        // solace-rs fork.  Until it lands, only synchronous publish errors
        // reach the controller; persistent-mode broker ACKs accumulate in the
        // session event channel.  See the connector improvement plan, P0.4.
        let context = Context::new(solace_log_level(self.config.log_level))
            .map_err(|e| anyhow::anyhow!("Solace context init: {e:?}"))?;

        // AsyncSessionBuilder::build() is synchronous despite the async session type —
        // the "async" refers to message delivery, not the build process.
        let mut builder = AsyncSessionBuilder::new(&context)
            .host_name(self.config.smf_url())
            .vpn_name(self.config.vpn.clone())
            .username(self.config.username.clone())
            .password(self.config.password.clone())
            .reconnect_retries(self.config.reconnect_retries)
            .reconnect_retry_wait_ms(self.config.reconnect_retry_wait_ms)
            .connect_timeout_ms(self.config.connect_timeout_secs.saturating_mul(1000));
        if let Some(name) = &self.config.client_name {
            builder = builder.client_name(name.clone());
        }
        if let Some(dir) = &self.config.ssl_trust_store_dir {
            builder = builder.ssl_trust_store_dir(dir.clone());
        }
        let session = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Solace session: {e:?}"))?;

        info!(
            "Output connected to Solace {} vpn={} topic={}",
            self.config.smf_url(),
            self.config.vpn,
            self.config.topic
        );

        // Both _context and session must live together — context must outlive session.
        // Fields are dropped in declaration order, so _context is dropped after session.
        self.conn = Some(SolaceConnection {
            _context: context,
            session,
        });
        Ok(())
    }

    fn max_buffer_size_bytes(&self) -> usize {
        1024 * 1024
    }

    fn push_buffer(&mut self, buffer: &[u8]) -> AnyResult<()> {
        if self.static_topic {
            // No template to resolve — publish straight to the fixed topic.
            self.publish(&self.config.topic, buffer)
        } else {
            let topic = resolve_topic(&self.config.topic, buffer);
            self.publish(&topic, buffer)
        }
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        if let Some(bytes) = val {
            if self.static_topic {
                self.publish(&self.config.topic, bytes)?;
            } else {
                let topic = resolve_topic(&self.config.topic, bytes);
                self.publish(&topic, bytes)?;
            }
        }
        Ok(())
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        Ok(())
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

impl Drop for SolaceOutputEndpoint {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            if let Err(e) = conn.session.disconnect() {
                warn!("Solace output disconnect error: {e:?}");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Topic template resolution
// ---------------------------------------------------------------------------

/// Substitute `{field}` placeholders in `template` with values from the JSON
/// record in `buffer`.
///
/// Feldera's `insert_delete` output format wraps records as `{"insert":{...}}`
/// or `{"delete":{...}}`. This function checks the wrapped value first before
/// falling back to the top-level object. Returns `template` unchanged if:
///   - `template` contains no `{` characters (static topic — fast path)
///   - `buffer` is not valid JSON
///   - a placeholder's field is absent from the record (placeholder kept as-is)
pub fn resolve_topic(template: &str, buffer: &[u8]) -> String {
    if !template.contains('{') {
        return template.to_string();
    }

    let val = match serde_json::from_slice::<serde_json::Value>(buffer) {
        Ok(v) => v,
        Err(_) => return template.to_string(),
    };

    // For insert_delete format, look inside "insert" or "delete" wrapper first.
    let record = val
        .get("insert")
        .or_else(|| val.get("delete"))
        .unwrap_or(&val);

    let mut result = String::with_capacity(template.len());
    let mut chars = template.chars();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut name = String::new();
            let mut closed = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    closed = true;
                    break;
                }
                name.push(inner);
            }
            if closed {
                let replacement = record
                    .get(&name)
                    .map(|v| match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    })
                    .unwrap_or_else(|| format!("{{{name}}}"));
                result.push_str(&replacement);
            } else {
                // Unclosed brace — keep literally.
                result.push('{');
                result.push_str(&name);
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::resolve_topic;

    #[test]
    fn static_topic_passthrough() {
        assert_eq!(resolve_topic("demo/results/all", b"{}"), "demo/results/all");
    }

    #[test]
    fn top_level_field_substitution() {
        let buf = br#"{"region":"us-east","event_type":"order"}"#;
        assert_eq!(
            resolve_topic("demo/results/{region}/{event_type}", buf),
            "demo/results/us-east/order"
        );
    }

    #[test]
    fn insert_delete_wrapper() {
        let buf = br#"{"insert":{"region":"eu-west","event_type":"login"}}"#;
        assert_eq!(
            resolve_topic("demo/{region}/{event_type}", buf),
            "demo/eu-west/login"
        );
    }

    #[test]
    fn delete_wrapper() {
        let buf = br#"{"delete":{"region":"ap-south","event_type":"logout"}}"#;
        assert_eq!(
            resolve_topic("{region}/{event_type}", buf),
            "ap-south/logout"
        );
    }

    #[test]
    fn missing_field_keeps_placeholder() {
        let buf = br#"{"region":"us-east"}"#;
        assert_eq!(
            resolve_topic("{region}/{missing}", buf),
            "us-east/{missing}"
        );
    }

    #[test]
    fn invalid_json_returns_template() {
        assert_eq!(
            resolve_topic("{region}/results", b"not json"),
            "{region}/results"
        );
    }

    #[test]
    fn numeric_field_value_no_quotes() {
        let buf = br#"{"count":42}"#;
        assert_eq!(resolve_topic("stats/{count}", buf), "stats/42");
    }

    #[test]
    fn same_placeholder_repeated() {
        let buf = br#"{"region":"us-east"}"#;
        assert_eq!(
            resolve_topic("{region}/{region}", buf),
            "us-east/us-east"
        );
    }

    #[test]
    fn unclosed_brace_kept_literally() {
        let buf = br#"{"region":"us-east"}"#;
        // `{region` without closing `}` — kept as-is.
        assert_eq!(resolve_topic("{region/end", buf), "{region/end");
    }

    #[test]
    fn empty_brace_kept_literally() {
        // `{}` has an empty field name; `record.get("")` finds nothing in an empty
        // object, so the placeholder is preserved as `{}`.
        assert_eq!(resolve_topic("{}/end", b"{}"), "{}/end");
    }

    #[test]
    fn no_braces_empty_buffer() {
        assert_eq!(resolve_topic("static/topic", b""), "static/topic");
    }
}
