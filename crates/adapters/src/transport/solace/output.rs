use anyhow::Result as AnyResult;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputBatchType, OutputEndpoint, Step};
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::message::{DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder};
use solace_rs::{Context, SolaceLogLevel};
use tracing::{info, warn};

use super::output_config::{OutputDeliveryMode, SolaceOutputConfig};

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
}

struct SolaceConnection {
    /// Kept alive so the session's C-SDK context pointer remains valid.
    _context: Context,
    session: solace_rs::async_support::AsyncSession,
}

impl SolaceOutputEndpoint {
    pub fn new(config: SolaceOutputConfig) -> Self {
        Self { config, conn: None }
    }

    fn publish(&self, topic: &str, payload: &[u8]) -> AnyResult<()> {
        let conn = self
            .conn
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("output endpoint not connected"))?;

        let dest = MessageDestination::new(DestinationType::Topic, topic)
            .map_err(|e| anyhow::anyhow!("invalid topic '{topic}': {e:?}"))?;

        let mode = match self.config.delivery_mode {
            OutputDeliveryMode::Direct => DeliveryMode::Direct,
            OutputDeliveryMode::Persistent => DeliveryMode::Persistent,
        };

        let msg = OutboundMessageBuilder::new()
            .destination(dest)
            .delivery_mode(mode)
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
        let context = Context::new(SolaceLogLevel::Warning)
            .map_err(|e| anyhow::anyhow!("Solace context init: {e:?}"))?;

        // AsyncSessionBuilder::build() is synchronous despite the async session type —
        // the "async" refers to message delivery, not the build process.
        let session = AsyncSessionBuilder::new(&context)
            .host_name(self.config.smf_url())
            .vpn_name(self.config.vpn.clone())
            .username(self.config.username.clone())
            .password(self.config.password.clone())
            .reconnect_retries(3)
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
        let topic = resolve_topic(&self.config.topic, buffer);
        self.publish(&topic, buffer)
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        if let Some(bytes) = val {
            let topic = resolve_topic(&self.config.topic, bytes);
            self.publish(&topic, bytes)?;
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
