use anyhow::Result as AnyResult;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputBatchType, OutputEndpoint, Step};
use feldera_types::transport::solace::SolaceLogLevel as ConfigLogLevel;
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::message::{DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder};
use solace_rs::session::SessionEvent;
use solace_rs::{Context, SessionError, SolaceLogLevel};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

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
    /// In-flight broker acknowledgments for `Persistent` delivery, awaiting
    /// resolution. Drained at each `batch_end` and whenever the count reaches
    /// `config.max_inflight_acks`.
    pending_acks: Vec<oneshot::Receiver<Result<(), SessionError>>>,
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
            pending_acks: Vec::new(),
        })
    }

    fn delivery_mode(&self) -> DeliveryMode {
        match self.config.delivery_mode {
            OutputDeliveryMode::Direct => DeliveryMode::Direct,
            OutputDeliveryMode::Persistent => DeliveryMode::Persistent,
        }
    }

    /// Resolve the destination topic for a record. Static topics return an
    /// owned copy of the configured topic (no template scan); dynamic topics
    /// substitute `{field}` placeholders from the record.
    fn topic_for(&self, buffer: &[u8]) -> String {
        if self.static_topic {
            self.config.topic.clone()
        } else {
            resolve_topic(&self.config.topic, buffer)
        }
    }

    fn publish(&mut self, topic: &str, payload: &[u8]) -> AnyResult<()> {
        let msg = OutboundMessageBuilder::new()
            .destination(
                MessageDestination::new(DestinationType::Topic, topic)
                    .map_err(|e| anyhow::anyhow!("invalid topic '{topic}': {e:?}"))?,
            )
            .delivery_mode(self.delivery_mode())
            .payload(payload.to_vec())
            .build()
            .map_err(|e| anyhow::anyhow!("message build: {e:?}"))?;

        match self.config.delivery_mode {
            OutputDeliveryMode::Direct => {
                // Fire-and-forget: no per-message broker acknowledgment.
                let conn = self
                    .conn
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("output endpoint not connected"))?;
                conn.session
                    .publish(msg)
                    .map_err(|e| anyhow::anyhow!("publish to '{topic}': {e:?}"))?;
            }
            OutputDeliveryMode::Persistent => {
                // Windowed acknowledgment: publish_with_ack returns a oneshot
                // that resolves when the broker persists (or rejects) the
                // message. Collect the receivers and drain them at the batch
                // boundary, bounding in-flight acks so a slow broker applies
                // backpressure instead of growing memory without limit.
                let rx = {
                    let conn = self
                        .conn
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("output endpoint not connected"))?;
                    conn.session
                        .publish_with_ack(msg)
                        .map_err(|e| anyhow::anyhow!("publish to '{topic}': {e:?}"))?
                };
                self.pending_acks.push(rx);
                if self.pending_acks.len() >= self.config.max_inflight_acks {
                    self.drain_pending_acks()?;
                }
            }
        }
        Ok(())
    }

    /// Block until every in-flight persistent publish has been acknowledged by
    /// the broker. A rejected or lost acknowledgment is a hard error so the
    /// controller does not treat unpersisted data as delivered.
    fn drain_pending_acks(&mut self) -> AnyResult<()> {
        for rx in self.pending_acks.drain(..) {
            match rx.blocking_recv() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    return Err(anyhow::anyhow!("broker rejected persistent publish: {e:?}"));
                }
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Solace ack channel closed before broker acknowledgment"
                    ));
                }
            }
        }
        Ok(())
    }
}

impl OutputEndpoint for SolaceOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
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
        let mut session = builder
            .build()
            .map_err(|e| anyhow::anyhow!("Solace session: {e:?}"))?;

        info!(
            "Output connected to Solace {} vpn={} topic={}",
            self.config.smf_url(),
            self.config.vpn,
            self.config.topic
        );

        // Drain session events so they cannot accumulate unboundedly, and route
        // connection-level failures to the controller via the async error
        // callback. Tracked `publish_with_ack` acknowledgments are resolved by
        // the SDK before reaching this channel (see publish/drain_pending_acks),
        // so only untracked events arrive here.
        let event_rx = session.take_event_receiver();
        TOKIO.spawn(drain_session_events(event_rx, async_error_callback));

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
        let topic = self.topic_for(buffer);
        self.publish(&topic, buffer)
    }

    fn push_key(
        &mut self,
        _key: Option<&[u8]>,
        val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        if let Some(bytes) = val {
            let topic = self.topic_for(bytes);
            self.publish(&topic, bytes)?;
        }
        Ok(())
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        // Persistent mode: block until the broker has acknowledged every
        // message in this batch before the step is reported complete, so a
        // downstream reader never sees data the broker has not persisted.
        // Direct mode leaves pending_acks empty, so this is a no-op.
        self.drain_pending_acks()
    }

    fn is_fault_tolerant(&self) -> bool {
        false
    }
}

/// Consume untracked session events for the lifetime of the session, reporting
/// connection-level failures through the controller's async error callback.
/// Returns when the session is dropped and the event channel closes.
async fn drain_session_events(
    mut event_rx: tokio::sync::mpsc::UnboundedReceiver<SessionEvent>,
    error_callback: AsyncErrorCallback,
) {
    while let Some(event) = event_rx.recv().await {
        match event {
            SessionEvent::DownError => error_callback(
                true,
                anyhow::anyhow!("Solace session down"),
                Some("solace_output_down"),
            ),
            SessionEvent::ConnectFailedError => error_callback(
                true,
                anyhow::anyhow!("Solace connection failed"),
                Some("solace_output_connect_failed"),
            ),
            SessionEvent::RejectedMsgError => error_callback(
                false,
                anyhow::anyhow!("Solace rejected a published message"),
                Some("solace_output_rejected"),
            ),
            // Reconnect notices, up-notice, can-send, and any stray
            // acknowledgments are informational.
            other => debug!("Solace output session event: {other:?}"),
        }
    }
    debug!("Solace output session event channel closed");
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
