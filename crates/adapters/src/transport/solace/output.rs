use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Error as AnyError, Result as AnyResult, bail};
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::transport::{AsyncErrorCallback, OutputBatchType, OutputEndpoint, Step};
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::message::{DeliveryMode, DestinationType, MessageDestination, OutboundMessageBuilder};
use solace_rs::session::SessionEvent;
use solace_rs::{Context, SessionError};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use super::config::{OutputDeliveryMode, SolaceOutputConfig};
use super::solace_log_level;

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
    /// The controller's error callback, stored at `connect` so session
    /// rebuilds can wire a fresh event drainer (see [`ensure_connected`]).
    error_cb: Option<Arc<dyn Fn(bool, AnyError, Option<&'static str>) + Send + Sync>>,
}

struct SolaceConnection {
    /// Kept alive so the session's C-SDK context pointer remains valid.
    _context: Context,
    session: solace_rs::async_support::AsyncSession,
    /// Set by the session-event drainer when the SDK reports a
    /// connection-level failure (its own reconnect budget exhausted).  The
    /// next publish rebuilds the session instead of failing forever.
    poisoned: Arc<AtomicBool>,
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
            error_cb: None,
        })
    }

    /// Ensure a healthy session, rebuilding after a connection-level failure.
    ///
    /// The session-event drainer marks the connection poisoned when the SDK
    /// reports `DownError`/`ConnectFailedError` (its own reconnect budget is
    /// exhausted).  Rather than failing the endpoint permanently, the next
    /// publish tears the dead session down and builds a fresh one.  In-flight
    /// persistent acks belong to the dead session and can never resolve, so
    /// the rebuild drops them and fails the current batch with an error; the
    /// controller logs it, and the next batch proceeds on the new session.
    fn ensure_connected(&mut self) -> AnyResult<()> {
        let healthy = self
            .conn
            .as_ref()
            .is_some_and(|c| !c.poisoned.load(Ordering::Acquire));
        if healthy {
            return Ok(());
        }
        if let Some(conn) = self.conn.take() {
            warn!("Solace output session lost; rebuilding");
            if let Err(e) = conn.session.disconnect() {
                debug!("Disconnect of poisoned Solace session: {e:?}");
            }
        }
        let inflight = self.pending_acks.len();
        self.pending_acks.clear();
        self.build_connection()?;
        if inflight > 0 {
            bail!(
                "Solace session was rebuilt with {inflight} unacknowledged persistent \
                 publish(es); their delivery is unconfirmed"
            );
        }
        Ok(())
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
        self.ensure_connected()?;
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
    /// the broker, or until `ack_timeout_secs` elapses.
    ///
    /// A rejected, lost, or timed-out acknowledgment returns an error, which
    /// the controller logs as a (non-fatal) transport error for the step;
    /// recovery is retried on the next batch.
    ///
    /// The timeout is load-bearing: the ack senders live inside the session
    /// for its whole lifetime, so if the broker never acks (broker death,
    /// spool over quota, session stuck mid-reconnect) the channel neither
    /// resolves nor closes.  An unbounded wait here would block the output
    /// thread's `batch_end` forever — and because step completion feeds the
    /// input connectors' deferred acks, that would stall the entire pipeline.
    fn drain_pending_acks(&mut self) -> AnyResult<()> {
        if self.pending_acks.is_empty() {
            return Ok(());
        }
        // One deadline bounds the whole drain: acks resolve in publish order,
        // so a healthy broker clears every receiver well inside the window.
        let deadline = Instant::now() + Duration::from_secs(self.config.ack_timeout_secs);
        let total = self.pending_acks.len();
        for (i, rx) in self.pending_acks.drain(..).enumerate() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            // The endpoint runs on a dedicated OS thread (no ambient runtime),
            // so blocking on the shared runtime here is safe — the same
            // reasoning that made the previous `blocking_recv()` legal.
            match TOKIO.block_on(tokio::time::timeout(remaining, rx)) {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    return Err(anyhow::anyhow!("broker rejected persistent publish: {e:?}"));
                }
                Ok(Err(_closed)) => {
                    return Err(anyhow::anyhow!(
                        "Solace ack channel closed before broker acknowledgment"
                    ));
                }
                Err(_elapsed) => {
                    return Err(anyhow::anyhow!(
                        "broker did not acknowledge {} of {total} in-flight persistent \
                         publish(es) within {}s",
                        total - i,
                        self.config.ack_timeout_secs
                    ));
                }
            }
        }
        Ok(())
    }
}

impl SolaceOutputEndpoint {
    /// Build a session (plus its event drainer) and install it as the live
    /// connection.  Used for both the initial `connect` and rebuilds after a
    /// connection-level failure.
    fn build_connection(&mut self) -> AnyResult<()> {
        let error_cb = self
            .error_cb
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("output endpoint not connected"))?
            .clone();

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
        // so only untracked events arrive here.  The poisoned flag is scoped
        // to this session: a drainer for a torn-down session cannot poison
        // its replacement.
        let poisoned = Arc::new(AtomicBool::new(false));
        let event_rx = session.take_event_receiver();
        TOKIO.spawn(drain_session_events(
            event_rx,
            error_cb,
            Arc::clone(&poisoned),
        ));

        // Both _context and session must live together — context must outlive session.
        // Fields are dropped in declaration order, so _context is dropped after session.
        self.conn = Some(SolaceConnection {
            _context: context,
            session,
            poisoned,
        });
        Ok(())
    }
}

impl OutputEndpoint for SolaceOutputEndpoint {
    fn connect(&mut self, async_error_callback: AsyncErrorCallback) -> AnyResult<()> {
        self.error_cb = Some(Arc::from(async_error_callback));
        self.build_connection()
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
        _val: Option<&[u8]>,
        _headers: &[(&str, Option<&[u8]>)],
    ) -> AnyResult<()> {
        // Publishing only `val` here would silently discard the key and
        // headers, so fail instead, per the OutputEndpoint contract.
        bail!(
            "Solace output transport does not support key-value pairs. \
This output endpoint was configured with a data format that produces outputs as key-value pairs; \
however the Solace transport does not support this representation."
        );
    }

    fn batch_start(&mut self, _step: Step, _batch_type: OutputBatchType) -> AnyResult<()> {
        Ok(())
    }

    fn batch_end(&mut self) -> AnyResult<()> {
        // Persistent mode: block (bounded by `ack_timeout_secs`) until the
        // broker has acknowledged every message in this batch before the step
        // is reported complete.  An error here surfaces as a non-fatal
        // transport error on the step — it is logged and the endpoint's error
        // count rises, but the step still completes, so operators monitoring
        // for delivery guarantees must watch the endpoint error metrics.
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
///
/// Connection-level failures are reported as *non-fatal* and mark the session
/// poisoned instead: the endpoint rebuilds the session on the next publish
/// (see `ensure_connected`), so a broker outage that outlives the SDK's own
/// reconnect budget degrades to failed batches rather than killing the
/// endpoint permanently.
async fn drain_session_events(
    mut event_rx: tokio::sync::mpsc::UnboundedReceiver<SessionEvent>,
    error_callback: Arc<dyn Fn(bool, AnyError, Option<&'static str>) + Send + Sync>,
    poisoned: Arc<AtomicBool>,
) {
    while let Some(event) = event_rx.recv().await {
        match event {
            SessionEvent::DownError => {
                poisoned.store(true, Ordering::Release);
                error_callback(
                    false,
                    anyhow::anyhow!("Solace session down; rebuilding on the next publish"),
                    Some("solace_output_down"),
                );
            }
            SessionEvent::ConnectFailedError => {
                poisoned.store(true, Ordering::Release);
                error_callback(
                    false,
                    anyhow::anyhow!("Solace connection failed; rebuilding on the next publish"),
                    Some("solace_output_connect_failed"),
                );
            }
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

/// Validates that a `{field}` topic template is paired with the JSON output
/// format.
///
/// [`resolve_topic`] substitutes placeholders by re-parsing each encoded
/// record as JSON.  With any other format the parse fails and every record
/// would silently publish to the literal template string (a topic containing
/// `{`…`}`), so reject the combination at connect time instead.
pub fn validate_output_format(
    config: &SolaceOutputConfig,
    format_name: Option<&str>,
) -> Result<(), String> {
    if config.topic.contains('{') && format_name != Some("json") {
        return Err(format!(
            "Solace output topic '{}' contains {{field}} placeholders, which are resolved \
             from JSON-encoded records; this endpoint uses {}. Configure the connector \
             with format \"json\" or use a static topic.",
            config.topic,
            format_name.map_or_else(
                || "no output format".to_string(),
                |name| format!("format \"{name}\"")
            ),
        ));
    }
    Ok(())
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
    use super::{SolaceOutputConfig, resolve_topic, validate_output_format};

    #[test]
    fn dynamic_topic_requires_json_format() {
        let config = SolaceOutputConfig {
            topic: "demo/{region}".into(),
            ..Default::default()
        };
        assert!(validate_output_format(&config, Some("json")).is_ok());
        assert!(validate_output_format(&config, Some("csv")).is_err());
        assert!(validate_output_format(&config, Some("avro")).is_err());
        assert!(validate_output_format(&config, None).is_err());
    }

    #[test]
    fn static_topic_accepts_any_format() {
        let config = SolaceOutputConfig {
            topic: "demo/results/all".into(),
            ..Default::default()
        };
        assert!(validate_output_format(&config, Some("csv")).is_ok());
        assert!(validate_output_format(&config, Some("json")).is_ok());
        assert!(validate_output_format(&config, None).is_ok());
    }

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
