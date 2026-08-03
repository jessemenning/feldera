use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result as AnyResult;
use chrono::Utc;
use dbsp::circuit::tokio::TOKIO;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::Parser;
use feldera_adapterlib::transport::{
    InputConsumer, InputEndpoint, InputQueue, InputReader, InputReaderCommand, TransportInputEndpoint,
};
use feldera_sqllib::{SqlString, Variant};
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::transport::solace::SolaceLogLevel as ConfigLogLevel;
use feldera_types::program_schema::Relation;
use serde_json::Value as JsonValue;
use solace_rs::async_support::{AsyncSession, AsyncSessionBuilder, OwnedAsyncFlow};
use solace_rs::flow::{AckMode, FlowEvent, MessageOutcome};
use solace_rs::message::{InboundMessage, Message};
use solace_rs::session::SessionEvent;
use solace_rs::{Context, SolaceLogLevel};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::watch;
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{Instrument, debug, error, info, info_span, warn};

use super::config::SolaceInputConfig;

// ---------------------------------------------------------------------------
// Endpoint
// ---------------------------------------------------------------------------

pub struct SolaceInputEndpoint {
    config: Arc<SolaceInputConfig>,
}

impl SolaceInputEndpoint {
    pub fn new(config: SolaceInputConfig) -> AnyResult<Self> {
        config
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid Solace input config: {e}"))?;
        Ok(Self {
            config: Arc::new(config),
        })
    }
}

/// Map the connector's log-level config to the Solace SDK enum, defaulting to
/// `Warning` when unset (a future change can derive this from the global
/// `log` crate level, matching the Kafka connector).
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

impl InputEndpoint for SolaceInputEndpoint {
    /// No fault tolerance is declared yet: the connector does not participate
    /// in checkpoint/replay.  Delivery is at-least-once relative to circuit
    /// step completion (see [`background_task`]); a future phase may return
    /// `Some(FtModel::AtLeastOnce)` once resume metadata is wired through.
    fn fault_tolerance(&self) -> Option<FtModel> {
        None
    }
}

impl TransportInputEndpoint for SolaceInputEndpoint {
    fn open(
        &self,
        consumer: Box<dyn InputConsumer>,
        parser: Box<dyn Parser>,
        _schema: Relation,
        _resume_info: Option<JsonValue>,
    ) -> AnyResult<Box<dyn InputReader>> {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let config = Arc::clone(&self.config);

        // Connector-init threads are plain OS threads with no ambient Tokio
        // runtime, so spawn onto the shared adapter runtime (as the NATS input
        // and the Solace output do).
        let span = info_span!("solace_input", queue = %config.queue, host = %config.host);
        TOKIO.spawn(background_task(config, consumer, parser, cmd_rx).instrument(span));

        Ok(Box::new(SolaceInputReader { cmd_tx }))
    }
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

pub struct SolaceInputReader {
    cmd_tx: UnboundedSender<InputReaderCommand>,
}

impl InputReader for SolaceInputReader {
    fn as_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync> {
        self
    }

    fn request(&self, command: InputReaderCommand) {
        let _ = self.cmd_tx.send(command);
    }

    fn is_closed(&self) -> bool {
        self.cmd_tx.is_closed()
    }
}

// ---------------------------------------------------------------------------
// RGMID dedup cache
// ---------------------------------------------------------------------------

/// Bounded FIFO set of replication-group message IDs seen this session.
///
/// The broker redelivers guaranteed messages that were received but not yet
/// acknowledged (e.g. after a connector restart between hand-off and ack).
/// This cache drops such redeliveries so a record is not ingested twice.
///
/// The cache is capacity-bounded: on a long-running, high-rate flow an
/// unbounded set would grow without limit.  A redelivery of a message older
/// than `cap` distinct messages falls out of the window and would re-ingest,
/// which is acceptable under at-least-once semantics.
struct RgmidCache {
    seen: HashSet<String>,
    order: VecDeque<String>,
    cap: usize,
}

impl RgmidCache {
    fn new(cap: usize) -> Self {
        Self {
            seen: HashSet::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    /// Records `id`.  Returns `true` if it is new, `false` if already seen.
    ///
    /// A `cap` of 0 disables deduplication: every message is reported as new.
    fn insert(&mut self, id: &str) -> bool {
        if self.cap == 0 {
            return true;
        }
        if self.seen.contains(id) {
            return false;
        }
        if self.order.len() >= self.cap {
            if let Some(evicted) = self.order.pop_front() {
                self.seen.remove(&evicted);
            }
        }
        self.order.push_back(id.to_owned());
        self.seen.insert(id.to_owned());
        true
    }
}

// ---------------------------------------------------------------------------
// Deferred acknowledgment
// ---------------------------------------------------------------------------

/// Message acknowledgments awaiting circuit-step completion.
///
/// Each entry is `(step, msg_ids)`: the broker message IDs handed to the
/// circuit in step `step`.  They are acked once the step is fully processed
/// (see [`acks_ready`]).
type PendingAcks = VecDeque<(u64, Vec<u64>)>;

/// Removes and returns the msg_ids whose ingesting step is now complete.
///
/// A record ingested in step `s` is fully processed once the completion
/// count exceeds `s` (`total_completed_steps` is a count, so the last
/// complete step is `completed - 1`).  Entries are ordered by step, so this
/// pops from the front while the front step is complete.  Pure function to
/// keep the drain logic unit-testable.
fn acks_ready(pending: &mut PendingAcks, completed: u64) -> Vec<u64> {
    let mut ready = Vec::new();
    while let Some((step, _)) = pending.front() {
        if completed > *step {
            let (_, ids) = pending.pop_front().expect("front checked above");
            ready.extend(ids);
        } else {
            break;
        }
    }
    ready
}

/// Source of the "steps completed" signal used to release deferred acks.
///
/// Prefers the checkpoint watcher (durable) when the pipeline is
/// fault-tolerant; otherwise falls back to the completion watcher (in-memory
/// step completion).
enum CompletionSource {
    Checkpoint(watch::Receiver<u64>),
    Completion(watch::Receiver<Completion>),
}

impl CompletionSource {
    fn from_consumer(consumer: &dyn InputConsumer) -> Option<Self> {
        if let Some(rx) = consumer.checkpoint_watcher() {
            Some(CompletionSource::Checkpoint(rx))
        } else {
            consumer.completion_watcher().map(CompletionSource::Completion)
        }
    }

    fn completed(&self) -> u64 {
        match self {
            CompletionSource::Checkpoint(rx) => *rx.borrow(),
            CompletionSource::Completion(rx) => rx.borrow().total_completed_steps,
        }
    }

    async fn changed(&mut self) -> Result<(), watch::error::RecvError> {
        match self {
            CompletionSource::Checkpoint(rx) => rx.changed().await,
            CompletionSource::Completion(rx) => rx.changed().await,
        }
    }
}

/// Awaits the next completion signal, or never resolves when there is no
/// completion source.  Returns `Some(completed_count)` on a signal, or `None`
/// when the sender has been dropped (pipeline shutting down).
async fn wait_completion(source: &mut Option<CompletionSource>) -> Option<u64> {
    match source {
        Some(src) => match src.changed().await {
            Ok(()) => Some(src.completed()),
            Err(_) => None,
        },
        None => std::future::pending().await,
    }
}

// ---------------------------------------------------------------------------
// Connection lifecycle
// ---------------------------------------------------------------------------

/// Polling interval for buffered flow events (bind failures, reconnect
/// notices).
///
/// `OwnedAsyncFlow::recv()` and `recv_event()` both take `&mut self`, so the
/// message and event channels cannot be selected on concurrently.  Instead
/// the event channel is drained non-blockingly on this tick (and after every
/// received message).  The interval bounds the detection latency for a dead
/// flow on an idle or paused queue.
const FLOW_EVENT_POLL_INTERVAL: Duration = Duration::from_millis(500);

/// A live broker connection.
///
/// `flow` must be dropped before `session.disconnect()`: the session refuses
/// to disconnect while flows hold a reference to it (see [`teardown`]).
struct Connection {
    session: AsyncSession,
    flow: OwnedAsyncFlow,
}

/// Failure classification for connection attempts.
///
/// Classification errs toward `Retryable`: a wrong `Fatal` permanently kills
/// the endpoint, while a wrong `Retryable` merely retries noisily.
enum ConnectorError {
    /// Authentication, authorization, or configuration failure that a retry
    /// cannot fix.
    Fatal(anyhow::Error),
    /// Transient failure (network, timeout, broker restart); retried with a
    /// fresh session after `retry_interval_secs`.
    Retryable(anyhow::Error),
}

/// Reports whether an SDK error text indicates a permanent failure.
///
/// solace-rs surfaces C SDK subcodes as strings, so classification is
/// substring matching against a deliberately short list; anything
/// unrecognized is treated as retryable.
fn is_fatal_error_text(text: &str) -> bool {
    const FATAL_MARKERS: &[&str] = &[
        "login failure",  // bad credentials
        "unauthorized",   // authorization failure
        "acl denied",     // client ACL profile rejects the connection
        "unknown queue",  // queue does not exist on the broker
        "queue not found",
    ];
    let lower = text.to_lowercase();
    FATAL_MARKERS.iter().any(|marker| lower.contains(marker))
}

/// Classifies a connection-attempt failure as fatal or retryable.
fn classify_connect_failure(err: anyhow::Error) -> ConnectorError {
    if is_fatal_error_text(&format!("{err:#}")) {
        ConnectorError::Fatal(err)
    } else {
        ConnectorError::Retryable(err)
    }
}

/// Flow events that invalidate the connection.
///
/// `Reconnecting`/`Reconnected` are the SDK handling a blip within its own
/// `reconnect_retries` budget and are logged only.
fn is_flow_failure(event: FlowEvent) -> bool {
    matches!(
        event,
        FlowEvent::DownError | FlowEvent::BindFailedError | FlowEvent::SessionDown
    )
}

/// Session events that invalidate the connection.
///
/// `DownError` is emitted after the SDK's own `reconnect_retries` are
/// exhausted; from there recovery needs a fresh session.
fn is_session_failure(event: SessionEvent) -> bool {
    matches!(
        event,
        SessionEvent::DownError | SessionEvent::ConnectFailedError
    )
}

/// Builds a session and queue flow.  Blocks on the C SDK connect, so callers
/// run it via [`connect`] inside `spawn_blocking`.
fn connect_blocking(
    config: &SolaceInputConfig,
    context: &Context,
) -> Result<Connection, ConnectorError> {
    let mut builder = AsyncSessionBuilder::new(context)
        .host_name(config.smf_url())
        .vpn_name(config.vpn.clone())
        .username(config.username.clone())
        .password(config.password.clone())
        .reconnect_retries(config.reconnect_retries)
        .reconnect_retry_wait_ms(config.reconnect_retry_wait_ms)
        .connect_timeout_ms(config.connect_timeout_secs.saturating_mul(1000))
        .generate_rcv_timestamps(true);
    if let Some(name) = &config.client_name {
        builder = builder.client_name(name.clone());
    }
    if let Some(dir) = &config.ssl_trust_store_dir {
        builder = builder.ssl_trust_store_dir(dir.clone());
    }

    let session = builder
        .build()
        .map_err(|e| classify_connect_failure(anyhow::anyhow!("session connect: {e}")))?;

    // The flow is created stopped; it is started on `Extend`.
    let flow = session
        .create_flow(
            &config.queue,
            AckMode::Client,
            config.window_size,
            config.max_unacked,
        )
        .map_err(|e| {
            classify_connect_failure(anyhow::anyhow!(
                "flow bind to queue {}: {e}",
                config.queue
            ))
        })?;

    Ok(Connection { session, flow })
}

/// Runs [`connect_blocking`] off the async thread.
async fn connect(
    config: Arc<SolaceInputConfig>,
    context: Context,
) -> Result<Connection, ConnectorError> {
    tokio::task::spawn_blocking(move || connect_blocking(&config, &context))
        .await
        .unwrap_or_else(|e| {
            Err(ConnectorError::Retryable(anyhow::anyhow!(
                "connect task failed: {e}"
            )))
        })
}

/// Drops a connection off the async thread (disconnect can block on network
/// I/O).  The flow drops before `disconnect()`, or the session would report
/// `ActiveFlowsOnDisconnect` and leak.
async fn teardown(conn: Connection) {
    let result = tokio::task::spawn_blocking(move || {
        let Connection { session, flow } = conn;
        drop(flow);
        if let Err(e) = session.disconnect() {
            warn!("Solace disconnect error: {e}");
        }
    })
    .await;
    if result.is_err() {
        warn!("Solace teardown task failed");
    }
}

// ---------------------------------------------------------------------------
// Background task
// ---------------------------------------------------------------------------

/// The controller-visible run state.  Connectivity is tracked separately (an
/// `Option<Connection>` in [`background_task`]): pause/run is what the
/// controller asked for, connected/retrying is what the broker allows, and
/// the two vary independently — a disconnect while paused must not cancel
/// the pause, and a pause while retrying must survive the reconnect.
#[derive(Debug, PartialEq)]
enum State {
    Paused,
    Running,
    Done,
}

async fn background_task(
    config: Arc<SolaceInputConfig>,
    consumer: Box<dyn InputConsumer>,
    mut parser: Box<dyn Parser>,
    mut cmd_rx: mpsc::UnboundedReceiver<InputReaderCommand>,
) {
    // Clone consumer for InputQueue; keep original for error()/extended() calls.
    let queue = Arc::new(InputQueue::<u64>::new(consumer.clone()));

    let context = match Context::new(solace_log_level(config.log_level)) {
        Ok(c) => c,
        Err(e) => {
            consumer.error(true, anyhow::anyhow!("{e}"), Some("solace-context"));
            return;
        }
    };

    let mut rgmids = RgmidCache::new(config.dedup_history_size);

    // Deferred acks: a message is acknowledged only after the circuit step
    // that ingested it has been fully processed.  This restores at-least-once
    // delivery — a crash before step completion leaves the message unacked, so
    // the broker redelivers it.
    //
    // Acks are drained from a dedicated select! branch (see below) and never
    // block the command loop.  An earlier design blocked the Queue reply on
    // step completion; with several connectors all blocking on the same step,
    // the circuit could never fire.  Replying to Queue immediately and acking
    // asynchronously avoids that deadlock.
    let mut pending: PendingAcks = PendingAcks::new();
    let mut step: u64 = 0;
    let mut completion = CompletionSource::from_consumer(&*consumer);
    // Without a completion source we cannot know when a step is done, so fall
    // back to acking immediately after hand-off (at-most-once for in-flight
    // records).  In a normal pipeline the completion watcher is always present.
    let defer_acks = completion.is_some();

    let retry_interval = Duration::from_secs(config.retry_interval_secs);
    let mut event_tick = tokio::time::interval(FLOW_EVENT_POLL_INTERVAL);
    event_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let mut state = State::Paused;
    // `None` while disconnected.  Starting disconnected with an expired retry
    // timer routes the initial connection through the same retry path as a
    // reconnect, so a broker that is down at pipeline start is retried rather
    // than fatal.
    let mut connection: Option<Connection> = None;
    let mut next_retry_at = Instant::now();

    loop {
        if state == State::Done {
            break;
        }

        let Some(conn) = connection.as_mut() else {
            // ---- Disconnected: wait for the retry timer or a command ----
            tokio::select! {
                biased;

                cmd = cmd_rx.recv() => match cmd {
                    Some(InputReaderCommand::Queue { .. }) => {
                        handle_queue_command(
                            &queue, &*consumer, None,
                            &mut pending, &mut step, defer_acks,
                        );
                    }
                    Some(c) => {
                        // No flow exists, so this only records the state; the
                        // flow is started on reconnect when the state is
                        // Running.
                        let _ = handle_non_queue_command(c, &mut state, None);
                    }
                    None => state = State::Done,
                },

                () = tokio::time::sleep_until(next_retry_at) => {
                    match connect(Arc::clone(&config), context.clone()).await {
                        Ok(conn) => {
                            if state == State::Running {
                                if let Err(e) = conn.flow.start() {
                                    warn!(
                                        "flow.start after reconnect failed: {e}; \
                                         retrying in {retry_interval:?}"
                                    );
                                    teardown(conn).await;
                                    next_retry_at = Instant::now() + retry_interval;
                                    continue;
                                }
                            }
                            info!(
                                "Connected to Solace {} vpn={} queue={}",
                                config.smf_url(),
                                config.vpn,
                                config.queue
                            );
                            connection = Some(conn);
                        }
                        Err(ConnectorError::Fatal(e)) => {
                            error!("Solace connection failed fatally: {e:#}");
                            consumer.error(true, e, Some("solace-connect"));
                            state = State::Done;
                        }
                        Err(ConnectorError::Retryable(e)) => {
                            warn!(
                                "Solace connection failed: {e:#}; \
                                 retrying in {retry_interval:?}"
                            );
                            consumer.error(false, e, Some("solace-connect"));
                            next_retry_at = Instant::now() + retry_interval;
                        }
                    }
                }
            }
            continue;
        };

        // ---- Connected (Paused or Running) ----
        //
        // Every branch evaluates to the error that invalidated the
        // connection, or `None` to stay connected.
        let trigger: Option<anyhow::Error> = tokio::select! {
            biased;

            cmd = cmd_rx.recv() => match cmd {
                Some(InputReaderCommand::Queue { .. }) => {
                    handle_queue_command(
                        &queue, &*consumer, Some(&conn.flow),
                        &mut pending, &mut step, defer_acks,
                    );
                    None
                }
                Some(c) => handle_non_queue_command(c, &mut state, Some(&conn.flow)),
                None => {
                    state = State::Done;
                    None
                }
            },

            completed = wait_completion(&mut completion) => {
                match completed {
                    Some(c) => ack_completed(&mut pending, c, &conn.flow),
                    None => {
                        completion = None;
                        flush_all_acks(&mut pending, &conn.flow);
                    }
                }
                None
            },

            maybe_msg = conn.flow.recv(), if state == State::Running => match maybe_msg {
                Some(msg) => {
                    match process_message(msg, &queue, &*consumer, &mut parser, &mut rgmids, &config) {
                        MsgDisposition::Queued => {}
                        MsgDisposition::AckNow(id) => {
                            if let Err(e) = conn.flow.ack(id) {
                                warn!("flow.ack({id}) failed: {e}");
                            }
                        }
                        MsgDisposition::SettleRejected(id) => {
                            if let Err(e) = conn.flow.settle(id, MessageOutcome::Rejected) {
                                warn!("flow.settle({id}, Rejected) failed: {e}");
                            }
                        }
                        MsgDisposition::Skip => {}
                    }
                    // Drain low-volume flow events (bind/reconnect notices)
                    // so their channel cannot grow between poll ticks.
                    drain_flow_events(&mut conn.flow)
                }
                None => Some(anyhow::anyhow!("flow message channel closed")),
            },

            maybe_event = conn.session.recv_event() => match maybe_event {
                Some(event) if is_session_failure(event) => {
                    Some(anyhow::anyhow!("session failure event: {event}"))
                }
                Some(event) => {
                    debug!("Solace session event: {event}");
                    None
                }
                None => Some(anyhow::anyhow!("session event channel closed")),
            },

            _ = event_tick.tick() => drain_flow_events(&mut conn.flow),
        };

        if let Some(err) = trigger {
            warn!(
                "Solace connection lost: {err:#}; reconnecting in {retry_interval:?}"
            );
            consumer.error(false, err, Some("solace-connection-lost"));
            // Pending msg_ids belong to the dead flow and cannot be acked on
            // the new one.  Drop them: the broker redelivers unacked messages
            // on the new flow, and the RGMID cache drops those already
            // ingested.
            pending.clear();
            let conn = connection.take().expect("connected arm holds a connection");
            teardown(conn).await;
            next_retry_at = Instant::now() + retry_interval;
        }
    }

    // Ack anything still pending on a clean shutdown so the broker window is
    // released rather than waiting for redelivery.
    if let Some(conn) = connection.take() {
        flush_all_acks(&mut pending, &conn.flow);
        teardown(conn).await;
    }
    info!("Solace background task exiting");
}

/// Flush the input queue into the circuit and route the flushed messages'
/// broker IDs to the ack path.
///
/// The controller sends exactly one `Queue` per step to every endpoint
/// regardless of pause or connection state, so every caller must flush (and
/// account for the returned msg_ids) or acks would leak while the connector
/// is backpressured.  `step` is incremented on every call so it always
/// mirrors the controller's step number.
///
/// With `flow: None` (disconnected) the msg_ids are discarded: they belong to
/// a dead flow and cannot be acked, so the broker redelivers those messages
/// on the next flow and the RGMID cache drops the ones already ingested.
fn handle_queue_command(
    queue: &Arc<InputQueue<u64>>,
    consumer: &dyn InputConsumer,
    flow: Option<&OwnedAsyncFlow>,
    pending: &mut PendingAcks,
    step: &mut u64,
    defer_acks: bool,
) {
    let (buffer_size, _hasher, aux_vec) = queue.flush_with_aux();
    // Reply immediately so the controller's step never stalls.
    consumer.extended(buffer_size, None, vec![]);

    let msg_ids: Vec<u64> = aux_vec.into_iter().map(|(_, id)| id).collect();
    match flow {
        None => {
            if !msg_ids.is_empty() {
                debug!("Discarding {} ack(s) for a closed flow", msg_ids.len());
            }
        }
        Some(_) if defer_acks => {
            if !msg_ids.is_empty() {
                pending.push_back((*step, msg_ids));
            }
        }
        Some(flow) => {
            // No completion source: ack on hand-off (at-most-once fallback).
            for id in msg_ids {
                if let Err(e) = flow.ack(id) {
                    warn!("flow.ack({id}) failed: {e}");
                }
            }
        }
    }
    *step += 1;
}

/// Ack every message whose ingesting step is complete.
fn ack_completed(pending: &mut PendingAcks, completed: u64, flow: &OwnedAsyncFlow) {
    let ready = acks_ready(pending, completed);
    let n = ready.len();
    for id in ready {
        if let Err(e) = flow.ack(id) {
            warn!("flow.ack({id}) failed: {e}");
        }
    }
    if n > 0 {
        debug!("Acked {n} messages after step completion");
    }
}

/// Ack all pending messages regardless of step (clean shutdown / lost watcher).
fn flush_all_acks(pending: &mut PendingAcks, flow: &OwnedAsyncFlow) {
    for (_, ids) in pending.drain(..) {
        for id in ids {
            if let Err(e) = flow.ack(id) {
                warn!("flow.ack({id}) failed: {e}");
            }
        }
    }
}

/// Drains buffered flow events without blocking.
///
/// Returns the error to treat as a lost connection when an event (or a
/// closed event channel) invalidates the flow; `None` otherwise.
fn drain_flow_events(flow: &mut OwnedAsyncFlow) -> Option<anyhow::Error> {
    loop {
        match flow.try_recv_event() {
            Ok(event) if is_flow_failure(event) => {
                return Some(anyhow::anyhow!("flow failure event: {event}"));
            }
            Ok(event) => debug!("Solace flow event: {event}"),
            Err(TryRecvError::Empty) => return None,
            Err(TryRecvError::Disconnected) => {
                return Some(anyhow::anyhow!("flow event channel closed"));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message processing
// ---------------------------------------------------------------------------

/// Outcome of classifying a received message; the caller acts on the flow.
///
/// Keeping the flow out of `process_message` lets the classification be
/// unit-tested without a live broker.
#[derive(Debug, PartialEq, Eq)]
enum MsgDisposition {
    /// Parsed and pushed to the input queue; ack is deferred to step completion.
    Queued,
    /// Nothing to ingest (duplicate or empty); ack now to release the window.
    AckNow(u64),
    /// Unparseable payload; reject to the dead-message queue.
    SettleRejected(u64),
    /// No usable msg_id; leave the message for broker redelivery.
    Skip,
}

fn process_message(
    msg: InboundMessage,
    queue: &Arc<InputQueue<u64>>,
    consumer: &dyn InputConsumer,
    parser: &mut Box<dyn Parser>,
    rgmids: &mut RgmidCache,
    config: &SolaceInputConfig,
) -> MsgDisposition {
    // msg_id is the handle flow.ack()/settle() need; fetch it first so every
    // early-return path can still release the broker window.
    let msg_id = match msg.get_msg_id() {
        Ok(Some(id)) => id,
        Ok(None) => {
            warn!("Received message with no msg_id — cannot ack; skipping");
            return MsgDisposition::Skip;
        }
        Err(e) => {
            warn!("get_msg_id error: {e:?} — skipping");
            consumer.error(false, anyhow::anyhow!("{e:?}"), Some("solace-msg-id"));
            return MsgDisposition::Skip;
        }
    };

    // RGMID dedup: a redelivery of data already ingested this session must be
    // acked (not silently dropped) or it stays unacked forever, leaking the
    // broker window.
    if let Ok(Some(rgmid)) = msg.get_replication_group_message_id() {
        if !rgmids.insert(&rgmid) {
            debug!("Duplicate RGMID {rgmid} on msg_id={msg_id}; acking");
            return MsgDisposition::AckNow(msg_id);
        }
    }

    let payload = match msg.get_payload() {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            debug!("Empty payload on msg_id={msg_id}; acking");
            return MsgDisposition::AckNow(msg_id);
        }
        Err(e) => {
            warn!("Payload error on msg_id={msg_id}: {e:?}");
            consumer.error(false, anyhow::anyhow!("{e:?}"), Some("solace-payload"));
            return MsgDisposition::AckNow(msg_id);
        }
    };

    // Build ConnectorMetadata from the Solace destination topic so table
    // columns can be populated from topic levels without the publisher
    // embedding them in the payload.
    let metadata = build_metadata(&msg, config);

    let (buffer, errors) = parser.parse(payload, metadata);

    if !errors.is_empty() {
        debug!("{} parse error(s) on msg_id={msg_id}", errors.len());
        // A completely unparseable payload can be routed to the DMQ when the
        // operator opts in; otherwise fall through and report the errors
        // normally via extended().
        if buffer.is_none() && config.reject_parse_errors {
            return MsgDisposition::SettleRejected(msg_id);
        }
    }

    // Store msg_id as aux — returned by flush_with_aux() so the message can be
    // acked once the ingesting step completes.
    queue.push_with_aux((buffer, errors), Utc::now(), msg_id);
    MsgDisposition::Queued
}

/// Build `ConnectorMetadata` from the Solace message destination topic.
///
/// Always inserts `solace_topic` with the full destination string.
/// If `config.topic_pattern` is set, also inserts named captures per
/// `super::config::parse_topic_fields`.
fn build_metadata(msg: &InboundMessage, config: &SolaceInputConfig) -> Option<ConnectorMetadata> {
    let topic = match msg.get_destination() {
        Ok(Some(dest)) => dest.dest.to_string_lossy().into_owned(),
        Ok(None) => return None,
        Err(e) => {
            debug!("get_destination error: {e:?}");
            return None;
        }
    };

    let mut meta = ConnectorMetadata::new();
    meta.insert("solace_topic", Variant::String(SqlString::from(topic.as_str())));

    // Broker receive timestamp — milliseconds since Unix epoch when the broker
    // enqueued the message.  Available as CONNECTOR_METADATA()['broker_ts']
    // (BIGINT) in SQL table DEFAULT expressions.
    if let Ok(Some(ts)) = msg.get_receive_timestamp() {
        if let Ok(ms) = i64::try_from(
            ts.duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        ) {
            meta.insert("broker_ts", Variant::BigInt(ms));
        }
    }

    if let Some(pattern) = &config.topic_pattern {
        for (name, value) in super::config::parse_topic_fields(pattern, &topic) {
            meta.insert(&name, Variant::String(SqlString::from(value.as_str())));
        }
    }

    Some(meta)
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

/// Handles all commands except Queue (which is handled inline in the main loop).
///
/// Returns the error to treat as a lost connection when a flow operation the
/// command requires fails; `None` otherwise.  With `flow: None`
/// (disconnected) state changes are recorded only — the flow is started on
/// reconnect when the recorded state is `Running`.
fn handle_non_queue_command(
    command: InputReaderCommand,
    state: &mut State,
    flow: Option<&OwnedAsyncFlow>,
) -> Option<anyhow::Error> {
    match command {
        InputReaderCommand::Queue { .. } => {
            // Unreachable — caller dispatches Queue to handle_queue_command.
            warn!("handle_non_queue_command received Queue — this is a bug");
            None
        }

        InputReaderCommand::Replay { .. } => {
            warn!("Replay issued to non-FT Solace connector — ignoring");
            None
        }

        InputReaderCommand::Extend => {
            debug!("Extend — starting flow");
            *state = State::Running;
            if let Some(flow) = flow {
                if let Err(e) = flow.start() {
                    return Some(anyhow::anyhow!("flow.start: {e}"));
                }
            }
            None
        }

        InputReaderCommand::Pause => {
            debug!("Pause — stopping flow");
            *state = State::Paused;
            if let Some(flow) = flow {
                // A failed stop leaves the broker pushing messages that the
                // paused loop no longer drains, so treat it as a lost
                // connection rather than ignoring it.
                if let Err(e) = flow.stop() {
                    return Some(anyhow::anyhow!("flow.stop: {e}"));
                }
            }
            None
        }

        InputReaderCommand::Disconnect => {
            debug!("Disconnect");
            if let Some(flow) = flow {
                let _ = flow.stop();
            }
            *state = State::Done;
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use feldera_types::config::FtModel;
    use feldera_types::transport::solace::SolaceInputConfig;
    use solace_rs::flow::FlowEvent;
    use solace_rs::session::SessionEvent;

    use super::{
        ConnectorError, InputEndpoint, PendingAcks, RgmidCache, SolaceInputEndpoint, State,
        acks_ready, classify_connect_failure, is_fatal_error_text, is_flow_failure,
        is_session_failure,
    };

    fn make_config() -> SolaceInputConfig {
        SolaceInputConfig {
            host: "localhost".into(),
            port: 55555,
            vpn: "default".into(),
            username: "user".into(),
            password: "pass".into(),
            queue: "test-q".into(),
            window_size: 255,
            topic_pattern: None,
            ..Default::default()
        }
    }

    #[test]
    fn fault_tolerance_is_none() {
        let ep = SolaceInputEndpoint::new(make_config()).expect("valid config");
        assert_eq!(ep.fault_tolerance(), None::<FtModel>);
    }

    #[test]
    fn state_eq_and_ne() {
        assert_eq!(State::Paused, State::Paused);
        assert_ne!(State::Running, State::Done);
    }

    #[test]
    fn state_debug() {
        assert_eq!(format!("{:?}", State::Paused), "Paused");
        assert_eq!(format!("{:?}", State::Running), "Running");
        assert_eq!(format!("{:?}", State::Done), "Done");
    }

    #[test]
    fn rgmid_cache_detects_duplicates() {
        let mut cache = RgmidCache::new(4);
        assert!(cache.insert("a"), "first sight is new");
        assert!(!cache.insert("a"), "second sight is a duplicate");
        assert!(cache.insert("b"));
        assert!(!cache.insert("b"));
    }

    #[test]
    fn rgmid_cache_evicts_fifo() {
        let mut cache = RgmidCache::new(2);
        assert!(cache.insert("a"));
        assert!(cache.insert("b"));
        // Inserting "c" evicts "a" (the oldest).
        assert!(cache.insert("c"));
        assert!(cache.insert("a"), "evicted id is treated as new again");
        // "b" is still present.
        assert!(!cache.insert("b"));
    }

    #[test]
    fn rgmid_cache_zero_cap_disables_dedup() {
        let mut cache = RgmidCache::new(0);
        assert!(cache.insert("a"));
        assert!(cache.insert("a"), "dedup disabled: every message is new");
    }

    #[test]
    fn acks_ready_releases_only_completed_steps() {
        let mut pending: PendingAcks = PendingAcks::new();
        pending.push_back((0, vec![10, 11]));
        pending.push_back((1, vec![12]));
        pending.push_back((2, vec![13]));

        // completed == 0: no step is done yet (step 0 done when completed > 0).
        assert!(acks_ready(&mut pending, 0).is_empty());
        // completed == 1: step 0 is done.
        assert_eq!(acks_ready(&mut pending, 1), vec![10, 11]);
        // completed == 3: steps 1 and 2 are done, in order.
        assert_eq!(acks_ready(&mut pending, 3), vec![12, 13]);
        assert!(pending.is_empty());
    }

    #[test]
    fn acks_ready_is_monotonic_across_calls() {
        let mut pending: PendingAcks = PendingAcks::new();
        pending.push_back((5, vec![1]));
        // A stale, lower completion count must not release a later step.
        assert!(acks_ready(&mut pending, 5).is_empty());
        assert_eq!(acks_ready(&mut pending, 6), vec![1]);
    }

    #[test]
    fn fatal_error_texts_are_recognized() {
        // Case-insensitive substring matches on SDK error strings.
        assert!(is_fatal_error_text("subcode: 3 string: Login Failure"));
        assert!(is_fatal_error_text("401 Unauthorized"));
        assert!(is_fatal_error_text("Client ACL Denied"));
        assert!(is_fatal_error_text("Unknown Queue"));
        assert!(is_fatal_error_text("queue not found: my-queue"));
    }

    #[test]
    fn ambiguous_error_texts_default_to_retryable() {
        // A wrong Fatal permanently kills the endpoint, so anything not on
        // the explicit list must classify as retryable.
        assert!(!is_fatal_error_text("Connection refused"));
        assert!(!is_fatal_error_text("Unresolved host"));
        assert!(!is_fatal_error_text("Timeout while connecting"));
        assert!(!is_fatal_error_text(""));
    }

    #[test]
    fn classify_connect_failure_splits_fatal_and_retryable() {
        assert!(matches!(
            classify_connect_failure(anyhow::anyhow!("session connect: Login Failure")),
            ConnectorError::Fatal(_)
        ));
        assert!(matches!(
            classify_connect_failure(anyhow::anyhow!("session connect: Unresolved host")),
            ConnectorError::Retryable(_)
        ));
    }

    #[test]
    fn flow_failure_events_trigger_reconnect() {
        assert!(is_flow_failure(FlowEvent::DownError));
        assert!(is_flow_failure(FlowEvent::BindFailedError));
        assert!(is_flow_failure(FlowEvent::SessionDown));
        // The SDK handles these itself; they must not tear the flow down.
        assert!(!is_flow_failure(FlowEvent::UpNotice));
        assert!(!is_flow_failure(FlowEvent::Reconnecting));
        assert!(!is_flow_failure(FlowEvent::Reconnected));
        assert!(!is_flow_failure(FlowEvent::Active));
        assert!(!is_flow_failure(FlowEvent::Inactive));
    }

    #[test]
    fn session_failure_events_trigger_reconnect() {
        assert!(is_session_failure(SessionEvent::DownError));
        assert!(is_session_failure(SessionEvent::ConnectFailedError));
        // Blip-handling and publish-side events must not tear the session down.
        assert!(!is_session_failure(SessionEvent::UpNotice));
        assert!(!is_session_failure(SessionEvent::ReconnectingNotice));
        assert!(!is_session_failure(SessionEvent::ReconnectedNotice));
        assert!(!is_session_failure(SessionEvent::Acknowledgement));
        assert!(!is_session_failure(SessionEvent::RejectedMsgError));
    }
}
