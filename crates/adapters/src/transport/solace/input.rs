use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use chrono::Utc;
use feldera_adapterlib::ConnectorMetadata;
use feldera_adapterlib::format::Parser;
use feldera_adapterlib::transport::{
    InputConsumer, InputEndpoint, InputQueue, InputReader, InputReaderCommand, TransportInputEndpoint,
};
use feldera_sqllib::{SqlString, Variant};
use feldera_types::config::FtModel;
use feldera_types::coordination::Completion;
use feldera_types::program_schema::Relation;
use serde_json::Value as JsonValue;
use solace_rs::{Context, SolaceLogLevel};
use solace_rs::async_support::AsyncSessionBuilder;
use solace_rs::flow::AckMode;
use solace_rs::message::Message;
use tokio::runtime::Handle;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use super::config::SolaceInputConfig;

// ---------------------------------------------------------------------------
// Endpoint
// ---------------------------------------------------------------------------

pub struct SolaceInputEndpoint {
    config: Arc<SolaceInputConfig>,
}

impl SolaceInputEndpoint {
    pub fn new(config: SolaceInputConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl InputEndpoint for SolaceInputEndpoint {
    /// Phase 2/3: no fault-tolerance declared to Feldera.
    /// Phase 5 will return Some(FtModel::ExactlyOnce) once we implement
    /// Resume::Replay with checkpoint_watcher.
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

        Handle::current().spawn(background_task(config, consumer, parser, cmd_rx));

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
// Background task
// ---------------------------------------------------------------------------

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

    // Grab the completion watcher now, before the consumer is moved into the queue.
    // This always returns Some in Feldera open-core.
    let mut completion_rx: Option<watch::Receiver<Completion>> = consumer.completion_watcher();

    let context = match Context::new(SolaceLogLevel::Warning) {
        Ok(c) => c,
        Err(e) => {
            consumer.error(true, anyhow::anyhow!("{e:?}"), Some("solace-context"));
            return;
        }
    };

    let session = match AsyncSessionBuilder::new(&context)
        .host_name(config.smf_url())
        .vpn_name(config.vpn.clone())
        .username(config.username.clone())
        .password(config.password.clone())
        .reconnect_retries(3)
        .build()
    {
        Ok(s) => s,
        Err(e) => {
            consumer.error(true, anyhow::anyhow!("{e:?}"), Some("solace-session"));
            return;
        }
    };

    info!(
        "Connected to Solace {} vpn={} queue={}",
        config.smf_url(), config.vpn, config.queue
    );

    // Phase 3: AckMode::Client — messages held unacked until after the circuit
    // step completes, then explicitly acked via flow.ack(msg_id).
    // Phase 2 used AckMode::Auto (acked on receive).
    let mut flow = match session.create_flow(
        &config.queue,
        AckMode::Client,
        config.window_size,
        None,
    ) {
        Ok(f) => f,
        Err(e) => {
            consumer.error(true, anyhow::anyhow!("{e:?}"), Some("solace-flow"));
            let _ = session.disconnect();
            return;
        }
    };

    // In-memory RGMID dedup: skip messages we've already pushed this session.
    // Handles the case where the connector crashes after extended() but before
    // ack — the broker redelivers, but Feldera has restarted clean so we'd
    // process them again. Phase 5 (Enterprise FT + checkpoint) makes this
    // cross-restart durable.
    let mut seen_rgmids: HashSet<[u8; 16]> = HashSet::new();

    let mut state = State::Paused;

    loop {
        match state {
            State::Done => break,

            State::Paused => {
                let cmd = match cmd_rx.recv().await {
                    Some(c) => c,
                    None => break,
                };
                handle_non_queue_command(cmd, &mut state, &flow, &*consumer);
            }

            State::Running => {
                tokio::select! {
                    biased;

                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(InputReaderCommand::Queue { .. }) => {
                                // Phase 3 deferred-ack path — inlined here so no
                                // &OwnedAsyncFlow reference crosses the await point.
                                let (buffer_size, _hasher, aux_vec) = queue.flush_with_aux();

                                let pre_step = completion_rx
                                    .as_ref()
                                    .map(|w| w.borrow().total_completed_steps)
                                    .unwrap_or(u64::MAX);

                                consumer.extended(buffer_size, None, vec![]);

                                // Wait until the circuit step that consumed our batch completes.
                                if let Some(ref mut watcher) = completion_rx {
                                    loop {
                                        if watcher.borrow().total_completed_steps > pre_step {
                                            break;
                                        }
                                        if watcher.changed().await.is_err() {
                                            debug!("completion_watcher sender dropped");
                                            break;
                                        }
                                    }
                                }

                                // Ack now — no flow reference was held across the await.
                                let n = aux_vec.len();
                                for (_, msg_id) in aux_vec {
                                    if let Err(e) = flow.ack(msg_id) {
                                        warn!("flow.ack({msg_id}) failed: {e:?}");
                                    }
                                }
                                if n > 0 {
                                    debug!("Acked {n} messages after circuit step");
                                }
                            }
                            Some(c) => handle_non_queue_command(c, &mut state, &flow, &*consumer),
                            None => break,
                        }
                    }

                    maybe_msg = flow.recv() => {
                        match maybe_msg {
                            Some(msg) => {
                                process_message(
                                    msg,
                                    &queue,
                                    &*consumer,
                                    &mut parser,
                                    &mut seen_rgmids,
                                    &config,
                                );
                            }
                            None => {
                                error!("Solace flow channel closed unexpectedly");
                                consumer.error(
                                    false,
                                    anyhow::anyhow!("flow channel closed"),
                                    Some("solace-flow-closed"),
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    drop(flow);
    if let Err(e) = session.disconnect() {
        warn!("Solace disconnect error: {e:?}");
    }
    info!("Solace background task exiting");
}

// ---------------------------------------------------------------------------
// Message processing
// ---------------------------------------------------------------------------

fn process_message(
    msg: solace_rs::message::InboundMessage,
    queue: &Arc<InputQueue<u64>>,
    consumer: &dyn InputConsumer,
    parser: &mut Box<dyn Parser>,
    seen_rgmids: &mut HashSet<[u8; 16]>,
    config: &SolaceInputConfig,
) {
    // RGMID dedup: skip broker redeliveries we've already ingested this session.
    if let Ok(Some(rgmid)) = msg.get_replication_group_message_id_raw() {
        if !seen_rgmids.insert(rgmid) {
            debug!("Skipping duplicate RGMID {:?}", rgmid);
            return;
        }
    }

    // msg_id is the handle used by flow.ack() to settle this message.
    let msg_id = match msg.get_msg_id() {
        Ok(Some(id)) => id,
        Ok(None) => {
            warn!("Received message with no msg_id — cannot defer ack; skipping");
            return;
        }
        Err(e) => {
            warn!("get_msg_id error: {e:?} — skipping");
            consumer.error(false, anyhow::anyhow!("{e:?}"), Some("solace-msg-id"));
            return;
        }
    };

    let payload = match msg.get_payload() {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            debug!("Empty payload on msg_id={msg_id}");
            return;
        }
        Err(e) => {
            warn!("Payload error on msg_id={msg_id}: {e:?}");
            consumer.error(false, anyhow::anyhow!("{e:?}"), Some("solace-payload"));
            return;
        }
    };

    // Build ConnectorMetadata from the Solace destination topic.
    // This lets table columns be populated from topic levels without requiring
    // the publisher to embed them in the payload.
    let metadata = build_metadata(&msg, config);

    let (buffer, errors) = parser.parse(payload, metadata);

    if !errors.is_empty() {
        debug!("{} parse error(s) on msg_id={msg_id}", errors.len());
    }

    // Store msg_id as aux — returned by flush_with_aux() so we can ack after
    // the circuit step completes.
    queue.push_with_aux((buffer, errors), Utc::now(), msg_id);
}

/// Build `ConnectorMetadata` from the Solace message destination topic.
///
/// Always inserts `solace_topic` with the full destination string.
/// If `config.topic_pattern` is set, also inserts named captures per
/// `super::config::parse_topic_fields`.
fn build_metadata(
    msg: &solace_rs::message::InboundMessage,
    config: &SolaceInputConfig,
) -> Option<ConnectorMetadata> {
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

/// Handles all commands except Queue (which is awaited inline in the main loop).
fn handle_non_queue_command(
    command: InputReaderCommand,
    state: &mut State,
    flow: &solace_rs::async_support::OwnedAsyncFlow,
    consumer: &dyn InputConsumer,
) {
    match command {
        InputReaderCommand::Queue { .. } => {
            // Should be unreachable — caller dispatches Queue to handle_queue_command.
            warn!("handle_non_queue_command received Queue — this is a bug");
        }

        InputReaderCommand::Replay { .. } => {
            warn!("Replay issued to non-FT Solace connector — ignoring");
        }

        InputReaderCommand::Extend => {
            debug!("Extend — starting flow");
            if let Err(e) = flow.start() {
                consumer.error(
                    false,
                    anyhow::anyhow!("flow.start: {e:?}"),
                    Some("solace-flow-start"),
                );
            }
            *state = State::Running;
        }

        InputReaderCommand::Pause => {
            debug!("Pause — stopping flow");
            if let Err(e) = flow.stop() {
                warn!("flow.stop: {e:?}");
            }
            *state = State::Paused;
        }

        InputReaderCommand::Disconnect => {
            debug!("Disconnect");
            let _ = flow.stop();
            *state = State::Done;
        }
    }
}
