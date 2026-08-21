# Solace input connector

:::info Sidecar alternative
This in-process connector requires a custom Feldera build (it links the Solace
C SDK into the pipeline).  For new deployments consider the
[Solace sidecar](https://github.com/jessemenning/feldera-solace-sidecar),
which offers the same behavior against an **unmodified upstream Feldera image**
by bridging data over the pipeline's HTTP API.
:::

Feldera can consume a stream of changes to a SQL table from a
[Solace Platform](https://solace.com/) event broker with the `solace_input`
connector.  The connector binds to a durable queue over the native SMF
protocol and supports at-least-once [fault
tolerance](/pipelines/fault-tolerance): pipelines containing Solace inputs can
checkpoint, suspend, and resume.

## How it works

The connector binds to a **durable queue** with client acknowledgment mode.  A
message is acknowledged to the broker only after the circuit step that
ingested it has been fully processed — or, in a fault-tolerant pipeline, after
a durable checkpoint covers that step.  A crash before that point leaves the
message unacknowledged, so the broker redelivers it on restart: **the queue
itself is the resume cursor**, and no offset metadata needs to be stored.

- **At-least-once delivery**: no message is lost; redeliveries of messages
  that were ingested but not yet acknowledged can appear as duplicates after
  a restart.
- **In-session deduplication**: broker redeliveries within one connector
  session (e.g. after a reconnect) are dropped using the message's
  replication-group message ID, so reconnects do not duplicate rows.  The
  dedup cache does not survive a pipeline restart.
- **Two-tier reconnect**: short network blips are retried inside the Solace C
  SDK (`reconnect_retries` / `reconnect_retry_wait_ms`); longer outages tear
  the session down and rebuild it every `retry_interval_secs` until the
  broker returns.  Authentication, authorization, and unknown-queue failures
  are fatal; everything else is retried.

## Configuration

### Connection

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `host` | string | Yes | Broker hostname (no scheme, no port) |
| `port` | integer | No | SMF port. Default: 55555 |
| `vpn` | string | No | Message VPN name. Default: `default` |
| `username` | string | Yes | Client username |
| `password` | string | Yes | Client password |
| `tls` | boolean | No | Use TLS (`tcps://`) instead of plaintext. Default: `false` |
| `ssl_trust_store_dir` | string | No | Directory holding trusted CA certificates. Requires `tls` |
| `client_name` | string | No | Client name reported to the broker. Default: SDK-generated |
| `connect_timeout_secs` | integer | No | Initial connection timeout. Must be ≥ 1. Default: 10 |
| `reconnect_retries` | integer | No | SDK-level reconnect attempts for transient blips; `-1` retries forever. Default: 3 |
| `reconnect_retry_wait_ms` | integer | No | Wait between SDK-level reconnect attempts. Default: 3000 |
| `retry_interval_secs` | integer | No | Wait between connector-level reconnect attempts after the SDK gives up. Must be ≥ 1. Default: 5 |
| `log_level` | string | No | Solace SDK log level: `critical`, `error`, `warning`, `notice`, `info`, or `debug`. Default: `warning` |

### Queue and delivery

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `queue` | string | Yes | Durable queue to bind to (must already exist on the broker) |
| `window_size` | integer | No | Flow window size: how many unacknowledged messages the broker delivers ahead. Must be 1–255. Default: 255 |
| `max_unacked` | integer | No | Maximum unacknowledged messages before the broker pauses the flow; `-1` for no limit. Default: broker default |
| `dedup_history_size` | integer | No | Recently-seen replication-group message IDs retained for redelivery deduplication; `0` disables. Maximum 10,000,000. Default: 100,000 |
| `reject_parse_errors` | boolean | No | Route completely unparseable payloads to the dead-message queue instead of reporting a parse error. Default: `false` |

:::tip Throughput and `window_size`

The flow window bounds how many messages the broker delivers between
acknowledgments, which makes it the per-step throughput ceiling.  Size it to
at least `target_msg_rate × step_interval_secs` (capped at the protocol
maximum of 255) to avoid stalling a high-rate queue.

:::

### Metadata

Metadata fields are **opt-in**: extracting them costs allocations on every
message, so enable only the fields a table consumes.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `topic_pattern` | string | No | Topic decomposition pattern with `{name}` placeholders (see below) |
| `include_topic` | boolean | No | Expose the full destination topic as the `solace_topic` metadata field (VARCHAR). Default: `false` |
| `include_broker_timestamp` | boolean | No | Expose the broker receive timestamp as the `broker_ts` metadata field (TIMESTAMP). Default: `false` |

#### Topic decomposition

Set `topic_pattern` to extract topic levels into table columns without
requiring publishers to embed them in the payload.  Each `{name}` segment
matches the corresponding slash-level of the message's destination topic and
becomes a metadata field:

```
topic_pattern:  demo/events/{region}/{event_type}
topic:          demo/events/us-east/order
metadata:       region = "us-east", event_type = "order"
```

Static segments capture nothing; levels beyond the shorter of pattern and
topic are ignored.

## Example

```sql
CREATE TABLE events (
    solace_topic VARCHAR,
    region       VARCHAR,
    event_type   VARCHAR,
    payload      VARCHAR
) WITH (
    'connectors' = '[{
        "transport": {
            "name": "solace_input",
            "config": {
                "host": "broker.example.com",
                "username": "feldera",
                "password": "secret",
                "queue": "feldera-events-q",
                "topic_pattern": "demo/events/{region}/{event_type}",
                "include_topic": true
            }
        },
        "format": { "name": "json" }
    }]'
);
```

## Fault tolerance

The connector declares at-least-once fault tolerance.  In a fault-tolerant
pipeline, acknowledgments are released only after a durable checkpoint covers
the ingesting step, so a resume from that checkpoint can never lose data.  At
suspend time, messages not yet covered by a checkpoint stay unacknowledged on
the broker and are redelivered after the resume — they can appear as
duplicate rows, which at-least-once semantics permit.  Exactly-once is not
supported: the connector cannot replay a step.

## Limitations

- The connector binds to a pre-existing durable queue; it does not create
  queues or subscribe directly to topics.  Add topic subscriptions to the
  queue on the broker.
- Basic (username/password) authentication only.
- The dedup cache is session-scoped: the first redelivery of each in-flight
  message after a full pipeline restart is re-ingested (an expected
  at-least-once duplicate).
