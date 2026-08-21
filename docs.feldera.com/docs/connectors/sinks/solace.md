# Solace output connector

:::info Sidecar alternative
This in-process connector requires a custom Feldera build (it links the Solace
C SDK into the pipeline).  For new deployments consider the
[Solace sidecar](https://github.com/jessemenning/feldera-solace-sidecar),
which offers the same behavior against an **unmodified upstream Feldera image**
by bridging data over the pipeline's HTTP API.
:::

Feldera can publish a stream of changes to a SQL view to a
[Solace Platform](https://solace.com/) event broker with the `solace_output`
connector.  Records are published to a topic over the native SMF protocol,
with optional per-record topic routing and per-topic rate throttling.

## How it works

Each serialized view record is published as one message to the configured
topic.  Two delivery modes are available:

- **`direct`** (default): fire-and-forget, lowest latency, no broker
  acknowledgment.  Messages published during a broker outage are lost.
- **`persistent`**: broker-acknowledged.  The connector windows up to
  `max_inflight_acks` outstanding acknowledgments and blocks at each batch
  boundary (bounded by `ack_timeout_secs`) until the broker has persisted
  every message in the batch.  An acknowledgment failure or timeout fails the
  batch with a transport error visible in the endpoint's error metrics.

If the broker connection is lost past the SDK's own reconnect budget, the
connector rebuilds the session on the next publish rather than failing
permanently; batches that raced the failure surface as non-fatal transport
errors.

## Configuration

### Connection

Identical to the [Solace input connector](/connectors/sources/solace)
connection options: `host`, `port`, `vpn`, `username`, `password`, `tls`,
`ssl_trust_store_dir`, `client_name`, `connect_timeout_secs`,
`reconnect_retries`, `reconnect_retry_wait_ms`, and `log_level`.

### Publishing

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `topic` | string | Yes | Destination topic; supports `{field}` placeholders (see below) |
| `delivery_mode` | string | No | `direct` or `persistent`. Default: `direct` |
| `max_inflight_acks` | integer | No | Maximum broker acknowledgments in flight before publishing blocks (`persistent` only). Must be ≥ 1. Default: 256 |
| `ack_timeout_secs` | integer | No | Maximum wait for broker acknowledgments at each batch boundary (`persistent` only). Must be ≥ 1. Default: 30 |
| `dedup_window_ms` | integer | No | Per-topic publish throttle with conflation (see below). Default: disabled |

### Dynamic topics

The topic may contain `{field}` placeholders that are resolved from each JSON
record, including Feldera's `insert_delete` wrapper format:

```
topic:    demo/results/{region}/{event_type}
record:   {"insert": {"region": "us-east", "event_type": "order", ...}}
publish:  demo/results/us-east/order
```

Dynamic topics require the JSON output format (the connector rejects other
formats at startup).  A placeholder whose field is absent from the record is
kept literally.

### Dedup window

Set `dedup_window_ms` to bound each topic's outbound message rate: at most
one message per resolved topic is published per window.  Records arriving
inside the window are **conflated** — only the latest payload per topic is
kept — and the survivor is published at a batch boundary once the window
expires.  Use this to throttle a high-churn view (e.g. a per-key aggregate
updating hundreds of times per second) without ever losing its newest value.

A record conflated on a stream that then goes quiet is published at the next
batch boundary after its window expires, not immediately on expiry.

## Example

```sql
CREATE MATERIALIZED VIEW region_counts
WITH (
    'connectors' = '[{
        "transport": {
            "name": "solace_output",
            "config": {
                "host": "broker.example.com",
                "username": "feldera",
                "password": "secret",
                "topic": "demo/results/{region}/{event_type}",
                "delivery_mode": "persistent",
                "dedup_window_ms": 1000
            }
        },
        "format": { "name": "json" }
    }]'
) AS
SELECT region, event_type, COUNT(*) AS event_count
FROM   events
GROUP  BY region, event_type;
```

## Delivery semantics

The output connector is not fault-tolerant: it does not participate in
checkpointing, and delivery is at-most-once across pipeline restarts.
Within a running pipeline, `persistent` mode guarantees that a step is not
reported complete until the broker has persisted every message of the batch;
`direct` mode offers no delivery guarantee.

## Limitations

- Topic destinations only (no direct-to-queue publishing); to deliver to a
  queue, add a topic subscription to the queue on the broker.
- JSON is required for `{field}` topic templates; static topics work with any
  output format.
- The one-record-per-message guarantee applies to the JSON output format.
  Other formats (e.g. CSV) may pack several records into one message.
- Key-value output formats (e.g. Debezium-style keyed formats) are not
  supported.
- Basic (username/password) authentication only.
