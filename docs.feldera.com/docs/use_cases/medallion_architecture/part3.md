# Part 3: Compare Feldera and Spark

Parts [1](./part1.md) and [2](./part2.md) built the medallion as an always-on Feldera pipeline that updates Gold within milliseconds of each change. This section runs the **same business logic as a batch job in Spark** so you can compare the two cost models side by side.

The batch job is provided as a Databricks notebook:

- [`spark_notebook.py`](https://github.com/feldera/feldera/blob/main/docs.feldera.com/docs/use_cases/medallion_architecture/spark_notebook.py) — *Compute Silver & Gold Delta Tables — Spark Batch*

```bash
curl -O https://raw.githubusercontent.com/feldera/feldera/main/docs.feldera.com/docs/use_cases/medallion_architecture/spark_notebook.py
```

Import it into Databricks (or any Spark 3.x environment with Delta Lake), or run it as a plain PySpark script — outside Databricks you'll need the `delta-spark` and `hadoop-aws` packages configured, and the `# COMMAND` / `# MAGIC` lines are just comments. It reads the same Bronze snapshot from `s3://feldera-demos/ecommerce-cdc-0-01/snapshot` and produces the **same 7 Silver and 7 Gold tables** as the SQL pipeline.

## What the notebook does

The notebook mirrors the demo SQL one-to-one, organized into the sections you'll see in the file:

- **Configuration** — sets the scale factor and the Bronze/Silver/Gold S3 paths. By default `WRITE_OUTPUT = False`, so Gold is computed but written to a `noop` sink; this isolates the *compute* cost from S3 write latency. Flip it to `True` to persist the Silver and Gold layers as Delta tables.
- **Silver & Gold pipeline** — `run_pipeline()` rebuilds all 14 tables with full-refresh (`overwrite`) semantics, the way a scheduled batch job would.
- **Run** — executes one full refresh and prints the per-step and total wall-clock time.
- **CDC incremental comparison** — `run_pipeline_with_cdc(up_to_hour)` reprocesses the snapshot *plus* a growing prefix of the CDC stream at a series of `CDC_CHECKPOINTS`. The incremental comparison shows **batch cost scaling**.

## Key Differences

A batch engine has no memory of its previous run. To reflect a single new CDC event, it must re-read the entire dataset and recompute every layer from scratch. As the dataset grows, each refresh gets slower — even when only a handful of rows changed.

| | Spark (batch) | Feldera (incremental) |
|---|---|---|
| Cost of a refresh | Proportional to **total** dataset size | Proportional to the **size of the change** |
| Reflecting one CDC event | Full re-scan + full recompute | Update only the affected Gold rows |
| Freshness | Bounded by the batch schedule | Milliseconds |
| Orchestration | Scheduler, dependency DAG, full-vs-incremental logic | None — views are always current |

At every `CDC_CHECKPOINTS` step, Spark re-scans the *entire* dataset and rebuilds every layer from scratch. At the demo's scale factor the dataset is small, so per-checkpoint times stay roughly constant — but each run still recomputes everything, and that cost grows directly with total data size as history accumulates. Feldera absorbed those same changes in milliseconds because its cost is tied to *what changed*, not *how much data exists* — so its latency and compute cost stay flat as the dataset grows, while the batch cost climbs with it.

## Running the comparison

1. Keep the Feldera pipeline from Parts 1–2 running and note how long each `push_changes.py` push took (printed per table, per hour).
2. Run `spark_notebook.py` end to end. Note the **total refresh time** from the **Run** section.
3. Work through the **CDC incremental comparison** section. Each checkpoint re-scans the whole dataset and recomputes every layer; at this scale factor the times stay roughly constant, but each is a full from-scratch refresh whose cost scales with total data size.
4. Compare: the Spark numbers are the cost of *one* full refresh of the whole dataset — paid again on every run and growing with the data; the Feldera numbers in Part 2 are the cost of *applying the changes* and stay flat as history grows.

Some of the silver views could be made incremental by hand by a data engineer — for example:

- `silver_customers`
- `silver_confirmed_order_items`
- `silver_inventory_current`
- `silver_inventory_by_supplier`

More complex views, such as `gold_weekly_revenue_trend`, would require merge logic that is difficult to validate and entails significant engineering effort.

With Feldera, you use the same SQL your batch engine runs now, get the same results, and pay a cost that tracks change size instead of data size. In addition, you get millisecond update latency across your full medallion architecture.
