#!/usr/bin/env python3
"""Push CDC data from S3 to a running Feldera pipeline and measure processing time.

Usage:
    # Push a single hour of CDC data
    python push_changes.py --hour 2025-11-30T00

    # Push all 24 hours for a date (times each hour individually)
    python push_changes.py --date 2025-11-30

    # Push all available CDC data
    python push_changes.py --all

Options:
    --pipeline   Pipeline name (default: ecommerce-demo)
    --feldera    Feldera URL (default: from FELDERA_URL env or http://localhost:8080)

The CDC data is read from a public S3 bucket using anonymous (unsigned) access,
so no AWS account or credentials are required. Set FELDERA_S3_SIGNED=1 to use
normal AWS credentials instead (e.g. when pointing at a private bucket).
"""

import argparse
import os
import time
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from dotenv import load_dotenv
from feldera import FelderaClient

load_dotenv()

CDC_TABLES = ["orders", "order_items", "clickstream_events", "inventory_events"]

# nginx default client_max_body_size is 1MB; chunk below that
MAX_CHUNK_BYTES = 900_000

SCALE_FACTOR = 0.01
S3_BUCKET = "feldera-demos"
S3_PREFIX = f"ecommerce-cdc-{str(SCALE_FACTOR).replace('.', '-')}"
CDC_S3_PREFIX = f"{S3_PREFIX}/cdc"


def read_cdc_file(s3_client, table, hour_str):
    """Read a CDC NDJSON file from S3. Returns (content_str, line_count)."""
    key = f"{CDC_S3_PREFIX}/{table}/{hour_str}.json"
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = obj["Body"].read().decode("utf-8").strip()
        line_count = content.count("\n") + 1 if content else 0
        return content, line_count
    except s3_client.exceptions.NoSuchKey:
        return None, 0


def list_cdc_hours(s3_client, table):
    """List all available hour files for a CDC table, sorted."""
    prefix = f"{CDC_S3_PREFIX}/{table}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    hours = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            filename = key.split("/")[-1]
            if filename.endswith(".json"):
                hours.append(filename.replace(".json", ""))
    return sorted(hours)


def chunk_ndjson(content, max_bytes=MAX_CHUNK_BYTES):
    """Split NDJSON content into chunks that fit under the nginx body size limit."""
    lines = content.split("\n")
    chunks = []
    current = []
    current_size = 0
    for line in lines:
        line_size = len(line.encode("utf-8")) + 1  # +1 for newline
        if current and current_size + line_size > max_bytes:
            chunks.append("\n".join(current))
            current = []
            current_size = 0
        current.append(line)
        current_size += line_size
    if current:
        chunks.append("\n".join(current))
    return chunks


def push_hour(s3_client, client, pipeline, hour_str, chunk_input=False, verbose=True):
    """Push one hour of CDC data for all tables. Returns (elapsed_seconds, total_rows)."""
    total_rows = 0
    t0 = time.time()

    for table in CDC_TABLES:
        content, count = read_cdc_file(s3_client, table, hour_str)
        if not content:
            continue

        table_name = f"bronze_{table}"
        chunks = chunk_ndjson(content) if chunk_input else [content]
        t_push = time.time()
        for chunk in chunks:
            client.push_to_pipeline(
                pipeline_name=pipeline,
                table_name=table_name,
                format="json",
                data=chunk,
                update_format="debezium",
                json_flavor="debezium_mysql",
                serialize=False,
                wait=True,
            )
        push_elapsed = time.time() - t_push
        total_rows += count

        if verbose:
            chunks_note = f" ({len(chunks)} chunks)" if len(chunks) > 1 else ""
            print(
                f"    {table_name:40s} {count:>6,} rows  →  {push_elapsed:.3f}s{chunks_note}"
            )

    elapsed = time.time() - t0

    return elapsed, total_rows


def push_date(s3_client, client, pipeline, date_str, chunk_input=False):
    """Push all 24 hours for a date. Prints per-hour and total timing."""
    print(f"\nPushing CDC for {date_str} (24 hours)")
    print("-" * 60)

    total_time = 0.0
    total_rows = 0

    for hour in range(24):
        hour_str = f"{date_str}T{hour:02d}"
        elapsed, rows = push_hour(
            s3_client, client, pipeline, hour_str, chunk_input=chunk_input
        )
        total_time += elapsed
        total_rows += rows

    print("-" * 60)
    print(f"  TOTAL: {total_rows:>6,} rows  →  {total_time:.3f}s")
    return total_time, total_rows


def push_all(s3_client, client, pipeline, chunk_input=False):
    """Push all available CDC data."""
    all_hours = list_cdc_hours(s3_client, CDC_TABLES[0])
    if not all_hours:
        print("No CDC files found.")
        return

    dates = sorted(set(h.split("T")[0] for h in all_hours))
    print(f"\nPushing all CDC data: {len(dates)} days, {len(all_hours)} hours")
    print("=" * 60)

    grand_total_time = 0.0
    grand_total_rows = 0

    for date_str in dates:
        t, r = push_date(s3_client, client, pipeline, date_str, chunk_input=chunk_input)
        grand_total_time += t
        grand_total_rows += r

    print("\n" + "=" * 60)
    print(f"ALL CDC DATA: {grand_total_rows:,} rows  →  {grand_total_time:.3f}s")
    print("=" * 60)


def main():
    parser = argparse.ArgumentParser(description="Push CDC data to Feldera pipeline")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--hour", help="Push a single hour (e.g., 2025-11-30T00)")
    group.add_argument("--date", help="Push all 24 hours for a date (e.g., 2025-11-30)")
    group.add_argument("--all", action="store_true", help="Push all available CDC data")
    parser.add_argument("--pipeline", default="ecommerce-demo", help="Pipeline name")
    parser.add_argument("--feldera", default=None, help="Feldera URL")
    parser.add_argument(
        "--chunk-input",
        action="store_true",
        help=f"Split each table's NDJSON payload into <{MAX_CHUNK_BYTES}-byte chunks before pushing (for nginx body-size limits)",
    )
    args = parser.parse_args()

    feldera_url = args.feldera or os.getenv("FELDERA_URL", "http://localhost:8080")
    feldera_api_key = os.getenv("FELDERA_API_KEY")
    client = FelderaClient(url=feldera_url, api_key=feldera_api_key)

    # The demo CDC data lives in a public S3 bucket, so read it anonymously
    # (unsigned) by default — no AWS account or credentials required. Set
    # FELDERA_S3_SIGNED=1 to fall back to normal AWS credentials (e.g. if you
    # repoint S3_BUCKET at a private bucket).
    if os.getenv("FELDERA_S3_SIGNED"):
        s3_client = boto3.client("s3", region_name="us-west-1")
    else:
        s3_client = boto3.client(
            "s3", region_name="us-west-1", config=Config(signature_version=UNSIGNED)
        )

    if args.hour:
        print(f"\nPushing CDC hour: {args.hour}")
        print("-" * 60)
        elapsed, rows = push_hour(
            s3_client, client, args.pipeline, args.hour, chunk_input=args.chunk_input
        )

    elif args.date:
        push_date(
            s3_client, client, args.pipeline, args.date, chunk_input=args.chunk_input
        )

    elif args.all:
        push_all(s3_client, client, args.pipeline, chunk_input=args.chunk_input)


if __name__ == "__main__":
    main()
