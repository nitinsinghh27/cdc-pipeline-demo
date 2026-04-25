#!/usr/bin/env python3
"""
CDC Loader — polls MinIO (S3) for new event files, applies them to PostgreSQL.

This is the second half of the Snowpipe pattern:
  MinIO (S3)  →  Loader  →  PostgreSQL

How it works:
  1. Every POLL_INTERVAL_SECONDS, list all objects in cdc-events bucket
  2. Compare against _cdc_loader_state table to find unprocessed files
  3. For each new file (sorted chronologically by key name):
       a. Download from MinIO
       b. Parse the CDC event
       c. Apply upsert or delete to the correct Postgres table
       d. Record the file key in _cdc_loader_state (marks it as done)
  4. Sleep and repeat

State table schema (auto-created on first run):
  _cdc_loader_state (file_key TEXT PK, table_name TEXT, loaded_at TIMESTAMP)
"""

import json
import logging
import os
import signal
import time
from typing import Optional

import boto3
import psycopg2
import psycopg2.extras
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cdc-loader")

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",     "cdc-events")
POLL_INTERVAL    = int(os.environ.get("POLL_INTERVAL_SECONDS", "5"))

PG_CONNECT = dict(
    host     = os.environ.get("POSTGRES_HOST",     "localhost"),
    port     = int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname   = os.environ.get("POSTGRES_DB",       "staging_db"),
    user     = os.environ.get("POSTGRES_USER",     "postgres"),
    password = os.environ.get("POSTGRES_PASSWORD", "postgres"),
    connect_timeout = 5,
)

# Primary key per table (used for upsert + delete)
TABLE_PK = {
    "stores":    "store_id",
    "products":  "product_id",
    "inventory": "inventory_id",
    "orders":    "order_id",
}

# Debezium internal fields — not written to destination tables
_INTERNAL_FIELDS = {"__table", "__topic", "__partition", "__offset", "__ingested", "__op", "__deleted"}

RUNNING = True


def _shutdown(sig, frame):
    global RUNNING
    log.info("Shutdown signal — stopping.")
    RUNNING = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# ── S3 / MinIO client ─────────────────────────────────────────────────────────

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _list_all_objects(s3, bucket: str) -> list[str]:
    """List every object key in the bucket (handles pagination)."""
    keys     = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return sorted(keys)  # chronological order because key = table/date/ts_partition_offset


def _download_event(s3, bucket: str, key: str) -> dict:
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


# ── PostgreSQL helpers ────────────────────────────────────────────────────────

_created_tables: set[str] = set()

_PG_TYPE = {int: "BIGINT", float: "NUMERIC", bool: "BOOLEAN", str: "TEXT", type(None): "TEXT"}


def _ensure_state_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS _cdc_loader_state (
                file_key   TEXT      PRIMARY KEY,
                table_name TEXT,
                loaded_at  TIMESTAMP DEFAULT NOW()
            )
        """)
    conn.commit()


def _get_processed_keys(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT file_key FROM _cdc_loader_state")
        return {row[0] for row in cur.fetchall()}


def _mark_processed(conn, key: str, table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO _cdc_loader_state (file_key, table_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (key, table),
        )
    conn.commit()


def _ensure_table(conn, table: str, row: dict) -> None:
    if table in _created_tables:
        return
    pk = TABLE_PK.get(table)
    col_defs = []
    for col, val in row.items():
        if col in _INTERNAL_FIELDS:
            continue
        pg_type = _PG_TYPE.get(type(val), "TEXT")
        if col == pk:
            col_defs.append(f'"{col}" {pg_type} PRIMARY KEY')
        else:
            col_defs.append(f'"{col}" {pg_type}')
    if not col_defs:
        return
    # Use a separate transaction so CREATE TABLE is committed before the INSERT
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(col_defs)})')
    conn.commit()
    _created_tables.add(table)
    log.info(f"Ensured table: {table}")


def _clean(row: dict) -> dict:
    """Remove Debezium internal fields before writing to Postgres."""
    return {k: v for k, v in row.items() if k not in _INTERNAL_FIELDS}


def _apply_event(conn, table: str, event: dict) -> None:
    """Apply a single CDC event to PostgreSQL."""
    pk  = TABLE_PK.get(table)
    row = _clean(event)

    if not row:
        return

    is_delete = event.get("__deleted") in (True, "true") or event.get("__op") == "d"

    if not is_delete:
        _ensure_table(conn, table, row)

    with conn.cursor() as cur:
        if is_delete:
            if pk and pk in row:
                cur.execute(f'DELETE FROM "{table}" WHERE "{pk}" = %s', (row[pk],))
        else:
            cols  = list(row.keys())
            vals  = [row[c] for c in cols]
            ph    = ", ".join(["%s"] * len(cols))
            clist = ", ".join(f'"{c}"' for c in cols)
            if pk and pk in row:
                updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols if c != pk)
                sql = f'INSERT INTO "{table}" ({clist}) VALUES ({ph}) ON CONFLICT ("{pk}") DO UPDATE SET {updates}'
            else:
                sql = f'INSERT INTO "{table}" ({clist}) VALUES ({ph})'
            cur.execute(sql, vals)

    conn.commit()


# ── Main loop ─────────────────────────────────────────────────────────────────

def _wait_for_postgres() -> psycopg2.extensions.connection:
    for attempt in range(24):
        try:
            conn = psycopg2.connect(**PG_CONNECT)
            log.info("Connected to PostgreSQL.")
            return conn
        except psycopg2.Error as e:
            log.info(f"Waiting for Postgres... ({attempt + 1}/24): {e}")
            time.sleep(5)
    raise RuntimeError("PostgreSQL never became reachable.")


def _wait_for_minio() -> object:
    s3 = _s3()
    for attempt in range(12):
        try:
            s3.head_bucket(Bucket=MINIO_BUCKET)
            log.info(f"MinIO reachable. Bucket '{MINIO_BUCKET}' exists.")
            return s3
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "404":
                log.warning(f"Bucket '{MINIO_BUCKET}' not found yet...")
            else:
                log.warning(f"MinIO: {e}")
        except Exception as e:
            log.warning(f"Waiting for MinIO ({attempt + 1}/12): {e}")
        time.sleep(5)
        s3 = _s3()
    raise RuntimeError("MinIO never became reachable.")


def main() -> None:
    log.info(f"Starting CDC loader | bucket={MINIO_BUCKET} | poll={POLL_INTERVAL}s")

    pg = _wait_for_postgres()
    s3 = _wait_for_minio()

    _ensure_state_table(pg)
    log.info("State table ready. Starting poll loop.")

    total_loaded = 0

    while RUNNING:
        try:
            # ── Reconnect if Postgres dropped ─────────────────────────────────
            if pg.closed:
                pg = _wait_for_postgres()
                _ensure_state_table(pg)

            # ── Find unprocessed files ────────────────────────────────────────
            all_keys       = _list_all_objects(s3, MINIO_BUCKET)
            processed_keys = _get_processed_keys(pg)
            pending        = [k for k in all_keys if k not in processed_keys]

            if not pending:
                time.sleep(POLL_INTERVAL)
                continue

            log.info(f"Processing {len(pending)} new file(s)...")

            for key in pending:
                if not RUNNING:
                    break
                try:
                    event      = _download_event(s3, MINIO_BUCKET, key)
                    table_name = event.get("__table") or key.split("/")[0]

                    _apply_event(pg, table_name, event)
                    _mark_processed(pg, key, table_name)

                    total_loaded += 1
                    log.debug(f"Loaded: {key}")

                except Exception as e:
                    log.error(f"Failed to process {key}: {e}", exc_info=True)
                    # Don't mark as processed — will retry next poll
                    # Clear in-memory table cache so CREATE TABLE is retried on next attempt
                    table_name_for_key = event.get("__table") or key.split("/")[0] if 'event' in dir() else key.split("/")[0]
                    _created_tables.discard(table_name_for_key)
                    try:
                        pg.rollback()
                    except Exception:
                        pg = None
                        break

            if total_loaded > 0 and total_loaded % 100 == 0:
                log.info(f"Total records loaded into Postgres: {total_loaded}")

        except (BotoCoreError, ClientError) as e:
            log.error(f"MinIO error: {e} — retrying fresh client.")
            time.sleep(5)
            s3 = _s3()
        except psycopg2.Error as e:
            log.error(f"Postgres error: {e} — reconnecting.")
            pg = None
            time.sleep(5)
        except Exception as e:
            log.error(f"Loader error: {e}", exc_info=True)
            time.sleep(POLL_INTERVAL)

        time.sleep(POLL_INTERVAL)

    log.info(f"Loader stopped. Total records loaded: {total_loaded}")
    if pg and not pg.closed:
        pg.close()


if __name__ == "__main__":
    main()
