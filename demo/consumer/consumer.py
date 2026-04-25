#!/usr/bin/env python3
"""
CDC Consumer — reads Debezium events from Redpanda/Kafka, writes raw JSON to MinIO (S3).

This is the first half of the Snowpipe pattern:
  Kafka  →  Consumer  →  MinIO (S3)
                          ↓
                        Loader  →  PostgreSQL

File naming in MinIO:
  {table}/{YYYY-MM-DD}/{timestamp_ms}_{partition}_{offset}.json

Each file is one CDC event. The loader picks these up in chronological order.
"""

import json
import logging
import os
import signal
import time
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("cdc-consumer")

# ── Config ────────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_PREFIX     = os.environ.get("TOPIC_PREFIX", "demo")
MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET", "cdc-events")

RUNNING = True


def _shutdown(sig, frame):
    global RUNNING
    log.info("Shutdown signal — stopping.")
    RUNNING = False


signal.signal(signal.SIGTERM, _shutdown)
signal.signal(signal.SIGINT,  _shutdown)


# ── MinIO / S3 client ─────────────────────────────────────────────────────────

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",  # required by boto3 even though MinIO ignores it
    )


def _write_event(s3, table: str, event: dict, partition: int, offset: int) -> str:
    """Write a single CDC event as a JSON file to MinIO. Returns the S3 key."""
    date_str = time.strftime("%Y-%m-%d")
    ts_ms    = int(time.time() * 1000)
    key      = f"{table}/{date_str}/{ts_ms}_{partition}_{offset}.json"

    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(event, default=str).encode("utf-8"),
        ContentType="application/json",
    )
    return key


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def _wait_for_kafka(bootstrap: str, retries: int = 24) -> bool:
    for attempt in range(retries):
        try:
            admin = AdminClient({"bootstrap.servers": bootstrap})
            admin.list_topics(timeout=5)
            log.info("Kafka is reachable.")
            return True
        except Exception:
            log.info(f"Waiting for Kafka... ({attempt + 1}/{retries})")
            time.sleep(5)
    return False


def _discover_topics(bootstrap: str, prefix: str, timeout: int = 180) -> list[str]:
    admin    = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + timeout
    log.info(f"Waiting for CDC topics (prefix='{prefix}')...")
    while time.time() < deadline:
        try:
            meta   = admin.list_topics(timeout=5)
            topics = [
                t for t in meta.topics
                if t.startswith(f"{prefix}.")
                and "schema-changes" not in t
                and not t.startswith(f"{prefix}._")
            ]
            if topics:
                log.info(f"Found topics: {topics}")
                return topics
        except Exception as e:
            log.debug(f"Topic discovery error: {e}")
        time.sleep(5)
    log.warning("No CDC topics found within timeout — will retry in main loop.")
    return []


# ── Main loop ─────────────────────────────────────────────────────────────────

def main() -> None:
    log.info(f"Starting CDC consumer | kafka={KAFKA_BOOTSTRAP} | bucket={MINIO_BUCKET}")

    if not _wait_for_kafka(KAFKA_BOOTSTRAP):
        log.error("Kafka never became reachable. Exiting.")
        return

    # Wait for MinIO to be reachable
    s3: Optional[object] = None
    for attempt in range(12):
        try:
            s3 = _s3_client()
            s3.head_bucket(Bucket=MINIO_BUCKET)
            log.info(f"MinIO reachable. Bucket '{MINIO_BUCKET}' exists.")
            break
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code == "404":
                log.warning(f"Bucket '{MINIO_BUCKET}' not found yet — retrying...")
            else:
                log.warning(f"MinIO error ({code}): {e}")
            s3 = None
            time.sleep(5)
        except Exception as e:
            log.warning(f"MinIO not ready ({attempt + 1}/12): {e}")
            s3 = None
            time.sleep(5)

    if s3 is None:
        log.error("MinIO never became reachable. Exiting.")
        return

    topics = _discover_topics(KAFKA_BOOTSTRAP, TOPIC_PREFIX)

    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP,
        "group.id":           "cdc-minio-consumer",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    })

    if topics:
        consumer.subscribe(topics)
    else:
        log.warning("No topics yet — will re-discover shortly.")

    last_discovery = 0.0
    events_written = 0

    while RUNNING:
        # ── Re-discover new topics every 60 s ────────────────────────────────
        if time.time() - last_discovery > 60:
            new = _discover_topics(KAFKA_BOOTSTRAP, TOPIC_PREFIX, timeout=5)
            if new and set(new) != set(topics):
                topics = new
                consumer.subscribe(topics)
                log.info(f"Re-subscribed to: {topics}")
            last_discovery = time.time()

        # ── Poll Kafka ────────────────────────────────────────────────────────
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error(f"Kafka error: {msg.error()}")
            continue

        topic = msg.topic()
        if not topic.startswith(f"{TOPIC_PREFIX}."):
            continue

        table = topic.split(".")[-1]

        try:
            raw = msg.value()
            if raw is None:
                continue   # Kafka tombstone

            event = json.loads(raw.decode("utf-8"))

            # Normalise: handle both raw Debezium envelope and pre-extracted records.
            #
            # Raw Debezium envelope:  {"payload": {"op": "r"/"c"/"u"/"d", "after": {...}, "before": {...}}}
            # ExtractNewRecordState:  {"schema": {...}, "payload": {"col": val, ..., "__deleted": "false"}}
            #   → op field is removed; __deleted="true"/"false" marks deletes
            if "payload" in event and isinstance(event.get("payload"), dict):
                payload = event["payload"]
                op      = payload.get("op")
                after   = payload.get("after")
                before  = payload.get("before")

                if op is not None:
                    # Raw Debezium envelope format
                    if op == "d":
                        record = {**(before or {}), "__deleted": True, "__op": "d"}
                    else:
                        record = {**(after or {}), "__op": op}
                else:
                    # ExtractNewRecordState format — payload IS the flattened record
                    deleted = str(payload.get("__deleted", "false")).lower() == "true"
                    record  = {**payload, "__deleted": deleted, "__op": "d" if deleted else "r"}
            else:
                # Already fully unwrapped (no schema/payload wrapper)
                record = event

            # Enrich with metadata
            record["__table"]     = table
            record["__topic"]     = topic
            record["__partition"] = msg.partition()
            record["__offset"]    = msg.offset()
            record["__ingested"]  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            key = _write_event(s3, table, record, msg.partition(), msg.offset())
            events_written += 1

            log.info(f"→ [{events_written}] s3://{MINIO_BUCKET}/{key}")

        except json.JSONDecodeError:
            log.warning(f"Non-JSON message on {topic}")
        except (BotoCoreError, ClientError) as e:
            log.error(f"MinIO write failed: {e} — retrying with fresh client.")
            time.sleep(2)
            try:
                s3 = _s3_client()
            except Exception:
                pass
        except Exception as e:
            log.error(f"Error processing {topic}: {e}", exc_info=True)

    log.info(f"Consumer stopped. Total events written: {events_written}")
    consumer.close()


if __name__ == "__main__":
    main()
