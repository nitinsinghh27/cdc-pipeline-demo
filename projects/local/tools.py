# =============================================================================
# FILE:    projects/local/tools.py
# PURPOSE: Tool functions for the local CDC demo agent.
#
# All functions return:  {"ok": bool, "data": ..., "error": str | None}
#
# Tools are grouped:
#   A. Kafka Connect (Debezium) — connector status & config
#   B. MinIO (S3)               — object storage inspection
#   C. PostgreSQL               — destination data queries
#   D. MySQL                    — source data queries (lag comparison)
#   E. Redpanda                 — broker health & topics
#   F. Grafana                  — dashboards, alerts, Loki logs, Prometheus metrics
# =============================================================================

from __future__ import annotations

import base64
import json
import os
import re
import time
from typing import Optional

import boto3
import psycopg2
import psycopg2.extras
import pymysql
import requests
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

from knowledge import INFRA, CDC_TABLES

# =============================================================================
# MINIO / S3 CLIENT
# =============================================================================

def _s3():
    cfg = INFRA.get("minio", {})
    return boto3.client(
        "s3",
        endpoint_url     = os.environ.get("MINIO_ENDPOINT",   cfg.get("endpoint",   "http://localhost:9000")),
        aws_access_key_id     = os.environ.get("MINIO_ACCESS_KEY", cfg.get("access_key", "minioadmin")),
        aws_secret_access_key = os.environ.get("MINIO_SECRET_KEY", cfg.get("secret_key", "minioadmin")),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

_CDC_BUCKET      = "cdc-events"
_INTERNAL_FIELDS = {"__table", "__topic", "__partition", "__offset", "__ingested", "__op", "__deleted"}

# =============================================================================
# SHARED HELPERS
# =============================================================================

def _ok(data) -> dict:
    return {"ok": True, "data": data, "error": None}

def _err(msg: str) -> dict:
    return {"ok": False, "data": None, "error": msg}

def _get(url: str, headers: dict = None, timeout: int = 10) -> dict:
    try:
        r = requests.get(url, headers=headers or {}, timeout=timeout)
        r.raise_for_status()
        return _ok(r.json())
    except requests.HTTPError as e:
        return _err(f"HTTP {e.response.status_code}: {e.response.text[:200]}")
    except requests.RequestException as e:
        return _err(f"Request failed: {e}")


# =============================================================================
# A. KAFKA CONNECT  (Debezium)
# =============================================================================

_KC_BASE = INFRA["kafka_connect"]["url"]


def list_kafka_connectors() -> dict:
    """Return all registered Kafka Connect connectors with their current state."""
    r = _get(f"{_KC_BASE}/connectors?expand=status&expand=info")
    if not r["ok"]:
        return r

    connectors = []
    for name, detail in r["data"].items():
        state  = detail.get("status", {}).get("connector", {}).get("state", "UNKNOWN")
        tasks  = detail.get("status", {}).get("tasks", [])
        failed = [t for t in tasks if t.get("state") == "FAILED"]
        connectors.append({
            "name":          name,
            "state":         state,
            "tasks_total":   len(tasks),
            "tasks_failed":  len(failed),
        })

    return _ok({"connectors": connectors, "count": len(connectors)})


def get_connector_status(connector_name: str) -> dict:
    """Return detailed status for a specific connector (state, tasks, errors)."""
    r = _get(f"{_KC_BASE}/connectors/{connector_name}/status")
    if not r["ok"]:
        return r

    raw     = r["data"]
    conn    = raw.get("connector", {})
    tasks   = raw.get("tasks", [])
    errors  = [
        {"task_id": t["id"], "trace": t.get("trace", "")[:500]}
        for t in tasks
        if t.get("state") == "FAILED"
    ]

    return _ok({
        "connector_name": connector_name,
        "state":          conn.get("state", "UNKNOWN"),
        "worker_id":      conn.get("worker_id"),
        "tasks":          tasks,
        "failed_tasks":   errors,
    })


def get_connector_config(connector_name: str) -> dict:
    """Return the current configuration of a connector."""
    return _get(f"{_KC_BASE}/connectors/{connector_name}/config")


# =============================================================================
# B. MINIO / S3  (staging layer between Kafka and Postgres)
# =============================================================================

def list_minio_objects(
    prefix:   str = "",
    max_keys: int = 50,
) -> dict:
    """
    List objects in the cdc-events MinIO bucket.
    Use this to see which CDC event files are sitting in S3 waiting to be loaded.
    prefix can filter by table, e.g. prefix='stores/' shows only stores events.
    """
    try:
        s3     = _s3()
        kwargs = {"Bucket": _CDC_BUCKET, "MaxKeys": max_keys}
        if prefix:
            kwargs["Prefix"] = prefix

        resp    = s3.list_objects_v2(**kwargs)
        objects = [
            {
                "key":           o["Key"],
                "size_bytes":    o["Size"],
                "last_modified": o["LastModified"].isoformat(),
            }
            for o in resp.get("Contents", [])
        ]
        return _ok({
            "bucket":        _CDC_BUCKET,
            "prefix":        prefix,
            "objects":       objects,
            "file_count":    len(objects),
            "truncated":     resp.get("IsTruncated", False),
        })
    except (BotoCoreError, ClientError) as e:
        return _err(f"MinIO error: {e}")
    except Exception as e:
        return _err(f"Unexpected error: {e}")


def get_minio_pipeline_stats() -> dict:
    """
    Show the S3 stage of the pipeline at a glance:
      - How many event files are in cdc-events per table (pending load)
      - How many files have been processed (from _cdc_loader_state)
      - The difference = files still waiting to be loaded into Postgres

    This is the S3-layer equivalent of checking consumer lag.
    A large pending count means the loader is behind or stopped.
    """
    # Count files in MinIO by table prefix
    try:
        s3         = _s3()
        paginator  = s3.get_paginator("list_objects_v2")
        all_keys   = []
        for page in paginator.paginate(Bucket=_CDC_BUCKET):
            all_keys.extend(o["Key"] for o in page.get("Contents", []))

        all_keys_set = set(all_keys)
        total_in_minio = len(all_keys)
        in_minio_by_table: dict[str, int] = {}
        for key in all_keys:
            table = key.split("/")[0]
            in_minio_by_table[table] = in_minio_by_table.get(table, 0) + 1

    except (BotoCoreError, ClientError) as e:
        return _err(f"MinIO error: {e}")

    # Count processed files from Postgres state table
    processed_keys: set[str] = set()
    processed_by_table: dict[str, int] = {}
    total_processed = 0
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT file_key, table_name FROM _cdc_loader_state")
                for file_key, table in cur.fetchall():
                    processed_keys.add(file_key)
                    processed_by_table[table] = processed_by_table.get(table, 0) + 1
                    total_processed += 1
            except psycopg2.Error:
                conn.rollback()   # state table may not exist yet
        conn.close()
    except Exception:
        pass   # Postgres might not be ready; proceed with MinIO counts only

    # Pending = files in MinIO not yet recorded in loader state
    unprocessed_keys = all_keys_set - processed_keys
    pending_by_table: dict[str, int] = {}
    for key in unprocessed_keys:
        table = key.split("/")[0]
        pending_by_table[table] = pending_by_table.get(table, 0) + 1

    total_pending = len(unprocessed_keys)

    return _ok({
        "total_in_minio":        total_in_minio,
        "in_minio_by_table":     in_minio_by_table,
        "pending_unprocessed":   pending_by_table,
        "processed_to_postgres": processed_by_table,
        "total_pending":         total_pending,
        "total_processed":       total_processed,
        "loader_healthy":        total_pending == 0,
        "note": "pending_unprocessed = files in MinIO not yet loaded into Postgres",
    })


def inspect_minio_event(key: str) -> dict:
    """
    Download and return the content of a specific CDC event file from MinIO.
    Use list_minio_objects first to find a key, then call this to read the event.
    Useful for debugging malformed events that the loader is failing on.
    """
    try:
        s3   = _s3()
        resp = s3.get_object(Bucket=_CDC_BUCKET, Key=key)
        data = json.loads(resp["Body"].read().decode("utf-8"))
        return _ok({"key": key, "event": data})
    except (BotoCoreError, ClientError) as e:
        return _err(f"MinIO error: {e}")
    except json.JSONDecodeError as e:
        return _err(f"File is not valid JSON: {e}")
    except Exception as e:
        return _err(f"Unexpected error: {e}")


# =============================================================================
# C. POSTGRESQL  (destination)
# =============================================================================

# SQL operations that must never execute in a read-only agent
_PG_BLOCKED = {"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
               "TRUNCATE", "MERGE", "COPY", "GRANT", "REVOKE"}


def _pg_conn():
    cfg = INFRA["postgres"]
    return psycopg2.connect(
        host     = os.environ.get("POSTGRES_HOST",     cfg["host"]),
        port     = int(os.environ.get("POSTGRES_PORT", cfg["port"])),
        dbname   = os.environ.get("POSTGRES_DB",       cfg["database"]),
        user     = os.environ.get("POSTGRES_USER",     cfg["user"]),
        password = os.environ.get("POSTGRES_PASSWORD", "postgres_pass"),
        connect_timeout = 5,
    )


def query_postgres(sql: str) -> dict:
    """
    Run a read-only SQL query against the PostgreSQL staging database.
    Returns up to 500 rows.
    """
    first_word = sql.strip().upper().split()[0] if sql.strip() else ""
    if first_word in _PG_BLOCKED:
        return _err(f"BLOCKED: '{first_word}' is a write operation — this agent is read-only.")

    try:
        conn = _pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql)
            rows = cur.fetchmany(500)
        conn.close()
        return _ok({"rows": [dict(r) for r in rows], "row_count": len(rows)})
    except psycopg2.Error as e:
        return _err(f"Postgres error: {e}")
    except Exception as e:
        return _err(f"Unexpected error: {e}")


def get_table_summary() -> dict:
    """
    Return row counts for all CDC-tracked tables in PostgreSQL.
    Use this to quickly check if data is arriving from MySQL.
    """
    try:
        conn   = _pg_conn()
        counts = {}
        tables_found = []
        tables_missing = []

        with conn.cursor() as cur:
            for table in CDC_TABLES:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                    counts[table] = cur.fetchone()[0]
                    tables_found.append(table)
                except psycopg2.Error:
                    conn.rollback()
                    tables_missing.append(table)
                    counts[table] = None

        conn.close()
        return _ok({
            "row_counts":      counts,
            "tables_found":    tables_found,
            "tables_missing":  tables_missing,
            "note": "tables_missing means they haven't been created yet (no CDC events received)"
        })
    except psycopg2.Error as e:
        return _err(f"Postgres connection failed: {e}")


# =============================================================================
# D. MYSQL  (source — for lag comparison)
# =============================================================================

def _mysql_conn():
    cfg = INFRA["mysql"]
    return pymysql.connect(
        host     = os.environ.get("MYSQL_HOST",     cfg["host"]),
        port     = int(os.environ.get("MYSQL_PORT", cfg["port"])),
        database = os.environ.get("MYSQL_DB",       cfg["database"]),
        user     = os.environ.get("MYSQL_USER",     cfg["user"]),
        password = os.environ.get("MYSQL_PASSWORD", "app_pass"),
        connect_timeout = 5,
    )


def get_pipeline_lag() -> dict:
    """
    Compare row counts between MySQL (source) and PostgreSQL (destination).
    The difference is the CDC lag — rows that haven't been replicated yet.
    """
    results = {}
    try:
        mysql_conn = _mysql_conn()
        with mysql_conn.cursor() as cur:
            for table in CDC_TABLES:
                cur.execute(f"SELECT COUNT(*) FROM `{table}`")
                results[table] = {"mysql": cur.fetchone()[0], "postgres": None, "lag": None}
        mysql_conn.close()
    except Exception as e:
        return _err(f"MySQL connection failed: {e}")

    try:
        pg_conn = _pg_conn()
        with pg_conn.cursor() as cur:
            for table in CDC_TABLES:
                try:
                    cur.execute(f'SELECT COUNT(*) FROM "{table}"')
                    pg_count = cur.fetchone()[0]
                    results[table]["postgres"] = pg_count
                    results[table]["lag"] = results[table]["mysql"] - pg_count
                except psycopg2.Error:
                    pg_conn.rollback()
                    results[table]["postgres"] = 0
                    results[table]["lag"] = results[table]["mysql"]
        pg_conn.close()
    except Exception as e:
        return _err(f"Postgres connection failed: {e}")

    total_lag = sum(v["lag"] for v in results.values() if v["lag"] is not None)
    return _ok({
        "tables":    results,
        "total_lag": total_lag,
        "healthy":   total_lag == 0,
    })


# =============================================================================
# E. REDPANDA
# =============================================================================

_RP_ADMIN = INFRA["redpanda"]["admin_url"]


def get_redpanda_health() -> dict:
    """Return the Redpanda cluster health overview (healthy, nodes down, etc.)."""
    r = _get(f"{_RP_ADMIN}/v1/cluster/health_overview")
    if not r["ok"]:
        return r

    d = r["data"]
    return _ok({
        "is_healthy":                d.get("is_healthy"),
        "all_nodes":                 d.get("all_nodes", []),
        "nodes_down":                d.get("nodes_down", []),
        "leaderless_partitions":     d.get("leaderless_partitions", []),
        "under_replicated_partitions": d.get("under_replicated_partitions", []),
    })


def get_kafka_topics() -> dict:
    """List all Kafka topics in Redpanda and their partition counts."""
    r = _get(f"{_RP_ADMIN}/v1/topics")
    if not r["ok"]:
        return r

    topics = []
    for t in r["data"]:
        topics.append({
            "name":       t.get("name"),
            "partitions": t.get("partitions_count", len(t.get("partitions", []))),
        })

    return _ok({"topics": topics, "count": len(topics)})


# =============================================================================
# F. GRAFANA  (alerts + Loki logs + Prometheus metrics)
# =============================================================================

class _GrafanaClient:
    """Thin Grafana HTTP API client with basic-auth fallback."""

    def __init__(self):
        self._base     = os.environ.get("GRAFANA_BASE_URL", INFRA["grafana"]["base_url"]).rstrip("/")
        api_key        = os.environ.get("GRAFANA_API_KEY", "")
        user           = os.environ.get("GRAFANA_USER",    "admin")
        password       = os.environ.get("GRAFANA_PASSWORD","admin")

        if api_key:
            self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        else:
            encoded = base64.b64encode(f"{user}:{password}".encode()).decode()
            self._headers = {"Authorization": f"Basic {encoded}", "Content-Type": "application/json"}

        self._ds_cache: dict[str, str] = {}

    def get(self, path: str) -> dict:
        return _get(f"{self._base}{path}", headers=self._headers)

    def post(self, path: str, body: dict) -> dict:
        try:
            r = requests.post(
                f"{self._base}{path}",
                headers=self._headers,
                json=body,
                timeout=30,
            )
            r.raise_for_status()
            return _ok(r.json())
        except requests.HTTPError as e:
            return _err(f"HTTP {e.response.status_code}: {e.response.text[:300]}")
        except requests.RequestException as e:
            return _err(f"Request failed: {e}")

    def datasource_uid(self, ds_type: str) -> Optional[str]:
        """Return the UID for the first datasource matching ds_type (e.g. 'loki', 'prometheus')."""
        if not self._ds_cache:
            r = self.get("/api/datasources")
            if r["ok"]:
                for ds in r["data"]:
                    dtype = ds.get("type", "").lower()
                    uid   = ds.get("uid", "")
                    if uid and dtype not in self._ds_cache:
                        self._ds_cache[dtype] = uid
        return self._ds_cache.get(ds_type.lower())


_grafana = _GrafanaClient()


def get_grafana_alerts() -> dict:
    """
    Return all currently firing or pending Grafana alerts.
    Use this to see if the monitoring stack has detected a pipeline issue.
    """
    r = _grafana.get("/api/alertmanager/grafana/api/v2/alerts")
    if not r["ok"]:
        return r

    alerts = []
    for a in r["data"]:
        labels  = a.get("labels", {})
        annots  = a.get("annotations", {})
        alerts.append({
            "name":        labels.get("alertname", "unknown"),
            "state":       a.get("status", {}).get("state", "unknown"),
            "severity":    labels.get("severity", "unknown"),
            "summary":     annots.get("summary", ""),
            "starts_at":   a.get("startsAt", ""),
        })

    return _ok({
        "alerts":      alerts,
        "firing_count": sum(1 for a in alerts if a["state"] == "active"),
        "total_count":  len(alerts),
    })


def query_container_logs(
    container_name:  str,
    minutes_back:    int = 30,
    search_term:     str = "",
    max_lines:       int = 100,
) -> dict:
    """
    Query Loki logs for a specific Docker container via the Grafana API.

    container_name: Docker container name, e.g. 'demo-kafka-connect', 'demo-consumer'
    minutes_back:   How far back to look (default 30 min)
    search_term:    Optional filter string, e.g. 'ERROR', 'WARN', 'Exception'
    max_lines:      Max log lines to return (default 100)
    """
    uid = _grafana.datasource_uid("loki")
    if not uid:
        return _err("Loki datasource not found in Grafana. Is Loki running?")

    logql = f'{{container="{container_name}"}}'
    if search_term:
        logql += f' |= `{search_term}`'

    body = {
        "queries": [{
            "refId":       "A",
            "datasource":  {"uid": uid, "type": "loki"},
            "expr":        logql,
            "queryType":   "range",
            "maxLines":    max_lines,
        }],
        "from": f"now-{minutes_back}m",
        "to":   "now",
    }

    r = _grafana.post("/api/ds/query", body)
    if not r["ok"]:
        return r

    lines = _parse_loki_response(r["data"], max_lines)
    return _ok({
        "container":   container_name,
        "search_term": search_term,
        "lines":       lines,
        "line_count":  len(lines),
        "logql":       logql,
    })


def _parse_loki_response(data: dict, max_lines: int) -> list[str]:
    lines = []
    for result in data.get("results", {}).values():
        for frame in result.get("frames", []):
            schema_fields = frame.get("schema", {}).get("fields", [])
            time_idx = next((i for i, f in enumerate(schema_fields) if f.get("name") == "Time"), None)
            line_idx = next((i for i, f in enumerate(schema_fields) if f.get("name") in ("Line", "log", "body")), None)
            if line_idx is None:
                continue
            field_data = frame.get("data", {}).get("values", [])
            if not field_data or line_idx >= len(field_data):
                continue
            for i, msg in enumerate(field_data[line_idx]):
                if len(lines) >= max_lines:
                    break
                ts = ""
                if time_idx is not None and time_idx < len(field_data):
                    ts_ms = field_data[time_idx][i]
                    if ts_ms:
                        ts = time.strftime("%H:%M:%S", time.gmtime(ts_ms / 1000)) + " "
                lines.append(f"{ts}{msg}")
    return lines


def query_prometheus_metric(promql: str, minutes_back: int = 30) -> dict:
    """
    Run a PromQL query via Grafana and return the result.

    Useful queries for this stack:
      rate(vectorized_cluster_partition_records_produced_total[1m])
        → Redpanda message produce rate
      vectorized_kafka_server_partition_count
        → Number of partitions
    """
    uid = _grafana.datasource_uid("prometheus")
    if not uid:
        return _err("Prometheus datasource not found in Grafana.")

    body = {
        "queries": [{
            "refId":      "A",
            "datasource": {"uid": uid, "type": "prometheus"},
            "expr":       promql,
            "range":      True,
            "step":       "60",
        }],
        "from": f"now-{minutes_back}m",
        "to":   "now",
    }

    r = _grafana.post("/api/ds/query", body)
    if not r["ok"]:
        return r

    points = _parse_prometheus_response(r["data"])
    return _ok({
        "promql":  promql,
        "points":  points,
        "count":   len(points),
    })


def _parse_prometheus_response(data: dict) -> list[dict]:
    points = []
    for result in data.get("results", {}).values():
        for frame in result.get("frames", []):
            fields     = frame.get("schema", {}).get("fields", [])
            field_data = frame.get("data",   {}).get("values", [])
            if not field_data or len(field_data) < 2:
                continue
            times  = field_data[0]
            values = field_data[1]
            labels = {f["name"]: f.get("labels", {}) for f in fields}
            for i, (t, v) in enumerate(zip(times, values)):
                if v is not None:
                    points.append({
                        "time":  time.strftime("%H:%M:%S", time.gmtime(t / 1000)) if t else "",
                        "value": v,
                    })
    return points[-50:]  # return last 50 data points
