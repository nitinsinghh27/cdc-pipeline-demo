# =============================================================================
# FILE:    projects/local/registry.py
# PURPOSE: Claude tool definitions + dispatch for the local CDC demo agent.
#
# TOOLS (11 total):
#   Kafka Connect:  list_kafka_connectors, get_connector_status, get_connector_config
#   PostgreSQL:     query_postgres, get_table_summary
#   MySQL:          get_pipeline_lag
#   Redpanda:       get_redpanda_health, get_kafka_topics
#   Grafana:        get_grafana_alerts, query_container_logs, query_prometheus_metric
# =============================================================================

from tools import (
    list_kafka_connectors,
    get_connector_status,
    get_connector_config,
    list_minio_objects,
    get_minio_pipeline_stats,
    inspect_minio_event,
    query_postgres,
    get_table_summary,
    get_pipeline_lag,
    get_redpanda_health,
    get_kafka_topics,
    get_grafana_alerts,
    query_container_logs,
    query_prometheus_metric,
)

# =============================================================================
# TOOL DEFINITIONS  (passed to Claude API as the `tools` parameter)
# =============================================================================

TOOLS = [

    # ── Kafka Connect ──────────────────────────────────────────────────────────

    {
        "name": "list_kafka_connectors",
        "description": (
            "List all registered Kafka Connect (Debezium) connectors and their current state "
            "(RUNNING, PAUSED, FAILED). Call this first when investigating a CDC issue — "
            "it tells you immediately if any connector is unhealthy."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    {
        "name": "get_connector_status",
        "description": (
            "Get the detailed status of a specific Kafka Connect connector, including "
            "per-task state and any error traces. Call this when a connector shows FAILED "
            "in list_kafka_connectors — the trace tells you exactly why it failed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_name": {
                    "type": "string",
                    "description": "Connector name, e.g. 'mysql-source'",
                }
            },
            "required": ["connector_name"],
        },
    },

    {
        "name": "get_connector_config",
        "description": (
            "Return the current configuration of a Kafka Connect connector. "
            "Use this to verify connector settings (hostname, credentials, table list, etc.) "
            "when diagnosing why it might not be capturing the expected tables."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "connector_name": {
                    "type": "string",
                    "description": "Connector name, e.g. 'mysql-source'",
                }
            },
            "required": ["connector_name"],
        },
    },

    # ── MinIO (S3 staging layer) ───────────────────────────────────────────────

    {
        "name": "get_minio_pipeline_stats",
        "description": (
            "Show the health of the S3 staging layer at a glance: "
            "how many CDC event files are sitting in MinIO waiting to be loaded into Postgres, "
            "vs how many have already been processed. "
            "A non-zero pending count means the loader is behind or stopped — "
            "this is the S3-layer equivalent of Kafka consumer lag. "
            "Call this when data is in Kafka but not appearing in Postgres."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    {
        "name": "list_minio_objects",
        "description": (
            "List files in the cdc-events MinIO bucket. "
            "Use this to see raw CDC event files that are pending or recent. "
            "Filter by table using prefix (e.g. 'stores/', 'orders/'). "
            "Useful for spotting stuck files or verifying events are landing in S3."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "prefix": {
                    "type": "string",
                    "description": "S3 prefix to filter by table, e.g. 'stores/', 'orders/2024-01-15/'",
                    "default": "",
                },
                "max_keys": {
                    "type": "integer",
                    "description": "Max number of files to list (default 50)",
                    "default": 50,
                },
            },
            "required": [],
        },
    },

    {
        "name": "inspect_minio_event",
        "description": (
            "Download and return the content of a specific CDC event file from MinIO. "
            "Use list_minio_objects first to find a key, then call this to read the raw event. "
            "Useful for debugging malformed events causing loader failures."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {
                    "type": "string",
                    "description": "Full S3 object key, e.g. 'stores/2024-01-15/1705123456_0_42.json'",
                }
            },
            "required": ["key"],
        },
    },

    # ── PostgreSQL ─────────────────────────────────────────────────────────────

    {
        "name": "query_postgres",
        "description": (
            "Run a read-only SQL SELECT query against the PostgreSQL staging database. "
            "Use this to inspect the data that has arrived via CDC — check record counts, "
            "data freshness, specific rows, or any anomalies. "
            "Tables available: stores, products, inventory, orders. "
            "Only SELECT is allowed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {
                    "type": "string",
                    "description": "A read-only SQL SELECT query. No writes or DDL.",
                }
            },
            "required": ["sql"],
        },
    },

    {
        "name": "get_table_summary",
        "description": (
            "Return row counts for all CDC-tracked tables in PostgreSQL "
            "(stores, products, inventory, orders). "
            "Call this for a quick 'is data flowing?' check — zero counts mean "
            "CDC events haven't arrived yet or the consumer is stuck."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    # ── MySQL ──────────────────────────────────────────────────────────────────

    {
        "name": "get_pipeline_lag",
        "description": (
            "Compare row counts between MySQL (source) and PostgreSQL (destination) "
            "for all CDC tables. The difference is the replication lag — "
            "how many rows have been written to MySQL but not yet arrived in Postgres. "
            "A lag of 0 means the pipeline is fully caught up."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    # ── Redpanda ───────────────────────────────────────────────────────────────

    {
        "name": "get_redpanda_health",
        "description": (
            "Return the Redpanda cluster health status: whether the broker is healthy, "
            "any nodes that are down, and leaderless or under-replicated partitions. "
            "Call this when you suspect the message broker itself is the problem."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    {
        "name": "get_kafka_topics",
        "description": (
            "List all Kafka topics in Redpanda and their partition counts. "
            "Use this to verify that Debezium has created CDC topics for the expected tables. "
            "Missing topics (e.g. demo.demo_db.stores) mean the connector hasn't snapshotted that table yet."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    # ── Grafana ────────────────────────────────────────────────────────────────

    {
        "name": "get_grafana_alerts",
        "description": (
            "Return all currently firing or pending Grafana alert rules. "
            "Call this to see if the monitoring stack has already detected pipeline issues "
            "— e.g. 'no stores data in PostgreSQL' or 'high consumer lag'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },

    {
        "name": "query_container_logs",
        "description": (
            "Query logs for a specific Docker container via Loki/Grafana. "
            "Use this to read error messages, stack traces, or startup output from any service. "
            "Most useful containers: 'demo-kafka-connect' (Debezium), 'demo-consumer' (Kafka→Postgres), "
            "'demo-mysql', 'demo-postgres'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "container_name": {
                    "type": "string",
                    "description": "Docker container name",
                    "enum": [
                        "demo-kafka-connect",
                        "demo-consumer",
                        "demo-mysql",
                        "demo-postgres",
                        "demo-redpanda",
                        "demo-grafana",
                        "demo-loki",
                        "demo-prometheus",
                    ],
                },
                "minutes_back": {
                    "type": "integer",
                    "description": "How many minutes of logs to retrieve (default 30)",
                    "default": 30,
                },
                "search_term": {
                    "type": "string",
                    "description": "Optional filter string (e.g. 'ERROR', 'WARN', 'Exception', 'FAILED')",
                    "default": "",
                },
                "max_lines": {
                    "type": "integer",
                    "description": "Max log lines to return (default 100, max 500)",
                    "default": 100,
                },
            },
            "required": ["container_name"],
        },
    },

    {
        "name": "query_prometheus_metric",
        "description": (
            "Run a PromQL query via Grafana/Prometheus and return time-series results. "
            "Use this to check Redpanda throughput, partition counts, or any numeric metric.\n"
            "Example queries:\n"
            "  rate(vectorized_cluster_partition_records_produced_total[1m])  — produce rate\n"
            "  vectorized_kafka_server_partition_count                         — partition count\n"
            "  sum(vectorized_kafka_rpc_received_bytes_total)                  — bytes received"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "promql": {
                    "type": "string",
                    "description": "A valid PromQL expression",
                },
                "minutes_back": {
                    "type": "integer",
                    "description": "Time range in minutes (default 30)",
                    "default": 30,
                },
            },
            "required": ["promql"],
        },
    },
]

# =============================================================================
# DISPATCH  (called by agent/core.py)
# =============================================================================

_DISPATCH = {
    "list_kafka_connectors":   list_kafka_connectors,
    "get_connector_status":    get_connector_status,
    "get_connector_config":    get_connector_config,
    "list_minio_objects":      list_minio_objects,
    "get_minio_pipeline_stats": get_minio_pipeline_stats,
    "inspect_minio_event":     inspect_minio_event,
    "query_postgres":          query_postgres,
    "get_table_summary":       get_table_summary,
    "get_pipeline_lag":        get_pipeline_lag,
    "get_redpanda_health":     get_redpanda_health,
    "get_kafka_topics":        get_kafka_topics,
    "get_grafana_alerts":      get_grafana_alerts,
    "query_container_logs":    query_container_logs,
    "query_prometheus_metric": query_prometheus_metric,
}


def dispatch(tool_name: str, tool_input: dict) -> dict:
    fn = _DISPATCH.get(tool_name)
    if fn is None:
        return {"ok": False, "data": None, "error": f"Unknown tool '{tool_name}'."}
    try:
        return fn(**tool_input)
    except TypeError as e:
        return {"ok": False, "data": None, "error": f"Invalid arguments for '{tool_name}': {e}"}
