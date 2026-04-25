# CDC Pipeline Debug Agent

An AI-powered debug agent for CDC (Change Data Capture) pipelines. Ask it a question in plain English — it queries Kafka, Postgres, MinIO, Grafana, and container logs on its own, correlates everything, and tells you exactly what's wrong and how to fix it.

Built with the Claude API (custom tool use — no framework, no MCP).

---

## What it does

When a CDC pipeline breaks, you normally jump across 4-5 tools before you even have a hypothesis. This agent does that loop for you.

You ask: *"Is the pipeline healthy?"*

It calls the right tools, connects the dots, and responds with a structured diagnosis — root cause, secondary issues, and recommended fix steps.

---

## Pipeline Architecture

```
MySQL (source)
   └─► Debezium (CDC)
          └─► Redpanda / Kafka
                 └─► Consumer (Python)
                        └─► MinIO / S3 (staging)
                               └─► Loader (Python)
                                      └─► PostgreSQL (destination)

Monitoring: Prometheus + Loki + Grafana (alerts, metrics, logs)
```

---

## Project Structure

```
cdc-pipeline-demo/
├── main.py                        # Entry point
├── monitor.py                     # Live row/file count monitor
├── requirements.txt
├── .env.example                   # Environment variable template
├── setup_agent.sh                 # One-step setup script
│
├── agent/
│   └── core.py                    # Claude API agentic loop
│
├── interfaces/
│   └── cli.py                     # Interactive terminal UI
│
├── projects/
│   └── local/
│       ├── registry.py            # Tool definitions (14 tools)
│       ├── tools.py               # Tool implementations
│       └── knowledge.py           # System prompt + infrastructure config
│
└── demo/                          # Docker infrastructure
    ├── docker-compose.yml         # Full stack (11 services)
    ├── setup.sh                   # Registers Debezium connector
    ├── connector/
    │   └── mysql-source.json      # Debezium connector config
    ├── mysql/
    │   ├── init.sql               # Source DB schema
    │   └── datagen.py             # Test data generator
    ├── consumer/                  # Kafka → MinIO service
    ├── loader/                    # MinIO → PostgreSQL service
    ├── grafana/                   # Dashboards + alert provisioning
    ├── loki/                      # Log aggregation config
    └── prometheus/                # Metrics scraping config
```

---

## Setup

### Prerequisites

- Python 3.9+
- Docker + Docker Compose
- Anthropic API key

### 1. Install dependencies

```bash
bash setup_agent.sh
```

This creates a virtual environment, installs dependencies, and copies `.env.example` → `.env`.

### 2. Set your API key

```bash
# .env
ANTHROPIC_API_KEY=sk-ant-...
```

### 3. Start the Docker stack

```bash
cd demo
docker compose up -d
```

Wait ~30 seconds for all services to be healthy.

### 4. Register the Debezium connector

```bash
bash demo/setup.sh
```

### 5. Run the agent

```bash
source venv/bin/activate
python main.py
```

---

## Usage

```
CDC Pipeline Debug Agent  env: local
Model: claude-sonnet-4-6  |  Read-only

Ask about data flow, connector health, lag, logs, alerts.
Commands: /help /reset /quit
```

**Example questions:**

```
Is the pipeline healthy?
Why is data not showing up in Postgres?
What does the Debezium connector status look like?
Are there any errors in the consumer logs?
How many orders have been replicated?
```

**Commands:**

| Command    | Description                        |
|------------|------------------------------------|
| `/help`    | Show available commands and tools  |
| `/reset`   | Clear conversation history         |
| `/history` | Show conversation so far           |
| `/tools`   | List all available tools           |
| `/quit`    | Exit                               |

---

## Agent Tools (14 total)

| Category         | Tool                        | Description                                      |
|------------------|-----------------------------|--------------------------------------------------|
| **Kafka Connect**| `list_kafka_connectors`     | List all connectors and their state              |
|                  | `get_connector_status`      | Detailed status + per-task errors                |
|                  | `get_connector_config`      | Return connector configuration                   |
| **MinIO / S3**   | `get_minio_pipeline_stats`  | Pending vs processed file counts                 |
|                  | `list_minio_objects`        | List CDC event files in staging                  |
|                  | `inspect_minio_event`       | Download and display a raw CDC event             |
| **PostgreSQL**   | `query_postgres`            | Run read-only SELECT queries on staging_db       |
|                  | `get_table_summary`         | Row counts for all CDC tables                    |
| **MySQL**        | `get_pipeline_lag`          | Compare source vs destination row counts         |
| **Redpanda**     | `get_redpanda_health`       | Broker health, leaderless partitions             |
|                  | `get_kafka_topics`          | List topics and partition counts                 |
| **Observability**| `get_grafana_alerts`        | Currently firing Grafana alerts                  |
|                  | `query_container_logs`      | Search container logs via Loki                   |
|                  | `query_prometheus_metric`   | Run a PromQL query                               |

The agent is **read-only** — it never writes to any system.

---

## How the Agent Works

The agent is a Python loop built on the Claude API with custom tool use.

1. You send a message
2. Claude decides which tools to call
3. Your Python functions execute (hitting real local endpoints)
4. Results are fed back to Claude
5. Claude reasons over everything and either calls more tools or returns a final answer

No external framework. Just the Anthropic SDK + custom functions.

```python
# Simplified view of the loop (agent/core.py)
while turns < MAX_TURNS:
    response = claude.messages.create(tools=TOOLS, messages=history)

    if response.stop_reason == "end_turn":
        return final_answer

    if response.stop_reason == "tool_use":
        results = [execute(tool) for tool in response.tool_calls]
        history.append(results)
        # loop continues
```

---

## Docker Services

| Service          | Image                      | Port(s)        | Purpose                        |
|------------------|----------------------------|----------------|--------------------------------|
| `mysql`          | mysql:8.0                  | 3306           | Source database (binlog on)    |
| `redpanda`       | redpanda:v23.3.11          | 9092, 9644     | Kafka-compatible broker        |
| `kafka-connect`  | debezium/connect:2.4       | 8083           | CDC engine                     |
| `minio`          | minio/minio                | 9000, 9001     | S3-compatible staging layer    |
| `consumer`       | custom                     | —              | Kafka → MinIO writer           |
| `loader`         | custom                     | —              | MinIO → PostgreSQL loader      |
| `postgres`       | postgres:15                | 5433           | Destination data warehouse     |
| `prometheus`     | prom/prometheus            | 9090           | Metrics collection             |
| `grafana`        | grafana/grafana:10.2.3     | 3000           | Dashboards and alerts          |
| `loki`           | grafana/loki:2.9.3         | 3100           | Log aggregation                |
| `promtail`       | grafana/promtail:2.9.3     | —              | Log shipper                    |

---

## Environment Variables

Copy `.env.example` to `.env` and fill in your values.

| Variable              | Description                        | Default             |
|-----------------------|------------------------------------|---------------------|
| `ANTHROPIC_API_KEY`   | Claude API key (required)          | —                   |
| `POSTGRES_HOST`       | PostgreSQL host                    | localhost           |
| `POSTGRES_PORT`       | PostgreSQL port                    | 5433                |
| `MYSQL_HOST`          | MySQL host                         | localhost           |
| `MYSQL_PORT`          | MySQL port                         | 3306                |
| `MINIO_ENDPOINT`      | MinIO endpoint                     | http://localhost:9000 |
| `MINIO_BUCKET`        | Staging bucket name                | cdc-events          |
| `KAFKA_CONNECT_URL`   | Kafka Connect REST API             | http://localhost:8083 |
| `GRAFANA_BASE_URL`    | Grafana URL                        | http://localhost:3000 |
| `LOKI_BASE_URL`       | Loki URL                           | http://localhost:3100 |

---

## Grafana

Open [http://localhost:3000](http://localhost:3000) (admin / admin) to see:

- Pipeline dashboard (row counts, lag, file counts)
- Pre-configured alerts for common failure scenarios
- Logs via Loki integration
