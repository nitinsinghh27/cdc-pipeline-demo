#!/usr/bin/env bash
# =============================================================================
# setup.sh — registers the Debezium MySQL connector after the stack is healthy
#
# Usage:  bash setup.sh
# =============================================================================
set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_FILE="connector/mysql-source.json"

echo "Waiting for Kafka Connect to be ready..."
for i in $(seq 1 30); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo "  Kafka Connect is up."
        break
    fi
    echo "  Attempt ${i}/30 — not ready yet, sleeping 5 s..."
    sleep 5
done

echo ""
echo "Registering MySQL source connector..."
RESPONSE=$(curl -s -o /tmp/connect_response.json -w "%{http_code}" \
    -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    -d @"${CONNECTOR_FILE}")

if [[ "${RESPONSE}" == "201" || "${RESPONSE}" == "409" ]]; then
    [[ "${RESPONSE}" == "409" ]] && echo "  (connector already exists — skipping)"
    echo "  Done."
else
    echo "  ERROR: HTTP ${RESPONSE}"
    cat /tmp/connect_response.json
    exit 1
fi

echo ""
echo "Checking connector status (waiting 5 s for it to start)..."
sleep 5
curl -s "${CONNECT_URL}/connectors/mysql-source/status" | python3 -m json.tool

echo ""
echo "============================================================"
echo "  Stack is ready!"
echo "  MySQL:           localhost:3306  (user: app_user / app_pass)"
echo "  Redpanda:        localhost:9092  (Kafka-compatible broker)"
echo "  Kafka Connect:   localhost:8083  (Debezium)"
echo "  PostgreSQL:      localhost:5432  (db: staging_db / postgres / postgres_pass)"
echo "  Prometheus:      http://localhost:9090"
echo "  Grafana:         http://localhost:3000  (admin / admin)"
echo "  Loki:            http://localhost:3100"
echo "============================================================"
echo ""
echo "  To run the AI agent:"
echo "    cd ../"
echo "    python main.py --env local"
echo ""
