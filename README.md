# CDC Pipeline Debug Agent  
### AI-Powered Root Cause Analysis for Distributed Data Pipelines

---

## Problem

Debugging CDC (Change Data Capture) pipelines in production is inherently complex:

- Failures span **multiple systems** (Kafka, DBs, storage, logs)
- Engineers must manually correlate signals across **4–6 tools**
- Root cause identification is slow and error-prone

Typical debugging workflow:
- Check connector status  
- Inspect Kafka lag  
- Query destination DB  
- Search logs  
- Check alerts  

This process takes **20–30 minutes per incident** and increases MTTR significantly.

---

## Solution

This project introduces an **AI-powered debugging agent** that:

- Automatically queries multiple systems  
- Correlates signals across infrastructure  
- Identifies root causes  
- Suggests actionable fixes  

You simply ask:

> “Is the pipeline healthy?”

And the agent:
- Calls relevant tools  
- Analyzes results  
- Returns a structured diagnosis  

---

## Real-World Impact

Inspired by production systems where similar architecture enabled:

- **~10x faster debugging** (20–30 min → 2–3 min)
- **MTTR reduced to <5 minutes**
- **95%+ root cause accuracy**
- Significant reduction in on-call fatigue and escalations  

---

## Pipeline Architecture
```
MySQL → Debezium → Kafka → Consumer → MinIO → Loader → PostgreSQL
```

Monitoring:
- Prometheus (metrics)  
- Loki (logs)  
- Grafana (alerts + dashboards)  

---

## How the Agent Works

The agent uses an **iterative reasoning loop**:

1. User asks a question  
2. LLM decides which tools to call  
3. Tools fetch real system data  
4. Results are fed back  
5. Agent continues reasoning or returns diagnosis  

```python
while turns < MAX_TURNS:
    response = claude.messages.create(tools=TOOLS, messages=history)

    if response.stop_reason == "tool_use":
        results = [execute(tool) for tool in response.tool_calls]
        history.append(results)
```

No framework. Pure function-based tool orchestration.

---

## Tooling System
The agent integrates 14 tools across systems:

Infrastructure Coverage:
- Kafka / Redpanda → topic health, partitions
- Kafka Connect → connector status, errors
- PostgreSQL → destination validation
- MySQL → source consistency
- MinIO → staging validation
- Grafana / Prometheus → alerts & metrics
- Loki → log analysis

Provides end-to-end observability


---

## Why AI (Not Rule-Based)?

Traditional debugging systems rely on static rules:
- Hard to maintain
- Break with new failure modes
- Cannot reason across systems

This agent uses LLM reasoning to:
- Dynamically choose tools
- Correlate signals across systems
- Adapt to new failure patterns

---

## Failure Handling

The agent can diagnose:
- Connector failures
- Kafka lag / partition issues
- Data not reaching destination
- Schema mismatches
- Consumer / loader failures
- Silent data inconsistencies

---

### Scalability

Tool-based architecture allows easy extension
Works across multiple environments
Supports complex distributed pipelines
Stateless design enables horizontal scaling

---

## Trade-offs

Trade-off
Impact
LLM dependency
Requires API calls
Slight latency
Tool + reasoning overhead
Requires good prompts
Quality depends on system context

Gains:
- Faster debugging
- Reduced human effort
- Better cross-system reasoning

---

## Tech Stack

- Python (agent loop)
- Claude API (LLM reasoning)
- Kafka / Redpanda
- Debezium (CDC)
- PostgreSQL / MySQL
- MinIO (S3 staging)
- Prometheus + Loki + Grafana

---

## Why This Matters

This project demonstrates a shift from:
Manual debugging → Automated system reasoning

It shows how AI can:
- Reduce MTTR
- Improve reliability
- Augment engineering workflows

---

## Summary

An AI-powered system that:
- Automates CDC pipeline debugging
- Correlates signals across distributed systems
- Reduces debugging time dramatically
- Enables faster incident resolution
