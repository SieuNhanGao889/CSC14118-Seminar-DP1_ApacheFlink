# Demo UI companion

Open `dashboard.html` directly in a browser before recording or presenting the demo.

Recommended screen order:

1. Show the overview slide: `Transaction Producer -> Kafka -> Flink Job -> Realtime Output`.
2. Open Flink Web UI at `http://localhost:8081` and point to the job graph.
3. Open `ui/dashboard.html` and show raw Kafka messages, Flink parse/filter, keyBy routing to two subtasks, window totals, and alerts.
4. Return to Flink Web UI and show completed checkpoints.

Suggested speaking line:

> Kafka is only the ingestion layer in this demo. It stores and delivers the raw transaction JSON. The transformation happens inside Flink: parse/filter, keyBy routing to parallel subtasks, window aggregation, stateful processing, realtime output, and checkpointing.

This dashboard is a visual companion, not a Kafka consumer. It is intentionally lightweight so the seminar does not spend time on extra setup. Use the real Flink Web UI to prove the job graph, real parallel subtasks, and checkpoint behavior.
