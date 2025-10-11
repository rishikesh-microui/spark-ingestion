Feature: Transparent Logging & Progress Monitoring
==================================================

Motivation
----------
- Operators lack a single, consistent view of ingestion progress, retries, and bottlenecks across sources/sinks.
- Current logging is ad-hoc (stdout, state events) which complicates investigations, automated alerting, or replay.
- Future guardrails (metadata precision, slicing, staging) need structured telemetry so teams can trust autonomous decisions.

Goals
-----
- Provide uniform logging primitives that describe ingestion lifecycle (plan -> pull -> stage -> commit) for every table and slice.
- Surface real-time progress, lag, throughput, and anomalies via machine-consumable events.
- Allow teams to opt into central observability stacks (Splunk, ELK, OpenTelemetry) without code changes.
- Ensure logs capture the `what`, `why`, and `next steps` for troubleshooting (e.g., precision guardrail downcast, slice skip reasons).
- Drive all telemetry through a single event bus so new sinks (structured log, notifier, OTEL) can subscribe without touching ingestion logic.

Non-Goals
---------
- Replacing existing state persistence (events tables) with a new store.
- Building a UI/dashboard; the framework emits structured telemetry, consumers decide how to visualize.
- Alerting logic in core runtime (will document integration points for alerting tools).

Functional Requirements
-----------------------
- **Structured Events**: Every major lifecycle transition emits a JSON payload (`stage`, `status`, `table`, `slice`, metadata like `rows`, `wm`, guardrail actions).
- **Progress Topic**: A dedicated channel (`progress` outbox, state, or optionally Kafka) receives cumulative counters (rows, duration, retries, lag).
- **Context Propagation**: All events include run identifiers (`run_id`, `job_name`, `pool`), table identifiers, slice keys, and optional metadata (guardrail config, catalog version).
- **Error Envelope**: When failures occur, log the exception type, sanitized message, stack trace hash, and recommended next actions (retry, fix configuration).
- **Precision Guardrail Audit**: When guardrails downcast or halt ingestion, log original precision, fallback settings, affected columns.
- **Metadata Collection Events**: Collectors log start/end, source, target, row counts, TTL decisions.
- **Configurable verbosity**: Support `INFO`, `DEBUG`, `TRACE` tiers, with default being informative but not verbose.
- **Progress Snapshots**: Periodic heartbeat summarizing total tables, done, inflight, failed, lag (mirror Heartbeat class but with structured payload).

Architecture Overview
---------------------
1. **Event Bus**: Reuse/extend the central emitter to publish structured events (`EventCategory.INGEST`, `EventType.INGEST_TABLE_SUCCESS`, etc.).
   - Legacy state updates continue through the same bus so behaviour stays backwards compatible.
2. **Subscribers**: Structured logger, notifier bridge, state writer, and future sinks all subscribe to the bus and act independently.
3. **Config Surface**: Toggle subscribers via config (structured log on/off, outbox mirroring, notifier verbosity).  
   ```json
   "runtime": {
     "logging": {
       "emit_structured": true,
       "event_sink": "outbox"
     }
   }
   ```
2. **Progress Bus**: Outbox (filesystem-agnostic) mirrors every event; additional subscribers can push to Kafka or OTEL.
3. **Event Schema Registry**: Document and version event types (e.g., `ingest.table.start`, `ingest.table.success`, `ingest.slice.progress`, `guardrail.precision`).
4. **State Integration**: Current state tables continue to store success/failure + metadata; logging is complementary. Optionally, state writer can consume progress events to keep DB consistent.
5. **Config Surface**:  
   ```json
   "runtime": {
     "logging": {
       "level": "INFO",
       "emit_structured": true,
       "event_sink": "outbox",   // or "stdout", future "kafka"
       "heartbeat_seconds": 120,
       "stacktrace": "hash"      // "full" to emit stack, "none" to suppress
     }
   }
   ```
6. **Scenario Branches**:
   - Guardrail downcast emits `guardrail.precision.adjusted`.
   - Metadata collection success emits `metadata.snapshot.success`.
   - Slice skip (due to staging markers) logs `ingest.slice.skip`.
   - Retry attempts log `ingest.table.retry`.
   - Final summary logs `ingest.run.summary`.

Event Examples
--------------
```json
{
  "ts": "2024-05-30T10:15:32.123+00:00",
  "level": "INFO",
  "run_id": "20240530-101500",
  "job": "brm_dl",
  "event": "ingest.table.start",
  "schema": "PIN01",
  "table": "ACCOUNT_T",
  "mode": "scd1",
  "load_date": "2024-05-30"
}
```

```json
{
  "ts": "2024-05-30T10:16:02.891+00:00",
  "level": "WARN",
  "run_id": "20240530-101500",
  "job": "brm_dl",
  "event": "guardrail.precision.adjusted",
  "schema": "PIN01",
  "table": "ACCOUNT_T",
  "columns": [
    {"name": "BALANCE", "original_precision": null, "fallback_precision": 38, "fallback_scale": 6, "action": "downcast"}
  ],
  "config": {"max_precision": 38, "action": "downcast"}
}
```

```json
{
  "ts": "2024-05-30T10:20:32.477+00:00",
  "level": "ERROR",
  "run_id": "20240530-101500",
  "job": "brm_dl",
  "event": "ingest.table.failed",
  "schema": "PIN01",
  "table": "ACCOUNT_T",
  "slice": {"lower": "1717040400", "upper": "1717062000"},
  "error_type": "AnalysisException",
  "error_hash": "0f2c93e7",
  "message": "column XYZ not found",
  "next_steps": "verify column mapping / regenerate metadata snapshot"
}
```

Operational Model
-----------------
- Logging defaults to structured JSON on stdout and HDFS outbox; connectors/installations can redirect to file, Kafka, or OTEL exporters.
- Heartbeat continues to run but now publishes structured payloads to both stdout and event sink.
- Metadata guardrails or precision adjustments emit events before ingestion writes to sinks.
- Operators can tail HDFS outbox or use existing monitoring DB to visualize progress without modifying ingestion loops.
- Config toggles allow quick triage: set `logging.level=DEBUG` for additional context, or disable structured events for legacy pipelines.
- Failure flows ensure event emission happens before raising exceptions so that control planes can react.

Security & Compliance
---------------------
- Sensitive information (passwords, tokens, PII) never logged; guardrail messages use column names but not values.
- Optionally hash stack traces or omit them; default is to emit a deterministic hash and short message.
- When writing to shared storage (HDFS/S3), restrict permissions to ingestion operators.

Future Extensions
-----------------
- Add OpenTelemetry spans for each ingestion stage for distributed tracing.
- Kafka sink for real-time dashboards.
- Alerting rules based on guardrail events (e.g., fail if repeated downcasts).
- Auto-scaling decisions using throughput metrics and slice completion times.

Telemetry Utilities
-------------------
### 1. Realtime Progress Viewer CLI
- Standalone python entrypoint (`salam progress watch --run <run_id>`) that tails structured events from stdout/outbox.
- Maintains an in-memory table keyed by `schema.table` with columns:
  `status`, `mode`, `load_date`, `rows_written`, `slices_done/total`, `last_event_ts`, `lag_minutes`.
- Refresh interval configurable (default 5s); redraws table using curses/ANSI for terminal-friendly display.
- Footer row aggregates totals:
  - Tables completed / total
  - Rows processed
  - Average throughput (rows/min)
  - Oldest outstanding slice age
- Supports filters (`--schema`, `--mode`, `--status`) and JSON export (`--output json`) for automation.
- Automatically detects guardrail events and surfaces them inline (e.g., column flagged) so operators see adjustments without digging into logs.

### 2. Failure Isolator & Aggregator
- CLI utility (`salam progress failures --run <run_id>`) that scans logged events (outbox, stdout capture, or ingest state tables).
- Groups failures by `(error_type, message_hash)` and prints summary:
  ```
  Error Type         Count  First Seen           Tables
  ---------------    -----  -------------------  ----------------------------------
  AnalysisException  3      2024-05-30T10:13:02  PIN01.ACCOUNT_T (slice 3), ...
  GuardrailFatal     1      2024-05-30T10:18:45  PIN01.PRODUCT_T (precision fail)
  ```
- Can emit detailed JSON or CSV for higher-level incident management systems (`--format csv/json`).
- Optional `--watch` mode keeps the summary updated in near real-time so teams can triage during live runs.
- Supports `--show-stack` to fetch stack traces for a specific hash, respecting logging configuration (full/hashed).
- Integrates with exit codes: returns `0` if no failures, `2` if guardrail violations, `1` for general errors—useful for CI/CD pipelines.
