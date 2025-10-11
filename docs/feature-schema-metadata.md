Feature: Catalog & Profiling Metadata Service

Motivation
- Precision failures between Oracle NUMBER columns and Spark/Parquet surfaced the gap between database-native metadata and what the ingestion service currently uses.
- Oracle (and other RDBMSs) already maintain rich catalogs with statistics, histograms, freshness indicators, and constraints that can guide ingestion, planning, and governance.
- Capturing and reusing this information is cheap compared with runtime profiling, and unlocks multiple downstream optimizations beyond numeric precision fixes.

Goals
- Pull authoritative schema, statistics, and profiling metadata from source catalogs without performing our own heavy scans.
- Cache metadata locally with TTL-based expiry, and version each snapshot so consumers can diff and audit changes.
- Offer a shared metadata layer usable by ingestion, planners, governance tools, and future observability features.
- Run metadata collection as a decoupled, orchestrator-managed process so ingestion remains functional without it while still benefiting when metadata is present.

Functional Requirements
- Metadata Harvesting: Collect schema field definitions (name, type, precision/scale, nullability, defaults), comments, constraints, and partitioning details.
- Statistics Capture: Ingest row counts, block counts, last analyzed timestamps, min/max values, histograms, distinct counts, and stale-flag indicators maintained by the database.
- Multi-Source Support: Start with Oracle (via `ALL_TAB_COLUMNS`, `ALL_TAB_STATS_HISTORY`, `ALL_TAB_STATISTICS`, `ALL_COL_STATISTICS`, `ALL_TAB_COMMENTS`), but design interfaces so MSSQL, Snowflake, etc. can plug in later.
- TTL & Refresh: Support configurable expiry (e.g., per source or per table). When metadata is expired or missing, refresh during the next ingestion cycle or via an explicit admin trigger.
- Versioning: Persist each metadata snapshot with monotonically increasing version IDs and created-at timestamps. Provide diff utilities for consumers.
- Access Layer: Expose a simple API/SDK for services to fetch the latest or historical metadata, with optional filters (table, schema, tag).

Architecture Overview
- Collector: Dedicated process/job invoked by the orchestrator; reuses existing endpoint connectors/execution tooling to query catalog views, normalize results, and write to storage. Endpoints advertise metadata capability so only supporting connectors (Oracle today) are invoked.
- Orchestrator ➜ Service ➜ Adapter chain: Runtime orchestrator builds metadata jobs (artifact target + endpoint) and hands them to the metadata service, which performs cache checks, invokes source adapters, and coordinates normalization.
- Orchestrator Integration: Scheduler decides when to refresh metadata (on demand, cron, or pre-ingestion hook) while keeping ingestion independent if metadata is missing or stale.
- Storage: File-based (JSON/Parquet) or lightweight embedded DB (SQLite) under `cache/catalog/{source}/{schema}/{table}/{version}.json`. Consider pluggable backends for shared storage (S3, GCS) later.
- Cache Manager: Evaluates TTL, orchestrates refreshes, and maintains an index (`catalog_index.json`) mapping tables to latest version and metadata summary for quick lookup.
- Client Libraries: Utilities inside `salam_ingest` to read cached metadata, detect drift, and feed planners or ingestion pipelines. Future work exposes the same artifacts over gRPC while keeping file-path access for lightweight consumers.
- Observability: Structured logs and metrics (count of refreshed tables, TTL misses, diff summaries) so operators trust the system.

Interaction Interfaces
- `MetadataContext` captures execution context (source, job/run identifiers, namespace) and travels with every metadata interaction so audit trails remain intact.
- Producers implement the `MetadataProducer` protocol (`capabilities`, `supports`, `produce`) and return one or more `MetadataRecord` envelopes describing the `kind` of metadata they emit (schema, stats, lineage, etc.).
- Produced records are written through a `MetadataRepository`, which abstracts persistence via `store`, `bulk_store`, `latest`, `history`, and ad-hoc `query` capabilities.
- Optional `MetadataTransformer` hooks can normalize or enrich records before storage/serving when additional business rules are needed.
- Consumers (planners, query builders, governance tools) conform to the `MetadataConsumer` protocol and express `requirements` before `consume` is invoked with the subset of records they need.
- `MetadataRequest` is the hand-off between orchestrator/service layers and producers, combining the target artifact, table configuration, and context flags (e.g., explicit refresh).
- `MetadataQuery` provides a structured way for consumers to look up metadata without relying on repository internals; it supports filtering by target, kind, history depth, and arbitrary filters.

Consumer Access Patterns
- Metadata consumer SDK exposes `MetadataRepository` implementations (JSON cache today, pluggable DB later) along with scenario-specific helpers.
- `JsonFileMetadataRepository` wraps the cache manager index for read-only lookup (`latest`, `history`, `query`). Multi-tenant deployments can swap in an object-store or SQL-backed repository without touching consumers.
- Consumers pull `MetadataRecord` envelopes and feed them into domain-specific evaluators such as `PrecisionGuardrailEvaluator`, `PartitionSizingAdvisor`, or `FreshnessMonitor`.
- Evaluators return structured results (`PrecisionGuardrailResult` envelopes) that can be emitted as events, logged, or short-circuit ingestion tasks.
- Result statuses are `ok`, `adjusted`, or `fatal`; only the last one halts ingestion, while `adjusted` indicates casts or scale clipping were applied automatically.
- Consumer interfaces stay symmetrical with producers: they accept `MetadataContext`, express requirements, and operate on `MetadataRecord` collections without caring about the underlying storage.
- Table configs can opt into guardrails via:

```json
{
  "metadata_guardrails": {
    "precision": {
      "enabled": true,
      "max_precision": 38,
      "violation_action": "downcast",
      "open_precision_action": "downcast",
      "fallback_precision": 38,
      "fallback_scale": 6
    }
  }
}
```

`violation_action` controls how declared precision exceeding the limit is handled (`downcast` or `fail`), and `open_precision_action` governs columns without explicit precision. The ingestion endpoints fetch metadata through the repository and apply casts before submitting queries to Spark when precision rules are enabled. Violations marked as `fail` halt ingestion; `downcast` rewrites the projection so the sink receives values within supported bounds.

Operational Model
- Metadata collection runs as its own pipeline stage; orchestrator coordinates execution windows and retries without blocking ingestion SLAs.
- Ingestion, transformation, or planner jobs treat metadata as an optional dependency: they consult cached versions when present and fall back to existing logic otherwise.
- Shared artifacts (schemas, statistics, indexes) live in conventional paths so current tooling can read them immediately; gRPC and other remote interfaces will layer on top without breaking file-based access.
- Feature flags or per-table configuration enable teams to opt in gradually and experiment before enforcing metadata-driven behavior.
- Versioned snapshots allow multiple consumers to operate on consistent metadata states even when new collections are in progress.
- Endpoint-specific probes (e.g., Oracle environment queries) are modular and configurable so different database versions or deployments can tailor metadata without code forks.
- Normalization layer maps vendor payloads into a neutral schema (`CatalogSnapshot`, `SchemaField`, `DatasetStatistics`, etc.) so downstream consumers never see dialect-specific quirks; each endpoint ships its own normalizer implementation.

Metadata Schema (initial draft)
- `source`: JDBC identifier (oracle, mssql, etc.).
- `schema`, `table`, optional `subobject` (e.g., materialized view).
- `version`, `created_at`, `expires_at`, `refreshed_by`, `catalog_snapshot_id` (database timestamp when stats collected).
- `schema_fields`: array of field objects including `name`, `data_type`, `precision`, `scale`, `length`, `nullable`, `default`, `comments`, `constraints`.
- `statistics`: `record_count`, `storage_blocks`, `average_record_size`, `stale`, `last_profiled_at`, distinct counts, histograms (support Oracle frequency, height-balanced).
- `partitioning`: keys, subpartitioning, partition row counts if available.
- `lineage`: foreign keys, referenced tables, dependent views.
- `quality_flags`: e.g., `requires_precision_cap`, `high_null_ratio`, `skewed_distribution`.
- `data_source`: normalized description of the upstream system (id, name, environment, version, tags).
- `dataset`: logical dataset descriptor (business name, physical name, location/path, tags, extended properties).
- `processes`: ETL/ELT job metadata (id, system, schedule, declared inputs/outputs) populated when pipeline services integrate.
- `ownerships`: ownership/contact assignments across data governance, security, or product owners.

See `docs/metadata_oracle_sample.json` for a concrete Oracle snapshot example.

Entities & Responsibilities

| Entity             | Typical Source/System               | Filled By                      | Notes                                      |
|--------------------|-------------------------------------|--------------------------------|--------------------------------------------|
| Data Source/System | Oracle, Snowflake, S3, Kafka        | Platform/connection prober     | Provides context, environment, version     |
| Dataset            | Tables, Views, Streams, Files       | Source metadata subsystem      | Available from day one                      |
| Schema Field       | Table/View column definitions       | Source metadata subsystem      | Captured during schema inspection          |
| Data Job/Process   | ETL/ELT orchestrators, batch jobs   | Pipeline services, schedulers  | Populated when ingestion/transform integrates |
| Ownership          | Access controls, governance tools   | Pipeline service, access layer | Emerges with process/job and governance data |

Caching & Expiry Strategy
- TTL defaults (e.g., 24h) are configurable per source; tables flagged with `stale_stats = 'YES'` trigger immediate refresh.
- Maintain `catalog_index.json` summarizing last fetch time and TTL status; ingestion checks index before running data loads.
- Allow manual invalidation via CLI command `salam catalog invalidate --table schema.table`.
- Use optimistic concurrency: refresh writes new version and updates index atomically to avoid race conditions.

Versioning & Diffing
- Version ID computed as `yyyymmddHHMMSS` or an incrementing counter persisted in index.
- Changes tracked by diffing column arrays and statistics. Provide CLI `salam catalog diff --table schema.table --from v1 --to v2`.
- Store diff summaries (e.g., columns added/removed, min/max shifts) for audit trails and alerts.

Integration Use Cases
- Precision Guardrails: `PrecisionGuardrailEvaluator` queries metadata via the repository, scans numeric columns for precision > 38 (configurable) or undefined precision, and returns structured issues; ingestion uses this to apply casts (when bounded) or halt with actionable messages.
- Query Planning: Batch planners use row counts, partitions, and histograms to size Spark partitions, choose broadcast vs shuffle joins, and skip redundant `count(*)`.
- Data Freshness Monitoring: Compare `last_analyzed` vs ingestion time to detect stale source statistics; alert if thresholds exceeded.
- Data Quality Rules: Tag high-null or highly skewed columns for targeted validation or sampling. Flag default values meaning missing data.
- Change Data Capture: When version diff indicates structural change (column added, type altered), trigger schema migration workflows and update downstream contracts.
- Access Governance: Expose column comments and sensitivity tags (if stored in comments metadata) to security tooling.
- Future Extensions: Feed BI semantic layers, auto-generate documentation, or integrate with lineage systems.
- Transformations & Agents: Use catalog statistics to drive transformation pipelines, agentic query routing, and adaptive data products without manual profiling.

Implementation Plan (incremental)
- Milestone 1: Build Oracle collector retrieving schema + basic stats (`row_count`, `last_analyzed`, precision/scale). Persist JSON snapshots with versioning and TTL-based refresh.
- Milestone 2: Extend storage index, add CLI/SDK to fetch metadata, integrate ingestion pipeline for precision handling using the new metadata.
- Milestone 3: Add additional stats (histograms, distinct counts), expose diff and invalidation CLI, wire metrics/logging.
- Milestone 4: Generalize collector interface, onboard MSSQL via `INFORMATION_SCHEMA` and `sys.dm_db_stats_properties`.

Risks & Mitigations
- Catalog Query Cost: Batch multiple tables per query when possible and respect database rate limits. Cache connection pools.
- Stale Metadata: TTL plus stale-stat triggers ensure freshness; surface TTL status to consumers so they can decide to block ingestion.
- Schema Drift Surprise: Automated diff notifications (Slack/email) alert teams before ingestion fails. Provide dry-run mode to evaluate impact.
- Storage Growth: Periodic pruning policy (keep last N versions) configurable per source while allowing archival to blob storage.

Test Scenarios
- Collector unit tests using fixtures for Oracle catalog views to ensure parsing covers precision/scale, histograms, and nullability.
- TTL logic tests verifying expired entries trigger refresh and fresh entries skip network calls.
- Integration test simulating ingestion reading metadata to build precision-safe SELECT statements.
- Regression test ensuring diff CLI highlights column type change and histograms updates.

Open Questions
- Where should long-term catalog history live (local disk, object store, metadata DB)?
- Do we need encryption or redaction for sensitive comments or column names?
- How will we authenticate service access to the metadata cache in shared environments?
- Should we store execution lineage (e.g., ingestion job ID) alongside catalog versions for audit?

Next Steps
- Validate Oracle SQL queries for all required stats and confirm privileges needed.
- Define configuration surface (`catalog.ttl_hours`, `catalog.storage_path`, `catalog.refresh_on_startup`) and integration points in `salam_ingest`.
- Prototype collector output for a representative high-precision table and review with ingestion and planning teams.
