Feature: Oracle High-Precision Numerics in Parquet

Context
- Oracle source tables include NUMBER columns whose precision can be defined up to 38, but the database allows values that implicitly exceed this cap when scale is very small or when precision is left unconstrained.
- Spark dataframes target Parquet outputs, which inherit the Apache Arrow/Spark hard limit of precision 38 for fixed-point decimals.
- Our ingestion endpoints (Oracle ➜ Spark ➜ Parquet) currently rely on the metadata returned by `JdbcUtils` without enforcing custom precision or scale adjustments.

Problem Statement
- When an Oracle NUMBER column stores values with effective precision > 38, Spark throws `java.lang.ArithmeticException: Decimal precision 39 exceeds max precision 38`.
- The job aborts before writing to Parquet, causing downstream reload operations to fail and leaving dependent pipelines without refreshed data.
- Operational teams must manually coerce or drop the problematic columns, which is error-prone and inconsistent.

Desired Behavior
- Automatically cap or normalize Oracle NUMBER precision/scale before Spark materializes the dataset.
- Allow per-table or global configuration so teams can opt into custom precision handling without code changes.
- Preserve numeric fidelity whenever possible while still producing Parquet files acceptable to Spark.

Solution Approach
- Schema Discovery: Enhance `salam_ingest/endpoints/jdbc_oracle.py` to fetch Oracle column metadata (precision, scale) via `ALL_TAB_COLUMNS` or a similar view before data extraction.
- Schema Cache: Persist the discovered schema (e.g., JSON under `cache/schemas/{table}.json`) so repeat loads avoid redundant queries and provide traceability.
- Precision Policy Configuration: Introduce a configuration object (YAML/JSON/env) with keys such as `numeric_precision_cap` and `numeric_scale_strategy` supporting modes like `truncate`, `round`, or `pad`.
- Query Generation: Update the Oracle endpoint to build SELECT statements that cast affected NUMBER columns, e.g., `CAST(col AS NUMBER(38, NVL(scale,0)))`, honoring the configured policy.
- Spark Ingestion: Ensure the JDBC options include `oracle.jdbc.mapDateToTimestamp=false` where needed and that resulting DataFrame schemas use `DecimalType(38, scale)` so parquet writers stay within limits.

Edge Cases
- Columns with NULL scale but wide numeric range (pure integers) should downcast to 38 with scale 0 safely.
- Columns participating in downstream aggregations must not lose more than one decimal place without explicit opt-in.
- Very large numeric values that cannot fit into precision 38 even after scale adjustments should be logged and optionally redirected to a quarantine dataset.

Assumptions
- We control both ingestion configs and deployment, so new configuration files can be shipped alongside endpoint code.
- All affected pipelines run Spark 3.x, keeping the DecimalType max precision at 38.
- Oracle JDBC driver version in use supports metadata queries for precision/scale.

Risks & Mitigations
- Silent data truncation: Provide audit logs summarizing columns adjusted, along with counts of rows impacted.
- Performance overhead: Cache schemas and reuse prepared statements to avoid repeated metadata lookups.
- Configuration drift: Validate configuration on startup and fail fast with actionable error messages.

Test Scenarios
- Unit tests creating mock JDBC metadata to ensure cast expressions honor the configuration.
- Integration test ingesting a table containing `NUMBER(40,0)` values, verifying Spark succeeds and output parquet matches expectations.
- Regression test for unaffected tables to confirm no change in schema or data fidelity.

Open Questions / Next Steps
- Confirm where to persist schema cache (local disk vs shared storage) and retention policy.
- Decide whether we expose precision policy via CLI flag, config file, or environment variable hierarchy.
- Evaluate whether similar precision handling is required for other JDBC endpoints (e.g., MSSQL DECIMAL).
