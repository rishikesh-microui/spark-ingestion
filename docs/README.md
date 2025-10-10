# Spark Ingestion Framework

Modular ingestion pipeline that runs on Apache Spark and supports both JDBC sources and Iceberg/HDFS sinks. The codebase is designed around pluggable endpoints, execution tools, and an event bus for monitoring, so the runtime can evolve without deep rewrites.

## Repository Layout

```
ingestion.py            # Entry point for spark-submit / pyspark
salam_ingest/           # Library code (endpoints, tools, planning, state, etc.)
conf/                   # Example configuration files
db/                     # DDL definitions and migrations for state stores
scripts/                # Utility scripts (packaging, flattening)
dist/                   # Build artefacts (created on demand)
```

## Prerequisites

- Python 3.9+
- Apache Spark 3.3.x (matching the jars listed below)
- Required JDBC/connector JARs on the Spark classpath:
  - `singlestore-spark-connector_2.12-4.1.10-spark-3.3.4.jar`
  - `mariadb-java-client-2.7.11.jar`
  - `spray-json_2.13-1.3.5.jar`
  - `ojdbc8-12.2.0.1.jar`
  - `singlestore-jdbc-client-1.1.8.jar`
  - `mssql-jdbc-13.2.0.jre8.jar`
  - `spark-mssql-connector_2.12-1.2.0.jar`

Adjust the JAR versions/paths to match your environment.

## Building a Deployment Zip

Use the helper script to bundle the Python modules (and optional configs) into a single zip that Spark can ship to executors.

```bash
python3 scripts/build_zip.py --output dist/salam_ingest_bundle.zip \
    --include conf --include db
```

Key options:

- `--output`: target zip path (default `dist/salam_ingest_bundle.zip`).
- `--include`: extra top-level directories to embed in the archive (repeatable).

The script always packages `ingestion.py` and the entire `salam_ingest` package. Run `python3 scripts/build_zip.py --help` for the full option list.

### Submitting the job

```bash
spark3-submit \
  --py-files dist/salam_ingest_bundle.zip \
  --jars /home/informaticaadmin/jars/singlestore-spark-connector_2.12-4.1.10-spark-3.3.4.jar,\
          /home/informaticaadmin/jars/mariadb-java-client-2.7.11.jar,\
          /home/informaticaadmin/jars/spray-json_2.13-1.3.5.jar,\
          /home/informaticaadmin/jars/ojdbc8-12.2.0.1.jar,\
          /home/informaticaadmin/jars/singlestore-jdbc-client-1.1.8.jar,\
          /home/informaticaadmin/jars/mssql-jdbc-13.2.0.jre8.jar,\
          /home/informaticaadmin/jars/spark-mssql-connector_2.12-1.2.0.jar \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.executor.cores=4 \
  ingestion.py --config conf/brm.json
```

Replace the config path with your desired configuration file.

### Interactive pyspark session (optional)

```bash
pyspark3 \
  --py-files dist/salam_ingest_bundle.zip \
  --jars /home/informaticaadmin/jars/singlestore-spark-connector_2.12-4.1.10-spark-3.3.4.jar,\
          /home/informaticaadmin/jars/mariadb-java-client-2.7.11.jar,\
          /home/informaticaadmin/jars/spray-json_2.13-1.3.5.jar,\
          /home/informaticaadmin/jars/ojdbc8-12.2.0.1.jar,\
          /home/informaticaadmin/jars/singlestore-jdbc-client-1.1.8.jar,\
          /home/informaticaadmin/jars/mssql-jdbc-13.2.0.jre8.jar,\
          /home/informaticaadmin/jars/spark-mssql-connector_2.12-1.2.0.jar \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.dynamicAllocation.initialExecutors=2 \
  --conf spark.executor.cores=4
```

Inside the shell you can `import ingestion` and call `ingestion.main(...)` as needed.

## One-file Flatten (optional)

If you need to copy/paste the framework into a REPL, generate a single consolidated Python file:

```bash
python3 scripts/flatten_project.py dist/ingestion_flat.py
```

The flattened file preserves module separators as comments. It is intended for quick prototyping; stick with the zip distribution for production runs.

## Development Notes

- Endpoints encapsulate source/sink semantics, while execution tools only know how to talk to the underlying system (Spark today, others later).
- All monitoring flows through the event emitter (`salam_ingest/events`), making it easy to add new subscribers (state store, logging, adaptive planners, etc.).
- Metadata related to profiling or catalogues will be emitted separately and persisted via a lightweight SQLite repository (coming soon).

Feel free to open issues or PRs with enhancements or additional connectors.
