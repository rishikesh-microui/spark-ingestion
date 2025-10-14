from salam_ingest import (
    RUN_ID,
    PrintLogger,
    main,
    run_cli,
    suggest_singlestore_ddl,
    validate_config,
    with_ingest_cols,
)

__all__ = [
    "RUN_ID",
    "PrintLogger",
    "main",
    "run_cli",
    "suggest_singlestore_ddl",
    "validate_config",
    "with_ingest_cols",
]


if __name__ == "__main__":
    run_cli()
