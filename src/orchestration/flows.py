"""Prefect orchestration flows for the stock market lakehouse pipeline.

Demonstrates:
- Parameterized flows
- Task retries with exponential backoff
- Concurrent task execution
- Flow-level error handling and notifications
"""

from __future__ import annotations

from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.tasks import task_input_hash

from config.settings import Settings, get_settings
from src.ingestion.api_ingestion import fetch_daily_prices, write_raw_parquet
from src.lakehouse.delta_manager import DeltaLakeManager
from src.quality.validators import DataQualityValidator


# ------------------------------------------------------------------ #
#  Tasks
# ------------------------------------------------------------------ #
@task(
    retries=3,
    retry_delay_seconds=[30, 60, 120],  # exponential backoff
    cache_key_fn=task_input_hash,
    cache_expiration=3600,
    tags=["ingestion"],
)
def ingest_symbol(symbol: str, api_key: str, raw_path: Path) -> Path | None:
    """Ingest daily prices for a single symbol."""
    logger = get_run_logger()
    logger.info("Ingesting %s", symbol)
    records = fetch_daily_prices(symbol, api_key)
    if not records:
        logger.warning("No data for %s — skipping", symbol)
        return None
    return write_raw_parquet(records, symbol, raw_path)


@task(tags=["lakehouse", "bronze"])
def load_bronze(raw_path: Path, table_name: str) -> str:
    """Load raw Parquet into Bronze Delta table."""
    logger = get_run_logger()
    dm = DeltaLakeManager()
    bronze_path = dm.write_bronze(raw_path, table_name)
    logger.info("Bronze loaded: %s", bronze_path)
    return bronze_path


@task(tags=["lakehouse", "silver"])
def load_silver(table_name: str) -> str:
    """Cleanse and load Silver layer."""
    logger = get_run_logger()
    dm = DeltaLakeManager()
    silver_path = dm.write_silver(table_name)
    logger.info("Silver loaded: %s", silver_path)
    return silver_path


@task(tags=["lakehouse", "gold"])
def load_gold_summary() -> str:
    """Build Gold daily summary."""
    logger = get_run_logger()
    dm = DeltaLakeManager()
    gold_path = dm.write_gold_daily_summary()
    logger.info("Gold loaded: %s", gold_path)
    return gold_path


@task(tags=["quality"])
def run_quality_checks(table_name: str) -> dict:
    """Run data quality checks on the Silver layer."""
    logger = get_run_logger()
    dm = DeltaLakeManager()
    df = dm.read_table("silver", table_name)

    validator = DataQualityValidator(table_name)
    validator.check_not_null(df, ["symbol", "date", "close", "volume"])
    validator.check_positive_values(df, ["open", "high", "low", "close"])
    validator.check_unique(df, ["symbol", "date"])
    validator.check_freshness(df, "date", max_age_days=3)
    validator.check_high_low_relationship(df)

    summary = validator.get_summary()
    if not summary["all_passed"]:
        logger.warning("DQ checks FAILED: %s", summary)
    else:
        logger.info("All DQ checks PASSED: %s", summary)
    return summary


@task(tags=["dbt"])
def run_dbt_transformations() -> bool:
    """Execute dbt models."""
    import subprocess

    logger = get_run_logger()
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", "dbt", "--project-dir", "dbt"],
        capture_output=True,
        text=True,
        timeout=300,
    )
    logger.info("dbt stdout: %s", result.stdout[-500:] if result.stdout else "")
    if result.returncode != 0:
        logger.error("dbt failed: %s", result.stderr[-500:] if result.stderr else "")
        raise RuntimeError("dbt run failed")
    return True


# ------------------------------------------------------------------ #
#  Main flow
# ------------------------------------------------------------------ #
@flow(
    name="stock-market-pipeline",
    description="End-to-end stock market data lakehouse pipeline",
    retries=1,
    retry_delay_seconds=60,
)
def stock_market_pipeline(
    symbols: list[str] | None = None,
    run_dbt: bool = True,
) -> dict:
    """Orchestrate the full pipeline: Ingest → Bronze → Silver → DQ → Gold → dbt.

    Args:
        symbols: List of ticker symbols to process.
        run_dbt: Whether to run dbt models after loading.

    Returns:
        Pipeline execution summary.
    """
    logger = get_run_logger()
    settings = get_settings()
    settings.ensure_directories()
    symbols = symbols or settings.symbols

    # Step 1: Ingest from API (concurrent per symbol)
    logger.info("Starting ingestion for %d symbols", len(symbols))
    raw_files: list[Path] = []
    ingest_futures = [
        ingest_symbol.submit(sym, settings.alpha_vantage_api_key, settings.raw_path)
        for sym in symbols
    ]
    for future in ingest_futures:
        result = future.result()
        if result:
            raw_files.append(result)

    if not raw_files:
        logger.warning("No data ingested — aborting pipeline")
        return {"status": "no_data", "symbols_processed": 0}

    # Step 2: Load Bronze
    logger.info("Loading %d files into Bronze", len(raw_files))
    for raw_file in raw_files:
        load_bronze(raw_file, "daily_prices")

    # Step 3: Load Silver
    logger.info("Building Silver layer")
    load_silver("daily_prices")

    # Step 4: Data quality checks
    logger.info("Running quality checks")
    dq_summary = run_quality_checks("daily_prices")

    # Step 5: Load Gold
    logger.info("Building Gold layer")
    load_gold_summary()

    # Step 6: dbt transformations
    dbt_status = "skipped"
    if run_dbt:
        try:
            run_dbt_transformations()
            dbt_status = "success"
        except RuntimeError:
            dbt_status = "failed"

    summary = {
        "status": "completed",
        "symbols_processed": len(raw_files),
        "total_symbols": len(symbols),
        "dq_summary": dq_summary,
        "dbt_status": dbt_status,
    }
    logger.info("Pipeline complete: %s", summary)
    return summary


# ------------------------------------------------------------------ #
#  Entrypoint for manual runs
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    stock_market_pipeline()
