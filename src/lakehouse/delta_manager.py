"""Delta Lake manager — handles Bronze / Silver / Gold table writes and reads.

Uses the `deltalake` library for ACID-compliant writes with schema enforcement
and time travel. Polars for fast in-process transformations.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
from deltalake import DeltaTable, write_deltalake

from config.settings import Settings, get_settings
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class DeltaLakeManager:
    """Manages the medallion architecture layers as Delta tables."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._settings.ensure_directories()

    # ------------------------------------------------------------------ #
    #  Bronze layer — raw data append with metadata
    # ------------------------------------------------------------------ #
    def write_bronze(self, raw_parquet_path: Path, table_name: str) -> str:
        """Read raw Parquet and write to Bronze Delta table with metadata columns.

        Args:
            raw_parquet_path: Path to raw Parquet file.
            table_name: Logical table name (e.g. 'daily_prices').

        Returns:
            Bronze Delta table path.
        """
        df = pl.read_parquet(raw_parquet_path)

        # Add bronze metadata
        df = df.with_columns(
            [
                pl.lit(str(raw_parquet_path)).alias("_source_file"),
                pl.lit(datetime.now(timezone.utc)).alias("_bronze_loaded_at"),
                pl.lit("raw_ingestion").alias("_load_pipeline"),
            ]
        )

        bronze_path = str(self._settings.bronze_path / table_name)
        write_deltalake(
            bronze_path,
            df.to_arrow(),
            mode="append",
            schema_mode="merge",
        )
        logger.info("Bronze write: %d rows → %s", len(df), bronze_path)
        return bronze_path

    # ------------------------------------------------------------------ #
    #  Silver layer — cleansed, deduplicated, typed
    # ------------------------------------------------------------------ #
    def write_silver(self, table_name: str) -> str:
        """Read Bronze, apply cleansing rules, write to Silver.

        - Remove duplicates by (symbol, date)
        - Cast types
        - Validate ranges
        """
        bronze_path = str(self._settings.bronze_path / table_name)
        dt = DeltaTable(bronze_path)
        df = pl.from_arrow(dt.to_pyarrow_table())

        # Deduplicate — keep latest ingestion
        df = df.sort("_bronze_loaded_at", descending=True).unique(
            subset=["symbol", "date"], keep="first"
        )

        # Validate price ranges
        df = df.filter(
            (pl.col("open") > 0)
            & (pl.col("high") > 0)
            & (pl.col("low") > 0)
            & (pl.col("close") > 0)
            & (pl.col("volume") >= 0)
            & (pl.col("high") >= pl.col("low"))
        )

        # Add silver metadata
        df = df.with_columns(
            [
                pl.lit(datetime.now(timezone.utc)).alias("_silver_loaded_at"),
                pl.lit(True).alias("_is_valid"),
            ]
        )

        silver_path = str(self._settings.silver_path / table_name)
        write_deltalake(
            silver_path,
            df.to_arrow(),
            mode="overwrite",
            schema_mode="overwrite",
        )
        logger.info("Silver write: %d clean rows → %s", len(df), silver_path)
        return silver_path

    # ------------------------------------------------------------------ #
    #  Gold layer — aggregated business metrics
    # ------------------------------------------------------------------ #
    def write_gold_daily_summary(self) -> str:
        """Build Gold layer daily summary from Silver daily_prices."""
        silver_path = str(self._settings.silver_path / "daily_prices")
        dt = DeltaTable(silver_path)
        df = pl.from_arrow(dt.to_pyarrow_table())

        summary = (
            df.group_by("symbol")
            .agg(
                [
                    pl.col("date").max().alias("latest_date"),
                    pl.col("date").min().alias("earliest_date"),
                    pl.col("close").last().alias("latest_close"),
                    pl.col("close").mean().alias("avg_close"),
                    pl.col("close").std().alias("std_close"),
                    pl.col("volume").mean().alias("avg_volume"),
                    pl.col("volume").sum().alias("total_volume"),
                    pl.len().alias("total_records"),
                    pl.col("high").max().alias("all_time_high"),
                    pl.col("low").min().alias("all_time_low"),
                ]
            )
            .with_columns(
                [
                    pl.lit(datetime.now(timezone.utc)).alias("_gold_loaded_at"),
                ]
            )
        )

        gold_path = str(self._settings.gold_path / "daily_summary")
        write_deltalake(
            gold_path,
            summary.to_arrow(),
            mode="overwrite",
            schema_mode="overwrite",
        )
        logger.info("Gold daily_summary: %d symbols → %s", len(summary), gold_path)
        return gold_path

    # ------------------------------------------------------------------ #
    #  Read helpers
    # ------------------------------------------------------------------ #
    def read_table(self, layer: str, table_name: str) -> pl.DataFrame:
        """Read a Delta table from any layer."""
        path = str(self._settings.lakehouse_path / layer / table_name)
        dt = DeltaTable(path)
        return pl.from_arrow(dt.to_pyarrow_table())

    def get_table_history(self, layer: str, table_name: str) -> list[dict]:
        """Get Delta table version history (time travel metadata)."""
        path = str(self._settings.lakehouse_path / layer / table_name)
        dt = DeltaTable(path)
        return dt.history()

    def read_table_version(self, layer: str, table_name: str, version: int) -> pl.DataFrame:
        """Read a specific version of a Delta table (time travel)."""
        path = str(self._settings.lakehouse_path / layer / table_name)
        dt = DeltaTable(path, version=version)
        return pl.from_arrow(dt.to_pyarrow_table())
