"""Ingest bulk historical data from Yahoo Finance via yfinance."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import yfinance as yf

from config.settings import Settings, get_settings
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


def fetch_historical_data(
    symbol: str,
    period: str = "2y",
    interval: str = "1d",
) -> list[dict]:
    """Download historical OHLCV data from Yahoo Finance.

    Args:
        symbol: Ticker symbol.
        period: Look-back period (e.g. '1y', '2y', '5y', 'max').
        interval: Data interval ('1d', '1wk', '1mo').

    Returns:
        List of price records.
    """
    ticker = yf.Ticker(symbol)
    df = ticker.history(period=period, interval=interval)
    if df.empty:
        logger.warning("No historical data for %s", symbol)
        return []

    now = datetime.now(timezone.utc)
    records = []
    for idx, row in df.iterrows():
        records.append(
            {
                "symbol": symbol,
                "date": idx.date(),
                "open": round(float(row["Open"]), 4),
                "high": round(float(row["High"]), 4),
                "low": round(float(row["Low"]), 4),
                "close": round(float(row["Close"]), 4),
                "volume": int(row["Volume"]),
                "dividends": round(float(row.get("Dividends", 0)), 4),
                "stock_splits": round(float(row.get("Stock Splits", 0)), 4),
                "ingested_at": now,
            }
        )
    logger.info("Fetched %d historical records for %s (%s, %s)", len(records), symbol, period, interval)
    return records


def write_historical_parquet(records: list[dict], symbol: str, raw_path: Path) -> Path:
    """Persist historical records as Parquet."""
    schema = pa.schema(
        [
            pa.field("symbol", pa.string()),
            pa.field("date", pa.date32()),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("volume", pa.int64()),
            pa.field("dividends", pa.float64()),
            pa.field("stock_splits", pa.float64()),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    table = pa.Table.from_pylist(records, schema=schema)
    output_dir = raw_path / f"historical_prices/symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "full_history.parquet"

    pq.write_table(table, output_file, compression="snappy")
    logger.info("Wrote %d historical rows to %s", len(records), output_file)
    return output_file


def ingest_historical(
    symbols: list[str] | None = None,
    period: str = "2y",
    settings: Settings | None = None,
) -> list[Path]:
    """Backfill historical data for all symbols."""
    settings = settings or get_settings()
    settings.ensure_directories()
    symbols = symbols or settings.symbols

    written: list[Path] = []
    for symbol in symbols:
        try:
            records = fetch_historical_data(symbol, period=period)
            if records:
                path = write_historical_parquet(records, symbol, settings.raw_path)
                written.append(path)
        except Exception as exc:
            logger.error("Failed historical ingest for %s: %s", symbol, exc)

    logger.info("Historical backfill complete — %d/%d symbols", len(written), len(symbols))
    return written
