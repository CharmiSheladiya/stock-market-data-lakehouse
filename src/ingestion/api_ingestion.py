"""Ingest daily stock data from Alpha Vantage REST API into raw layer (Parquet)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from config.settings import Settings, get_settings
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

# Alpha Vantage response schema
DAILY_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("date", pa.date32()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
    ]
)

API_BASE_URL = "https://www.alphavantage.co/query"


def fetch_daily_prices(symbol: str, api_key: str, output_size: str = "compact") -> list[dict]:
    """Fetch daily adjusted prices from Alpha Vantage for a single symbol.

    Args:
        symbol: Ticker symbol (e.g. 'AAPL').
        api_key: Alpha Vantage API key.
        output_size: 'compact' (last 100 days) or 'full' (20+ years).

    Returns:
        List of daily price records.
    """
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": output_size,
        "apikey": api_key,
    }
    response = requests.get(API_BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    time_series = data.get("Time Series (Daily)", {})
    if not time_series:
        logger.warning("No daily data returned for %s — check API key or rate limits", symbol)
        return []

    now = datetime.now(timezone.utc)
    records = []
    for date_str, ohlcv in time_series.items():
        records.append(
            {
                "symbol": symbol,
                "date": datetime.strptime(date_str, "%Y-%m-%d").date(),
                "open": float(ohlcv["1. open"]),
                "high": float(ohlcv["2. high"]),
                "low": float(ohlcv["3. low"]),
                "close": float(ohlcv["4. close"]),
                "volume": int(ohlcv["5. volume"]),
                "ingested_at": now,
            }
        )
    logger.info("Fetched %d daily records for %s", len(records), symbol)
    return records


def write_raw_parquet(records: list[dict], symbol: str, raw_path: Path) -> Path:
    """Write raw records to a date-partitioned Parquet file.

    Returns:
        Path to the written Parquet file.
    """
    if not records:
        raise ValueError(f"No records to write for {symbol}")

    table = pa.Table.from_pylist(records, schema=DAILY_SCHEMA)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_dir = raw_path / f"daily_prices/symbol={symbol}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{today}.parquet"

    pq.write_table(table, output_file, compression="snappy")
    logger.info("Wrote %d rows to %s", len(records), output_file)
    return output_file


def ingest_daily_prices(symbols: list[str] | None = None, settings: Settings | None = None) -> list[Path]:
    """Full ingestion job: fetch from API → write raw Parquet for each symbol.

    Args:
        symbols: Override default symbol list.
        settings: Override default settings.

    Returns:
        List of Parquet file paths written.
    """
    settings = settings or get_settings()
    settings.ensure_directories()
    symbols = symbols or settings.symbols

    written_files: list[Path] = []
    for symbol in symbols:
        try:
            records = fetch_daily_prices(symbol, settings.alpha_vantage_api_key)
            if records:
                path = write_raw_parquet(records, symbol, settings.raw_path)
                written_files.append(path)
        except requests.RequestException as exc:
            logger.error("API error for %s: %s", symbol, exc)
        except Exception as exc:
            logger.error("Failed to ingest %s: %s", symbol, exc)

    logger.info("Ingestion complete — %d/%d symbols written", len(written_files), len(symbols))
    return written_files
