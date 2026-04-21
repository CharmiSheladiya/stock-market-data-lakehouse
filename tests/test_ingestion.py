"""Tests for API ingestion module."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest

from src.ingestion.api_ingestion import (
    fetch_daily_prices,
    ingest_daily_prices,
    write_raw_parquet,
)


# ------------------------------------------------------------------ #
#  Fixtures
# ------------------------------------------------------------------ #
MOCK_API_RESPONSE = {
    "Meta Data": {"2. Symbol": "AAPL"},
    "Time Series (Daily)": {
        "2025-01-10": {
            "1. open": "150.00",
            "2. high": "155.00",
            "3. low": "149.00",
            "4. close": "153.50",
            "5. volume": "1000000",
        },
        "2025-01-09": {
            "1. open": "148.00",
            "2. high": "152.00",
            "3. low": "147.50",
            "4. close": "150.00",
            "5. volume": "950000",
        },
    },
}


@pytest.fixture
def sample_records() -> list[dict]:
    now = datetime.now(timezone.utc)
    return [
        {
            "symbol": "AAPL",
            "date": date(2025, 1, 10),
            "open": 150.0,
            "high": 155.0,
            "low": 149.0,
            "close": 153.5,
            "volume": 1000000,
            "ingested_at": now,
        },
        {
            "symbol": "AAPL",
            "date": date(2025, 1, 9),
            "open": 148.0,
            "high": 152.0,
            "low": 147.5,
            "close": 150.0,
            "volume": 950000,
            "ingested_at": now,
        },
    ]


# ------------------------------------------------------------------ #
#  API fetch tests
# ------------------------------------------------------------------ #
class TestFetchDailyPrices:
    @patch("src.ingestion.api_ingestion.requests.get")
    def test_successful_fetch(self, mock_get):
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = MOCK_API_RESPONSE
        mock_get.return_value.raise_for_status.return_value = None

        records = fetch_daily_prices("AAPL", "test_key")

        assert len(records) == 2
        assert records[0]["symbol"] == "AAPL"
        assert records[0]["close"] == 153.5
        assert isinstance(records[0]["date"], date)

    @patch("src.ingestion.api_ingestion.requests.get")
    def test_empty_response(self, mock_get):
        mock_get.return_value.json.return_value = {"Note": "Rate limited"}
        mock_get.return_value.raise_for_status.return_value = None

        records = fetch_daily_prices("AAPL", "test_key")
        assert records == []

    @patch("src.ingestion.api_ingestion.requests.get")
    def test_api_error_raises(self, mock_get):
        from requests.exceptions import HTTPError

        mock_get.return_value.raise_for_status.side_effect = HTTPError("500")

        with pytest.raises(HTTPError):
            fetch_daily_prices("AAPL", "test_key")


# ------------------------------------------------------------------ #
#  Parquet writing tests
# ------------------------------------------------------------------ #
class TestWriteRawParquet:
    def test_creates_parquet_file(self, sample_records, tmp_path):
        output = write_raw_parquet(sample_records, "AAPL", tmp_path)

        assert output.exists()
        assert output.suffix == ".parquet"

        table = pq.read_table(output)
        assert table.num_rows == 2
        assert "symbol" in table.column_names

    def test_empty_records_raises(self, tmp_path):
        with pytest.raises(ValueError, match="No records"):
            write_raw_parquet([], "AAPL", tmp_path)


# ------------------------------------------------------------------ #
#  Integration-style test
# ------------------------------------------------------------------ #
class TestIngestDailyPrices:
    @patch("src.ingestion.api_ingestion.fetch_daily_prices")
    def test_full_ingestion(self, mock_fetch, sample_records, tmp_path):
        mock_fetch.return_value = sample_records

        from config.settings import Settings

        settings = Settings(lakehouse_path=tmp_path)
        result = ingest_daily_prices(symbols=["AAPL"], settings=settings)

        assert len(result) == 1
        assert result[0].exists()
