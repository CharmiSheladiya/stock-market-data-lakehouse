"""FastAPI application — serves analytics data from the Gold layer via DuckDB."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query

from config.settings import get_settings

app = FastAPI(
    title="Stock Market Lakehouse API",
    description="Serves pre-computed analytics from the Gold layer",
    version="1.0.0",
)

settings = get_settings()


def _get_connection() -> duckdb.DuckDBPyConnection:
    """Return a read-only DuckDB connection to the lakehouse."""
    db_path = str(settings.lakehouse_path / "lakehouse.duckdb")
    if not Path(db_path).exists():
        # Fallback: query Delta/Parquet directly
        return duckdb.connect(":memory:")
    return duckdb.connect(db_path, read_only=True)


# ------------------------------------------------------------------ #
#  Health check
# ------------------------------------------------------------------ #
@app.get("/health")
def health_check() -> dict:
    return {"status": "healthy", "lakehouse_path": str(settings.lakehouse_path)}


# ------------------------------------------------------------------ #
#  Symbol summary — Gold layer
# ------------------------------------------------------------------ #
@app.get("/api/v1/symbols")
def list_symbols() -> dict:
    """List all available stock symbols with summary stats."""
    gold_path = settings.gold_path / "daily_summary"
    if not gold_path.exists():
        raise HTTPException(status_code=503, detail="Gold layer not yet populated")

    con = duckdb.connect(":memory:")
    df = con.execute(
        f"SELECT * FROM delta_scan('{gold_path}')"
    ).fetchdf()
    return {"symbols": df.to_dict(orient="records")}


@app.get("/api/v1/symbols/{symbol}")
def get_symbol_detail(symbol: str) -> dict:
    """Get detailed summary for a specific symbol."""
    silver_path = settings.silver_path / "daily_prices"
    if not silver_path.exists():
        raise HTTPException(status_code=503, detail="Silver layer not yet populated")

    symbol = symbol.upper()
    con = duckdb.connect(":memory:")
    result = con.execute(
        f"""
        SELECT symbol, date, open, high, low, close, volume
        FROM delta_scan('{silver_path}')
        WHERE symbol = ?
        ORDER BY date DESC
        LIMIT 30
        """,
        [symbol],
    ).fetchdf()

    if result.empty:
        raise HTTPException(status_code=404, detail=f"Symbol '{symbol}' not found")
    return {"symbol": symbol, "recent_prices": result.to_dict(orient="records")}


# ------------------------------------------------------------------ #
#  Price history with date range filtering
# ------------------------------------------------------------------ #
@app.get("/api/v1/prices/{symbol}")
def get_price_history(
    symbol: str,
    start_date: date | None = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: date | None = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000),
) -> dict:
    """Retrieve historical prices for a symbol with optional date range."""
    silver_path = settings.silver_path / "daily_prices"
    if not silver_path.exists():
        raise HTTPException(status_code=503, detail="Silver layer not yet populated")

    symbol = symbol.upper()
    con = duckdb.connect(":memory:")

    query = f"SELECT * FROM delta_scan('{silver_path}') WHERE symbol = ?"
    params: list = [symbol]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += f" ORDER BY date DESC LIMIT {limit}"

    result = con.execute(query, params).fetchdf()
    if result.empty:
        raise HTTPException(status_code=404, detail=f"No price data for '{symbol}'")
    return {"symbol": symbol, "count": len(result), "prices": result.to_dict(orient="records")}


# ------------------------------------------------------------------ #
#  Data quality report
# ------------------------------------------------------------------ #
@app.get("/api/v1/quality")
def get_quality_report() -> dict:
    """Return the latest data quality summary."""
    return {
        "status": "available",
        "message": "Run pipeline to generate DQ report",
        "layers": {
            "bronze": (settings.bronze_path / "daily_prices").exists(),
            "silver": (settings.silver_path / "daily_prices").exists(),
            "gold": (settings.gold_path / "daily_summary").exists(),
        },
    }


# ------------------------------------------------------------------ #
#  Entrypoint
# ------------------------------------------------------------------ #
def start_server() -> None:
    """Start the API server (for local development)."""
    import uvicorn

    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    start_server()
