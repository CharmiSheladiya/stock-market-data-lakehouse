# Stock Market Data Lakehouse

An end-to-end data lakehouse pipeline that ingests stock market data from multiple sources, transforms it through a **medallion architecture** (Bronze → Silver → Gold), serves analytics via a REST API, and is fully orchestrated with **Prefect**.

## Architecture

```
┌──────────────────┐    ┌─────────────┐    ┌───────────────┐    ┌──────────────┐
│   Data Sources   │───▶│  Raw Layer  │───▶│  Bronze (DL)  │───▶│ Silver (DL)  │
│                  │    │  (Parquet)  │    │  + metadata   │    │ deduplicated │
│ - Alpha Vantage  │    └─────────────┘    └───────────────┘    │ + validated  │
│ - Yahoo Finance  │                                            └──────┬───────┘
└──────────────────┘                                                   │
                                                                       ▼
┌──────────────────┐    ┌─────────────┐    ┌───────────────┐    ┌──────────────┐
│   FastAPI        │◀───│  DuckDB     │◀───│  dbt Models   │◀───│ Gold (DL)    │
│   REST API       │    │  Analytics  │    │  (staging →   │    │ aggregated   │
└──────────────────┘    └─────────────┘    │   marts)      │    └──────────────┘
                                           └───────────────┘
        ┌──────────────────────────────────────────────────────┐
        │  Prefect Orchestration  |  Data Quality Framework    │
        │  Terraform (AWS IaC)    |  GitHub Actions CI/CD      │
        └──────────────────────────────────────────────────────┘
```

## Tech Stack

| Layer              | Technology                                |
|--------------------|-------------------------------------------|
| Ingestion          | Python, `requests`, `yfinance`            |
| Storage            | Delta Lake (ACID transactions)            |
| Processing         | Polars (fast DataFrames), PyArrow         |
| Transformations    | dbt (staging → intermediate → marts)      |
| Analytics Engine   | DuckDB (in-process OLAP)                  |
| Serving            | FastAPI + Uvicorn                         |
| Orchestration      | Prefect 2.x (flows, tasks, deployments)   |
| Cloud Storage      | AWS S3 (via `boto3`)                      |
| Infrastructure     | Terraform (S3, IAM, Glue, CloudWatch)     |
| Testing            | pytest, moto (AWS mocks), mocking         |
| CI/CD              | GitHub Actions                            |
| Containers         | Docker, Docker Compose (MinIO + Prefect)  |

## Project Structure

```
stock-market-data-lakehouse/
├── config/
│   └── settings.py                    # Pydantic-based configuration
├── src/
│   ├── ingestion/
│   │   ├── api_ingestion.py           # Alpha Vantage REST API connector
│   │   ├── yahoo_ingestion.py         # Yahoo Finance bulk loader
│   │   └── s3_uploader.py             # AWS S3 upload with partitioning
│   ├── lakehouse/
│   │   └── delta_manager.py           # Delta Lake CRUD (Bronze/Silver/Gold)
│   ├── quality/
│   │   └── validators.py              # Composable DQ checks framework
│   ├── api/
│   │   └── app.py                     # FastAPI analytics endpoints
│   ├── orchestration/
│   │   ├── flows.py                   # Prefect flows & tasks
│   │   └── deployments.py             # Scheduled deployments
│   └── utils/
│       └── logging_utils.py           # Structured JSON logging
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/stg_daily_prices.sql
│   │   ├── intermediate/int_price_with_indicators.sql
│   │   └── marts/
│   │       ├── fact_daily_performance.sql
│   │       └── agg_weekly_performance.sql
│   └── macros/safe_divide.sql
├── terraform/
│   ├── main.tf                        # Provider + backend config
│   ├── variables.tf
│   ├── resources.tf                   # S3, IAM, Glue, CloudWatch
│   └── outputs.tf
├── tests/
│   ├── test_ingestion.py
│   ├── test_validators.py
│   ├── test_api.py
│   └── test_s3_uploader.py
├── data/sample/
│   └── daily_prices.csv
├── .github/workflows/ci.yml
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── requirements.txt
└── .gitignore
```

## Quick Start

```bash
# Clone
git clone https://github.com/<your-username>/stock-market-data-lakehouse.git
cd stock-market-data-lakehouse

# Setup
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Mac/Linux
pip install -r requirements.txt

# Copy environment config
copy .env.example .env       # Windows
# cp .env.example .env       # Mac/Linux

# Run tests
pytest tests/ -v --cov=src

# Start infrastructure (MinIO + Prefect)
docker-compose up -d

# Run the pipeline manually
python -m src.orchestration.flows

# Start API server
uvicorn src.api.app:app --reload
# → http://localhost:8000/docs  (Swagger UI)
```

## API Endpoints

| Endpoint                     | Description                            |
|------------------------------|----------------------------------------|
| `GET /health`                | Health check                           |
| `GET /api/v1/symbols`       | All symbols with summary stats         |
| `GET /api/v1/symbols/{sym}` | Last 30 days for a specific symbol     |
| `GET /api/v1/prices/{sym}`  | Historical prices with date filtering  |
| `GET /api/v1/quality`       | Data quality layer status              |

## dbt Models (Medallion Architecture)

```
sources (raw) → stg_daily_prices → int_price_with_indicators → fact_daily_performance
                                                              → agg_weekly_performance
```

- **stg_daily_prices** — Deduplication, type casting, invalid record filtering
- **int_price_with_indicators** — Moving averages (7/21/50d), daily returns, volatility, trend signals
- **fact_daily_performance** — Final analytics fact table
- **agg_weekly_performance** — Weekly OHLCV, return, volume, up/down day counts

## Data Quality Checks

| Check                  | Description                               |
|------------------------|-------------------------------------------|
| Not null               | Key columns must not be null              |
| Positive values        | OHLC prices must be > 0                   |
| Uniqueness             | No duplicate (symbol, date) pairs         |
| Freshness              | Most recent data within N days            |
| Range validation       | Values within expected bounds             |
| High ≥ Low             | Domain rule: high price ≥ low price       |

## Infrastructure (Terraform)

```bash
cd terraform
terraform init
terraform plan -var="environment=dev"
terraform apply -var="environment=dev"
```

Creates: S3 bucket (versioned, encrypted, lifecycle rules), IAM role, Glue catalog, CloudWatch log group.

## License

MIT
