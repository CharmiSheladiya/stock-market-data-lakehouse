"""Prefect deployment configuration — schedule and parameterize flows."""

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from src.orchestration.flows import stock_market_pipeline

# Daily pipeline — runs every weekday at 6 PM ET (after market close)
daily_deployment = Deployment.build_from_flow(
    flow=stock_market_pipeline,
    name="daily-stock-pipeline",
    schedule=CronSchedule(cron="0 18 * * 1-5", timezone="America/New_York"),
    parameters={
        "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"],
        "run_dbt": True,
    },
    tags=["stock-market", "production"],
    description="Daily pipeline triggered after US market close",
    work_queue_name="default",
)

# Weekly full backfill — runs every Saturday
weekly_backfill = Deployment.build_from_flow(
    flow=stock_market_pipeline,
    name="weekly-backfill",
    schedule=CronSchedule(cron="0 10 * * 6", timezone="America/New_York"),
    parameters={
        "symbols": ["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"],
        "run_dbt": True,
    },
    tags=["stock-market", "backfill"],
    description="Weekly full backfill on Saturdays",
    work_queue_name="default",
)

if __name__ == "__main__":
    daily_deployment.apply()
    weekly_backfill.apply()
    print("Deployments registered successfully.")
