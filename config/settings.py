"""Centralized configuration using pydantic-settings."""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings


class Environment(str, Enum):
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class Settings(BaseSettings):
    """Application settings loaded from environment variables / .env file."""

    # General
    environment: Environment = Environment.DEV
    log_level: str = "INFO"

    # Paths — medallion architecture
    lakehouse_path: Path = Path("./data")

    @property
    def raw_path(self) -> Path:
        return self.lakehouse_path / "raw"

    @property
    def bronze_path(self) -> Path:
        return self.lakehouse_path / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.lakehouse_path / "silver"

    @property
    def gold_path(self) -> Path:
        return self.lakehouse_path / "gold"

    # API keys
    alpha_vantage_api_key: str = Field(default="demo")

    # AWS
    aws_region: str = "us-east-1"
    s3_bucket: str = "stock-market-lakehouse"

    # Serving API
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # Stock symbols to track
    symbols: list[str] = Field(
        default=["AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "NVDA", "JPM"]
    )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def ensure_directories(self) -> None:
        """Create lakehouse directories if they don't exist."""
        for path in [self.raw_path, self.bronze_path, self.silver_path, self.gold_path]:
            path.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    """Factory for settings — enables test overrides."""
    return Settings()
