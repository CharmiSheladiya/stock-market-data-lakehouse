"""Tests for the FastAPI serving layer."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.api.app import app

client = TestClient(app)


class TestHealthEndpoint:
    def test_health_returns_200(self):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "lakehouse_path" in data


class TestQualityEndpoint:
    def test_quality_returns_200(self):
        response = client.get("/api/v1/quality")
        assert response.status_code == 200
        data = response.json()
        assert "layers" in data


class TestSymbolEndpoints:
    def test_symbols_returns_503_when_no_data(self):
        response = client.get("/api/v1/symbols")
        # 503 when gold layer not populated
        assert response.status_code in [200, 503]

    def test_invalid_symbol_returns_404_or_503(self):
        response = client.get("/api/v1/symbols/ZZZZZ")
        assert response.status_code in [404, 503]


class TestPriceEndpoints:
    def test_prices_with_no_data(self):
        response = client.get("/api/v1/prices/AAPL")
        assert response.status_code in [200, 404, 503]

    def test_prices_limit_validation(self):
        response = client.get("/api/v1/prices/AAPL?limit=0")
        # FastAPI validation — limit must be >= 1
        assert response.status_code == 422
