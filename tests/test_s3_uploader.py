"""Tests for the S3 uploader using moto (AWS mock)."""

from __future__ import annotations

from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from config.settings import Settings
from src.ingestion.s3_uploader import S3Uploader


@pytest.fixture
def aws_credentials(monkeypatch):
    """Mock AWS credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_bucket(aws_credentials):
    with mock_aws():
        conn = boto3.client("s3", region_name="us-east-1")
        conn.create_bucket(Bucket="test-bucket")
        yield conn


@pytest.fixture
def uploader(s3_bucket):
    settings = Settings(s3_bucket="test-bucket", aws_region="us-east-1")
    return S3Uploader(settings=settings)


class TestS3Uploader:
    def test_upload_file(self, uploader, tmp_path):
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"fake parquet data")

        uri = uploader.upload_file(test_file, "raw/test.parquet")

        assert uri == "s3://test-bucket/raw/test.parquet"

    def test_check_key_exists(self, uploader, tmp_path):
        # Upload a file first
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"fake data")
        uploader.upload_file(test_file, "raw/exists.parquet")

        assert uploader.check_key_exists("raw/exists.parquet") is True
        assert uploader.check_key_exists("raw/missing.parquet") is False

    def test_upload_directory(self, uploader, tmp_path):
        # Create a directory structure
        sub = tmp_path / "symbol=AAPL"
        sub.mkdir()
        (sub / "data.parquet").write_bytes(b"aapl data")
        (sub / "meta.parquet").write_bytes(b"aapl meta")

        uris = uploader.upload_directory(tmp_path, "bronze/daily_prices")

        assert len(uris) == 2
