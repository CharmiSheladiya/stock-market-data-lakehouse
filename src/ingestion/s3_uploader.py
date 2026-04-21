"""Upload local Parquet files to S3 with proper partitioning."""

from __future__ import annotations

from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from config.settings import Settings, get_settings
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)


class S3Uploader:
    """Handles uploading lakehouse data to S3."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._client = boto3.client("s3", region_name=self._settings.aws_region)
        self._bucket = self._settings.s3_bucket

    def upload_file(self, local_path: Path, s3_key: str) -> str:
        """Upload a single file to S3.

        Args:
            local_path: Local file path.
            s3_key: Target S3 key (path inside the bucket).

        Returns:
            Full S3 URI.
        """
        self._client.upload_file(str(local_path), self._bucket, s3_key)
        s3_uri = f"s3://{self._bucket}/{s3_key}"
        logger.info("Uploaded %s → %s", local_path, s3_uri)
        return s3_uri

    def upload_directory(self, local_dir: Path, s3_prefix: str) -> list[str]:
        """Recursively upload all Parquet files in a directory tree."""
        uploaded: list[str] = []
        for file_path in local_dir.rglob("*.parquet"):
            relative = file_path.relative_to(local_dir)
            s3_key = f"{s3_prefix}/{relative.as_posix()}"
            uri = self.upload_file(file_path, s3_key)
            uploaded.append(uri)
        logger.info("Uploaded %d files from %s to s3://%s/%s", len(uploaded), local_dir, self._bucket, s3_prefix)
        return uploaded

    def check_key_exists(self, s3_key: str) -> bool:
        """Check if an object exists in S3."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=s3_key)
            return True
        except ClientError:
            return False

    def sync_lakehouse_to_s3(self) -> dict[str, list[str]]:
        """Sync all lakehouse layers to S3."""
        results: dict[str, list[str]] = {}
        for layer in ["raw", "bronze", "silver", "gold"]:
            local_dir = self._settings.lakehouse_path / layer
            if local_dir.exists():
                results[layer] = self.upload_directory(local_dir, layer)
        return results
