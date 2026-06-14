"""
GWASStudio S3 Storage Module
============================

This module provides the S3 storage backend for GWASStudio.
It handles interactions with S3-compatible storage systems.
"""

from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from gwasstudio.core.config import GWASStudioConfig, S3Config
from gwasstudio.core.exceptions import GWASStudioError


class S3Error(GWASStudioError):
    """Exception raised for S3-specific errors."""

    pass


class S3Storage:
    """
    S3 storage backend for GWASStudio.

    This class provides utilities for interacting with S3 storage systems,
    including AWS S3 and S3-compatible services (e.g., MinIO).
    """

    def __init__(self, config: GWASStudioConfig):
        """
        Initialize the S3 storage backend.

        Args:
            config: GWASStudio configuration.
        """
        self.config = config
        self._s3_config = config.s3
        self._client = self._setup_s3_client()

    def _setup_s3_client(self) -> Any:
        """
        Set up S3 client from configuration.

        Returns:
            boto3.client: S3 client instance.

        Raises:
            S3Error: If client setup fails.
        """
        try:
            config = Config(
                connect_timeout=self._s3_config.connect_timeout_ms / 1000,
                read_timeout=self._s3_config.request_timeout_ms / 1000,
            )

            return boto3.client(
                "s3",
                aws_access_key_id=self._s3_config.aws_access_key_id,
                aws_secret_access_key=self._s3_config.aws_secret_access_key,
                endpoint_url=self._s3_config.endpoint_override,
                region_name=self._s3_config.region,
                config=config,
                verify=self._s3_config.verify_ssl,
            )
        except Exception as e:
            raise S3Error(f"Failed to set up S3 client: {str(e)}")

    def upload_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
        **kwargs,
    ) -> None:
        """
        Upload a file to S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            file_path: Local path to the file to upload.
            **kwargs: Additional arguments for the upload.

        Raises:
            S3Error: If upload fails.
        """
        try:
            self._client.upload_file(str(file_path), bucket, key, ExtraArgs=kwargs)
        except Exception as e:
            raise S3Error(f"Failed to upload file to S3: {str(e)}")

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        **kwargs,
    ) -> None:
        """
        Upload bytes to S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            data: Bytes to upload.
            **kwargs: Additional arguments for the upload.

        Raises:
            S3Error: If upload fails.
        """
        try:
            self._client.put_object(Bucket=bucket, Key=key, Body=data, **kwargs)
        except Exception as e:
            raise S3Error(f"Failed to upload bytes to S3: {str(e)}")

    def download_file(
        self,
        bucket: str,
        key: str,
        file_path: Union[str, Path],
        **kwargs,
    ) -> None:
        """
        Download a file from S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            file_path: Local path to save the downloaded file.
            **kwargs: Additional arguments for the download.

        Raises:
            S3Error: If download fails.
        """
        try:
            self._client.download_file(bucket, key, str(file_path), ExtraArgs=kwargs)
        except Exception as e:
            raise S3Error(f"Failed to download file from S3: {str(e)}")

    def download_bytes(
        self,
        bucket: str,
        key: str,
        **kwargs,
    ) -> bytes:
        """
        Download bytes from S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            **kwargs: Additional arguments for the download.

        Returns:
            bytes: Downloaded data.

        Raises:
            S3Error: If download fails.
        """
        try:
            response = self._client.get_object(Bucket=bucket, Key=key, **kwargs)
            return response["Body"].read()
        except Exception as e:
            raise S3Error(f"Failed to download bytes from S3: {str(e)}")

    def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        **kwargs,
    ) -> list[str]:
        """
        List objects in an S3 bucket.

        Args:
            bucket: S3 bucket name.
            prefix: Prefix to filter objects.
            **kwargs: Additional arguments for the list operation.

        Returns:
            list: List of object keys.

        Raises:
            S3Error: If listing fails.
        """
        try:
            response = self._client.list_objects_v2(Bucket=bucket, Prefix=prefix, **kwargs)
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            raise S3Error(f"Failed to list objects in S3: {str(e)}")

    def delete_object(
        self,
        bucket: str,
        key: str,
        **kwargs,
    ) -> None:
        """
        Delete an object from S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            **kwargs: Additional arguments for the delete operation.

        Raises:
            S3Error: If deletion fails.
        """
        try:
            self._client.delete_object(Bucket=bucket, Key=key, **kwargs)
        except Exception as e:
            raise S3Error(f"Failed to delete object from S3: {str(e)}")

    def object_exists(
        self,
        bucket: str,
        key: str,
        **kwargs,
    ) -> bool:
        """
        Check if an object exists in S3.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            **kwargs: Additional arguments for the check.

        Returns:
            bool: True if the object exists, False otherwise.
        """
        try:
            self._client.head_object(Bucket=bucket, Key=key, **kwargs)
            return True
        except Exception:
            return False

    def get_object_info(
        self,
        bucket: str,
        key: str,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Get information about an S3 object.

        Args:
            bucket: S3 bucket name.
            key: S3 object key.
            **kwargs: Additional arguments for the head operation.

        Returns:
            dict: Dictionary with object information (size, last modified, etc.).

        Raises:
            S3Error: If getting object info fails.
        """
        try:
            response = self._client.head_object(Bucket=bucket, Key=key, **kwargs)
            return {
                "size": response.get("ContentLength"),
                "last_modified": response.get("LastModified"),
                "content_type": response.get("ContentType"),
                "metadata": response.get("Metadata", {}),
            }
        except Exception as e:
            raise S3Error(f"Failed to get object info from S3: {str(e)}")
