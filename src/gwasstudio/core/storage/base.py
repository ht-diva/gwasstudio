"""
GWASStudio Storage Base Module
===============================

This module defines the abstract base class for storage backends.
"""

from abc import ABC, abstractmethod
from collections.abc import Generator
from typing import Any

import pandas as pd

from gwasstudio.core import GWASStudioError


class StorageError(GWASStudioError):
    """Exception raised for storage-related errors."""

    pass


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.

    All storage backends (TileDB, MongoDB, S3) should inherit from this class
    and implement the required methods.
    """

    @abstractmethod
    def ingest_data(
        self,
        df: pd.DataFrame,
        project_name: str,
        **kwargs,
    ) -> str:
        """
        Ingest a DataFrame into the storage backend.

        Args:
            df: DataFrame containing the genomic data to ingest.
            project_name: Name of the project/dataset.
            **kwargs: Additional arguments for the ingestion.

        Returns:
            str: Unique identifier for the ingested project.

        Raises:
            StorageError: If ingestion fails.
        """
        pass

    @abstractmethod
    def query_data(
        self,
        project_id: str,
        region: dict[str, Any] | None = None,
        snp_list: list[str] | None = None,
        pval_threshold: float | None = None,
        limit: int | None = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Query data from the storage backend.

        Args:
            project_id: Unique identifier for the project.
            region: Genomic region to query (e.g., {"chr": "1", "start": 100000, "end": 200000}).
            snp_list: List of SNP IDs to query.
            pval_threshold: Only return variants with p-value <= threshold.
            limit: Maximum number of records to return.
            **kwargs: Additional arguments for the query.

        Returns:
            pd.DataFrame: DataFrame with the query results.

        Raises:
            StorageError: If the query fails.
        """
        pass

    @abstractmethod
    def query_data_stream(
        self,
        project_id: str,
        region: dict[str, Any] | None = None,
        snp_list: list[str] | None = None,
        pval_threshold: float | None = None,
        chunk_size: int = 10000,
        **kwargs,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Query data from the storage backend in chunks (streaming).

        Args:
            project_id: Unique identifier for the project.
            region: Genomic region to query.
            snp_list: List of SNP IDs to query.
            pval_threshold: Only return variants with p-value <= threshold.
            chunk_size: Number of records per chunk.
            **kwargs: Additional arguments for the query.

        Yields:
            pd.DataFrame: Chunks of DataFrames with query results.

        Raises:
            StorageError: If the query fails.
        """
        pass

    @abstractmethod
    def project_exists(self, project_id: str) -> bool:
        """
        Check if a project exists in the storage backend.

        Args:
            project_id: Unique identifier for the project.

        Returns:
            bool: True if the project exists, False otherwise.
        """
        pass

    @abstractmethod
    def delete_project(self, project_id: str) -> None:
        """
        Delete a project from the storage backend.

        Args:
            project_id: Unique identifier for the project.

        Raises:
            StorageError: If deletion fails.
        """
        pass
