"""
Tests for GWASStudio Core Storage Base Module
=============================================

Tests for the base storage classes in gwasstudio.core.storage.base.
"""

from collections.abc import Generator
from typing import Any, Optional

import pandas as pd
import pytest

from gwasstudio.core.storage.base import StorageBackend, StorageError


class TestStorageError:
    """Tests for StorageError exception."""

    def test_storage_error_inheritance(self):
        """Test that StorageError inherits from GWASStudioError."""
        from gwasstudio.core.exceptions import GWASStudioError

        error = StorageError("Test storage error")
        assert isinstance(error, GWASStudioError)

    def test_storage_error_message(self):
        """Test that StorageError has the correct message."""
        error = StorageError("Test storage error")
        assert str(error) == "Test storage error"


class TestStorageBackend:
    """Tests for StorageBackend abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that StorageBackend cannot be instantiated directly."""
        with pytest.raises(TypeError):
            StorageBackend()

    def test_requires_ingest_data(self):
        """Test that concrete classes must implement ingest_data."""

        class ConcreteStorage(StorageBackend):
            def query_data(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                limit: Optional[int] = None,
                **kwargs,
            ) -> pd.DataFrame:
                return pd.DataFrame()

            def query_data_stream(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                chunk_size: int = 10000,
                **kwargs,
            ) -> Generator[pd.DataFrame, None, None]:
                yield pd.DataFrame()

            def project_exists(self, project_id: str) -> bool:
                return False

            def delete_project(self, project_id: str) -> None:
                pass

        with pytest.raises(TypeError):
            ConcreteStorage()

    def test_requires_query_data(self):
        """Test that concrete classes must implement query_data."""

        class ConcreteStorage(StorageBackend):
            def ingest_data(self, df: pd.DataFrame, project_name: str, **kwargs) -> str:
                return "test"

            def query_data_stream(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                chunk_size: int = 10000,
                **kwargs,
            ) -> Generator[pd.DataFrame, None, None]:
                yield pd.DataFrame()

            def project_exists(self, project_id: str) -> bool:
                return False

            def delete_project(self, project_id: str) -> None:
                pass

        with pytest.raises(TypeError):
            ConcreteStorage()

    def test_requires_query_data_stream(self):
        """Test that concrete classes must implement query_data_stream."""

        class ConcreteStorage(StorageBackend):
            def ingest_data(self, df: pd.DataFrame, project_name: str, **kwargs) -> str:
                return "test"

            def query_data(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                limit: Optional[int] = None,
                **kwargs,
            ) -> pd.DataFrame:
                return pd.DataFrame()

            def project_exists(self, project_id: str) -> bool:
                return False

            def delete_project(self, project_id: str) -> None:
                pass

        with pytest.raises(TypeError):
            ConcreteStorage()

    def test_requires_project_exists(self):
        """Test that concrete classes must implement project_exists."""

        class ConcreteStorage(StorageBackend):
            def ingest_data(self, df: pd.DataFrame, project_name: str, **kwargs) -> str:
                return "test"

            def query_data(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                limit: Optional[int] = None,
                **kwargs,
            ) -> pd.DataFrame:
                return pd.DataFrame()

            def query_data_stream(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                chunk_size: int = 10000,
                **kwargs,
            ) -> Generator[pd.DataFrame, None, None]:
                yield pd.DataFrame()

            def delete_project(self, project_id: str) -> None:
                pass

        with pytest.raises(TypeError):
            ConcreteStorage()

    def test_requires_delete_project(self):
        """Test that concrete classes must implement delete_project."""

        class ConcreteStorage(StorageBackend):
            def ingest_data(self, df: pd.DataFrame, project_name: str, **kwargs) -> str:
                return "test"

            def query_data(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                limit: Optional[int] = None,
                **kwargs,
            ) -> pd.DataFrame:
                return pd.DataFrame()

            def query_data_stream(
                self,
                project_id: str,
                region: Optional[dict[str, Any]] = None,
                snp_list: Optional[list[str]] = None,
                pval_threshold: Optional[float] = None,
                chunk_size: int = 10000,
                **kwargs,
            ) -> Generator[pd.DataFrame, None, None]:
                yield pd.DataFrame()

            def project_exists(self, project_id: str) -> bool:
                return False

        with pytest.raises(TypeError):
            ConcreteStorage()
