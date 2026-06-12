"""
Pytest Fixtures for GWASStudio Core Tests
=========================================

This module provides shared fixtures for testing the GWASStudio core module.
"""

import tempfile
from pathlib import Path
from typing import Generator

import numpy as np
import pandas as pd
import pytest

from gwasstudio.core import DaskConfig, GWASStudioConfig, MongoConfig, S3Config, TileDBConfig, VaultConfig

# --- Fixtures for Configuration ---


@pytest.fixture
def base_config() -> GWASStudioConfig:
    """Provide a base GWASStudioConfig for testing."""
    return GWASStudioConfig(
        dask=DaskConfig(deployment="local", workers=2),
        mongo=MongoConfig(db_name="test_gwas"),
        s3=S3Config(verify_ssl=False),
        vault=VaultConfig(auth="basic"),
        tiledb=TileDBConfig(),
        log_level="DEBUG",
    )


@pytest.fixture
def temp_data_dir() -> Generator[Path, None, None]:
    """Provide a temporary directory for test data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def sample_gwas_dataframe() -> pd.DataFrame:
    """Provide a sample DataFrame with GWAS data for testing."""
    np.random.seed(42)
    n = 1000
    return pd.DataFrame(
        {
            "snp": [f"rs{i}" for i in range(n)],
            "chr": np.random.choice(["1", "2", "3", "X"], n),
            "pos": np.random.randint(100000, 1000000, n),
            "pval": np.random.uniform(0, 1, n),
            "beta": np.random.randn(n),
            "se": np.random.uniform(0.1, 0.5, n),
            "a1": np.random.choice(["A", "C", "G", "T"], n),
            "a2": np.random.choice(["A", "C", "G", "T"], n),
        }
    )


@pytest.fixture
def sample_parquet_file(temp_data_dir: Path, sample_gwas_dataframe: pd.DataFrame) -> Path:
    """Provide a temporary Parquet file with sample GWAS data."""
    file_path = temp_data_dir / "test_data.parquet"
    sample_gwas_dataframe.to_parquet(file_path)
    return file_path


@pytest.fixture
def sample_csv_file(temp_data_dir: Path, sample_gwas_dataframe: pd.DataFrame) -> Path:
    """Provide a temporary CSV file with sample GWAS data."""
    file_path = temp_data_dir / "test_data.csv"
    sample_gwas_dataframe.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def sample_metadata() -> dict:
    """Provide sample metadata for testing."""
    return {
        "name": "Test GWAS",
        "description": "A test GWAS dataset",
        "author": "Test User",
        "date": "2026-06-05",
        "num_samples": 10000,
        "num_snps": 500000,
    }


@pytest.fixture
def invalid_config() -> GWASStudioConfig:
    """Provide an invalid configuration for testing error handling."""
    return GWASStudioConfig(
        dask=DaskConfig(deployment="invalid"),
        mongo=MongoConfig(uri="mongodb://invalid-host:9999"),
    )


# --- Fixtures for Storage Backends ---


@pytest.fixture
def mock_tiledb_storage(base_config: GWASStudioConfig, temp_data_dir: Path, monkeypatch) -> Generator:
    """Provide a mocked TileDBStorage for testing."""
    from unittest.mock import patch

    from gwasstudio.core.storage.tiledb import TileDBStorage

    # Mock TileDB functions
    with (
        patch("tiledb.Array.create"),
        patch("tiledb.open"),
        patch("tiledb.Dim"),
        patch("tiledb.Domain"),
        patch("tiledb.Attr"),
        patch("tiledb.ArraySchema"),
    ):
        # Set data directory to temp dir
        base_config.data_dir = temp_data_dir
        storage = TileDBStorage(base_config)
        yield storage


@pytest.fixture
def mock_mongo_storage(base_config: GWASStudioConfig) -> Generator:
    """Provide a MongoDBStorage instance with mongomock for testing."""
    from gwasstudio.core.storage.mongo import MongoDBStorage

    # Use mongomock for testing
    base_config.mongo.deployment = "embedded"
    storage = MongoDBStorage(base_config)
    yield storage

    # Clean up
    storage._client.drop_database(base_config.mongo.db_name)
    storage.close()


@pytest.fixture
def mock_s3_storage(base_config: GWASStudioConfig) -> Generator:
    """Provide a mocked S3Storage for testing."""
    from unittest.mock import MagicMock, patch

    from gwasstudio.core.storage.s3 import S3Storage

    with patch("boto3.client") as mock_client:
        mock_s3 = MagicMock()
        mock_client.return_value = mock_s3
        storage = S3Storage(base_config)
        yield storage


# --- Fixtures for Test Data ---


@pytest.fixture
def region_dict() -> dict:
    """Provide a sample region dictionary for testing."""
    return {"chr": "1", "start": 100000, "end": 200000}


@pytest.fixture
def snp_list() -> list:
    """Provide a sample list of SNP IDs for testing."""
    return ["rs1", "rs2", "rs3", "rs4", "rs5"]


@pytest.fixture
def empty_dataframe() -> pd.DataFrame:
    """Provide an empty DataFrame with GWAS columns."""
    return pd.DataFrame(
        {
            "snp": pd.Series([], dtype=str),
            "chr": pd.Series([], dtype=str),
            "pos": pd.Series([], dtype=int),
            "pval": pd.Series([], dtype=float),
        }
    )


@pytest.fixture
def invalid_dataframe() -> pd.DataFrame:
    """Provide an invalid DataFrame (missing required columns)."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [0.1, 0.2, 0.3],
        }
    )
