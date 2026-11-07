"""
Tests for the TileDB Storage Backend (core.storage.tiledb)
==========================================================

These tests exercise :class:`gwasstudio.core.storage.tiledb.TileDBStorageBackend`
against a local-filesystem warehouse (no S3, no Dask, no MongoDB) and verify that
it delegates to the existing CLI/TileDB code paths with the same behaviour.
"""

import gzip
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.exceptions import GWASStudioError
from gwasstudio.core.hashing import Hashing
from gwasstudio.core.storage import get_storage_backend
from gwasstudio.core.storage.base import StorageBackend, StorageError
from gwasstudio.core.storage.tiledb import TileDBError, TileDBStorageBackend, _build_regions_df


@pytest.fixture()
def config() -> GWASStudioConfig:
    return GWASStudioConfig()


@pytest.fixture()
def warehouse(tmp_path) -> str:
    return str(tmp_path / "warehouse" / "project" / "study")


@pytest.fixture()
def backend(config, warehouse) -> TileDBStorageBackend:
    return TileDBStorageBackend(config, warehouse_uri=warehouse)


def _sample_df() -> pd.DataFrame:
    """Four variants across two chromosomes (z-scores: 4.0, 2.0, 2.0, 2.0)."""
    return pd.DataFrame(
        {
            "CHR": np.array([1, 1, 2, 2], dtype=np.uint8),
            "POS": np.array([100, 200, 300, 400], dtype=np.uint32),
            "EA": ["A", "G", "C", "T"],
            "NEA": ["T", "A", "G", "A"],
            "EAF": np.array([0.4, 0.3, 0.2, 0.1], dtype=np.float32),
            "SE": np.array([0.5, 0.5, 0.4, 0.2], dtype=np.float32),
            "BETA": np.array([2.0, 1.0, 0.8, 0.4], dtype=np.float32),
        }
    )


class TestTileDBError:
    def test_inheritance(self):
        error = TileDBError("boom")
        assert isinstance(error, StorageError)
        assert isinstance(error, GWASStudioError)


class TestFactory:
    def test_returns_tiledb_backend(self, config, warehouse):
        backend = get_storage_backend(config, warehouse_uri=warehouse)
        assert isinstance(backend, TileDBStorageBackend)
        assert isinstance(backend, StorageBackend)

    def test_name_is_case_insensitive(self, config, warehouse):
        config.storage_backend = "TileDB"
        assert isinstance(get_storage_backend(config, warehouse_uri=warehouse), TileDBStorageBackend)

    def test_mongo_not_a_data_storage_backend(self, config):
        """MongoDB is a metadata store, not a data storage backend."""
        config.storage_backend = "mongo"
        with pytest.raises(StorageError, match="get_metadata_store"):
            get_storage_backend(config)

    def test_unknown_backend_raises(self, config):
        config.storage_backend = "iceberg"
        with pytest.raises(StorageError, match="Unknown storage backend"):
            get_storage_backend(config)


class TestUriResolution:
    def test_requires_a_uri_source(self, config):
        backend = TileDBStorageBackend(config)
        with pytest.raises(TileDBError, match="Cannot resolve a TileDB array URI"):
            backend.query_data("some_trait")

    def test_uri_kwarg_overrides_constructor(self, config, tmp_path):
        uri = str(tmp_path / "elsewhere")
        backend = TileDBStorageBackend(config, warehouse_uri=str(tmp_path / "ignored"))
        assert backend._resolve_uri(uri) == uri

    def test_composes_project_study_uri(self, config, tmp_path):
        base = str(tmp_path / "warehouse")
        backend = TileDBStorageBackend(config, warehouse_uri=base, project="proj", study="stud")
        assert backend._resolve_uri() == f"{base}/proj/stud"

    def test_project_exists_reflects_filesystem(self, config, tmp_path):
        uri = str(tmp_path / "never" / "created")
        backend = TileDBStorageBackend(config, warehouse_uri=uri)
        assert backend.project_exists("x") is False


class TestIngestQueryRoundTrip:
    def test_ingest_dataframe_returns_trait_id(self, backend):
        trait = backend.ingest_data(_sample_df(), "test_trait")
        assert trait == "test_trait"

    def test_ingest_honours_preexisting_traitid(self, backend):
        df = _sample_df()
        df["TRAITID"] = "preset_id"
        assert backend.ingest_data(df, "ignored") == "preset_id"

    def test_query_full_stats(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data("test_trait")

        assert len(df) == 4
        # process_dataframe contract: SNPID first, TRAITID dropped, MLOG10P present
        # (computed from BETA/SE via the MLOG10P-attribute fallback path).
        assert df.columns[0] == "SNPID"
        assert "TRAITID" not in df.columns
        assert "MLOG10P" in df.columns
        assert set(df["SNPID"]) == {"1:100:A:T", "1:200:G:A", "2:300:C:G", "2:400:T:A"}

    def test_query_region(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data("test_trait", region={"CHR": 1, "START": 90, "END": 210})
        assert sorted(df["POS"].tolist()) == [100, 200]

    def test_query_multiple_regions(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data(
            "test_trait",
            region=[
                {"chr": 1, "start": 90, "end": 210},
                {"chr": 2, "start": 390, "end": 410},
            ],
        )
        assert sorted(df["POS"].tolist()) == [100, 200, 400]

    def test_query_snp_list(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data("test_trait", snp_list=[(1, 200), (2, 400)])
        assert sorted(df["POS"].tolist()) == [200, 400]

    def test_query_snp_list_string_form(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data("test_trait", snp_list=["1:200:G:A"])
        assert sorted(df["POS"].tolist()) == [200]

    def test_query_limit(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data("test_trait", limit=2)
        assert len(df) == 2

    def test_query_pvalue_threshold(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        # Only row 1 (z=4.0, MLOG10P≈5.6) passes a -log10(p) threshold of 3.0.
        df = backend.query_data("test_trait", pval_threshold=3.0)
        assert list(df["POS"].tolist()) == [100]

    def test_query_stream_chunks(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        chunks = list(backend.query_data_stream("test_trait", chunk_size=3))
        assert [len(chunk) for chunk in chunks] == [3, 1]
        assert sum(len(chunk) for chunk in chunks) == 4

    def test_query_stream_empty(self, backend):
        # Array exists but the trait has no rows.
        backend.ingest_data(_sample_df(), "other_trait")
        assert list(backend.query_data_stream("missing_trait")) == []

    def test_project_exists_after_ingest(self, backend):
        assert backend.project_exists("test_trait") is False
        backend.ingest_data(_sample_df(), "test_trait")
        assert backend.project_exists("test_trait") is True

    def test_delete_project(self, backend, warehouse):
        backend.ingest_data(_sample_df(), "test_trait")
        assert backend.project_exists("test_trait") is True

        backend.delete_project("test_trait")

        assert backend.project_exists("test_trait") is False
        assert not Path(warehouse).exists()


class TestFileBasedIngestion:
    """The df=None path must delegate to the CLI pipeline (process_and_ingest)."""

    def test_ingest_from_tsv_gz(self, config, tmp_path):
        file_path = tmp_path / "study-a.tsv.gz"
        df = _sample_df()
        with gzip.open(file_path, "wt") as f:
            f.write(df.to_csv(sep="\t", index=False))

        warehouse = str(tmp_path / "warehouse" / "project" / "study")
        backend = TileDBStorageBackend(config, warehouse_uri=warehouse)

        trait = backend.ingest_data(None, "ignored", file_path=str(file_path))
        assert trait == Hashing().compute_hash(str(file_path))

        result = backend.query_data(trait)
        assert len(result) == 4
        assert set(result["SNPID"]) == {"1:100:A:T", "1:200:G:A", "2:300:C:G", "2:400:T:A"}


class TestIngestValidation:
    def test_requires_df_or_file_path(self, backend):
        with pytest.raises(TileDBError, match="requires a DataFrame or a 'file_path'"):
            backend.ingest_data(None, "test_trait")

    def test_derives_traitid_from_file_path_kwarg(self, backend, tmp_path):
        file_path = tmp_path / "study-b.tsv.gz"
        with gzip.open(file_path, "wt") as f:
            f.write(_sample_df().to_csv(sep="\t", index=False))
        trait = backend.ingest_data(_sample_df(), "ignored", file_path=str(file_path))
        assert trait == Hashing().compute_hash(str(file_path))

    def test_empty_region_returns_empty_frame(self, backend):
        backend.ingest_data(_sample_df(), "test_trait")
        df = backend.query_data("test_trait", region=[])
        assert df.empty


class TestBuildRegionsDf:
    def test_empty(self):
        df = _build_regions_df(None, None)
        assert df.empty
        assert list(df.columns) == ["CHR", "START", "END"]

    def test_regions_and_snps(self):
        df = _build_regions_df({"CHR": 1, "START": 10, "END": 20}, [(2, 30), {"chr": 3, "pos": 40}, "4:50:A:T"])
        assert df["START"].tolist() == [10, 30, 40, 50]
        # SNP rows must trigger extract_regions_snps' SNP mode (END == START + 1).
        assert ((df["END"] == df["START"] + 1).tolist()) == [False, True, True, True]
