"""
Tests for GWASStudio Core Ingestion Module
============================================

Tests for the ingestion module in gwasstudio.core.ingestion.
Covers _document_generator, ingest_metadata, and process_metadata_dict
with extensive mocking of MongoDB dependencies.
"""

import copy
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

# -- Test helpers -----------------------------------------------------------


def _template(**overrides):
    """Helper to create a standard metadata template for testing."""
    t = {
        "project": "My Project",
        "study": "My Study",
        "file_path": "/tmp/test_file.parquet",
        "population": "EUR",
    }
    t.update(overrides)
    return t


# -- ProcessMetadataDict -----------------------------------------------------


class TestProcessMetadataDict:
    """Tests for process_metadata_dict function."""

    def test_basic_transformation(self, monkeypatch):
        """Test basic transformation of metadata dictionary."""
        from gwasstudio.core.ingestion import process_metadata_dict
        from gwasstudio.core.str_utils import lower_and_replace

        hg_mock = MagicMock()
        hg_mock.compute_hash.return_value = "test_hash_123"

        class MockHashing:
            _instance = None

            def __new__(cls, *a, **kw):
                return hg_mock

        monkeypatch.setattr("gwasstudio.core.ingestion.Hashing", MockHashing)

        tpl = _template()
        original = copy.deepcopy(tpl)
        result = process_metadata_dict(tpl)

        assert result is not None
        assert result["project"] == lower_and_replace(original["project"])
        assert result["study"] == lower_and_replace(original["study"])
        assert result["data_id"] == "test_hash_123"
        assert result["population"] == [original["population"]]
        assert "file_path" not in result

    def test_project_lowercase_and_underscore(self, monkeypatch):
        """Test that project is converted to lowercase with underscores."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(project="Some Project Name")
        result = process_metadata_dict(tpl)
        assert result["project"] == "some_project_name"

    def test_study_lowercase_and_underscore(self, monkeypatch):
        """Test that study is converted to lowercase with underscores."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(study="Another Study Title")
        result = process_metadata_dict(tpl)
        assert result["study"] == "another_study_title"

    def test_data_id_from_file_path(self, monkeypatch):
        """Test that data_id is computed from file path."""
        from gwasstudio.core.ingestion import process_metadata_dict

        hg_mock = MagicMock()
        hg_mock.compute_hash.return_value = "hash_test_data.parquet"

        class FakeHash:
            _instance = None

            def __new__(cls, *a, **kw):
                return hg_mock

        monkeypatch.setattr("gwasstudio.core.ingestion.Hashing", FakeHash)

        tpl = _template(file_path="/tmp/test_data.parquet")
        result = process_metadata_dict(tpl)
        assert result["data_id"] == "hash_test_data.parquet"

    def test_population_is_list(self, monkeypatch):
        """Test that population is wrapped in a list."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(population="EUR")
        result = process_metadata_dict(tpl)
        assert isinstance(result["population"], list)
        assert result["population"] == ["EUR"]

    def test_file_path_removed(self, monkeypatch):
        """Test that file_path is removed from result."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(file_path="/tmp/test.parquet")
        result = process_metadata_dict(tpl)
        assert "file_path" not in result

    def test_additional_fields_preserved(self, monkeypatch):
        """Test that additional fields in metadata are preserved."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(file_path="/tmp/test.parquet", extra_field="extra_value", another_field=123)
        result = process_metadata_dict(tpl)
        assert result["extra_field"] == "extra_value"
        assert result["another_field"] == 123

    def test_modifies_in_place(self, monkeypatch):
        """Test that process_metadata_dict modifies the input dict in place."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(file_path="/tmp/test.parquet")
        result = process_metadata_dict(tpl)
        assert result is tpl

    def test_population_none(self, monkeypatch):
        """Test handling of None population."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(population=None)
        result = process_metadata_dict(tpl)
        assert result["population"] == [None]

    def test_empty_population(self, monkeypatch):
        """Test handling of empty population."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(population="")
        result = process_metadata_dict(tpl)
        assert result["population"] == [""]

    def test_special_characters_in_project(self, monkeypatch):
        """Test project with special characters passes through correctly (lowercased)."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(project="Project@#$%Special")
        result = process_metadata_dict(tpl)
        # lower_and_replace converts to lowercase, so it becomes "project@#$%special"
        assert result["project"] == "project@#$%special"

    def test_unicode_characters(self, monkeypatch):
        """Test project with Unicode characters."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(project="研究项目")
        result = process_metadata_dict(tpl)
        assert result["project"] == "研究项目"

    def test_all_spaces_project(self, monkeypatch):
        """Test project with only spaces."""
        from gwasstudio.core.ingestion import process_metadata_dict

        _add_hashing_mock(monkeypatch)

        tpl = _template(project="   ")
        result = process_metadata_dict(tpl)
        assert result["project"] == "___"


# -- DocumentGenerator -------------------------------------------------------


class TestDocumentGenerator:
    """Tests for _document_generator generator function."""

    def test_generator_iteration(self, monkeypatch):
        """Test that _document_generator yields processed documents."""
        from gwasstudio.core.ingestion import _document_generator

        _add_hashing_mock(monkeypatch)

        tpls = [_template()]
        result = list(_document_generator(tpls))
        assert len(result) == 1
        assert isinstance(result[0], dict)
        assert "project" in result[0]
        assert "study" in result[0]
        assert "data_id" in result[0]
        assert "population" in result[0]

    def test_multiple_documents(self, monkeypatch):
        """Test that _document_generator processes multiple documents."""
        from gwasstudio.core.ingestion import _document_generator

        _add_hashing_mock(monkeypatch)

        tpls = [
            _template(project="Project1", study="Study1"),
            _template(project="Project2", study="Study2"),
            _template(project="Project3", study="Study3"),
        ]
        result = list(_document_generator(tpls))
        assert len(result) == 3
        assert result[0]["project"] == "project1"
        assert result[1]["project"] == "project2"
        assert result[2]["project"] == "project3"

    def test_empty_input(self, monkeypatch):
        """Test that _document_generator handles empty input."""
        from gwasstudio.core.ingestion import _document_generator

        _add_hashing_mock(monkeypatch)

        result = list(_document_generator([]))
        assert len(result) == 0

    def test_is_generator(self, monkeypatch):
        """Test that _document_generator returns a generator object."""
        from gwasstudio.core.ingestion import _document_generator

        _add_hashing_mock(monkeypatch)

        gen = _document_generator([_template()])
        assert hasattr(gen, "__next__")
        assert hasattr(gen, "__iter__")

    def test_lazy_evaluation(self, monkeypatch):
        """Test that _document_generator is lazy (not evaluated until consumed)."""
        from gwasstudio.core.ingestion import _document_generator

        hg_mock = MagicMock()
        hg_mock.compute_hash.return_value = "hash"

        class FakeHash:
            _instance = None

            def __new__(cls, *a, **kw):
                return hg_mock

        monkeypatch.setattr("gwasstudio.core.ingestion.Hashing", FakeHash)

        gen = _document_generator([_template(), _template(project="P2")])
        # At this point compute_hash should not have been called yet
        assert hg_mock.compute_hash.call_count == 0
        # Consume one item
        first = next(gen)
        assert hg_mock.compute_hash.call_count == 1
        # Consume second item
        second = next(gen)
        assert hg_mock.compute_hash.call_count == 2


# -- IngestMetadata ----------------------------------------------------------


class TestIngestMetadata:
    """Tests for ingest_metadata function."""

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_basic_ingestion(self, mock_hashing_cls, mock_mongo_cls):
        """Test basic metadata ingestion flow."""
        from gwasstudio.core import GWASStudioConfig
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "abc123"
        mock_hashing_cls.return_value = mock_hg

        # Don't mock GWASStudioConfig — let it be called
        templates = [_template()]
        ingest_metadata(templates)

        mock_mongo.bulk_store_metadata.assert_called_once()
        docs = mock_mongo.bulk_store_metadata.call_args[0][0]
        assert len(docs) == 1
        assert docs[0]["project"] == "my_project"
        assert docs[0]["data_id"] == "abc123"

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.GWASStudioConfig")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_with_config(self, mock_hashing_cls, mock_config_cls, mock_mongo_cls):
        """Test ingestion with provided configuration."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h1"
        mock_hashing_cls.return_value = mock_hg

        custom_config = MagicMock()
        mock_config_cls.return_value = custom_config

        templates = [_template()]
        ingest_metadata(templates, config=custom_config)

        # MongoDBStorage should be called with the provided config
        mock_mongo_cls.assert_called_once_with(custom_config)

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.GWASStudioConfig")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_without_config_creates_default(self, mock_hashing_cls, mock_config_cls, mock_mongo_cls):
        """Test that ingest_metadata creates a default config if none provided."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h1"
        mock_hashing_cls.return_value = mock_hg

        mock_cfg = MagicMock()
        mock_config_cls.return_value = mock_cfg

        templates = [_template()]
        ingest_metadata(templates)

        mock_config_cls.assert_called_once()

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_multiple_documents_ingested(self, mock_hashing_cls, mock_mongo_cls):
        """Test that multiple documents are collected and sent."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h"
        mock_hashing_cls.return_value = mock_hg

        templates = [
            _template(project="Project1", study="Study1"),
            _template(project="Project2", study="Study2"),
            _template(project="Project3", study="Study3"),
        ]
        ingest_metadata(templates)

        call_args = mock_mongo.bulk_store_metadata.call_args
        assert call_args is not None
        docs = call_args[0][0]
        assert len(docs) == 3
        assert docs[0]["project"] == "project1"
        assert docs[1]["project"] == "project2"
        assert docs[2]["project"] == "project3"

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_document_structure_after_ingestion(self, mock_hashing_cls, mock_mongo_cls):
        """Test that ingested documents have correct structure."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "d00d"
        mock_hashing_cls.return_value = mock_hg

        tpl = _template(project="Test Project", study="Test Study", file_path="/tmp/test.parquet", population="EUR")
        ingest_metadata([tpl])

        docs = mock_mongo.bulk_store_metadata.call_args[0][0]
        doc = docs[0]

        assert "project" in doc
        assert "study" in doc
        assert "data_id" in doc
        assert "population" in doc
        assert "file_path" not in doc
        assert doc["project"] == "test_project"
        assert doc["study"] == "test_study"
        assert doc["population"] == ["EUR"]
        assert doc["data_id"] == "d00d"

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_ingestion_error_on_mongo_failure(self, mock_hashing_cls, mock_mongo_cls):
        """Test that ingestion errors are raised when MongoDBStorage fails."""
        from gwasstudio.core.exceptions import IngestionError
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo.bulk_store_metadata.side_effect = Exception("MongoDB connection failed")
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h"
        mock_hashing_cls.return_value = mock_hg

        templates = [_template()]
        with pytest.raises(IngestionError) as exc_info:
            ingest_metadata(templates)

        assert "MongoDB connection failed" in str(exc_info.value)


# -- IngestionIntegration ----------------------------------------------------


class TestIngestionIntegration:
    """Integration tests for the ingestion module."""

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_full_ingestion_pipeline(self, mock_hashing_cls, mock_mongo_cls):
        """Test the full ingestion pipeline from input to MongoDB call."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.side_effect = lambda **kw: (
            f"hash_{kw.get('fpath', 'none').split('/')[-1]}" if kw.get("fpath") else "h"
        )
        mock_hashing_cls.return_value = mock_hg

        templates = [
            {
                "project": "GWAS Study 1",
                "study": "European Population",
                "file_path": "/data/study1.parquet",
                "population": "EUR",
                "trait": "Height",
                "sample_size": 100000,
            },
            {
                "project": "GWAS Study 2",
                "study": "Asian Population",
                "file_path": "/data/study2.parquet",
                "population": "EAS",
                "trait": "Weight",
                "sample_size": 200000,
            },
        ]

        ingest_metadata(templates)

        assert mock_mongo.bulk_store_metadata.called
        docs = mock_mongo.bulk_store_metadata.call_args[0][0]
        assert len(docs) == 2

        assert docs[0]["project"] == "gwas_study_1"
        assert docs[0]["study"] == "european_population"
        assert docs[0]["population"] == ["EUR"]
        assert docs[0]["trait"] == "Height"
        assert docs[0]["sample_size"] == 100000
        assert "file_path" not in docs[0]

        assert docs[1]["project"] == "gwas_study_2"
        assert docs[1]["study"] == "asian_population"
        assert docs[1]["population"] == ["EAS"]
        assert docs[1]["trait"] == "Weight"
        assert docs[1]["sample_size"] == 200000
        assert "file_path" not in docs[1]

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_empty_template_list(self, mock_hashing_cls, mock_mongo_cls):
        """Test ingestion with empty template list."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        ingest_metadata([])

        assert mock_mongo.bulk_store_metadata.called
        docs = mock_mongo.bulk_store_metadata.call_args[0][0]
        assert len(docs) == 0

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_long_file_path(self, mock_hashing_cls, mock_mongo_cls):
        """Test with very long file path."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h1"
        mock_hashing_cls.return_value = mock_hg

        long_path = "/very/long/" + "/path" * 100 + "/file.parquet"
        tpl = _template(file_path=long_path)
        ingest_metadata([tpl])
        assert mock_mongo.bulk_store_metadata.called


# -- EdgeCases ---------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases and error scenarios."""

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_special_characters_in_project(self, mock_hashing_cls, mock_mongo_cls):
        """Test project with special characters."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h"
        mock_hashing_cls.return_value = mock_hg

        tpl = _template(project="Project@#$%Special")
        ingest_metadata([tpl])

        docs = mock_mongo.bulk_store_metadata.call_args[0][0]
        # lower_and_replace converts to lowercase
        assert docs[0]["project"] == "project@#$%special"

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_unicode_characters(self, mock_hashing_cls, mock_mongo_cls):
        """Test project with Unicode characters."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h"
        mock_hashing_cls.return_value = mock_hg

        tpl = _template(project="研究项目")
        ingest_metadata([tpl])

        docs = mock_mongo.bulk_store_metadata.call_args[0][0]
        assert docs[0]["project"] == "研究项目"

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_long_file_path(self, mock_hashing_cls, mock_mongo_cls):
        """Test with very long file path."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = "h"
        mock_hashing_cls.return_value = mock_hg

        long_path = "/very/long/" + "/path" * 100 + "/file.parquet"
        tpl = _template(file_path=long_path)
        ingest_metadata([tpl])
        assert mock_mongo.bulk_store_metadata.called

    @patch("gwasstudio.core.ingestion.MongoDBStorage")
    @patch("gwasstudio.core.ingestion.Hashing")
    def test_missing_optional_fields(self, mock_hashing_cls, mock_mongo_cls):
        """Test template with missing optional fields raises as expected."""
        from gwasstudio.core.ingestion import ingest_metadata

        mock_mongo = MagicMock()
        mock_mongo_cls.return_value = mock_mongo

        mock_hg = MagicMock()
        mock_hg.compute_hash.return_value = None
        mock_hashing_cls.return_value = mock_hg

        minimal_tpl = {"project": "Test Project", "study": "Test Study"}
        # The current implementation calls metadata.pop("file_path") unconditionally,
        # which raises KeyError when file_path is missing.
        with pytest.raises(KeyError, match="file_path"):
            ingest_metadata([minimal_tpl])


# -- Test utilities ----------------------------------------------------------


@pytest.fixture
def mock_config():
    """Create a mock GWASStudioConfig for testing."""
    from gwasstudio.core import GWASStudioConfig

    config = MagicMock(spec=GWASStudioConfig)
    config.mongo_uri = "mongodb://localhost:27017"
    config.mongo_db_name = "test_db"
    return config


@pytest.fixture
def mock_mongo_storage():
    """Create a mock MongoDBStorage for testing."""
    storage = MagicMock()
    storage.bulk_store_metadata = MagicMock()
    return storage


def _add_hashing_mock(monkeypatch):
    """Helper to add a mocking Hashing class to the ingestion module."""
    hg_mock = MagicMock()
    hg_mock.compute_hash.return_value = "mock_hash"

    class FakeHash:
        _instance = None

        def __new__(cls, *a, **kw):
            return hg_mock

    monkeypatch.setattr("gwasstudio.core.ingestion.Hashing", FakeHash)
