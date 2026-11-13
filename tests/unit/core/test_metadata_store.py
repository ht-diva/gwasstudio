"""
Tests for the MetadataStore interface and get_metadata_store factory.
=====================================================================

Covers the config-driven backend selector in
:func:`gwasstudio.core.storage.metadata_store.get_metadata_store`.
"""

import pytest

from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.exceptions import StorageError
from gwasstudio.core.storage import get_metadata_store
from gwasstudio.core.storage.metadata_store import MetadataStore
from gwasstudio.core.storage.mongodb import MongoDBStorage


class TestMetadataStoreInterface:
    """Tests for the MetadataStore abstract interface."""

    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            MetadataStore()

    def test_declares_abstract_methods(self):
        assert {"bulk_store_metadata", "query_metadata", "list_projects"} <= set(
            getattr(MetadataStore, "__abstractmethods__", set())
        )

    def test_mongodb_storage_is_a_metadata_store(self):
        assert issubclass(MongoDBStorage, MetadataStore)


class TestGetMetadataStoreFactory:
    """Tests for the get_metadata_store backend selector."""

    def test_default_backend_returns_mongo(self):
        store = get_metadata_store(GWASStudioConfig())
        assert isinstance(store, MongoDBStorage)
        assert isinstance(store, MetadataStore)

    def test_none_config_uses_default(self):
        store = get_metadata_store(None)
        assert isinstance(store, MongoDBStorage)

    @pytest.mark.parametrize("name", ["mongo", "mongodb"])
    def test_known_backends(self, name):
        store = get_metadata_store(GWASStudioConfig(metadata_backend=name))
        assert isinstance(store, MongoDBStorage)

    def test_backend_name_case_and_whitespace_insensitive(self):
        store = get_metadata_store(GWASStudioConfig(metadata_backend=" Mongo "))
        assert isinstance(store, MongoDBStorage)

    @pytest.mark.parametrize("name", ["duckdb", "iceberg"])
    def test_unknown_backend_raises(self, name):
        with pytest.raises(StorageError):
            get_metadata_store(GWASStudioConfig(metadata_backend=name))

    def test_empty_backend_falls_back_to_mongo(self):
        assert isinstance(get_metadata_store(GWASStudioConfig(metadata_backend="")), MongoDBStorage)


class TestConfigBackendField:
    """Tests for the metadata_backend config field round-trip."""

    def test_default_value(self):
        assert GWASStudioConfig().metadata_backend == "mongo"

    def test_to_dict_includes_backend(self):
        assert GWASStudioConfig(metadata_backend="mongodb").to_dict()["metadata_backend"] == "mongodb"

    def test_from_dict_roundtrip(self):
        restored = GWASStudioConfig.from_dict(GWASStudioConfig(metadata_backend="mongodb").to_dict())
        assert restored.metadata_backend == "mongodb"

    def test_from_dict_defaults_to_mongo(self):
        assert GWASStudioConfig.from_dict({}).metadata_backend == "mongo"
