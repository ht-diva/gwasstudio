"""
GWASStudio Core Storage Module
===============================

This module provides storage backends for GWASStudio.
"""

from gwasstudio.core.storage.base import StorageBackend, StorageError
from gwasstudio.core.storage.metadata_store import MetadataStore, get_metadata_store

# # Import all storage backends
from gwasstudio.core.storage.mongodb import MongoDBError, MongoDBStorage

__all__ = [
    # Base classes
    "StorageBackend",
    "StorageError",
    # Metadata store
    "MetadataStore",
    "get_metadata_store",
    #
    # # MongoDB
    "MongoDBStorage",
    "MongoDBError",
    #
    # # TileDB (imported lazily by get_storage_backend to keep the package light)
    "TileDBStorageBackend",
    "TileDBError",
    #
    # # S3
    # "S3Storage",
    # "S3Error",
]


def get_storage_backend(backend_name: str, config, **kwargs):
    """
    Factory function to get a storage backend instance.

    Args:
        backend_name: Name of the backend ("tiledb", "mongo", "s3").
        config: GWASStudioConfig instance.
        **kwargs: Additional arguments for the backend
            (e.g. ``warehouse_uri``/``project``/``study`` for "tiledb").

    Returns:
        StorageBackend: Instance of the requested storage backend.

    Raises:
        ValueError: If the backend name is not recognized or not available.
    """
    name = (backend_name or "").strip().lower()

    if name in ("mongo", "mongodb"):
        return MongoDBStorage(config, **kwargs)

    if name == "tiledb":
        # Deferred import: importing gwasstudio.core (and the light CLI commands)
        # must not pull in TileDB or the CLI layer — see tests/unit/cli/test_lazy_cli.py.
        from gwasstudio.core.storage.tiledb import TileDBStorageBackend

        return TileDBStorageBackend(config, **kwargs)

    available = ["mongo", "mongodb", "tiledb"]
    raise ValueError(f"Unknown storage backend: {backend_name}. Available backends: {available}")
