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
    # # S3
    # "S3Storage",
    # "S3Error",
]


def get_storage_backend(backend_name: str, config, **kwargs):
    """
    Factory function to get a storage backend instance.

    Args:
        backend_name: Name of the backend ("tiledb", "mongo", "s3", "iceberg").
        config: GWASStudioConfig instance.
        **kwargs: Additional arguments for the backend.

    Returns:
        StorageBackend: Instance of the requested storage backend.

    Raises:
        ValueError: If the backend name is not recognized or not available.
    """
    backends = {
        "mongo": MongoDBStorage,
        # "s3": S3Storage,
    }

    if backend_name not in backends:
        available = list(backends.keys())
        raise ValueError(f"Unknown storage backend: {backend_name}. Available backends: {available}")

    return backends[backend_name](config, **kwargs)
