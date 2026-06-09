"""
GWASStudio Core Storage Module
===============================

This module provides storage backends for GWASStudio.
It includes implementations for TileDB, MongoDB, S3, and Apache Iceberg.
"""

from gwasstudio.core.storage.base import StorageBackend, StorageError

# # Import all storage backends
# from gwasstudio.core.storage.tiledb import TileDBStorage, TileDBError
from gwasstudio.core.storage.mongodb import MongoDBStorage, MongoDBError
# from gwasstudio.core.storage.s3 import S3Storage, S3Error
#
# # Import Iceberg backend if available
# try:
#     from gwasstudio.core.storage.iceberg import IcebergStorage, IcebergError, IcebergConfig
#     ICEBERG_AVAILABLE = True
# except ImportError:
#     IcebergStorage = None
#     IcebergError = None
#     IcebergConfig = None
#     ICEBERG_AVAILABLE = False

__all__ = [
    # Base classes
    "StorageBackend",
    "StorageError",
    #
    # # TileDB
    # "TileDBStorage",
    # "TileDBError",
    #
    # # MongoDB
    "MongoDBStorage",
    "MongoDBError",
    #
    # # S3
    # "S3Storage",
    # "S3Error",
    #
    # # Iceberg (if available)
    # "IcebergStorage",
    # "IcebergError",
    # "IcebergConfig",
    # "ICEBERG_AVAILABLE",
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
        # "tiledb": TileDBStorage,
        "mongo": MongoDBStorage,
        # "s3": S3Storage,
    }

    # Add Iceberg if available
    # if ICEBERG_AVAILABLE:
    #     backends["iceberg"] = IcebergStorage

    if backend_name not in backends:
        available = list(backends.keys())
        raise ValueError(f"Unknown storage backend: {backend_name}. Available backends: {available}")

    return backends[backend_name](config, **kwargs)
