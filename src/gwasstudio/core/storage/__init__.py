"""
GWASStudio Core Storage Module
===============================

This module provides storage backends for GWASStudio. It is the composition
root for both the data-storage layer (``StorageBackend``) and the
metadata layer (``MetadataStore``).
"""

from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.storage.base import StorageBackend, StorageError
from gwasstudio.core.storage.metadata_store import MetadataStore

__all__ = [
    # Base classes
    "StorageBackend",
    "StorageError",
    # Metadata store
    "MetadataStore",
    "get_metadata_store",
    #
    # # Backends (import from their submodules for lazy loading)
    # "MongoDBStorage",       # gwasstudio.core.storage.mongodb
    # "MongoDBError",         # gwasstudio.core.storage.mongodb
    # "TileDBStorageBackend", # gwasstudio.core.storage.tiledb
    # "TileDBError",          # gwasstudio.core.storage.tiledb
    #
    # # S3
    # "S3Storage",
    # "S3Error",
]


def get_metadata_store(config: GWASStudioConfig | None = None) -> MetadataStore:
    """
    Factory used at application setup to obtain a :class:`MetadataStore`.

    This is the single place where the concrete metadata backend is chosen.
    The backend name is read from ``config.metadata_backend`` (default
    ``"mongo"``). New backends should be registered in :data:`_METADATA_BACKENDS`
    so that ingestion, query and authorization services stay backend-agnostic.

    Args:
        config: GWASStudio configuration. A default is created when omitted.

    Returns:
        MetadataStore: A concrete metadata-store instance.

    Raises:
        StorageError: If the backend name is unknown or the backend cannot be
            created.
    """
    # Deferred import: keep the package importable without pymongo.
    from gwasstudio.core.storage.mongodb import MongoDBStorage

    if config is None:
        config = GWASStudioConfig()

    backends: dict[str, type] = {"mongo": MongoDBStorage, "mongodb": MongoDBStorage}
    backend_name = (config.metadata_backend or "mongo").strip().lower()
    if backend_name not in backends:
        available = sorted(backends)
        raise StorageError(f"Unknown metadata store backend: {backend_name!r}. Available backends: {available}")
    return backends[backend_name](config)


def get_storage_backend(config: GWASStudioConfig | None = None, **kwargs) -> StorageBackend:
    """
    Factory used at application setup to obtain a :class:`StorageBackend`.

    This is the single place where the concrete data-storage backend is chosen.
    The backend name is read from ``config.storage_backend`` (default
    ``"tiledb"``). New backends should be registered in :data:`_STORAGE_BACKENDS`
    so that ingest and export services stay backend-agnostic.

    Args:
        config: GWASStudio configuration. A default is created when omitted.
        **kwargs: Additional arguments forwarded to the backend constructor
            (e.g. ``warehouse_uri``/``project``/``study`` for TileDB).

    Returns:
        StorageBackend: A concrete storage-backend instance.

    Raises:
        StorageError: If the backend name is unknown or the backend cannot be
            created.
    """
    # Deferred import: importing gwasstudio.core (and the light CLI commands)
    # must not pull in TileDB or the CLI layer — see tests/unit/cli/test_lazy_cli.py.
    from gwasstudio.core.storage.tiledb import TileDBStorageBackend

    if config is None:
        config = GWASStudioConfig()

    backends: dict[str, type] = {"tiledb": TileDBStorageBackend}
    backend_name = (config.storage_backend or "tiledb").strip().lower()
    if backend_name not in backends:
        available = sorted(backends)
        raise StorageError(
            f"Unknown storage backend: {backend_name!r}. "
            f"Available backends: {available}. "
            "For the metadata store, use get_metadata_store()."
        )
    return backends[backend_name](config, **kwargs)
