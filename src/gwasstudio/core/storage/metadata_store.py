"""
GWASStudio Metadata Store
==========================

A thin, swappable interface for reading and writing dataset metadata.

The :class:`MetadataStore` interface exposes only the operations the
application actually needs: bulk-ingesting metadata documents, querying them,
and listing projects. Backends (MongoDB today, others tomorrow) implement the
interface, and :func:`get_metadata_store` is the factory used at application
setup to hand a concrete instance to the ingestion, query and authorization
services.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from gwasstudio.core.config import GWASStudioConfig


class MetadataStore(ABC):
    """Interface for a dataset-metadata store.

    Implementations must provide the three read/write operations below.
    ``close`` is optional and defaults to a no-op so backends without
    dedicated cleanup (e.g. in-memory or connection-less stores) need not
    override it.
    """

    @abstractmethod
    def bulk_store_metadata(self, metadata_list: list[dict[str, Any]]) -> None:
        """Insert or update a batch of metadata documents.

        Args:
            metadata_list: List of metadata dictionaries. Each document must
                contain the store's identifier key (e.g. ``data_id``).

        Raises:
            gwasstudio.core.exceptions.StorageError: If the bulk write fails.
        """

    @abstractmethod
    def query_metadata(self, query: dict[str, Any], **kwargs) -> list[dict[str, Any]]:
        """Return metadata documents matching ``query``.

        Args:
            query: A store-specific query (for MongoDB, a Mongo filter dict).
            **kwargs: Backend-specific options (e.g. ``limit``, ``skip``).

        Returns:
            list[dict[str, Any]]: Matching metadata documents.
        """

    @abstractmethod
    def list_projects(self, **kwargs) -> list[dict[str, Any]]:
        """Return every metadata document (all projects/studies).

        Args:
            **kwargs: Backend-specific options.

        Returns:
            list[dict[str, Any]]: All metadata documents.
        """

    def close(self) -> None:
        """Release any underlying resources (default no-op)."""


def get_metadata_store(config: GWASStudioConfig | None = None) -> MetadataStore:
    """Factory used at application setup to obtain a :class:`MetadataStore`.

    This is the single place where the concrete backend is chosen. The backend
    name is read from ``config.metadata_backend`` (default ``"mongo"``). New
    backends should be registered in :data:`_METADATA_BACKENDS` so that
    ingestion, query and authorization services stay backend-agnostic.

    Args:
        config: GWASStudio configuration. A default is created when omitted.

    Returns:
        MetadataStore: A concrete metadata-store instance.

    Raises:
        gwasstudio.core.exceptions.StorageError: If the backend name is unknown
            or the backend cannot be created.
    """
    # Deferred imports: avoid a circular import between the interface module
    # and the MongoDB backend (which imports this interface at module load).
    from gwasstudio.core.exceptions import StorageError
    from gwasstudio.core.storage.mongodb import MongoDBStorage

    if config is None:
        config = GWASStudioConfig()

    backends: dict[str, type] = {"mongo": MongoDBStorage, "mongodb": MongoDBStorage}
    backend_name = (config.metadata_backend or "mongo").strip().lower()
    if backend_name not in backends:
        available = sorted(backends)
        raise StorageError(f"Unknown metadata store backend: {backend_name!r}. Available backends: {available}")
    return backends[backend_name](config)
