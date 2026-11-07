"""
GWASStudio Metadata Store
==========================

A thin, swappable interface for reading and writing dataset metadata.

The :class:`MetadataStore` interface exposes only the operations the
application actually needs: bulk-ingesting metadata documents, querying them,
and listing projects. Backends (MongoDB today, others tomorrow) implement the
interface. The factory :func:`get_metadata_store` lives in
:mod:`gwasstudio.core.storage` (the composition root).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


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
