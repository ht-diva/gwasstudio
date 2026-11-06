"""
GWASStudio MongoDB Metadata Store
===================================

This module provides the MongoDB implementation of the
:class:`gwasstudio.core.storage.metadata_store.MetadataStore` interface.

MongoDB is used to store GWASStudio dataset metadata. Large genomic data is
stored elsewhere (e.g. TileDB/S3), so this backend only manages the metadata
collection.
"""

from __future__ import annotations

from typing import Any

import pymongo

from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.storage.base import StorageError
from gwasstudio.core.storage.metadata_store import MetadataStore


class MongoDBError(StorageError):
    """Exception raised for MongoDB-specific errors."""

    pass


class MongoDBStorage(MetadataStore):
    """
    MongoDB metadata store for GWASStudio.

    This class implements the :class:`MetadataStore` interface (``bulk_store_metadata``,
    ``query_metadata``, ``list_projects``, ``close``) against a single MongoDB
    collection, making it a drop-in metadata store for the ingestion, query and
    authorization services.
    """

    def __init__(self, config: GWASStudioConfig):
        """
        Initialize the MongoDB metadata store.

        Args:
            config: GWASStudio configuration.
        """
        self.config = config
        self._mongo_config = config.mongo
        self._client = self._setup_mongo_client()
        self._db = self._client[self._mongo_config.db_name]
        self._projects_collection = self._db[self._mongo_config.collection]

    def _setup_mongo_client(self) -> pymongo.MongoClient:
        """
        Set up MongoDB client from configuration.

        Returns:
            pymongo.MongoClient: MongoDB client instance.

        Raises:
            MongoDBError: If client setup fails.
        """
        try:
            uri = self._mongo_config.uri or "mongodb://localhost:27017"
            return pymongo.MongoClient(uri, maxPoolSize=50)
        except Exception as e:
            raise MongoDBError(f"Failed to set up MongoDB client: {str(e)}")

    def project_exists(self, project_id: str) -> bool:
        """
        Check if a project exists in MongoDB storage.

        Args:
            project_id: Unique identifier for the project.

        Returns:
            bool: True if the project exists, False otherwise.
        """
        return bool(self.get_metadata(project_id))

    def delete_project(self, project_id: str) -> None:
        """
        Delete a project's metadata document.

        Args:
            project_id: Unique identifier for the project.

        Raises:
            MongoDBError: If deletion fails.
        """
        try:
            self._projects_collection.delete_one({"project_id": project_id})
        except Exception as e:
            raise MongoDBError(f"Failed to delete project {project_id}: {str(e)}")

    def store_metadata(self, project_id: str, metadata: dict[str, Any]) -> None:
        """
        Store metadata for a project.

        Args:
            project_id: Unique identifier for the project.
            metadata: Metadata dictionary to store.

        Raises:
            MongoDBError: If storing metadata fails.
        """
        try:
            metadata["project_id"] = project_id
            self._projects_collection.update_one(
                {"project_id": project_id},
                {"$set": metadata},
                upsert=True,
            )
        except Exception as e:
            raise MongoDBError(f"Failed to store metadata for project {project_id}: {str(e)}")

    def bulk_store_metadata(self, metadata_list: list[dict[str, Any]]) -> None:
        """
        Insert or update multiple metadata documents in bulk.

        Args:
            metadata_list: List of metadata dictionaries to store.
                         Each dict should contain a 'data_id' key.

        Raises:
            MongoDBError: If bulk operation fails.
        """
        if not metadata_list:
            return

        try:
            # Prepare bulk operations
            operations = []
            for metadata in metadata_list:
                if "data_id" not in metadata:
                    raise MongoDBError("Each metadata document must contain a 'data_id'")

                operations.append(
                    pymongo.UpdateOne(
                        {"data_id": metadata["data_id"]},
                        {"$set": metadata},
                        upsert=True,
                    )
                )

            # Execute bulk write operation
            if operations:
                self._projects_collection.bulk_write(operations)
        except Exception as e:
            raise MongoDBError(f"Failed to bulk store metadata: {str(e)}")

    def get_metadata(self, project_id: str) -> dict[str, Any] | None:
        """
        Get metadata for a project.

        Args:
            project_id: Unique identifier for the project.

        Returns:
            dict: Metadata dictionary, or None if project not found.
        """
        return self._projects_collection.find_one({"project_id": project_id})

    def query_metadata(self, query: dict[str, Any], **kwargs) -> list[dict[str, Any]]:
        """
        Query metadata for projects.

        Args:
            query: MongoDB query dictionary.
            **kwargs: Additional arguments for the query.

        Returns:
            list: List of metadata dictionaries matching the query.
        """
        return list(self._projects_collection.find(query, **kwargs))

    def list_projects(self, **kwargs) -> list[dict[str, Any]]:
        """
        List all projects.

        Args:
            **kwargs: Additional arguments for the query.

        Returns:
            list: List of all project metadata dictionaries.
        """
        return list(self._projects_collection.find({}, {"_id": 0}, **kwargs))

    def get_collection_name(self, project_id: str) -> str:
        """
        Get the collection name for a project.

        Args:
            project_id: Unique identifier for the project.

        Returns:
            str: Collection name for the project.
        """
        return f"project_{project_id}"

    def close(self) -> None:
        """Close the MongoDB connection."""
        self._client.close()
