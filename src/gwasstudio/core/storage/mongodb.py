"""
GWASStudio MongoDB Storage Module
===================================

This module provides the MongoDB storage backend for GWASStudio.
MongoDB is used for storing metadata and, optionally, genomic data.
"""

import uuid
from typing import Optional, Dict, Any, List, Generator
import pandas as pd
import pymongo
from gridfs import GridFS

from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.storage.base import StorageBackend, StorageError


class MongoDBError(StorageError):
    """Exception raised for MongoDB-specific errors."""

    pass


class MongoDBStorage(StorageBackend):
    """
    MongoDB storage backend for GWASStudio.

    This class implements the StorageBackend interface using MongoDB for
    storing metadata and genomic data. It uses GridFS for large datasets.
    """

    def __init__(self, config: GWASStudioConfig):
        """
        Initialize the MongoDB storage backend.

        Args:
            config: GWASStudio configuration.
        """
        self.config = config
        self._mongo_config = config.mongo
        self._client = self._setup_mongo_client()
        self._db = self._client[self._mongo_config.db_name]
        self._fs = GridFS(self._db)
        self._projects_collection = self._db["metadata"]

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

    def ingest_data(
        self,
        df: pd.DataFrame,
        project_name: str,
        **kwargs,
    ) -> str:
        """
        Ingest a DataFrame into MongoDB storage.

        Args:
            df: DataFrame with genomic data.
            project_name: Name of the project/dataset.
            **kwargs: Additional arguments (ignored).

        Returns:
            str: Unique identifier for the ingested project.

        Raises:
            MongoDBError: If ingestion fails.
        """
        project_id = str(uuid.uuid4())

        try:
            # Store data in GridFS (for large datasets)
            data_json = df.to_json(orient="records")
            file_id = self._fs.put(data_json.encode(), filename=f"{project_id}.json")

            # Store metadata
            metadata = {
                "project_id": project_id,
                "name": project_name,
                "data_file_id": file_id,
                "num_records": len(df),
                "columns": list(df.columns),
                **kwargs,
            }
            self.store_metadata(project_id, metadata)

            return project_id
        except Exception as e:
            raise MongoDBError(f"Failed to ingest data into MongoDB: {str(e)}")

    def query_data(
        self,
        project_id: str,
        region: Optional[Dict[str, Any]] = None,
        snp_list: Optional[List[str]] = None,
        pval_threshold: Optional[float] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Query data from MongoDB storage.

        Args:
            project_id: Unique identifier for the project.
            region: Genomic region to query (e.g., {"chr": "1", "start": 100000, "end": 200000}).
            snp_list: List of SNP IDs to query.
            pval_threshold: Only return variants with p-value <= threshold.
            limit: Maximum number of records to return.
            **kwargs: Additional arguments (ignored).

        Returns:
            pd.DataFrame: DataFrame with the query results.

        Raises:
            MongoDBError: If the query fails.
        """
        metadata = self.get_metadata(project_id)
        if not metadata:
            raise MongoDBError(f"Project {project_id} not found in MongoDB storage")

        try:
            # Retrieve data from GridFS
            data_json = self._fs.get(metadata["data_file_id"]).read().decode()
            df = pd.read_json(data_json, orient="records")

            # Apply filters
            if region:
                if "chr" in df.columns and region.get("chr"):
                    chr_cond = df["chr"] == region["chr"]
                    if region.get("start") is not None and "pos" in df.columns:
                        chr_cond &= df["pos"] >= region["start"]
                    if region.get("end") is not None and "pos" in df.columns:
                        chr_cond &= df["pos"] <= region["end"]
                    df = df[chr_cond]

            if snp_list and "snp" in df.columns:
                df = df[df["snp"].isin(snp_list)]

            if pval_threshold is not None and "pval" in df.columns:
                df = df[df["pval"] <= pval_threshold]

            if limit is not None:
                df = df.head(limit)

            return df
        except Exception as e:
            raise MongoDBError(f"Failed to query MongoDB data: {str(e)}")

    def query_data_stream(
        self,
        project_id: str,
        region: Optional[Dict[str, Any]] = None,
        snp_list: Optional[List[str]] = None,
        pval_threshold: Optional[float] = None,
        chunk_size: int = 10000,
        **kwargs,
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Query data from MongoDB storage in chunks (streaming).

        Args:
            project_id: Unique identifier for the project.
            region: Genomic region to query.
            snp_list: List of SNP IDs to query.
            pval_threshold: Only return variants with p-value <= threshold.
            chunk_size: Number of records per chunk.
            **kwargs: Additional arguments (ignored).

        Yields:
            pd.DataFrame: Chunks of DataFrames with query results.

        Raises:
            MongoDBError: If the query fails.
        """
        df = self.query_data(
            project_id,
            region=region,
            snp_list=snp_list,
            pval_threshold=pval_threshold,
            **kwargs,
        )
        for i in range(0, len(df), chunk_size):
            yield df.iloc[i : i + chunk_size]

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
        Delete a project from MongoDB storage.

        Args:
            project_id: Unique identifier for the project.

        Raises:
            MongoDBError: If deletion fails.
        """
        metadata = self.get_metadata(project_id)
        if metadata:
            try:
                self._fs.delete(metadata["data_file_id"])
                self._projects_collection.delete_one({"project_id": project_id})
            except Exception as e:
                raise MongoDBError(f"Failed to delete project {project_id}: {str(e)}")

    def store_metadata(self, project_id: str, metadata: Dict[str, Any]) -> None:
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

    def get_metadata(self, project_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a project.

        Args:
            project_id: Unique identifier for the project.

        Returns:
            dict: Metadata dictionary, or None if project not found.
        """
        return self._projects_collection.find_one({"project_id": project_id})

    def query_metadata(self, query: Dict[str, Any], **kwargs) -> List[Dict[str, Any]]:
        """
        Query metadata for projects.

        Args:
            query: MongoDB query dictionary.
            **kwargs: Additional arguments for the query.

        Returns:
            list: List of metadata dictionaries matching the query.
        """
        return list(self._projects_collection.find(query, **kwargs))

    def list_projects(self, **kwargs) -> List[Dict[str, Any]]:
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
