"""
GWASStudio Core Query Module
=============================

This module provides functions for querying genomic data and metadata
stored in GWASStudio backends (TileDB, MongoDB).
"""

from typing import Optional, Dict, Any, List

from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.storage import MongoDBStorage  # TileDBStorage
from gwasstudio.core.exceptions import QueryError


# class QueryError(GWASStudioError):
#     """Exception raised for errors during data querying."""
#
#     pass


class ProjectNotFoundError(QueryError):
    """Exception raised when a project is not found."""

    pass


class InvalidQueryError(QueryError):
    """Exception raised for invalid query parameters."""

    pass


def _validate_project_id(project_id: str) -> None:
    """Validate that the project_id is not empty."""
    if not project_id:
        raise InvalidQueryError("project_id cannot be empty")


def _validate_region(region: str) -> Dict[str, Any]:
    """
    Validate and parse a genomic region string.

    Expected format: "chr:start-end" or "chr:start" or "chr"

    Args:
        region: Genomic region string.

    Returns:
        Dictionary with 'chr', 'start', and 'end' keys.
    """
    if not region:
        return {}

    parts = region.split(":")
    if len(parts) == 1:
        return {"chr": parts[0], "start": None, "end": None}
    elif len(parts) == 2:
        chr_part = parts[0]
        range_part = parts[1]
        if "-" in range_part:
            start, end = range_part.split("-")
            try:
                start = int(start) if start else None
                end = int(end) if end else None
            except ValueError:
                raise InvalidQueryError(f"Invalid region format: {region}")
            return {"chr": chr_part, "start": start, "end": end}
        else:
            try:
                start = int(range_part) if range_part else None
            except ValueError:
                raise InvalidQueryError(f"Invalid region format: {region}")
            return {"chr": chr_part, "start": start, "end": None}
    else:
        raise InvalidQueryError(f"Invalid region format: {region}")


def query_metadata(
    template: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
    config: Optional[GWASStudioConfig] = None,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    Query metadata for projects stored in GWASStudio.

    Args:
        template: Dictionary to match against metadata (MongoDB query).
        project_id: Optional project ID to filter by.
        config: GWASStudio configuration.
        **kwargs: Additional arguments for the query.

    Returns:
        List of metadata dictionaries matching the query.

    Raises:
        QueryError: If the query fails.
    """
    if config is None:
        config = GWASStudioConfig()

    mongo_storage = MongoDBStorage(config)

    query = {}
    if template:
        query.update(template)
    if project_id:
        query["project_id"] = project_id

    try:
        results = mongo_storage.query_metadata(query, **kwargs)
        return list(results)
    except Exception as e:
        raise QueryError(f"Failed to query metadata: {str(e)}")


# def query_data(
#     project_id: str,
#     region: Optional[str] = None,
#     snp_list: Optional[List[str]] = None,
#     pval_threshold: Optional[float] = None,
#     config: Optional[GWASStudioConfig] = None,
#     limit: Optional[int] = None,
#     **kwargs,
# ) -> pd.DataFrame:
#     """
#     Query genomic data for a project.
#
#     Args:
#         project_id: Unique identifier for the project.
#         region: Genomic region to query (e.g., "1:100000-200000").
#         snp_list: List of SNP IDs to query.
#         pval_threshold: Only return variants with p-value <= threshold.
#         config: GWASStudio configuration.
#         limit: Maximum number of records to return.
#         **kwargs: Additional arguments for the query.
#
#     Returns:
#         pandas DataFrame with the query results.
#
#     Raises:
#         QueryError: If the query fails.
#     """
#     _validate_project_id(project_id)
#
#     if config is None:
#         config = GWASStudioConfig()
#
#     # Parse region
#     region_info = _validate_region(region) if region else None
#
#     # Initialize storage backends
#     tiledb_storage = TileDBStorage(config)
#     mongo_storage = MongoDBStorage(config)
#
#     try:
#         # Check if project exists
#         if not tiledb_storage.project_exists(project_id):
#             raise ProjectNotFoundError(f"Project {project_id} not found")
#
#         # Query data from TileDB
#         df = tiledb_storage.query_data(
#             project_id,
#             region=region_info,
#             snp_list=snp_list,
#             pval_threshold=pval_threshold,
#             limit=limit,
#             **kwargs,
#         )
#
#         return df
#     except Exception as e:
#         raise QueryError(f"Failed to query data for project {project_id}: {str(e)}")
#
#
# def query_data_stream(
#     project_id: str,
#     region: Optional[str] = None,
#     snp_list: Optional[List[str]] = None,
#     pval_threshold: Optional[float] = None,
#     config: Optional[GWASStudioConfig] = None,
#     chunk_size: int = 10000,
#     **kwargs,
# ) -> Generator[pd.DataFrame, None, None]:
#     """
#     Query genomic data for a project in chunks (streaming).
#
#     Args:
#         project_id: Unique identifier for the project.
#         region: Genomic region to query.
#         snp_list: List of SNP IDs to query.
#         pval_threshold: Only return variants with p-value <= threshold.
#         config: GWASStudio configuration.
#         chunk_size: Number of records per chunk.
#         **kwargs: Additional arguments for the query.
#
#     Yields:
#         Chunks of pandas DataFrames with query results.
#
#     Raises:
#         QueryError: If the query fails.
#     """
#     _validate_project_id(project_id)
#
#     if config is None:
#         config = GWASStudioConfig()
#
#     region_info = _validate_region(region) if region else None
#
#     tiledb_storage = TileDBStorage(config)
#
#     try:
#         for chunk in tiledb_storage.query_data_stream(
#             project_id,
#             region=region_info,
#             snp_list=snp_list,
#             pval_threshold=pval_threshold,
#             chunk_size=chunk_size,
#             **kwargs,
#         ):
#             yield chunk
#     except Exception as e:
#         raise QueryError(f"Failed to stream data for project {project_id}: {str(e)}")


def list_projects(
    config: Optional[GWASStudioConfig] = None,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    List all projects stored in GWASStudio.

    Args:
        config: GWASStudio configuration.
        **kwargs: Additional arguments for the query.

    Returns:
        List of dictionaries with project metadata.
    """
    if config is None:
        config = GWASStudioConfig()

    mongo_storage = MongoDBStorage(config)

    try:
        return list(mongo_storage.list_projects(**kwargs))
    except Exception as e:
        raise QueryError(f"Failed to list projects: {str(e)}")
