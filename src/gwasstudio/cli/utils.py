"""
GWASStudio CLI Utilities
=====================================================

This module provides utility functions for the CLI that work with GWASStudioConfig.
These functions are designed to be used with the new core configuration system.
"""

import urllib.parse
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd

from gwasstudio import logger
from gwasstudio.core import GWASStudioConfig, DaskConfig, MongoConfig, S3Config, VaultConfig, TileDBConfig, MetadataEnum
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils.metadata import process_row


def get_tiledb_config(config: GWASStudioConfig, prefix: Optional[str] = None) -> Dict[str, Any]:
    """
    Get TileDB configuration from GWASStudioConfig.

    Args:
        config: GWASStudio configuration object.
        prefix: Optional prefix to filter keys ('vfs' or 'sm').
                If None, returns merged vfs_config and sm_config.
                If 'vfs', returns only vfs_config.
                If 'sm', returns only sm_config.

    Returns:
        dict: TileDB configuration dictionary.
    """
    vfs_config = config.tiledb.vfs_config
    sm_config = config.tiledb.sm_config

    if prefix is None:
        return vfs_config | sm_config
    elif prefix == "vfs":
        return vfs_config
    elif prefix == "sm":
        return sm_config
    else:
        # Return filtered dict based on key prefix
        combined = vfs_config | sm_config
        return {k: v for k, v in combined.items() if k.startswith(prefix)}


def get_mongo_uri(config: GWASStudioConfig) -> Optional[str]:
    """
    Get MongoDB URI from GWASStudioConfig or Vault.

    Priority:
    1. Direct URI from config.mongo.uri
    2. URI from Vault (if configured)

    Args:
        config: GWASStudio configuration object.

    Returns:
        str or None: MongoDB connection URI.
    """
    # 1. Try direct URI
    if config.mongo.uri:
        return config.mongo.uri

    # 2. Try Vault
    if config.vault and config.vault.path and config.vault.url:
        try:
            from gwasstudio.utils.vault import get_config_from_vault

            # Convert VaultConfig dataclass to dict
            vault_options = config.vault.__dict__
            mongo_config = get_config_from_vault("mongo", vault_options)
            if mongo_config:
                return mongo_config.get("uri")
        except ImportError:
            pass
        except Exception as e:
            logger.warning(f"Failed to get MongoDB URI from Vault: {str(e)}")

    return None


def get_dask_batch_size(config: GWASStudioConfig, capacity_mode: bool = False) -> int:
    """
    Get the Dask batch size. When capacity_mode is true, return the total worker capacity.
    Otherwise, fall back to the batch size from GWASStudioConfig.

    Args:
        config: GWASStudio configuration object.
        capacity_mode: If True, use capacity-based batch sizing.

    Returns:
        int: Batch size for Dask operations.
    """
    workers = config.dask.workers
    cores_per_worker = config.dask.cores_per_worker

    return workers * cores_per_worker if capacity_mode else config.dask.batch_size


def get_dask_deployment(config: GWASStudioConfig) -> str:
    """
    Get Dask deployment type from GWASStudioConfig.

    Args:
        config: GWASStudio configuration object.

    Returns:
        str: Dask deployment type ("local", "gateway", "slurm").
    """
    return config.dask.deployment


def create_config_from_context(ctx) -> GWASStudioConfig:
    """
    Create a GWASStudioConfig object from the Click context.

    This function extracts configuration from the Click context (ctx.obj)
    and creates a standardized GWASStudioConfig object that can be used
    by the core modules.

    Args:
        ctx: Click context object.

    Returns:
        GWASStudioConfig: Configuration object for GWASStudio core.
    """
    # Extract configuration from context
    mongo_config = ctx.obj.get("mongo", {})
    dask_config = ctx.obj.get("dask", {})
    s3_config = ctx.obj.get("tiledb", {})
    vault_config = ctx.obj.get("vault", {})

    mongo_uri = mongo_config.get("uri")
    if mongo_uri:
        _, _, db_name = parse_uri(mongo_uri)
        db_name = db_name.replace("/", "")
    else:
        db_name = "gwasstudio"

    # Create GWASStudioConfig with extracted values
    config = GWASStudioConfig(
        dask=DaskConfig(
            deployment=dask_config.get("deployment", "local"),
            workers=dask_config.get("workers", 2),
            cores_per_worker=dask_config.get("cores_per_worker", 2),
            memory_per_worker=dask_config.get("memory_per_worker", "4GiB"),
            walltime=dask_config.get("walltime", "12:00:00"),
            batch_size=dask_config.get("batch_size", 0),
            interface=dask_config.get("interface"),
            gw_address=dask_config.get("gw_address"),
            gw_image=dask_config.get("gw_image"),
            job_script_prologue=dask_config.get("job_script_prologue", []),
            python=dask_config.get("python"),
            local_directory=Path(dask_config.get("local_directory")) if dask_config.get("local_directory") else None,
        ),
        mongo=MongoConfig(
            uri=mongo_uri,
            db_name=db_name,
        ),
        s3=S3Config(
            aws_access_key_id=s3_config.get("vfs.s3.aws_access_key_id"),
            aws_secret_access_key=s3_config.get("vfs.s3.aws_secret_access_key"),
            endpoint_override=s3_config.get("vfs.s3.endpoint_override"),
            use_virtual_addressing=s3_config.get("vfs.s3.use_virtual_addressing", False),
            scheme=s3_config.get("vfs.s3.scheme", "https"),
            region=s3_config.get("vfs.s3.region", ""),
            verify_ssl=s3_config.get("vfs.s3.verify_ssl", False),
            connect_timeout_ms=s3_config.get("vfs.s3.connect_timeout_ms", 30000),
            request_timeout_ms=s3_config.get("vfs.s3.request_timeout_ms", 300000),
        ),
        vault=VaultConfig(
            auth=vault_config.get("auth", "basic"),
            mount_point=vault_config.get("mount_point", "secret"),
            path=vault_config.get("path"),
            token=vault_config.get("token"),
            url=vault_config.get("url"),
        ),
        tiledb=TileDBConfig(
            vfs_config=s3_config,  # Reuse S3 config for TileDB,
            sm_config={"sm.dedup_coords": "false"},
        ),
    )

    return config


def parse_uri(uri: str) -> tuple[str, str, str]:
    try:
        parsed = urllib.parse.urlparse(uri)
        scheme, netloc, path = parsed.scheme, parsed.netloc, parsed.path
        if scheme in ["s3", "https"]:
            path = path.strip("/")
        return scheme, netloc, path
    except ValueError as e:
        raise ValueError(f"Invalid URI: {uri}") from e


def load_metadata(file_path: Path, delimiter: str = "\t") -> pd.DataFrame:
    """Load metadata from a file in tabular format."""
    try:
        return pd.read_csv(file_path, sep=delimiter, dtype=MetadataEnum.get_all_dtypes_dict(), dtype_backend="pyarrow")
    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
        raise ValueError("File not found")
    except pd.errors.EmptyDataError:
        logger.error("No data found in the file. Please check the file content.")
        raise ValueError("No data found in the file")
    except pd.errors.ParserError:
        logger.error("Error parsing the file. Please check the file format.")
        raise ValueError("Error parsing the file")


def ingest_metadata(df: pd.DataFrame, mongo_uri: str = None) -> None:
    """Ingest data into the MongoDB collection."""

    def _document_generator(df):
        for row in df.itertuples(index=False):
            yield process_row(row)

    logger.info("Starting metadata ingestion")
    rows = len(df.axes[0])
    processed_rows = 0
    logger.info(f"{rows} documents to ingest")

    # Helper that creates and saves a single document
    def _save_document(doc):
        obj = EnhancedDataProfile(uri=mongo_uri, **doc)
        obj.save()
        return 1  # count of one processed row

    for document in _document_generator(df):
        processed_rows += _save_document(document)

        # Print the row counter every 100 rows
        if processed_rows % 100 == 0:
            logger.info(f"{processed_rows} documents processed")


def ingest_metadata_bulk(df: pd.DataFrame, mongo_uri: str = None, batch_size: int = 1000) -> None:
    """Ingest data into the MongoDB collection in bulk using generator pattern."""

    def _document_generator(df):
        for row in df.itertuples(index=False):
            yield process_row(row)

    logger.info("Starting bulk metadata ingestion")
    rows = len(df.axes[0])
    logger.info(f"{rows} documents to ingest")

    # Process in batches to avoid memory issues
    processed = 0

    batch = []
    for i, document in enumerate(_document_generator(df), 1):
        batch.append(document)

        if i % batch_size == 0:
            result = EnhancedDataProfile.bulk_create(batch, mongo_uri, batch_size=batch_size)
            if "invalid_documents" in result:
                for invalid in result["invalid_documents"]:
                    logger.error(f"Invalid document: {invalid}")
            processed += result["total"]
            logger.info(f"Processed batch: {processed}/{rows} documents")
            batch = []

    # Process remaining documents
    if batch:
        result = EnhancedDataProfile.bulk_create(batch, mongo_uri, batch_size=batch_size)
        if "invalid_documents" in result:
            for invalid in result["invalid_documents"]:
                logger.error(f"Invalid document: {invalid}")

        processed += result["total"]
        logger.info(f"Processed final batch: {processed}/{rows} documents")

    logger.info(f"Bulk ingestion complete: {processed} documents processed")
