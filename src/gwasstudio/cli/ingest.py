"""
GWASStudio CLI Ingest Command (Core version)
==================================================================

This module provides the CLI command for data ingestion.
"""

import math
from pathlib import Path

import click
import cloup
from dask import compute, delayed

from gwasstudio import logger
from gwasstudio.cli.path_utils import compose_tiledb_uri, join_path
from gwasstudio.cli.s3 import does_uri_path_exist
from gwasstudio.cli.utils import (
    create_config_from_context,
    load_metadata,
    parse_uri,
    process_and_ingest,
    validate_metadata_columns,
)
from gwasstudio.core import (
    ConfigurationError,
    GWASStudioConfig,
    GWASStudioError,
    IngestionError,
    InvalidInputError,
    MetadataEnum,
    StorageError,
)
from gwasstudio.core import ingest_metadata as core_ingest_metadata
from gwasstudio.core.config import get_dask_batch_size, get_dask_deployment, get_tiledb_config
from gwasstudio.dask_client import dask_deployment_types, manage_daskcluster
from gwasstudio.utils.tdb_schema import TileDBSchemaCreator

help_doc = """
Ingest data in a TileDB-unified dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--file-path",
        required=True,
        help="Path to the tabular file containing details for the ingestion",
    ),
    cloup.option(
        "--delimiter",
        default="\t",
        help="Character or regex pattern to treat as the delimiter.",
    ),
    cloup.option(
        "--uri",
        default=None,
        help="Destination path where to store the tiledb dataset. The prefix can be s3:// or file://",
    ),
    cloup.option(
        "--ingestion-type",
        type=click.Choice(["metadata", "data", "both"], case_sensitive=False),
        default="both",
        help="Choose between metadata ingestion, data ingestion, or both.",
    ),
    cloup.option(
        "--pvalue",
        is_flag=True,
        default=True,
        help="Indicate whether to ingest the p-value from the summary statistics instead of calculating it (Default: True).",
    ),
)
@click.pass_context
def ingest(ctx, file_path, delimiter, uri, ingestion_type, pvalue):
    """
    Ingest data into a TileDB-unified dataset.

    This function reads metadata from a specified file, validates the required columns,
    and processes the metadata for ingestion into a MongoDB collection, and data files
    for ingestion into a TileDB dataset. It supports both S3 and local file system storage.

    Args:
        ctx (click.Context): The click context.
        file_path (str): Path to the tabular file containing details for the ingestion.
        delimiter (str): Character or regex pattern to treat as the delimiter.
        uri (str): Destination path where to store the tiledb dataset.
        ingestion_type (str): Choose between metadata ingestion, data ingestion, or both.
        pvalue (bool): Indicate whether to ingest the p-value from the summary statistics.

    Raises:
        IngestionError: If ingestion fails due to configuration or storage errors.
        InvalidInputError: If the input file is invalid or missing required columns.
        ValueError: For backward compatibility with existing code.
    """
    try:
        # Validate file existence using core exception
        if not Path(file_path).exists():
            raise InvalidInputError(f"File not found: {file_path}")

        if not uri:
            raise InvalidInputError("URI is required")

        # Load metadata
        raw_df = load_metadata(Path(file_path), delimiter)

        # Validate columns
        df = validate_metadata_columns(raw_df)

        logger.info("Starting data ingestion: {} file to process".format(len(df["file_path"].tolist())))

        # Create GWASStudioConfig from context (for core compatibility)
        try:
            config = create_config_from_context(ctx)
        except Exception as e:
            raise ConfigurationError(f"Failed to create configuration from context: {str(e)}")

        # Process metadata ingestion
        if ingestion_type in ["metadata", "both"]:
            # mongo_uri = get_mongo_uri(config)
            try:
                core_ingest_metadata(df.to_dict(orient="records"), config=config)
                # ingest_metadata_bulk(df, mongo_uri)
                logger.info("Metadata ingestion completed successfully")
            except Exception as e:
                raise IngestionError(f"Failed to ingest metadata: {str(e)}")

        # Process data ingestion
        if ingestion_type in ["data", "both"]:
            scheme, netloc, path = parse_uri(uri)
            with manage_daskcluster(config):
                grouped = df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
                for name, group in grouped:
                    input_file_list = group["file_path"].tolist()
                    group_name, tiledb_uri = compose_tiledb_uri(uri, name, logger)
                    logger.debug(f"tiledb_uri: {tiledb_uri}")

                    if scheme == "s3":
                        try:
                            ingest_to_s3(input_file_list, tiledb_uri, pvalue, config)
                        except Exception as e:
                            raise StorageError(f"Failed to ingest S3 data for group {group_name}: {str(e)}")
                    else:
                        # Assuming file system ingestion if not S3
                        try:
                            ingest_to_fs(input_file_list, tiledb_uri, pvalue, config)
                        except Exception as e:
                            raise StorageError(f"Failed to ingest filesystem data for group {group_name}: {str(e)}")

        logger.info("Ingestion done")

    except GWASStudioError:
        # Re-raise GWASStudioError and its subclasses
        raise
    except Exception as e:
        # For backward compatibility, convert unexpected errors to IngestionError
        raise IngestionError(f"Unexpected error during ingestion: {str(e)}")


def ingest_to_s3(input_file_list, uri, pvalue, config: GWASStudioConfig):
    """
    Ingest data into an S3-based TileDB dataset.

    This function processes a list of input files and ingests them into a TileDB dataset
    stored in an S3 bucket. It supports batch processing using Dask.

    Args:
        input_file_list (list): List of file paths to be ingested.
        uri (str): Destination path where to store the tiledb dataset in S3.
        pvalue (bool): Indicate whether to ingest the p-value from the summary statistics.
        config (GWASStudioConfig): GWASStudio configuration object.

    Raises:
        StorageError: If S3 ingestion fails.
    """
    try:
        cfg = get_tiledb_config(config)

        if not does_uri_path_exist(uri, cfg):
            logger.info("Creating TileDB schema")
            TileDBSchemaCreator(uri, cfg, pvalue).create_schema()

        if get_dask_deployment(config) in dask_deployment_types:
            batch_size = get_dask_batch_size(config, capacity_mode=True)
            for i in range(0, len(input_file_list), batch_size):
                batch_files = {file_path: Path(file_path).exists() for file_path in input_file_list[i : i + batch_size]}
                total_batches = math.ceil(len(input_file_list) / batch_size)
                batch_no = i // batch_size + 1
                logger.info(f"Running batch {batch_no}/{total_batches} ({batch_size} items)")

                # Log skipped files
                skipped_files = [file_path for file_path, exists in batch_files.items() if not exists]
                if skipped_files:
                    logger.warning(f"Skipping files: {skipped_files}")

                # Create a list of delayed tasks
                tasks = [
                    delayed(process_and_ingest)(file_path, uri, cfg, pvalue)
                    for file_path in batch_files
                    if batch_files[file_path]
                ]

                # Submit tasks and wait for completion
                compute(*tasks)
                logger.info(f"Batch {batch_no} completed.", flush=True)
        else:
            for file_path in input_file_list:
                if Path(file_path).exists():
                    logger.debug(f"processing {file_path}")
                    process_and_ingest(file_path, uri, cfg, pvalue)
                else:
                    logger.warning(f"skipping {file_path}")

    except Exception as e:
        raise StorageError(f"S3 ingestion failed: {str(e)}")


def ingest_to_fs(input_file_list, uri, pvalue, config: GWASStudioConfig):
    """
    Ingest data into a local file system-based TileDB dataset.

    This function processes a list of input files and ingests them into a TileDB dataset
    stored in the local file system. It supports batch processing using Dask.

    Args:
        input_file_list (list): List of file paths to be ingested.
        uri (str): Destination path where to store the tiledb dataset in the local file system.
        pvalue (bool): Indicate whether to ingest the p-value from the summary statistics.
        config (GWASStudioConfig): GWASStudio configuration object.

    Raises:
        StorageError: If filesystem ingestion fails.
    """
    try:
        cfg = get_tiledb_config(config, prefix="sm")
        _, __, path = parse_uri(uri)
        if not Path(path).exists():
            logger.info("Creating TileDB schema")
            TileDBSchemaCreator(uri, {}, pvalue).create_schema()

        if get_dask_deployment(config) in dask_deployment_types:
            batch_size = get_dask_batch_size(config, capacity_mode=True)
            for i in range(0, len(input_file_list), batch_size):
                batch_files = {file_path: Path(file_path).exists() for file_path in input_file_list[i : i + batch_size]}
                total_batches = math.ceil(len(input_file_list) / batch_size)
                batch_no = i // batch_size + 1
                logger.info(f"Running batch {batch_no}/{total_batches} ({batch_size} items)")

                # Log skipped files
                skipped_files = [file_path for file_path, exists in batch_files.items() if not exists]
                if skipped_files:
                    logger.warning(f"Skipping files: {skipped_files}")

                # Create a list of delayed tasks
                tasks = [
                    delayed(process_and_ingest)(file_path, uri, cfg, pvalue)
                    for file_path in batch_files
                    if batch_files[file_path]
                ]

                # Submit tasks and wait for completion
                compute(*tasks)
                logger.info(f"Batch {batch_no} completed.", flush=True)
        else:
            for file_path in input_file_list:
                if Path(file_path).exists():
                    logger.debug(f"processing {file_path}")
                    process_and_ingest(file_path, uri, {}, pvalue)
                else:
                    logger.warning(f"{file_path} not found. Skipping it")

    except Exception as e:
        raise StorageError(f"Filesystem ingestion failed: {str(e)}")
