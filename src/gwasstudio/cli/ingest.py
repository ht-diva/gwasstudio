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
from gwasstudio.cli.path_utils import compose_tiledb_uri
from gwasstudio.cli.utils import (
    create_config_from_context,
    load_metadata,
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
    get_metadata_store,
)
from gwasstudio.core import ingest_metadata as core_ingest_metadata
from gwasstudio.core.config import get_dask_batch_size, get_dask_deployment
from gwasstudio.core.storage.tiledb import TileDBStorageBackend
from gwasstudio.dask_client import dask_deployment_types, manage_daskcluster

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
        help="Warehouse path for storing the tiledb dataset. The prefix can be s3:// or file://",
    ),
    cloup.option(
        "--ingestion-type",
        type=click.Choice(["metadata", "data", "both"], case_sensitive=False),
        default="both",
        help="Choose between metadata ingestion, data ingestion, or both.",
    ),
    # cloup.option(
    #     "--pvalue",
    #     is_flag=True,
    #     default=True,
    #     help="Indicate whether to ingest the p-value from the summary statistics instead of calculating it (Default: True).",
    # ),
    cloup.option(
        "--add-cols",
        default=None,
        help="string delimited by comma with the columns to add",
    ),
)
@click.pass_context
def ingest(ctx, file_path, delimiter, uri, ingestion_type, add_cols):
    """
    Ingest data into a TileDB-unified dataset.

    This function reads metadata from a specified file, validates the required columns,
    and processes the metadata for ingestion into a MongoDB collection, and data files
    for ingestion into a TileDB dataset. It supports both S3 and local file system storage.

    Args:
        ctx (click.Context): The click context.
        file_path (str): Path to the tabular file containing details for the ingestion.
        delimiter (str): Character or regex pattern to treat as the delimiter.
        uri (str): Warehouse path for storing the tiledb dataset.
        ingestion_type (str): Choose between metadata ingestion, data ingestion, or both.
        add_cols (str): string delimited by comma with the columns to add.

    Raises:
        IngestionError: If ingestion fails due to configuration or storage errors.
        InvalidInputError: If the input file is invalid or missing required columns.
        ValueError: For backward compatibility with existing code.
    """
    try:
        # Validate file existence using core exception
        if not Path(file_path).exists():
            raise InvalidInputError(f"File not found: {file_path}")

        # Load metadata
        raw_df = load_metadata(Path(file_path), delimiter)

        # Validate columns
        df = validate_metadata_columns(raw_df)

        # Validate that if uri is None, warehouse_uri is in metadata file
        if uri is None:
            if MetadataEnum.WAREHOUSE_URI.get_value() not in df.columns:
                raise InvalidInputError(
                    f"URI not provided via --uri and not found in the metadata file. "
                    f"Re-ingest data with --uri or provide {MetadataEnum.WAREHOUSE_URI.get_value()}."
                )
            else:
                grouped = df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
                for name, group in grouped:
                    unique_uris = group[MetadataEnum.WAREHOUSE_URI.get_value()].dropna().unique()
                    if len(unique_uris) == 0:
                        raise InvalidInputError(
                            f"No {MetadataEnum.WAREHOUSE_URI.get_value()} found in metadata for group {name}"
                        )
                    if len(unique_uris) > 1:
                        raise InvalidInputError(
                            f"Multiple URIs found for group {name}: {list(unique_uris)}. "
                            "All datasets in a project-study group must have the same URI."
                        )
        else:
            # Add warehouse_uri column
            df[MetadataEnum.WAREHOUSE_URI.get_value()] = uri

        logger.info(f"Starting data ingestion: {len(df['file_path'].tolist())} file to process")

        # Create GWASStudioConfig from context (for core compatibility)
        try:
            config = create_config_from_context(ctx)
        except Exception as e:
            raise ConfigurationError(f"Failed to create configuration from context: {str(e)}")

        # Process metadata ingestion
        if ingestion_type in ["metadata", "both"]:
            metadata_store = get_metadata_store(config)
            try:
                core_ingest_metadata(df.to_dict(orient="records"), config=config, metadata_store=metadata_store)
                logger.info("Metadata ingestion completed successfully")
            except Exception as e:
                raise IngestionError(f"Failed to ingest metadata: {str(e)}")

        # Process data ingestion
        if ingestion_type in ["data", "both"]:
            with manage_daskcluster(config):
                grouped = df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
                for name, group in grouped:
                    warehouse_uri = group[MetadataEnum.WAREHOUSE_URI.get_value()].dropna().unique()[0]
                    input_file_list = group[MetadataEnum.FILE_PATH.get_value()].tolist()
                    group_name, tiledb_uri = compose_tiledb_uri(warehouse_uri, name, logger)
                    additional_columns = add_cols.split(",") if add_cols else []
                    logger.debug(f"tiledb_uri: {tiledb_uri}")

                    try:
                        _ingest_files(input_file_list, tiledb_uri, additional_columns, config)
                    except Exception as e:
                        raise StorageError(f"Failed to ingest data for group {group_name}: {str(e)}") from e

        logger.info("Ingestion done")

    except GWASStudioError:
        # Re-raise GWASStudioError and its subclasses
        raise
    except Exception as e:
        # For backward compatibility, convert unexpected errors to IngestionError
        raise IngestionError(f"Unexpected error during ingestion: {str(e)}")


def _ingest_files(input_file_list: list[str], uri: str, add_cols: list[str], config: GWASStudioConfig) -> None:
    """
    Ingest a list of files into the TileDB array at ``uri`` via the storage backend.

    Handles both S3 and local filesystem URIs — the backend internally selects
    the correct config and existence check for each scheme.

    Args:
        input_file_list: List of file paths to ingest.
        uri: Destination TileDB array URI (s3:// or local path).
        add_cols: Extra attribute columns for schema creation.
        config: GWASStudio configuration object.

    Raises:
        StorageError: If ingestion fails.
    """
    backend = TileDBStorageBackend(config)

    if get_dask_deployment(config) in dask_deployment_types:
        batch_size = get_dask_batch_size(config, capacity_mode=True)
        for i in range(0, len(input_file_list), batch_size):
            batch_files = {file_path: Path(file_path).exists() for file_path in input_file_list[i : i + batch_size]}
            total_batches = math.ceil(len(input_file_list) / batch_size)
            batch_no = i // batch_size + 1
            logger.info(f"Running batch {batch_no}/{total_batches} ({batch_size} items)")

            skipped_files = [file_path for file_path, exists in batch_files.items() if not exists]
            if skipped_files:
                logger.warning(f"Skipping files: {skipped_files}")

            tasks = [
                delayed(backend.ingest_data)(file_path=file_path, uri=uri, additional_columns=add_cols)
                for file_path in batch_files
                if batch_files[file_path]
            ]
            compute(*tasks)
            logger.info(f"Batch {batch_no} completed.", flush=True)
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                logger.debug(f"processing {file_path}")
                backend.ingest_data(file_path=file_path, uri=uri, additional_columns=add_cols)
            else:
                logger.warning(f"{file_path} not found. Skipping it")
