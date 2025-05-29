from pathlib import Path

import click
import cloup
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import dask_deployment_types, manage_daskcluster
from gwasstudio.utils import create_tiledb_schema, parse_uri, process_and_ingest, check_file_exists
from gwasstudio.utils.cfg import get_tiledb_config, get_dask_batch_size, get_dask_deployment, get_mongo_uri
from gwasstudio.utils.metadata import load_metadata, ingest_metadata
from gwasstudio.utils.mongo_manager import manage_mongo
from gwasstudio.utils.s3 import does_uri_path_exist

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
        help="Destination path where to store the tiledb dataset. The prefix must be s3:// or file://",
    ),
    cloup.option(
        "--ingestion-type",
        type=click.Choice(["metadata", "data", "both"], case_sensitive=False),
        default="both",
        help="Choose between metadata ingestion, data ingestion, or both.",
    ),
    cloup.option(
        "--pvalue",
        default=True,
        help="Indicate whether to ingest the p-value from the summary statistics instead of calculating it (Default: True).",
    )
)
@click.pass_context
def ingest(ctx, file_path, delimiter, uri, ingestion_type, pvalue):
    """
    Ingest data into a TileDB-unified dataset.

    This function reads metadata from a specified file, validates the required columns,
    and processes the metadata for ingestion into a MongoDB collection, and data files for ingestion into a TileDB dataset. It supports both S3 and
    local file system storage.

    Args:
        ctx (click.Context): The click context.
        file_path (str): Path to the tabular file containing details for the ingestion.
        delimiter (str): Character or regex pattern to treat as the delimiter.
        uri (str): Destination path where to store the tiledb dataset.
        ingestion_type (str): Choose between metadata ingestion, data ingestion, or both.

    Raises:
        ValueError: If the file does not exist or required columns are missing.
    """
    if not check_file_exists(file_path, logger=logger):
        raise ValueError(f"File {file_path} does not exist")
    if not uri:
        raise ValueError("URI is required")

    df = load_metadata(Path(file_path), delimiter)
    required_columns = ["project", "study", "file_path", "category"]
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing column(s) in the input file: {', '.join(missing_cols)}")

    if ingestion_type in ["metadata", "both"]:
        with manage_mongo(ctx):
            mongo_uri = get_mongo_uri(ctx)
            ingest_metadata(df, mongo_uri)

    if ingestion_type in ["data", "both"]:
        input_file_list = df["file_path"].tolist()
        logger.info("Starting data ingestion: {} file to process".format(len(input_file_list)))

        scheme, netloc, path = parse_uri(uri)
        with manage_daskcluster(ctx):
            if scheme == "s3":
                ingest_to_s3(ctx, input_file_list, uri, pvalue)
            else:
                # Assuming file system ingestion if not S3
                ingest_to_fs(ctx, input_file_list, uri, pvalue)

        logger.info("Ingestion done")


def ingest_to_s3(ctx, input_file_list, uri, pvalue):
    """
    Ingest data into an S3-based TileDB dataset.

    This function processes a list of input files and ingests them into a TileDB dataset
    stored in an S3 bucket. It supports batch processing using Dask.

    Args:
        ctx (click.Context): The click context.
        input_file_list (list): List of file paths to be ingested.
        uri (str): Destination path where to store the tiledb dataset in S3.
    """
    cfg = get_tiledb_config(ctx)

    if not does_uri_path_exist(uri, cfg):
        logger.info("Creating TileDB schema")
        create_tiledb_schema(uri, cfg, pvalue)

    if get_dask_deployment(ctx) in dask_deployment_types:
        batch_size = get_dask_batch_size(ctx)
        for i in range(0, len(input_file_list), batch_size):
            batch_files = {file_path: Path(file_path).exists() for file_path in input_file_list[i : i + batch_size]}
            logger.info(f"Processing a batch of {len(batch_files)} items for batch {i // batch_size + 1}")

            # Log skipped files
            skipped_files = [file_path for file_path, exists in batch_files.items() if not exists]
            if skipped_files:
                logger.warning(f"Skipping files: {skipped_files}")
            # Create a list of delayed tasks
            tasks = [
                delayed(process_and_ingest)(file_path, uri, cfg, pvalue) for file_path in batch_files if batch_files[file_path]
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                logger.debug(f"processing {file_path}")
                process_and_ingest(file_path, uri, cfg, pvalue)
            else:
                logger.warning(f"skipping {file_path}")


def ingest_to_fs(ctx, input_file_list, uri, pvalue):
    """
    Ingest data into a local file system-based TileDB dataset.

    This function processes a list of input files and ingests them into a TileDB dataset
    stored in the local file system. It supports batch processing using Dask.

    Args:
        ctx (click.Context): The click context.
        input_file_list (list): List of file paths to be ingested.
        uri (str): Destination path where to store the tiledb dataset in the local file system.
    """
    _, __, path = parse_uri(uri)
    if not Path(path).exists():
        logger.info("Creating TileDB schema")
        create_tiledb_schema(uri, {}, pvalue)

    if get_dask_deployment(ctx) in dask_deployment_types:
        batch_size = get_dask_batch_size(ctx)
        for i in range(0, len(input_file_list), batch_size):
            batch_files = {file_path: Path(file_path).exists() for file_path in input_file_list[i : i + batch_size]}
            logger.info(f"Processing a batch of {len(batch_files)} items for batch {i // batch_size + 1}")

            # Log skipped files
            skipped_files = [file_path for file_path, exists in batch_files.items() if not exists]
            if skipped_files:
                logger.warning(f"Skipping files: {skipped_files}")
            # Create a list of delayed tasks
            tasks = [
                delayed(process_and_ingest)(file_path, uri, {}, pvalue) for file_path in batch_files if batch_files[file_path]
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                logger.debug(f"processing {file_path}")
                process_and_ingest(file_path, uri, {}, pvalue)
            else:
                logger.warning(f"{file_path} not found. Skipping it")
