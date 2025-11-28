import math
from pathlib import Path

import click
import cloup
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import dask_deployment_types, manage_daskcluster
from gwasstudio.utils import parse_uri, process_and_ingest, check_file_exists
from gwasstudio.utils.cfg import (
    get_tiledb_config,
    get_tiledb_sm_config,
    get_dask_batch_size,
    get_dask_deployment,
    get_mongo_uri,
)
from gwasstudio.utils.enums import MetadataEnum
from gwasstudio.utils.metadata import load_metadata, ingest_metadata
from gwasstudio.utils.mongo_manager import manage_mongo
from gwasstudio.utils.path_joiner import join_path
from gwasstudio.utils.s3 import does_uri_path_exist
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
    and processes the metadata for ingestion into a MongoDB collection, and data files for ingestion into a TileDB dataset. It supports both S3 and
    local file system storage.

    Args:
        ctx (click.Context): The click context.
        file_path (str): Path to the tabular file containing details for the ingestion.
        delimiter (str): Character or regex pattern to treat as the delimiter.
        uri (str): Destination path where to store the tiledb dataset.
        ingestion_type (str): Choose between metadata ingestion, data ingestion, or both.
        pvalue (bool): Indicate whether to ingest the p-value from the summary statistics instead of calculating it.

    Raises:
        ValueError: If the file does not exist or required columns are missing.
    """
    if not check_file_exists(file_path, logger=logger):
        raise ValueError(f"File {file_path} does not exist")
    if not uri:
        raise ValueError("URI is required")

    df = load_metadata(Path(file_path), delimiter)
    required_columns = MetadataEnum.required_fields()
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing column(s) in the input file: {', '.join(missing_cols)}")

    logger.info("Starting data ingestion: {} file to process".format(len(df["file_path"].tolist())))

    if ingestion_type in ["metadata", "both"]:
        with manage_mongo(ctx):
            mongo_uri = get_mongo_uri(ctx)
            ingest_metadata(df, mongo_uri)

    if ingestion_type in ["data", "both"]:
        scheme, netloc, path = parse_uri(uri)
        with manage_daskcluster(ctx):
            grouped = df.groupby(MetadataEnum.get_tiledb_grouping_fields(), observed=False)
            for name, group in grouped:
                group_name = "_".join(name)
                logger.info(f"Processing the group {group_name}")
                input_file_list = group["file_path"].tolist()
                tiledb_uri = join_path(uri, group_name)
                logger.debug(f"tiledb_uri: {tiledb_uri}")
                if scheme == "s3":
                    ingest_to_s3(ctx, input_file_list, tiledb_uri, pvalue)
                else:
                    # Assuming file system ingestion if not S3
                    ingest_to_fs(ctx, input_file_list, tiledb_uri, pvalue)

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
        pvalue (bool): Indicate whether to ingest the p-value from the summary statistics instead of calculating it.
    """
    cfg = get_tiledb_config(ctx)

    if not does_uri_path_exist(uri, cfg):
        logger.info("Creating TileDB schema")
        TileDBSchemaCreator(uri, cfg, pvalue).create_schema()

    if get_dask_deployment(ctx) in dask_deployment_types:
        batch_size = get_dask_batch_size(ctx, capacity_mode=True)
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


def ingest_to_fs(ctx, input_file_list, uri, pvalue):
    """
    Ingest data into a local file system-based TileDB dataset.

    This function processes a list of input files and ingests them into a TileDB dataset
    stored in the local file system. It supports batch processing using Dask.

    Args:
        ctx (click.Context): The click context.
        input_file_list (list): List of file paths to be ingested.
        uri (str): Destination path where to store the tiledb dataset in the local file system.
        pvalue (bool): Indicate whether to ingest the p-value from the summary statistics instead of calculating it.
    """
    cfg = get_tiledb_sm_config()
    _, __, path = parse_uri(uri)
    if not Path(path).exists():
        logger.info("Creating TileDB schema")
        TileDBSchemaCreator(uri, {}, pvalue).create_schema()

    if get_dask_deployment(ctx) in dask_deployment_types:
        batch_size = get_dask_batch_size(ctx, capacity_mode=True)
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
