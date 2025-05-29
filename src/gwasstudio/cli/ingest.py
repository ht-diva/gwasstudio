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
from dask import delayed, compute

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


def check_file_exists(input_file_list, logger):
    """Check if the input files exist."""
    for file_path in input_file_list:
        if not Path(file_path).exists():
            logger.error(f"File {file_path} does not exist")
            return False
    return True

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
    batch_size = ctx.obj["dask"]["batch_size"]
    for file_path in input_file_list:
        if Path(file_path).exists():
            continue
        else:
            #throw an error and exit
            logger.info(f"File {file_path} does not exist")
            exit()
        for i in range(0, len(input_file_list), batch_size):
            batch_files = input_file_list[i : i + batch_size]
            tasks = [
                delayed(process_and_ingest)(file, uri, cfg, pvalue) for file in batch_files
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        # Process files in batches
        for file_path in input_file_list:
            process_and_ingest(file_path, uri, cfg, pvalue)


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
    batch_size = ctx.obj["dask"]["batch_size"]
    if not Path(path).exists():
        create_tiledb_schema(uri, {}, pvalue)
        for i in range(0, len(input_file_list), batch_size):
            batch_files = input_file_list[i : i + batch_size]
            tasks = [
                delayed(process_and_ingest)(file, uri, {}, pvalue) for file in batch_files
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        for file_path in input_file_list:
            process_and_ingest(file_path, uri, {}, pvalue)

