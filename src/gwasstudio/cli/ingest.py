from pathlib import Path

import click
import cloup
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import dask_deployment_types
from gwasstudio.utils import create_tiledb_schema, parse_uri, process_and_ingest
from gwasstudio.utils.cfg import get_tiledb_config, get_dask_batch_size, get_dask_deployment
from gwasstudio.utils.s3 import does_uri_path_exist

help_doc = """
Ingest data in a TileDB-unified dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--single-input",
        default=None,
        help="Path to the file to ingest",
    ),
    cloup.option(
        "--multiple-input",
        default=None,
        help="Path to the file containing a list of files to ingest. One path per line",
    ),
    cloup.option(
        "--uri",
        default=None,
        help="Destination path where to store the tiledb dataset. The prefix must be s3:// or file://",
    ),
)
@click.pass_context
def ingest(ctx, single_input, multiple_input, uri):
    input_file_list = []
    if single_input:
        input_file_list.append(single_input)
    elif multiple_input:
        with open(multiple_input, "r") as fp:
            for line in fp:
                input_file_list.append(line.strip())
    else:
        logger.error(f"No input provided: {uri}")
        exit()

    scheme, netloc, path = parse_uri(uri)
    logger.info("Starting ingestion: {} file to process".format(len(input_file_list)))
    if scheme == "s3":
        ingest_to_s3(ctx, input_file_list, uri)
    elif scheme == "file":
        ingest_to_fs(ctx, input_file_list, uri)
    else:
        logger.error(f"Do not recognize the uri's scheme: {uri}")
        exit()
    logger.info("Ingestion done")


def ingest_to_s3(ctx, input_file_list, uri):
    cfg = get_tiledb_config(ctx)

    if not does_uri_path_exist(uri, cfg):
        logger.info("Creating TileDB schema")
        create_tiledb_schema(uri, cfg)

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
                delayed(process_and_ingest)(file_path, uri, cfg) for file_path in batch_files if batch_files[file_path]
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                logger.debug(f"processing {file_path}")
                process_and_ingest(file_path, uri, cfg)
            else:
                logger.warning(f"skipping {file_path}")


def ingest_to_fs(ctx, input_file_list, uri):
    _, __, path = parse_uri(uri)
    if not Path(path).exists():
        logger.info("Creating TileDB schema")
        create_tiledb_schema(uri, {})

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
                delayed(process_and_ingest)(file_path, uri, {}) for file_path in batch_files if batch_files[file_path]
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                logger.debug(f"processing {file_path}")
                process_and_ingest(file_path, uri, {})
            else:
                logger.warning(f"{file_path} not found. Skipping it")
