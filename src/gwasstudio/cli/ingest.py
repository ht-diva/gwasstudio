from pathlib import Path

import click
import cloup

from gwasstudio import logger
from gwasstudio.dask_client import dask_deployment_types
from gwasstudio.utils import create_tiledb_schema, parse_uri, process_and_ingest
from gwasstudio.utils.cfg import get_tiledb_config
from gwasstudio.utils.s3 import does_uri_path_exist
from dask import delayed, compute
import pandas as pd

help_doc = """
Ingest data in a TileDB-unified dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
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
def ingest(ctx, multiple_input, uri):
    """Ingest data in a TileDB-unified dataset."""
    # Check if the file exists
    input_file_list = pd.read_csv(multiple_input, header = None)
    input_file_list = input_file_list[0].tolist()
    if not check_file_exists(input_file_list, logger):
        exit(1)
    # Parse the uri
    scheme, netloc, path = parse_uri(uri)
    if scheme == "s3":
        ingest_to_s3(ctx, input_file_list, uri)
    elif scheme == "file":
        ingest_to_fs(ctx, input_file_list, uri)
    else:
        logger.error(f"Do not recognize the uri's scheme: {uri}")
        exit()


def check_file_exists(input_file_list, logger):
    """Check if the input files exist."""
    for file_path in input_file_list:
        if not Path(file_path).exists():
            logger.error(f"File {file_path} does not exist")
            return False
    return True

def ingest_to_s3(ctx, input_file_list, uri):
    cfg = get_tiledb_config(ctx)
    if not does_uri_path_exist(uri, cfg):
        logger.info("Creating TileDB schema")
        create_tiledb_schema(uri, cfg)
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
                delayed(process_and_ingest)(file, uri, cfg) for file in batch_files
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        # Process files in batches
        for file_path in input_file_list:
            process_and_ingest(file_path, uri, cfg)


def ingest_to_fs(ctx, input_file_list, uri):
    _, __, path = parse_uri(uri)
    batch_size = ctx.obj["dask"]["batch_size"]
    if not Path(path).exists():
        create_tiledb_schema(uri, {})
        for i in range(0, len(input_file_list), batch_size):
            batch_files = input_file_list[i : i + batch_size]
            tasks = [
                delayed(process_and_ingest)(file, uri, {}) for file in batch_files
            ]
            # Submit tasks and wait for completion
            compute(*tasks)
            logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
    else:
        for file_path in input_file_list:
            process_and_ingest(file_path, uri, {})

