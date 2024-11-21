import os

import click
import cloup
import dask
import pandas as pd
from gwasstudio.utils import process_and_ingest
from gwasstudio.utils import create_tiledb_schema
from gwasstudio import logger


help_doc = """
Ingest data data in a TileDB-unified dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--checksum",
        default=None,
        help="Path to the checksum file",
    ),
    cloup.option(
        "--uri",
        default=None,
        help="S3 path where to store the tiledb dataset.",
    )
)
@click.pass_context
def ingest(ctx, checksum, uri):
    # Parse checksum for mapping ids to files
    checksum = pd.read_csv(checksum, sep="\t", header=None)
    checksum.columns = ["hash", "filename"]
    checksum_dict = checksum.set_index("hash")["filename"].to_dict()
    # Getting the file list and iterate through it using Dask
    cfg = ctx.obj["cfg"]

    # Process files in batches
    if not os.path.exists(uri):
        create_tiledb_schema(uri, cfg)

    file_list = list(checksum_dict.keys())
    print(ctx.obj)
    batch_size = ctx.obj["batch_size"]
    for i in range(0, len(file_list), batch_size):
        batch_files = file_list[i : i + batch_size]
        tasks = [
            dask.delayed(process_and_ingest)(file, uri, checksum_dict, cfg)
            for file in batch_files
        ]
        # Submit tasks and wait for completion
        dask.compute(*tasks)
        logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
