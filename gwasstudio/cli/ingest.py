import os

import click
import cloup

import numpy as np
import pandas as pd
import tiledb
from gwasstudio.utils import process_and_ingest
from gwasstudio.utils import create_tiledb_schema
import os
help_doc = """
Ingest data data in a TileDB-unified dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--checksum",
        default=None,
        help="Path to a folder where all the vcf.gz files to ingest are stored",
    ),
    cloup.option(
        "--uri",
        default=None,
        help="S3 path for the tiledb dataset.",
    ),
    cloup.option(
        "--restart",
        default=False,
        help="Restart the ingestion from a previous run.",
    ),
)
@cloup.option_group(
    "TileDB configurations",
    cloup.option("--batch-size", default=5, help="The number of files to ingest in parallel."),
)
@click.pass_context
def ingest(ctx, checksum, uri, batch_size, restart):
        # Parse checksum for mapping ids to files
        #logging.info(f"Batch {i // batch_size + 1} completed.", flush=True)
        checksum = pd.read_csv(checksum, sep = "\t", header=None)
        checksum.columns = ["hash","filename"]
        #checksum_dict = pd.Series(checksum.hash.values, index=checksum.filename).to_dict()
        checksum_dict = checksum.set_index('hash')['filename'].to_dict()
        print(checksum_dict)
        # Getting the file list and iterate through it using Dask
        cfg = ctx.obj["cfg"]

        # Process files in batches
        if not os.path.exists(uri):
            create_tiledb_schema(uri, cfg)
        
        file_list = list(checksum_dict.keys())
        
        for i in range(0, len(file_list), batch_size):
            batch_files = file_list[i:i + batch_size]
            tasks = [dask.delayed(process_and_ingest)(file, uri, checksum_dict, ctx) for file in batch_files]
            # Submit tasks and wait for completion
            dask.compute(*tasks)
            #logging.info(f"Batch {i // batch_size + 1} completed.", flush=True)

# what is dict_type?
#    for i in range(0, len(file_list), batch_size):
#        batch_files = file_list[i : i + batch_size]
#
#        tasks = [
#            dask.delayed(process_and_ingest)(input_path + file, uri, checksum_dict, dict_type, cfg)
#            for file in batch_files
#        ]
#        # Submit tasks and wait for completion
#        dask.compute(*tasks)
#        logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
#
