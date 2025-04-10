from pathlib import Path

import click
import cloup

from gwasstudio import logger
from gwasstudio.dask_client import dask_deployment_types
from gwasstudio.utils import create_tiledb_schema, parse_uri, process_and_ingest
from gwasstudio.utils.cfg import get_tiledb_config
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
    if scheme == "s3":
        ingest_to_s3(ctx, input_file_list, uri)
    elif scheme == "file":
        ingest_to_fs(ctx, input_file_list, uri)
    else:
        logger.error(f"Do not recognize the uri's scheme: {uri}")
        exit()


def ingest_to_s3(ctx, input_file_list, uri):
    cfg = get_tiledb_config(ctx)

    if not does_uri_path_exist(uri, cfg):
        logger.info("Creating TileDB schema")
        create_tiledb_schema(uri, cfg)

    if ctx.obj["dask"]["deployment"] in dask_deployment_types:
        pass
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                logger.info(f"processing {file_path}")
                process_and_ingest(file_path, uri, cfg)
            else:
                logger.info(f"skipping {file_path}")


def ingest_to_fs(ctx, input_file_list, uri):
    _, __, path = parse_uri(uri)
    if not Path(path).exists():
        create_tiledb_schema(uri, {})

    if ctx.obj["dask"]["deployment"] in dask_deployment_types:
        pass
    else:
        for file_path in input_file_list:
            if Path(file_path).exists():
                process_and_ingest(file_path, uri, {})

    # Commenting temporarly
    # # Parse checksum for mapping ids to files
    # checksum = pd.read_csv(checksum, sep="\t", header=None)
    # checksum.columns = ["hash", "filename"]
    # checksum_dict = checksum.set_index("hash")["filename"].to_dict()
    # # Getting the file list and iterate through it using Dask
    # cfg = ctx.obj["cfg"]
    #
    # # Process files in batches
    # if not os.path.exists(uri):
    #     create_tiledb_schema(uri, cfg)
    #
    # file_list = list(checksum_dict.keys())
    # print(ctx.obj)
    # batch_size = ctx.obj["batch_size"]
    # for i in range(0, len(file_list), batch_size):
    #     batch_files = file_list[i : i + batch_size]
    #     tasks = [dask.delayed(process_and_ingest)(file, uri, checksum_dict, cfg) for file in batch_files]
    #     # Submit tasks and wait for completion
    #     dask.compute(*tasks)
    #     logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)
