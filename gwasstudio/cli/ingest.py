import click
import cloup
import pathlib
from utils import process_and_ingest
from utils import create_tiledb_schema

help_doc = """
Ingest data data in a TileDB-unified dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--input-path",
        default=None,
        help="Path to a folder where all the vcf.gz files to ingest are stored",
    ),
    cloup.option(
        "-u",
        "--uri",
        default=None,
        help="S3 path for the tiledb dataset.",
    ),
    cloup.option(
        "-r",
        "--restart",
        default=False,
        help="Restart the ingestion from a previous run.",
    )
)
@cloup.option_group(
    "TileDB configurations",
    cloup.option("--batch-size", default=5, help="The number of files to ingest in parallel."),
)
@click.pass_context
def ingest(ctx, input_path,checksum_path, attrs, uri, mem_budget_mb, threads, batch_size, restart):
        # Parse checksum for mapping ids to files
        checksum = pd.read_csv(file_path + "checksum.txt", sep = "\t", header=None)
        checksum.columns = ["hash","filename"]
        checksum_dict = pd.Series(checksum.hash.values,index=checksum.filename).to_dict()

        # Getting the file list and iterate through it using Dask
        cfg = ctx.obj["cfg"]
        if restart:
            test_tiledb = tiledb.open(uri, "r")
            arrow_table = test_tiledb.query(return_arrow=True, dims=['trait_id'], attrs=[]).df[1, 1:10000000, :]
            unique_arrow = (np.unique(arrow_table))
            checksum_dict = pd.Series(checksum.filename.values,index=checksum.hash).to_dict()
            file_list = []
            checksum_dict_keys = checksum_dict.keys()
            for record in checksum_dict_keys:
                if record not in unique_arrow:
                    file_list.append(checksum_dict[record])

        # Process files in batches
        else:
            create_tiledb_schema(uri, cfg)
            file_list = os.listdir(file_path)
        
        for i in range(0, len(file_list), batch_size):
            batch_files = file_list[i:i + batch_size]
            tasks = [dask.delayed(process_and_ingest)(file_path + file, uri, checksum_dict, dict_type, cfg) for file in batch_files]
            # Submit tasks and wait for completion
            dask.compute(*tasks)
            logging.info(f"Batch {i // batch_size + 1} completed.", flush=True)

