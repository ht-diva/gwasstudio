from pathlib import Path

import click
import cloup
import pandas as pd
import tiledb
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import manage_daskcluster, dask_deployment_types
from gwasstudio.methods.extraction_methods import extract_full_stats, extract_regions, extract_snp_list
from gwasstudio.methods.locus_breaker import _process_locusbreaker
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config, get_dask_batch_size, get_dask_deployment
from gwasstudio.utils.metadata import (
    load_search_topics,
    query_mongo_obj,
    dataframe_from_mongo_objs,
)
from gwasstudio.utils.mongo_manager import manage_mongo


def _process_function_tasks(tiledb_array, trait_id_list, attr, batch_size, output_prefix_dict, output_format, **kwargs):
    """This function schedules and executes generic delayed tasks for various export processes"""

    def get_snp_list(snp_list_file):
        snp_list = None
        if snp_list_file:
            snp_list = pd.read_csv(snp_list_file, usecols=["CHR", "POS"], dtype={"CHR": str, "POS": int})
            snp_list = snp_list[snp_list["CHR"].str.isnumeric()]
        return snp_list

    function_name = kwargs.pop("function_name", None)
    snp_list_file = kwargs.pop("snp_list_file", None)

    kwargs["attributes"] = attr.split(",") if attr else None
    if snp_list_file:
        kwargs["snp_list"] = delayed(get_snp_list)(snp_list_file)

    tasks = [
        delayed(function_name)(tiledb_array, trait, output_prefix_dict.get(trait), output_format, **kwargs)
        for trait in trait_id_list
    ]
    for i in range(0, len(tasks), batch_size):
        logger.info(f"Processing a batch of {batch_size} items for batch {i // batch_size + 1}")
        # Create a list of delayed tasks
        # Submit tasks and wait for completion
        batch = tasks[i : i + batch_size]
        compute(*batch)
        logger.info(f"Batch {i // batch_size + 1} completed.", flush=True)


help_doc = """
Export summary statistics from TileDB datasets with various filtering options.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB options",
    cloup.option("--uri", required=True, default=None, help="TileDB dataset URI"),
    cloup.option("--output-prefix", default="out", help="Prefix to be used for naming the output files"),
    cloup.option(
        "--output-format", type=click.Choice(["parquet", "csv.gz", "csv"]), default="csv.gz", help="Output file format"
    ),
    cloup.option("--search-file", required=True, default=None, help="The search file used for querying metadata"),
    cloup.option(
        "--attr", required=True, default="BETA,SE,EAF", help="string delimited by comma with the attributes to export"
    ),
)
@cloup.option_group(
    "Locusbreaker options",
    cloup.option("--locusbreaker", default=False, is_flag=True, help="Option to run locusbreaker"),
    cloup.option("--pvalue-sig", default=5.0, help="Maximum log p-value threshold within the window"),
    cloup.option("--pvalue-limit", default=3.3, help="Log p-value threshold for loci borders"),
    cloup.option(
        "--hole-size",
        default=250000,
        help="Minimum pair-base distance between SNPs in different loci (default: 250000)",
    ),
)
@cloup.option_group(
    "SNP ID list filtering options",
    cloup.option(
        "--snp-list-file",
        default=None,
        help="A txt file which must include CHR and POS columns",
    ),
)
@cloup.option_group(
    "Regions filtering options",
    cloup.option(
        "--get-regions",
        default=None,
        help="Bed file with regions to filter",
    ),
    cloup.option(
        "--maf",
        default=0.01,
        help="MAF filter to apply to each region",
    ),
    cloup.option(
        "--phenovar",
        default=False,
        is_flag=True,
        help="Boolean to compute phenovariance (Work in progress, not fully implemented yet)",
    ),
    cloup.option(
        "--nest",
        default=False,
        is_flag=True,
        help="Estimate effective population size (Work in progress, not fully implemented yet)",
    ),
)
@click.pass_context
def export(
    ctx,
    uri,
    search_file,
    attr,
    output_prefix,
    output_format,
    pvalue_sig,
    pvalue_limit,
    hole_size,
    phenovar,
    nest,
    maf,
    snp_list_file,
    locusbreaker,
    get_regions,
):
    """Export summary statistics based on selected options."""
    cfg = get_tiledb_config(ctx)

    if not check_file_exists(search_file, logger):
        exit(1)

    # Open TileDB dataset
    with tiledb.open(uri, mode="r", config=cfg) as tiledb_array:
        logger.info("TileDB dataset loaded")
        search_topics, output_fields = load_search_topics(search_file)
        with manage_mongo(ctx):
            mongo_uri = get_mongo_uri(ctx)
            obj = EnhancedDataProfile(uri=mongo_uri)
            objs = query_mongo_obj(search_topics, obj)
        df = dataframe_from_mongo_objs(output_fields, objs)

        trait_id_list = list(df["data_id"])

        # Create output prefix dictionary
        key_column = "data_id"
        value_column = "output_prefix"
        df[value_column] = f"{output_prefix}_" + df.get("notes.source_id", df[key_column])
        output_prefix_dict = df.set_index(key_column)[value_column].to_dict()

        # write metadata query result
        path = Path(output_prefix)
        output_path = path.with_suffix("").with_name(path.stem + "_meta")
        kwargs = {"index": False}
        write_table(df, str(output_path), logger, file_format="csv", **kwargs)

        # Process according to selected options
        if get_dask_deployment(ctx) in dask_deployment_types:
            batch_size = get_dask_batch_size(ctx)
            with manage_daskcluster(ctx):
                args = [tiledb_array, trait_id_list, attr, batch_size, output_prefix_dict, output_format]
                if locusbreaker:
                    kwargs = {
                        "function_name": _process_locusbreaker,
                        "maf": maf,
                        "hole_size": hole_size,
                        "pvalue_sig": pvalue_sig,
                        "pvalue_limit": pvalue_limit,
                        "phenovar": phenovar,
                    }
                    _process_function_tasks(*args, **kwargs)

                elif snp_list_file:
                    kwargs = {
                        "function_name": extract_snp_list,
                        "snp_list_file": snp_list_file,
                    }
                    _process_function_tasks(*args, **kwargs)

                elif get_regions:
                    bed_region = pd.read_csv(get_regions, sep="\t", header=None)
                    bed_region.columns = ["CHR", "START", "END"]
                    bed_region["CHR"] = bed_region["CHR"].astype(int)
                    kwargs = {"function_name": extract_regions, "bed_region": bed_region.groupby("CHR")}
                    _process_function_tasks(*args, **kwargs)

                else:
                    kwargs = {"function_name": extract_full_stats}
                    _process_function_tasks(*args, **kwargs)

        else:
            logger.error(f"A valid dask deployment type needed: {dask_deployment_types}")
            exit(1)
