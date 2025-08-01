from pathlib import Path

import click
import cloup
import pandas as pd
import tiledb
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import manage_daskcluster, dask_deployment_types
from gwasstudio.methods.extraction_methods import extract_full_stats, \
    extract_regions, extract_snp_list
from gwasstudio.methods.locus_breaker import _process_locusbreaker
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config, \
    get_dask_batch_size, get_dask_deployment
from gwasstudio.utils.enums import MetadataEnum
from gwasstudio.utils.metadata import load_search_topics, query_mongo_obj, \
    dataframe_from_mongo_objs
from gwasstudio.utils.mongo_manager import manage_mongo


def create_output_prefix_dict(df: pd.DataFrame, output_prefix: str, source_id_column: str) -> dict:
    """
    Generates a dictionary mapping data IDs to output prefixes based on column values.

    Parameters:
        df (pd.DataFrame): Input DataFrame containing the required columns.
        output_prefix (str): Prefix to prepend to output filenames.
        source_id_column (str): Column name containing source id

    Returns:
        dict: Dictionary with 'data_id' as keys and corresponding output prefixes as values.
    """
    logger.debug("Creating output prefix dictionary")
    key_column = "data_id"
    value_column = "output_prefix"

    # Determine the column to use for prefixing
    column_to_get = source_id_column if source_id_column in df.columns else key_column
    logger.debug(f"Selected column for prefixing: {column_to_get}")

    # Construct the output prefix column with fallback
    df[value_column] = f"{output_prefix}_" + df[column_to_get].fillna(df[key_column]).astype(str)

    # Create dictionary mapping data IDs to prefixes
    output_prefix_dict = df.set_index(key_column)[value_column].to_dict()
    logger.debug("Output prefix dictionary created")

    return output_prefix_dict


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
        delayed(function_name)(tiledb_array, trait, output_prefix_dict.get(trait), output_format, plot_out, **kwargs)
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
        "--attr",
        required=True,
        default="BETA,SE,EAF,MLOG10P",
        help="string delimited by comma with the attributes to export",
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
    plot_out=False
):
    """Export summary statistics based on selected options."""
    cfg = get_tiledb_config(ctx)
    if not check_file_exists(search_file, logger):
        exit(1)
        
    search_topics, output_fields = load_search_topics(search_file)
    if plot_out:
        plot_config = get_plot_config(ctx)
        if not plot_config:
            logger.error("Plotting configuration is required for plotting output.")
            exit(1)
        if "data_id" not in search_topics:
            logger.error("Plotting option is enabled but no data_id is provided in the search file.")
            exit(1)
        if len(search_topics["data_id"]) > 20:
            logger.error("Plotting option is enabled but too many data_ids are provided in the search file. Please limit to 20 data_ids.")
            exit(1)
           

    # Open TileDB dataset
    with tiledb.open(uri, mode="r", config=cfg) as tiledb_array:
        logger.info("TileDB dataset loaded")
        with manage_mongo(ctx):
            mongo_uri = get_mongo_uri(ctx)
            obj = EnhancedDataProfile(uri=mongo_uri)
            objs = query_mongo_obj(search_topics, obj)
        df = dataframe_from_mongo_objs(output_fields, objs)

        trait_id_list = list(df["data_id"])

        # Create an output prefix dictionary to generate output filenames
        source_id_column = MetadataEnum.get_source_id_field()
        output_prefix_dict = create_output_prefix_dict(df, output_prefix, source_id_column=source_id_column)

        # write metadata query result
        path = Path(output_prefix)
        output_path = path.with_suffix("").with_name(path.stem + "_meta")
        kwargs = {"index": False}
        log_msg = f"{len(objs)} results found. Writing to {output_path}.csv"
        write_table(df, str(output_path), logger, file_format="csv", log_msg=log_msg, **kwargs)

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
