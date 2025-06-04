from pathlib import Path

import click
import cloup
import pandas as pd
import tiledb
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.dask_client import manage_daskcluster, dask_deployment_types
from gwasstudio.methods.locus_breaker import locus_breaker
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table, get_log_p_value_from_z
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config, get_dask_batch_size, get_dask_deployment
from gwasstudio.utils.metadata import (
    load_search_topics,
    query_mongo_obj,
    dataframe_from_mongo_objs,
)
from gwasstudio.utils.mongo_manager import manage_mongo

def _build_snpid(attributes, tiledb_query_df):
    if "SNPID" in attributes:
            tiledb_query_df["SNPID"] = (
                    tiledb_query_df["CHR"].astype(str)
                    + ":"
                    + tiledb_query_df["POS"].astype(str)
                    + ":"
                    + tiledb_query_df["EA"]
                    + ":"
                    + tiledb_query_df["NEA"]
                )
    return tiledb_query_df

def _process_locusbreaker(tiledb_unified, 
                            trait, 
                            output_file, 
                            bed_region = None, 
                            attr= None, 
                            snp_list = None, 
                            maf = None, 
                            hole_size = None, 
                            pvalue_sig = None, 
                            pvalue_limit = None, 
                            phenovar = None):
    """Process data using the locus breaker algorithm."""
    logger.info("Running locus breaker")
    subset_SNPs_pd = tiledb_unified.query().df[:, trait, :]

    subset_SNPs_pd = subset_SNPs_pd[(subset_SNPs_pd["EAF"] >= maf) & (subset_SNPs_pd["EAF"] <= (1 - maf))]
    if "MLOG10P" not in subset_SNPs_pd.columns:
        subset_SNPs_pd["MLOG10P"] = (
            (subset_SNPs_pd["BETA"] / subset_SNPs_pd["SE"]).abs().apply(lambda x: get_log_p_value_from_z(x))
        )

    results_lb_segments, results_lb_intervals = locus_breaker(
        subset_SNPs_pd, hole_size=hole_size, pvalue_sig=pvalue_sig, pvalue_limit=pvalue_limit, phenovar=phenovar
    )

    logger.info(f"Saving locus-breaker output in {output_file} segments and intervals")
    kwargs = {"index": False}
    write_table(results_lb_segments, f"{output_file}_{trait}_segments", logger, file_format="csv", **kwargs)
    write_table(results_lb_intervals, f"{output_file}_{trait}_intervals", logger, file_format="csv", **kwargs)


def _process_snp_list(tiledb_unified, 
                            trait, 
                            output_file, 
                            bed_region = None, 
                            attr= None, 
                            snp_list = None, 
                            maf = None, 
                            hole_size = None, 
                            pvalue_sig = None, 
                            pvalue_limit = None, 
                            phenovar = None):
    """Process data filtering by a list of SNPs."""
    chromosomes_dict = SNP_list.groupby("CHR")["POS"].apply(list).to_dict()
    # parallelize by n_workers traits at a time with dask the query on tiledb
    attributes = attr.split(",")
    for chrom, positions in chromosomes_dict.items():
            chromosome = int(chrom)
            unique_positions = list(set(positions))

            tiledb_iterator_query_df = (
                tiledb_unified.query(dims=["CHR", "TRAITID", "POS"], attrs=attributes, return_arrow=True)
                .df[chromosome, trait, unique_positions]
                .to_pandas()
            )

            if "MLOG10P" not in tiledb_iterator_query_df.columns:
                tiledb_iterator_query_df["MLOG10P"] = (
                    (1 - tiledb_iterator_query_df["BETA"] / tiledb_iterator_query_df["SE"])
                    .abs()
                    .apply(lambda x: get_log_p_value_from_z(x))
                )
            tiledb_iterator_query_df = _build_snpid(attributes, tiledb_iterator_query_df)

            kwargs = {"header": False, "index": False, "mode": "a"}
            write_table(tiledb_iterator_query_df, f"{output_file}_{trait}", logger, file_format="csv", **kwargs)

def _process_export_all_stats(tiledb_unified, 
                            trait, 
                            output_file,
                            bed_region = None, 
                            attr= None, 
                            snp_list = None, 
                            maf = None, 
                            hole_size = None, 
                            pvalue_sig = None, 
                            pvalue_limit = None, 
                            phenovar = None):
    """Export all summary statistics."""
    attributes = attr.split(",")
    tiledb_query_df = (
            tiledb_unified.query(
                dims=["CHR", "TRAITID", "POS"],
                attrs=attributes,
                return_arrow=True,
            )
            .df[:, trait, :]
            .to_pandas()
        )

    if "MLOG10P" not in tiledb_query_df.columns:
            tiledb_query_df["MLOG10P"] = (
                (tiledb_query_df["BETA"] / tiledb_query_df["SE"]).abs().apply(get_log_p_value_from_z)
        )

    tiledb_query_df = _build_snpid(attributes, tiledb_query_df)
    kwargs = {"index": False}
    write_table(tiledb_query_df, f"{output_file}_{trait}", logger, file_format="parquet", **kwargs)


def _process_regions(tiledb_unified, 
                            trait, 
                            output_file,
                            bed_region = None, 
                            attr= None, 
                            snp_list = None, 
                            maf = None, 
                            hole_size = None, 
                            pvalue_sig = None, 
                            pvalue_limit = None, 
                            phenovar = None):
    """Process data filtering by genomic regions and output as concatenated DataFrame in Parquet format."""
    attributes = attr.split(",")
    dataframes = []
    for chr, group in bed_region:
        # Get all (start, end) tuples for this chromosome
        min_pos = min(group["START"])
        if min_pos < 0:
            min_pos = 1
        max_pos = max(group["END"])

        # Query TileDB and convert directly to Pandas DataFrame
        tiledb_query_df = (
            tiledb_unified.query(attrs=attributes, dims=["CHR", "POS", "TRAITID"], return_arrow=True)
            .df[chr, trait, min_pos:max_pos]
            .to_pandas()
        )

        dataframes.append(tiledb_query_df)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    #Add SNPID column
    concatenated_df = _build_snpid(attributes, concatenated_df)
    # Write to Parquet
    kwargs = {"index": False}
    write_table(concatenated_df, f"{output_file}_{trait}", logger, file_format="parquet", **kwargs)

def _process_function_tasks(function_name, 
                            tiledb_unified, 
                            trait_id_list, 
                            output_file, 
                            batch_size, 
                            bed_region = None, 
                            attr= None, 
                            snp_list_file = None, 
                            maf = None, 
                            hole_size = None, 
                            pvalue_sig = None, 
                            pvalue_limit = None, 
                            phenovar = None):
    """This function schedules and executes generic delayed tasks for various export processes"""
    if snp_list_file:
        snp_list = pd.read_csv(snp_list_file, usecols=["CHR", "POS"], dtype={"CHR": str, "POS": int})
        snp_list = snp_list[snp_list["CHR"].astype(str).str.isnumeric()]
    tasks = []
    for trait in trait_id_list:
        task = delayed(function_name(
            tiledb_unified, trait, output_file, bed_region, attr, snp_list, maf, hole_size, pvalue_sig, pvalue_limit, phenovar
        ))
        tasks.append(task)
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
    cloup.option("--output-file", default="out", help="Prefix to be used for naming the output files"),
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
        help="A txt file with a column containing the SNP ids",
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
        help="Boolean to compute phenovariance",
    ),
    cloup.option(
        "--nest",
        default=False,
        is_flag=True,
        help="Estimate effective population size",
    ),
)
@click.pass_context
def export(
    ctx,
    uri,
    search_file,
    attr,
    output_file,
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
    with tiledb.open(uri, mode="r", config=cfg) as tiledb_unified:
        logger.info("TileDB dataset loaded")
        search_topics, output_fields = load_search_topics(search_file)
        with manage_mongo(ctx):
            mongo_uri = get_mongo_uri(ctx)
            obj = EnhancedDataProfile(uri=mongo_uri)
            objs = query_mongo_obj(search_topics, obj)
        df = dataframe_from_mongo_objs(output_fields, objs)
        trait_id_list = list(df["data_id"])

        # write metadata query result
        path = Path(output_file)
        output_path = path.with_suffix("").with_name(path.stem + "_meta")
        kwargs = {"index": False}
        write_table(df, str(output_path), logger, file_format="csv", **kwargs)

        # Process according to selected options
        if get_dask_deployment(ctx) in dask_deployment_types:
            batch_size = get_dask_batch_size(ctx)
            with manage_daskcluster(ctx):

                if locusbreaker:
                    _process_function_tasks(
                    function_name = _process_locusbreaker,
                    tiledb_unified = tiledb_unified,
                    trait_id_list = trait_id_list,
                    maf = maf,
                    hole_size = hole_size,
                    pvalue_sig = pvalue_sig,
                    pvalue_limit = pvalue_limit,
                    phenovar = phenovar,
                    output_file = output_file,
                    batch_size = batch_size,
                    )

                elif snp_list_file:
                    _process_function_tasks(
                        function_name = _process_snp_list, 
                        tiledb_unified = tiledb_unified, 
                        trait_id_list = trait_id_list, 
                        attr =  attr, 
                        snp_list_file = snp_list_file, 
                        output_file = output_file,
                        batch_size = batch_size)
                elif get_regions:
                    bed_region = pd.read_csv(get_regions, sep="\t", header=None)
                    bed_region.columns = ["CHR", "START", "END"]
                    bed_region["CHR"] = bed_region["CHR"].astype(int)
                    _process_function_tasks(_process_regions,
                                            tiledb_unified = tiledb_unified, 
                                            bed_region = bed_region.groupby("CHR") , 
                                            trait_id_list = trait_id_list, 
                                            maf = maf, 
                                            attr = attr, 
                                            output_file = output_file, 
                                            batch_size = batch_size)
                else:
                    _process_function_tasks(_process_export_all_stats, 
                                            tiledb_unified = tiledb_unified, 
                                            trait_id_list = trait_id_list, 
                                            output_file = output_file, 
                                            attr = attr,
                                            batch_size = batch_size)
        else:
            logger.error(f"A valid dask deployment type needed: {dask_deployment_types}")
            exit(1)
