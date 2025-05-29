from pathlib import Path

import click
import cloup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tiledb
from dask import delayed, compute

from gwasstudio import logger
from gwasstudio.methods.locus_breaker import locus_breaker
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table, get_log_p_value_from_z
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config, get_dask_batch_size
from gwasstudio.utils.metadata import (
    load_search_topics,
    query_mongo_obj,
    dataframe_from_mongo_objs,
)
from gwasstudio.utils.mongo_manager import manage_mongo



def _process_locusbreaker(tiledb_unified, trait, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_file):
    """Process data using the locus breaker algorithm."""
    logger.info("Running locus breaker")
    subset_SNPs_pd = tiledb_unified.query(
    ).df[:, :, trait]

    subset_SNPs_pd = subset_SNPs_pd[(subset_SNPs_pd["EAF"] >= maf) & (subset_SNPs_pd["EAF"] <= (1 - maf))]
    if("MLOG10P" not in subset_SNPs_pd.columns):
        subset_SNPs_pd["MLOG10P"] = (
            subset_SNPs_pd["BETA"] / subset_SNPs_pd["SE"]
        ).abs().apply(lambda x: get_log_p_value_from_z(x))

    results_lb_segments, results_lb_intervals = locus_breaker(
        subset_SNPs_pd, hole_size=hole_size, pvalue_sig=pvalue_sig, pvalue_limit=pvalue_limit, phenovar=phenovar
    )

    logger.info(f"Saving locus-breaker output in {output_file} segments and intervals")
    kwargs = {"index": False}
    write_table(results_lb_segments, f"{output_file}_{trait}_segments", logger, file_format="csv", **kwargs)
    write_table(results_lb_intervals, f"{output_file}_{trait}_intervals", logger, file_format="csv", **kwargs)


@delayed
def _delayed_locus_breaker(tiledb_unified, trait, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_file):
    # Call locus_breaker with Dask delayed option
    return _process_locusbreaker(tiledb_unified, trait, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_file)


def _process_locusbreaker_tasks(
    tiledb_unified, trait_id_list, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_prefix, batch_size
):
    """Process locus breaker tasks in batches."""
    tasks = []
    for trait in trait_id_list:
        task = _delayed_locus_breaker(
            tiledb_unified, trait, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_prefix
        )
        tasks.append(task)
    for i in range(0, len(tasks), batch_size):
        logger.info(f"Batch {i} of {len(tasks)}")
        batch = tasks[i : i + batch_size]
        compute(*batch)


def _process_snp_list(tiledb_unified, snp_list_file, trait_id_list, attr, output_file):
    """Process data filtering by a list of SNPs."""
    SNP_list = pd.read_csv(snp_list_file, usecols=["CHR", "POS"], dtype={"CHR": str, "POS": int})
    SNP_list = SNP_list[SNP_list["CHR"].astype(str).str.isnumeric()]
    chromosome_dict = SNP_list.groupby("CHR")["POS"].apply(list).to_dict()
    # parallelize by n_workers traits at a time with dask the query on tiledb

    for trait in trait_id_list:
        for chrom, positions in chromosome_dict.items():
            chromosomes = int(chrom)
            unique_positions = list(set(positions))

            tiledb_iterator_query = tiledb_unified.query(
                dims=["CHR", "TRAITID", "POS"], attrs=attr.split(","), return_arrow=True
            ).df[chromosomes, trait, unique_positions]

            if("MLOG10P" not in tiledb_iterator_query_df.columns):
                tiledb_iterator_query["MLOG10P"] = ( 
                    1 - tiledb_iterator_query["BETA"] / tiledb_iterator_query["SE"]
                ).abs().apply(lambda x:  get_log_p_value_from_z(x))
            tiledb_iterator_query_df = tiledb_iterator_query.to_pandas()
            if "SNPID" in attr.split(","):
                tiledb_iterator_query_df["SNPID"] = (
                    tiledb_iterator_query_df["CHR"].astype(str)
                    + ":"
                    + tiledb_iterator_query_df["POS"].astype(str)
                    + ":"
                    + tiledb_iterator_query_df["EA"]
                    + ":"
                    + tiledb_iterator_query_df["NEA"]
                )   

            kwargs = {"header": False, "index": False, "mode": "a"}
            write_table(tiledb_iterator_query_df, f"{output_file}_{trait}", logger, file_format="csv", **kwargs)


def _export_all_stats(tiledb_unified, trait_id_list, output_file, attr):
    """Export all summary statistics."""
    for trait in trait_id_list:
        tiledb_query = tiledb_unified.query(
            dims=["CHR", "TRAITID", "POS"],
            attrs=attr.split(","),
            return_arrow=True,
        ).df[:, trait, :]
        if "MLOG10P" not in tiledb_query.columns:
            tiledb_query["MLOG10P"] = (
                tiledb_query["BETA"] / tiledb_query["SE"]
            ).abs().apply(lambda x: get_log_p_value_from_z(x))
        if "SNPID" in attr.split(","):
            tiledb_query["SNPID"] = (
                tiledb_query["CHR"].astype(str)
                + ":"
                + tiledb_query["POS"].astype(str)
                + ":"
                + tiledb_query["EA"]
                + ":"
                + tiledb_query["NEA"]
            )
        kwargs = {"index": False}
        write_table(tiledb_query.to_pandas(), f"{output_file}_{trait}", logger, file_format="parquet", **kwargs)


def _process_regions(tiledb_unified, bed_region, trait, maf, attr, output_file):
    """Process data filtering by genomic regions and output as concatenated Arrow table in Parquet format."""
    arrow_tables = []
    for chr, group in bed_region:
        # Get all (start, end) tuples for this chromosome
        min_pos = min(group["START"])
        if min_pos < 0:
            min_pos = 1
        max_pos = max(group["END"])

        # Query TileDB and convert directly to Arrow table
        arrow_table = tiledb_unified.query(attrs=attr.split(","), dims=["CHR", "POS", "TRAITID"], return_arrow=True).df[
            chr, trait, min_pos:max_pos
        ]

        arrow_tables.append(arrow_table)

    # Concatenate all Arrow tables
    concatenated = pa.concat_tables(arrow_tables)

    if "SNPID" in attr.split(","):
        # Create SNPID column if it doesn't exist
        concatenated = concatenated.append_column(
            "SNPID",
            pa.array(
                [
                    f"{row['CHR']}:{row['POS']}:{row['EA']}:{row['NEA']}"
                    for row in concatenated.to_pydict().values()
                ]
            ),
        )

    # Write to Parquet
    pq.write_table(concatenated, f"{output_file}_{trait}.parquet")


@delayed
def _delayed_process_regions(tiledb_unified, bed_region, trait, maf, attr, output_file):
    # Call locus_breaker with Dask delayed option
    return _process_regions(tiledb_unified, bed_region, trait, maf, attr, output_file)


def _process_region_tasks(tiledb_unified, bed_region, trait_id_list, maf, attr, output_prefix, batch_size):
    """Process region tasks in batches."""
    grouped = bed_region.groupby("CHR")
    tasks = []
    if batch_size == 1:
        for trait in trait_id_list:
            _process_regions(tiledb_unified, grouped, trait, maf, attr, output_prefix)
            logger.info(f"Processed trait {trait}")
    else:
        for trait in trait_id_list:
            task = _delayed_process_regions(tiledb_unified, grouped, trait, maf, attr, output_prefix)
            tasks.append(task)
        for i in range(0, len(tasks), batch_size):
            logger.info(f"Batch {i} of {len(tasks)}")
            batch = tasks[i : i + batch_size]
            compute(*batch)


help_doc = """
Export summary statistics from TileDB datasets with various filtering options.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB options",
    cloup.option("--uri", required=True, default=None, help="TileDB dataset URI"),
    cloup.option("--output-prefix", default="out", help="Prefix to be used for naming the output files"),
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
        "--snp-list",
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
    output_prefix,
    pvalue_sig,
    pvalue_limit,
    hole_size,
    phenovar,
    nest,
    maf,
    snp_list,
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
        path = Path(output_prefix)
        output_path = path.with_suffix("").with_name(path.stem + "_meta")
        kwargs = {"index": False}
        write_table(df, str(output_path), logger, file_format="csv", **kwargs)

        # Process according to selected options

        batch_size = get_dask_batch_size(ctx)
        if locusbreaker:
            _process_locusbreaker_tasks(
                tiledb_unified,
                trait_id_list,
                maf,
                hole_size,
                pvalue_sig,
                pvalue_limit,
                phenovar,
                output_prefix,
                batch_size,
            )

        elif snp_list:
            _process_snp_list(tiledb_unified, snp_list, trait_id_list, attr, output_prefix)
        elif get_regions:
            bed_region = pd.read_csv(get_regions, sep="\t", header=None)
            bed_region.columns = ["CHR", "START", "END"]
            bed_region["CHR"] = bed_region["CHR"].astype(int)
            _process_region_tasks(tiledb_unified, bed_region, trait_id_list, maf, attr, output_prefix, batch_size)
        else:
            _export_all_stats(tiledb_unified, trait_id_list, output_prefix)
