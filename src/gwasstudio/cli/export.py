from pathlib import Path

import click
import cloup
import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.methods.compute_pheno_variance import compute_pheno_variance
from gwasstudio.methods.locus_breaker import locus_breaker
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists
from gwasstudio.utils.cfg import get_mongo_uri, get_tiledb_config
from gwasstudio.utils.metadata import (
    load_search_topics,
    query_mongo_obj,
    dataframe_from_mongo_objs,
)


def _process_locusbreaker(
    tiledb_unified, trait_id_list, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_file
):
    """Process data using the locus breaker algorithm."""
    logger.info("Running locus breaker")

    for trait in trait_id_list:
        subset_SNPs_pd = tiledb_unified.query(
            cond=f"EAF > {maf} and EAF < {1 - float(maf)}",
            dims=["CHR", "POS", "TRAITID"],
            attrs=["SNPID", "BETA", "SE", "EAF", "MLOG10P"],
        ).df[:, :, trait]

        results_lb_segments, results_lb_intervals = locus_breaker(
            subset_SNPs_pd, hole_size=hole_size, pvalue_sig=pvalue_sig, pvalue_limit=pvalue_limit, phenovar=phenovar
        )

        logger.info(f"Saving locus-breaker output in {output_file} segments and intervals")
        # results_lb_segments.to_csv(f"{output_file}_{trait}_segments.csv", index=False)
        # results_lb_intervals.to_csv(f"{output_file}_{trait}_intervals.csv", index=False)
        write_table(results_lb_segments, f"{output_file}_{trait}_segments", file_format="csv")
        write_table(results_lb_intervals, f"{output_file}_{trait}_intervals", file_format="csv")


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
                dims=["CHR", "POS", "TRAITID"], attrs=attr.split(","), return_arrow=True
            ).df[chromosomes, unique_positions, trait]

            tiledb_iterator_query_df = tiledb_iterator_query.to_pandas()
            kwargs = {"header": False, "mode": "a"}
            write_table(tiledb_iterator_query_df, f"{output_file}_{trait}", file_format="csv", **kwargs)


def _export_all_stats(tiledb_unified, trait_id_list, output_file):
    """Export all summary statistics."""
    for trait in trait_id_list:
        tiledb_query = tiledb_unified.query(
            dims=["CHR", "POS", "TRAITID"],
            attrs=["SNPID", "BETA", "SE", "EAF", "MLOG10P"],
            return_arrow=True,
        ).df[:, :, trait]

        write_table(tiledb_query.to_pandas(), f"{output_file}_{trait}", file_format="parquet")


def _process_regions(tiledb_unified, regions_file, trait_id_list, maf, attr, phenovar, nest, output_file):
    """Process data filtering by genomic regions."""
    bed_region = pd.read_csv(regions_file, sep="\t", header=None)
    bed_region.columns = ["CHR", "START", "END"]

    for trait in trait_id_list:
        df_trait = []

        for _, row in bed_region.iterrows():
            subset_SNPs_pd = tiledb_unified.query(
                cond=f"EAF > {maf} and EAF < {1 - float(maf)}", dims=["CHR", "POS", "TRAITID"], attrs=attr.split(",")
            ).df[row["CHR"], row["START"] : row["END"], trait]

            if phenovar:
                pv = compute_pheno_variance(subset_SNPs_pd)
                subset_SNPs_pd["S"] = pv

                if nest:
                    neff = pv / (
                        (2 * subset_SNPs_pd["EAF"] * (1 - subset_SNPs_pd["EAF"])) * (subset_SNPs_pd["SE"] ** 2)
                    )
                    subset_SNPs_pd["NEFF"] = neff

            df_trait.append(subset_SNPs_pd)

        df_trait_concat = pd.concat(df_trait)
        write_table(df_trait_concat, f"{output_file}_{trait}", file_format="parquet")


def write_table(
    df: pd.DataFrame,
    where: str,
    compression: str = "snappy",
    file_format: str = "parquet",
    log_msg: str = "none",
    **kwargs,
):
    """
    Writes the given DataFrame to a specified location on disk in the desired format. Two file
    formats are supported: "parquet" and "csv". The function handles file compression for
    "parquet" format. Logs a custom or default message indicating the status.

    :param df: The pandas DataFrame to be saved.
    :param where: Destination path where the file should be saved without extension.
    :param compression: Compression type for parquet files. Default is "snappy".
    :param file_format: File format to save the data, either "parquet" or "csv". Default is "parquet".
    :param log_msg: Custom log message. If "none", a default message will be logged. Default is "none".
    :param kwargs: Any additional keyword arguments to be passed to the underlying `to_parquet` or
        `to_csv` pandas methods.
    :return: None
    """
    # Check if format is valid
    if file_format not in ["parquet", "csv"]:
        raise ValueError("Format must be either 'parquet' or 'csv'")

    # Set the output filename based on the provided format and extension
    file_extension = "." + file_format

    # Create the full path by joining the output directory and filename with extension
    output_path = f"{where}{file_extension}"

    msg = log_msg if log_msg != "none" else f"Saving DataFrame to {output_path}"
    logger.info(msg)

    if file_format == "parquet":
        df.to_parquet(output_path, index=False, engine="pyarrow", compression=compression, **kwargs)
    elif file_format == "csv":
        df.to_csv(output_path, index=False, **kwargs)


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
        default=None,
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

    mongo_uri = get_mongo_uri(ctx)

    # Open TileDB dataset
    with tiledb.open(uri, mode="r", config=cfg) as tiledb_unified:
        logger.info("TileDB dataset loaded")
        search_topics, output_fields = load_search_topics(search_file)
        obj = EnhancedDataProfile(uri=mongo_uri)
        objs = query_mongo_obj(search_topics, obj)
        df = dataframe_from_mongo_objs(output_fields, objs)
        trait_id_list = list(df["data_id"])

        # write metadata query result
        path = Path(output_prefix)
        output_path = path.with_suffix("").with_name(path.stem + "_meta")
        write_table(df, str(output_path), file_format="csv")

        # Process according to selected options
        if locusbreaker:
            _process_locusbreaker(
                tiledb_unified, trait_id_list, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_prefix
            )
        elif snp_list:
            _process_snp_list(tiledb_unified, snp_list, trait_id_list, attr, output_prefix)
        elif get_regions:
            _process_regions(tiledb_unified, get_regions, trait_id_list, maf, attr, phenovar, nest, output_prefix)
        else:
            _export_all_stats(tiledb_unified, trait_id_list, output_prefix)
