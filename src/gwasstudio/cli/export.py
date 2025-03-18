import click
import cloup
import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.cli.metadata.utils import (
    load_search_topics,
    query_mongo_obj,
    dataframe_from_mongo_objs,
)
from gwasstudio.methods.compute_pheno_variance import compute_pheno_variance
from gwasstudio.methods.locus_breaker import locus_breaker
from gwasstudio.mongo.models import EnhancedDataProfile


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
        write_table(results_lb_segments, f"{output_file}_{trait}_segments", format="csv")
        write_table(results_lb_intervals, f"{output_file}_{trait}_intervals", format="csv")


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
            # tiledb_iterator_query_df.to_csv(f"{output_file}_{trait}", index=False, header=False, mode="a")
            kwargs = {"header": False, "index": False, "mode": "a"}
            write_table(
                tiledb_iterator_query_df, f"{output_file}_{trait}", compression="snappy", format="csv", **kwargs
            )

        logger.info(f"Saved filtered summary statistics by SNPs in {output_file}_{trait}")


def _export_all_stats(tiledb_unified, trait_id_list, output_file):
    """Export all summary statistics."""
    for trait in trait_id_list:
        tiledb_query = tiledb_unified.query(
            dims=["CHR", "POS", "TRAITID"],
            attrs=["SNPID", "BETA", "SE", "EAF", "MLOG10P"],
            return_arrow=True,
        ).df[:, :, trait]

        # logger.info(f"Saving all summary statistics in {output_file}")
        # pq.write_table(tiledb_query, f"{output_file}_{trait}.parquet", compression="snappy")
        write_table(tiledb_query, f"{output_file}_{trait}", compression="snappy", format="parquet")


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
        # logger.info(f"Saving summary statistics in {output_file}")
        # pq.write_table(df_trait_concat, f"{output_file}_{trait}.parquet", compression="snappy")
        write_table(df_trait_concat, f"{output_file}_{trait}", compression="snappy", format="parquet")


def write_table(
    df: pd.DataFrame, where: str, compression: str = "snappy", format: str = "parquet", kwargs: dict = None
):
    """
    Write a Pandas DataFrame to a file.

    Args:
        df (pd.DataFrame): The input DataFrame.
        where (str): The output path and filename without extension.
        compression (str): Compression type. Default is 'snappy' for Parquet files.
        format (str): Output file format, can be 'parquet' or 'csv'. Default is 'parquet'.
        kwargs (dict): keyword arguments for Pandas IO functions.
    """
    # Check if format is valid
    if format not in ["parquet", "csv"]:
        raise ValueError("Format must be either 'parquet' or 'csv'")

    # Set the output filename based on the provided format and extension
    file_extension = "." + format

    # Create the full path by joining the output directory and filename with extension
    output_path = f"{where}{file_extension}"

    logger.info(f"Saving Dataframe to {output_path}")

    if format == "parquet":
        df.to_parquet(output_path, index=False, engine="pyarrow", compression=compression, **kwargs)
    elif format == "csv":
        df.to_csv(output_path, index=False, **kwargs)


help_doc = """
Export summary statistics from TileDB datasets with various filtering options.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB mandatory options",
    cloup.option("--uri", required=True, default=None, help="TileDB dataset URI"),
    cloup.option("--output-file", required=True, default="out", help="Path to output file"),
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
    output_file,
    pvalue_sig,
    pvalue_limit,
    hole_size,
    phenovar,
    nest,
    maf,
    snp_list,
    locusbreaker,
    get_all,
    get_regions,
):
    """Export summary statistics based on selected options."""
    cfg = ctx.obj["cfg"]

    # Open TileDB dataset
    with tiledb.open(uri, mode="r", config=cfg) as tiledb_unified:
        logger.info("TileDB dataset loaded")
        search_topics, output_fields = load_search_topics(search_file)
        obj = EnhancedDataProfile(uri=ctx.obj["mongo"]["uri"])
        objs = query_mongo_obj(search_topics, obj)
        df = dataframe_from_mongo_objs(output_fields, objs)
        trait_id_list = list(df["data_id"])

        # Process according to selected options
        if locusbreaker:
            _process_locusbreaker(
                tiledb_unified, trait_id_list, maf, hole_size, pvalue_sig, pvalue_limit, phenovar, output_file
            )
        elif snp_list:
            _process_snp_list(tiledb_unified, snp_list, trait_id_list, attr, output_file)
        elif get_regions:
            _process_regions(tiledb_unified, get_regions, trait_id_list, maf, attr, phenovar, nest, output_file)
        else:
            _export_all_stats(tiledb_unified, trait_id_list, output_file)
