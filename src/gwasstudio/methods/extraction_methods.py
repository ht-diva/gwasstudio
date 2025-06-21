import pandas as pd

from gwasstudio import logger
from gwasstudio.utils import write_table, get_log_p_value_from_z, _build_snpid

TILEDB_DIMS = ["CHR", "TRAITID", "POS"]


def _extract_snp_list(
    tiledb_unified,
    trait,
    output_prefix,
    bed_region=None,
    attr=None,
    snp_list=None,
    maf=None,
    hole_size=None,
    pvalue_sig=None,
    pvalue_limit=None,
    phenovar=None,
):
    """Process data filtering by a list of SNPs."""
    chromosomes_dict = snp_list.groupby("CHR")["POS"].apply(list).to_dict()
    # parallelize by n_workers traits at a time with dask the query on tiledb
    attributes = attr.split(",")
    for chrom, positions in chromosomes_dict.items():
        chromosome = int(chrom)
        unique_positions = list(set(positions))

        tiledb_iterator_query_df = (
            tiledb_unified.query(dims=TILEDB_DIMS, attrs=attributes, return_arrow=True)
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
        write_table(tiledb_iterator_query_df, f"{output_prefix}", logger, file_format="csv", **kwargs)


def _extract_all_stats(
    tiledb_unified,
    trait,
    output_prefix,
    bed_region=None,
    attr=None,
    snp_list=None,
    maf=None,
    hole_size=None,
    pvalue_sig=None,
    pvalue_limit=None,
    phenovar=None,
):
    """Export all summary statistics."""
    attributes = attr.split(",")
    tiledb_query_df = (
        tiledb_unified.query(
            dims=TILEDB_DIMS,
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
    write_table(tiledb_query_df, f"{output_prefix}", logger, file_format="parquet", **kwargs)


def _extract_regions(
    tiledb_unified,
    trait,
    output_prefix,
    bed_region=None,
    attr=None,
    snp_list=None,
    maf=None,
    hole_size=None,
    pvalue_sig=None,
    pvalue_limit=None,
    phenovar=None,
):
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
            tiledb_unified.query(attrs=attributes, dims=TILEDB_DIMS, return_arrow=True)
            .df[chr, trait, min_pos:max_pos]
            .to_pandas()
        )

        dataframes.append(tiledb_query_df)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    # Add SNPID column
    concatenated_df = _build_snpid(attributes, concatenated_df)
    # Write to Parquet
    kwargs = {"index": False}
    write_table(concatenated_df, f"{output_prefix}", logger, file_format="parquet", **kwargs)
