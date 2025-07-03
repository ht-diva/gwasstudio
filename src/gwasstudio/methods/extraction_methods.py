import pandas as pd

from gwasstudio import logger
from gwasstudio.methods.dataframe import process_dataframe
from gwasstudio.utils import write_table

TILEDB_DIMS = ["CHR", "TRAITID", "POS"]


def extract_snp_list(
    tiledb_array,
    trait: str,
    output_prefix: str,
    output_format: str,
    attributes: list[str] = None,
    snp_list: pd.DataFrame = None,
) -> None:
    """
    Process data filtering by a list of SNPs.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        output_format (str): The format for the output file.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.
        snp_list (pd.DataFrame, optional): A DataFrame containing the list of SNPs to filter by. Defaults to None.

    Returns:
        None
    """
    """Process data filtering by a list of SNPs."""
    chromosomes_dict = snp_list.groupby("CHR")["POS"].apply(list).to_dict()
    # parallelize by n_workers traits at a time with dask the query on tiledb

    for chrom, positions in chromosomes_dict.items():
        chromosome = int(chrom)
        unique_positions = list(set(positions))

        tiledb_iterator_query_df = (
            tiledb_array.query(dims=TILEDB_DIMS, attrs=attributes, return_arrow=True)
            .df[chromosome, trait, unique_positions]
            .to_pandas()
        )

        tiledb_iterator_query_df = process_dataframe(tiledb_iterator_query_df, attributes)

        kwargs = {"header": False, "index": False, "mode": "a"}
        write_table(tiledb_iterator_query_df, f"{output_prefix}", logger, file_format=output_format, **kwargs)


def extract_full_stats(
    tiledb_array,
    trait: str,
    output_prefix: str,
    output_format: str,
    attributes: list[str] = None,
) -> None:
    """
    Export full summary statistics.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        output_format (str): The format for the output file.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.

    Returns:
        None
    """
    tiledb_query_df = tiledb_array.query(dims=TILEDB_DIMS, attrs=attributes).df[:, trait, :]

    tiledb_query_df = process_dataframe(tiledb_query_df, attributes)

    kwargs = {"index": False}
    write_table(tiledb_query_df, f"{output_prefix}", logger, file_format=output_format, **kwargs)


def extract_regions(
    tiledb_array,
    trait: str,
    output_prefix: str,
    output_format: str,
    bed_region: pd.DataFrame = None,
    attributes: list[str] = None,
) -> None:
    """
    Process data filtering by genomic regions and output as concatenated DataFrame in Parquet format.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        output_format (str): The format for the output file.
        bed_region (pd.DataFrame, optional): A DataFrame containing the genomic regions to filter by. Defaults to None.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.

    Returns:
        None
    """
    dataframes = []
    for chr, group in bed_region:
        # Get all (start, end) tuples for this chromosome
        min_pos = min(group["START"])
        if min_pos < 0:
            min_pos = 1
        max_pos = max(group["END"])

        # Query TileDB and convert directly to Pandas DataFrame
        tiledb_query_df = (
            tiledb_array.query(attrs=attributes, dims=TILEDB_DIMS, return_arrow=True)
            .df[chr, trait, min_pos:max_pos]
            .to_pandas()
        )

        dataframes.append(tiledb_query_df)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)

    concatenated_df = process_dataframe(concatenated_df, attributes)

    # Write output
    kwargs = {"index": False}
    write_table(concatenated_df, f"{output_prefix}", logger, file_format=output_format, **kwargs)
