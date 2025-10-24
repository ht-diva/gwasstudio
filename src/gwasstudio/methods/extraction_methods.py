from typing import Tuple, Any

import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.methods.dataframe import process_dataframe
from gwasstudio.methods.manhattan_plot import _plot_manhattan
from gwasstudio.utils.tdb_schema import AttributeEnum as an, DimensionEnum as dn

TILEDB_DIMS = dn.get_names()


def tiledb_array_query(
    tiledb_array: tiledb.Array, dims: Tuple[str] = TILEDB_DIMS, attrs: Tuple[str] = ()
) -> tuple[tuple[str], Any]:
    """
    Query a TileDB array with specified dimensions and attributes.

    Args:
        tiledb_array (tiledb.Array): The TileDB array to query.
        dims (List[str], optional): The dimensions to query. Defaults to TILEDB_DIMS.
        attrs (Tuple[str, ...], optional): The attributes to query. Defaults to an empty tuple.

    Returns:
        Tuple[List[str], tiledb.Query]: A tuple containing the list of attributes and the query object.

    Raises:
        ValueError: If any attribute in attrs is not found in the TileDB array.
    """
    # Validate attributes
    valid_attrs = an.get_names()
    for attr in attrs:
        if attr not in valid_attrs:
            raise ValueError(f"Attribute {attr} not found")
    try:
        query = tiledb_array.query(dims=dims, attrs=attrs)
    except tiledb.TileDBError as e:
        logger.debug(e)
        attrs = tuple(attr for attr in attrs if attr != an.MLOG10P.name)
        query = tiledb_array.query(dims=dims, attrs=attrs)

    return attrs, query


def extract_snp_list(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    attributes: Tuple[str] = None,
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
        plot_out (bool, optional): Whether to plot the results. Defaults to True.

    Returns:
        None
    """
    """Process data filtering by a list of SNPs."""
    chromosomes_dict = snp_list.groupby("CHR")["POS"].apply(list).to_dict()

    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
    dataframes = []
    for chrom, positions in chromosomes_dict.items():
        chromosome = int(chrom)
        unique_positions = list(set(positions))

        tiledb_iterator_query_df = tiledb_query.df[chromosome, trait, unique_positions]
        tiledb_iterator_query_df = process_dataframe(tiledb_iterator_query_df, attributes)
        if tiledb_iterator_query_df.empty:
            logger.warning(f"No SNPs found for chromosome {chromosome}.")
            continue

        # Plot the dataframe
        title_plot = f"{trait} - {chromosome}:{min(unique_positions)}-{max(unique_positions)}"
        if plot_out:
            _plot_manhattan(
                locus=tiledb_iterator_query_df,
                title_plot=title_plot,
                out=f"{output_prefix}",
                color_thr=color_thr,
                s_value=s_value,
            )
        dataframes.append(tiledb_iterator_query_df)

    # No SNPs found
    if not dataframes:
        return pd.DataFrame(columns=attributes)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    concatenated_df = process_dataframe(concatenated_df, attributes)
    return concatenated_df


def extract_full_stats(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    pvalue_thr: float,
    attributes: Tuple[str] = None,
) -> None:
    """
    Export full summary statistics.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        output_format (str): The format for the output file.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.
        pvalue_thr: P-value threshold in -log10 format used to filter significant SNPs (default: 0, no filter)
        plot_out (bool, optional): Whether to plot the results. Defaults to True.

    Returns:
        None
    """
    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
    tiledb_query_df = tiledb_query.df[:, trait, :]
    if pvalue_thr > 0:
        tiledb_query_df = tiledb_query_df[tiledb_query_df["MLOG10P"] > pvalue_thr]

    tiledb_query_df = process_dataframe(tiledb_query_df, attributes)
    if plot_out:
        # Plot the dataframe
        _plot_manhattan(
            locus=tiledb_query_df, title_plot=trait, out=f"{output_prefix}", color_thr=color_thr, s_value=s_value
        )
    return tiledb_query_df


def extract_regions(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    bed_region: pd.DataFrame = None,
    attributes: Tuple[str] = None,
) -> None:
    """
    Process data filtering by genomic regions and output as concatenated DataFrame in Parquet format.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        bed_region (pd.DataFrame, optional): A DataFrame containing the genomic regions to filter by. Defaults to None.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.
        plot_out (bool, optional): Whether to plot the results. Defaults to True.

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
        attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
        tiledb_query_df = tiledb_query.df[chr, trait, min_pos:max_pos]
        if tiledb_query_df.empty:
            logger.warning(f"Skipping empty region for chromosome {chr}.")
            continue

        title_plot = f"{trait} - {chr}:{min(tiledb_query_df['POS'])}-{max(tiledb_query_df['POS'])}"
        if plot_out:
            # Plot the dataframe
            _plot_manhattan(
                locus=tiledb_query_df,
                title_plot=title_plot,
                out=f"{output_prefix}_{title_plot}",
                color_thr=color_thr,
                s_value=s_value,
            )
        dataframes.append(tiledb_query_df)

    # No regions found
    if not dataframes:
        return pd.DataFrame(columns=attributes)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    concatenated_df = process_dataframe(concatenated_df, attributes)
    return concatenated_df
