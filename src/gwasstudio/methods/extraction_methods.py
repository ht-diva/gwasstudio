from typing import Tuple, Any

import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.methods.dataframe import process_dataframe
from gwasstudio.methods.manhattan_plot import _plot_manhattan
from gwasstudio.utils.cfg import get_plot_config

from gwasstudio.utils import write_table
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
    output_format: str,
    attributes: Tuple[str] = None,
    snp_list: pd.DataFrame = None,
    plot: bool = True,
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
    plot (bool, optional): Whether to plot the results. Defaults to True.

    Returns:
        None
    """
    """Process data filtering by a list of SNPs."""
    chromosomes_dict = snp_list.groupby("CHR")["POS"].apply(list).to_dict()

    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
    for chrom, positions in chromosomes_dict.items():
        chromosome = int(chrom)
        unique_positions = list(set(positions))

        tiledb_iterator_query_df = tiledb_query.df[chromosome, trait, unique_positions]
        tiledb_iterator_query_df = process_dataframe(tiledb_iterator_query_df, attributes)
        # Plot the dataframe
        title_plot = f"{trait} - {chromosome}:{min(unique_positions)}-{max(unique_positions)}"
        if plot:
            _plot_manhattan(
                locus = tiledb_iterator_query_df,
                title_plot=title_plot,
                out=f"{trait}_{output_prefix}_{chromosome}"
            )
       


        kwargs = {"header": False, "index": False, "mode": "a"}
        write_table(tiledb_iterator_query_df, f"{output_prefix}", logger, file_format=output_format, **kwargs)


def extract_full_stats(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    output_format: str,
    attributes: Tuple[str] = None,
    plot: bool = True,
) -> None:
    """
    Export full summary statistics.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        output_format (str): The format for the output file.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.
        plot (bool, optional): Whether to plot the results. Defaults to True.

    Returns:
        None
    """
    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
    tiledb_query_df = tiledb_query.df[:, trait, :]

    tiledb_query_df = process_dataframe(tiledb_query_df, attributes)
    if plot:
        # Plot the dataframe
        _plot_manhattan(
            locus = tiledb_query_df,
            title_plot=trait,
            out=f"{trait}"
            )
    kwargs = {"index": False}
    write_table(tiledb_query_df, f"{output_prefix}", logger, file_format=output_format, **kwargs)


def extract_regions(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    output_format: str,
    bed_region: pd.DataFrame = None,
    attributes: Tuple[str] = None,
    plot: bool = True,
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
        plot (bool, optional): Whether to plot the results. Defaults to True.

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
        title_plot = f"{trait} - {chr}:{min(tiledb_query_df["POS"])}-{max(tiledb_query_df["POS"])}"
        if plot:
            # Plot the dataframe
            _plot_manhattan(
            locus = tiledb_query_df,
            title_plot=title_plot,
            out=f"{trait}"
            )
        dataframes.append(tiledb_query_df)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)

    concatenated_df = process_dataframe(concatenated_df, attributes)
    # Write output
    kwargs = {"index": False}
    write_table(concatenated_df, f"{output_prefix}", logger, file_format=output_format, **kwargs)
