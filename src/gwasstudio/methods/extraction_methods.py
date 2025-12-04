from typing import Tuple, Any

import numpy as np
import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.methods.dataframe import process_dataframe
from gwasstudio.methods.manhattan_plot import _plot_manhattan
from gwasstudio.utils.snps import is_multiallelic
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


def extract_full_stats(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    pvalue_thr: float,
    attributes: Tuple[str] = None,
) -> pd.DataFrame:
    """
    Export full summary statistics.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.
        pvalue_thr: P-value threshold in -log10 format used to filter significant SNPs (default: 0, no filter)
        plot_out (bool, optional): Whether to plot the results. Defaults to True.

    Returns:
        pd.Dataframe
    """
    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
    tiledb_query_df = tiledb_query.df[:, trait, :]
    if pvalue_thr > 0:
        tiledb_query_df = tiledb_query_df[tiledb_query_df["MLOG10P"] > pvalue_thr]

    tiledb_query_df = process_dataframe(tiledb_query_df)
    if plot_out:
        # Plot the dataframe
        _plot_manhattan(
            locus=tiledb_query_df, title_plot=trait, out=f"{output_prefix}", color_thr=color_thr, s_value=s_value
        )
    return tiledb_query_df


def extract_regions_snps(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    plot_out: bool,
    color_thr: str,
    s_value: int,
    regions_snps: pd.DataFrame = None,
    attributes: Tuple[str] = None,
) -> pd.DataFrame:
    """
    Process data filtering by genomic regions or a list of SNPs and output as concatenated DataFrame in Parquet format.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file..
        regions_snps (pd.DataFrame, optional): A DataFrame containing the genomic regions or SNPs to filter by. Defaults to None.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.
        plot_out (bool, optional): Whether to plot the results. Defaults to True.

    Returns:
        pd.Dataframe
    """
    snp_filter = (regions_snps["END"] == regions_snps["START"] + 1).all()
    regions_snps = regions_snps.groupby("CHR")
    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)
    dataframes = []
    for chr, group in regions_snps:
        if snp_filter:
            # Get all unique positions for this chromosome
            unique_positions = list(set(group["START"]))
            tiledb_query_df = tiledb_query.df[chr, trait, unique_positions]
            if not tiledb_query_df.empty:
                title_plot = f"{trait} - {chr}:{min(unique_positions)}-{max(unique_positions)}"
            warning_mx = f"No SNPs found for chromosome {chr}."
        else:
            # Get all (start, end) tuples of genomic regions for this chromosome
            min_pos = min(group["START"])
            if min_pos < 0:
                min_pos = 1
            max_pos = max(group["END"])
            tiledb_query_df = tiledb_query.df[chr, trait, min_pos:max_pos]
            if not tiledb_query_df.empty:
                title_plot = f"{trait} - {chr}:{min(tiledb_query_df['POS'])}-{max(tiledb_query_df['POS'])}"
            warning_mx = f"No region found for chromosome {chr}."

        if tiledb_query_df.empty:
            logger.warning(warning_mx)
            continue

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

    # No regions/SNPs found
    if not dataframes:
        return pd.DataFrame(columns=attributes)

    # Concatenate all DataFrames
    concatenated_df = pd.concat(dataframes, ignore_index=True)
    concatenated_df = process_dataframe(concatenated_df)

    return concatenated_df


def extract_regions_leadsnps(
    tiledb_array: tiledb.Array,
    trait: str,
    output_prefix: str,
    trait_snps: pd.DataFrame,
    region_width: int = 500000,
    attributes: Tuple[str] = None,
) -> pd.DataFrame:
    """
    Process data filtering by genomic regions or a list of SNPs and output as concatenated DataFrame in Parquet format.

    Args:
        tiledb_array: The TileDB array to query.
        trait (str): The trait to filter by.
        output_prefix (str): The prefix for the output file.
        trait_snps (pd.DataFrame, optional): A DataFrame containing SOURCE_ID (trait), CHR and POS for lead-SNP search.
        region_width (int): Region width (in bp) around POS for lead-SNP search. Default 500000.
        attributes (list[str], optional): A list of attributes to include in the output. Defaults to None.

    Returns:
        pd.Dataframe
    """
    expected_cols = [
        "SOURCEID_SNP",
        "SNPID_LEAD",
        "MLOG10P_LEAD",
        "BETA_LEAD",
        "SE_LEAD",
        "SNPID_EXACT",
        "MLOG10P_EXACT",
        "BETA_EXACT",
        "SE_EXACT",
    ]

    # Make regions to query
    trait_snps["START"] = (trait_snps["POS"] - round(region_width / 2)).clip(lower=1)
    trait_snps["END"] = trait_snps["POS"] + round(region_width / 2)

    # Unique source identifier
    trait_snps["SOURCEID_SNP"] = (
        trait_snps["SOURCE_ID"].astype(str)
        + ":"
        + trait_snps["CHR"].astype(str)
        + ":"
        + trait_snps["POS"].astype(str)
        + ":"
        + trait_snps["EA"].astype(str)
        + ":"
        + trait_snps["NEA"].astype(str)
    )

    attributes, tiledb_query = tiledb_array_query(tiledb_array, attrs=attributes)

    # Loop through chromosomes
    trait_snps = trait_snps.groupby("CHR")
    dataframes = []
    for chr, group in trait_snps:
        # Query largest region
        min_pos = min(group["START"])
        max_pos = max(group["END"])
        tiledb_query_df = tiledb_query.df[chr, trait, min_pos:max_pos]
        if tiledb_query_df.empty:
            for sid in group["SOURCEID_SNP"]:
                dataframes.append({col: np.nan for col in expected_cols} | {"SOURCEID_SNP": sid})
            continue

        # Loop SNPs within this chromosome
        for _, row in group.iterrows():
            src = row["SOURCEID_SNP"]
            region = tiledb_query_df[(tiledb_query_df.POS >= row.START) & (tiledb_query_df.POS <= row.END)]
            if region.empty:
                dataframes.append({col: np.nan for col in expected_cols} | {"SOURCEID_SNP": src})
                continue

            # Lead SNP
            lead = region[region["MLOG10P"] == region["MLOG10P"].max()]
            lead = process_dataframe(lead)
            if len(lead) > 1:  # if multiple lead SNPs
                lead["is_multi"] = lead["SNPID"].apply(is_multiallelic)
                mono = lead[not lead["is_multi"]]
                if len(mono) > 0:
                    lead = mono.iloc[0]  # keep first bi-allelic
                else:
                    lead = lead.iloc[0]  # keep first multi-allelic
            else:
                lead = lead.iloc[0]

            # Exact SNP
            exact = region[(region.POS == row.POS) & (region.EA == row.EA) & (region.NEA == row.NEA)]
            exact = process_dataframe(pd.DataFrame([exact.iloc[0]])).iloc[0] if not exact.empty else None

            dataframes.append(
                {
                    "SOURCEID_SNP": src,
                    "SNPID_LEAD": lead["SNPID"],
                    "MLOG10P_LEAD": lead["MLOG10P"],
                    "BETA_LEAD": lead["BETA"],
                    "SE_LEAD": lead["SE"],
                    "SNPID_EXACT": exact["SNPID"] if exact is not None else np.nan,
                    "MLOG10P_EXACT": exact["MLOG10P"] if exact is not None else np.nan,
                    "BETA_EXACT": exact["BETA"] if exact is not None else np.nan,
                    "SE_EXACT": exact["SE"] if exact is not None else np.nan,
                }
            )

    return pd.DataFrame(dataframes)
