from typing import Tuple

import numpy as np
import pandas as pd
from scipy import stats


def _get_log_p_value_from_z(z_score: np.ndarray) -> np.ndarray:
    """
    Calculate the negative base-10 logarithm of the p-value from an array of z-scores.

    The p-value is computed using the cumulative distribution function (CDF) of the
    standard normal distribution. The result is then transformed to the negative
    base-10 logarithm scale.

    Args:
        z_score (np.ndarray): An array of z-score values.

    Returns:
        np.ndarray: An array of negative base-10 logarithm of the p-values corresponding to each z-score.
    """
    # Use the cumulative distribution function (CDF) for the normal distribution
    p_values = 2 * (1 - stats.norm.cdf(np.abs(z_score)))
    log10_p_values = -np.log10(p_values)
    return log10_p_values


def _build_snpid(df: pd.DataFrame) -> pd.Series:
    """
    Construct a SNPID Series from the given DataFrame.

    The SNPID is constructed by concatenating the chromosome (CHR), position (POS),
    effect allele (EA), and non-effect allele (NEA) columns with colons (:) as separators.

    Args:
        df (pd.DataFrame): A DataFrame containing the columns 'CHR', 'POS', 'EA', and 'NEA'.

    Returns:
        pd.Series: A Series of SNPIDs.

    Raises:
        KeyError: If any of the required columns ('CHR', 'POS', 'EA', 'NEA') are missing from the DataFrame.
    """
    required_columns = {"CHR", "POS", "EA", "NEA"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise KeyError(f"Missing required columns in DataFrame: {', '.join(missing_columns)}")

    snpid_series = df["CHR"].astype(str) + ":" + df["POS"].astype(str) + ":" + df["EA"] + ":" + df["NEA"]
    return snpid_series


def process_dataframe(df: pd.DataFrame, attributes: Tuple[str], drop_tid: bool = True) -> pd.DataFrame:
    """
    Process the DataFrame by calculating MLOG10P and building SNIPID.

    Args:
        df (pd.DataFrame): The input DataFrame containing the columns 'BETA', 'SE', 'CHR', 'POS', 'EA', and 'NEA'.
        attributes (List[str]): A list of attributes to process. If 'SNIPID' is in the list, SNPID will be built.
        drop_tid (bool, optional): Whether to drop the 'TRAITID' column from the DataFrame. Defaults to True.

    Returns:
        pd.DataFrame: The processed DataFrame with the 'MLOG10P' column added, optionally the 'SNIPID' column, and optionally without the 'TRAITID' column.
    """
    # Apply the function to the column in a vectorized manner
    if "MLOG10P" not in df.columns:
        df["MLOG10P"] = _get_log_p_value_from_z(df["BETA"] / df["SE"]).astype(np.float32)

    if "SNIPID" in attributes:
        df["SNIPID"] = _build_snpid(df[["CHR", "POS", "EA", "NEA"]])

    if drop_tid and "TRAITID" in df.columns:
        return df.drop(columns=["TRAITID"])

    return df
