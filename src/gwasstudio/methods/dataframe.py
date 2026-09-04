import numpy as np
import pandas as pd
from scipy import stats


def _get_log_p_value_from_z(z_score: np.ndarray) -> np.ndarray:
    """
    Calculate two-sided negative base-10 log p-values from z-scores.

    The log p-values are calculated using the standard normal log survival
    function. Calculating directly in log space avoids loss of precision and
    underflow for large absolute z-scores and extremely small p-values.

    Args:
        z_score (np.ndarray): An array of z-score values.

    Returns:
        np.ndarray: Array of two-sided negative base-10 log p-values.
    """
    # Calculate two-sided -log10(p) values from z-scores
    log_p_values = np.log(2.0) + stats.norm.logsf(np.abs(z_score))
    return np.maximum(-log_p_values / np.log(10.0), 0.0) # avoid negative rounding artifacts


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
    _check_required_columns(required_columns, df)
    snpid_series = df["CHR"].astype(str) + ":" + df["POS"].astype(str) + ":" + df["EA"] + ":" + df["NEA"]
    return snpid_series


def _check_required_columns(required_columns: set[str], df: pd.DataFrame) -> None:
    """
    Check if the required columns are present in the DataFrame.

    Args:
        required_columns (Set[str]): A set of required column names.
        df (pd.DataFrame): The DataFrame to check.

    Raises:
        KeyError: If any required columns are missing from the DataFrame.
    """
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise KeyError(f"Missing required columns in DataFrame: {', '.join(sorted(missing_columns))}")


def add_mlog10p(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'MLOG10P' calculated from BETA and SE.

    Args:
        df (pd.DataFrame): The input DataFrame containing the columns 'BETA' and 'SE'.

    Returns:
        pd.DataFrame: The DataFrame with the 'MLOG10P' column added.
    """

    if "MLOG10P" not in df.columns:
        # Direct NumPy arithmetic
        z = df["BETA"].values / df["SE"].values
        df.loc[:, "MLOG10P"] = _get_log_p_value_from_z(z).astype(np.float32)

    return df


def process_dataframe(df: pd.DataFrame, drop_tid: bool = True) -> pd.DataFrame:
    """
    Process the DataFrame by calculating MLOG10P and building SNPID.

    Args:
        df (pd.DataFrame): The input DataFrame containing the columns 'BETA', 'SE', 'CHR', 'POS', 'EA', and 'NEA'.
        drop_tid (bool, optional): Whether to drop the 'TRAITID' column from the DataFrame. Defaults to True.

    Returns:
        pd.DataFrame: The processed DataFrame with the 'MLOG10P' and the 'SNPID columns added, and optionally without the 'TRAITID' column.
    """

    df = add_mlog10p(df)

    if drop_tid and "TRAITID" in df.columns:
        df.drop(columns=["TRAITID"], inplace=True)

    # _build_snpid should also be vectorised; assign in‑place.
    df.loc[:, "SNPID"] = _build_snpid(df)

    # Move SNPID to the front without copying the whole frame.
    cols = df.columns.tolist()
    cols.insert(0, cols.pop(cols.index("SNPID")))
    df = df.reindex(columns=cols)

    return df
