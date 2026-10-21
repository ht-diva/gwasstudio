import pandas as pd


def compute_pheno_variance(df: pd.DataFrame) -> str:
    """
    Compute the median per-variant phenotypic variance estimate.

    Args:
        df: DataFrame with SE, N and AF columns.

    Returns:
        The median of SE^2 * N * 2 * AF * (1 - AF) as a string.
    """
    variance = (df["SE"] ** 2) * df["N"] * 2 * df["AF"] * (1 - df["AF"])
    median_value = variance.median()
    return str(median_value)
