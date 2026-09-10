import pandas as pd

from gwasstudio.methods.dataframe import _build_snpid

VALID_BASES = {"A", "C", "G", "T"}


def keep_best_multiallelic_variant(df: pd.DataFrame) -> pd.DataFrame:
    """
    At multiallelic positions:

    1. Remove non-SNV variants (i.e. indels).
    2. For pseudo-biallelic (multiallelic variants split on multiple rows), retain the variant with the highest Minor Allele Frequency (MAF).

    Ordinary (non-multiallelic) variants are preserved.

    Args:
        df (pd.DataFrame): A DataFrame containing the columns 'CHR', 'POS', 'EA', and 'NEA'.

    Returns:
        pd.DataFrame: A DataFrame containing ordinary loci and best multiallelic variants 
        (i.e. biallelic variants with the highest MAF).
    """

    if "SNPID" not in df:
        df["SNPID"] = _build_snpid(df)

    # A valid biallelic SNP has two different, single-nucleotide alleles
    valid_biallelic_snv = (df["EA"].isin(VALID_BASES) & df["NEA"].isin(VALID_BASES) & df["EA"].ne(df["NEA"]))

    # Find multiallelic loci
    is_multiallelic_locus = df.groupby(["CHR", "POS"])["SNPID"].transform("nunique").gt(1)

    # Minor Allele Frequency (MAF) 
    df["_MAF"] = df["EAF"].where(df["EAF"].le(0.5), 1 - df["EAF"])

    # Keep biallelic variants with the highest MAF
    best_multiallelic = df.loc[is_multiallelic_locus & valid_biallelic_snv & df["_MAF"].notna()].groupby(["CHR", "POS"])["_MAF"].idxmax()

    # Keep ordinary loci and best multiallelic variants
    keep = ~is_multiallelic_locus
    keep.loc[best_multiallelic] = True

    return df.loc[keep].drop(columns=["_MAF"]).copy()
