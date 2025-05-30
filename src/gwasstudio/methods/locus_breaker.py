import numpy as np
import pandas as pd

from gwasstudio.methods.compute_pheno_variance import compute_pheno_variance


def locus_breaker(
    tiledb_results_pd, pvalue_limit: float = 3.3, pvalue_sig: float = 5, hole_size: int = 250000, phenovar: bool = False
) -> list[pd.DataFrame] | pd.DataFrame:
    """
    Breaking genome in locus
    Returns a series of parquet files describing the loci created dynamically around significant SNPs.
    :param tiledb_results_pd: Pandas dataframe from TileDB (default: None)
    :param pvalue_sig: P-value threshold in -log10 format used to create the regions around significant SNPs (default: 5)
    :param pvalue_limit: P-value threshold in -log10 format for loci borders (default: 5)
    :param hole_size: Minimum pair-base distance between SNPs in different loci (default: 250000)
    :return: DataFrame with the loci information
    """
    expected_schema = {
        "CHR": pd.Series(dtype=np.uint8),
        "POS": pd.Series(dtype=np.uint32),
        "EA": pd.Series(dtype="object"),
        "NEA": pd.Series(dtype="object"),
        "EAF": pd.Series(dtype=np.float32),
        "MLOG10P": pd.Series(dtype=np.float32),
        "BETA": pd.Series(dtype=np.float32),
        "SE": pd.Series(dtype=np.float32),
        "TRAITID": pd.Series(dtype="object"),
    }

    # Convert fmt_LP from list to float
    if tiledb_results_pd.empty:
        print("this region is empty")
        return pd.DataFrame(expected_schema)

    original_data = tiledb_results_pd.copy()

    if phenovar:
        pv = compute_pheno_variance(tiledb_results_pd)
        tiledb_results_pd["S"] = pv
    else:
        tiledb_results_pd["S"] = 1.0

    # Filter rows based on the p_limit threshold
    tiledb_results_pd = tiledb_results_pd[tiledb_results_pd["MLOG10P"] > pvalue_limit]
    tiledb_results_pd["SNPID"] = (
        tiledb_results_pd["CHR"].astype(str)
        + ":"
        + tiledb_results_pd["POS"].astype(str)
        + ":"
        + tiledb_results_pd["EA"]
        + ":"
        + tiledb_results_pd["NEA"]
    )
    # If no rows remain after filtering, return an empty DataFrame
    if tiledb_results_pd.empty:
        return pd.DataFrame(expected_schema)

    # Group by 'contig' (chromosome) first, then calculate regions within each chromosome
    trait_res = []
    trait_res_allsnp = []

    for contig, chrom_df in tiledb_results_pd.groupby("CHR"):
        # Find regions where gaps between positions exceed hole_size within each chromosome
        gaps = chrom_df["POS"].diff() > hole_size
        group = gaps.cumsum()

        # Group by the identified regions within the chromosome
        for _, group_df in chrom_df.groupby(group):
            if group_df["MLOG10P"].max() > pvalue_sig:
                lower_pos = group_df["POS"].min()
                if int(lower_pos) - 100000 < 0:
                    start_pos = 1
                else:
                    start_pos = lower_pos - 100000
                end_pos = group_df["POS"].max() + 100000
                best_snp = group_df.loc[group_df["MLOG10P"].idxmax()]
                locus = str(group_df["CHR"].iloc[0]) + ":" + str(start_pos) + ":" + str(end_pos)

                line_res = [start_pos, end_pos, best_snp["POS"], best_snp["MLOG10P"]] + best_snp.tolist()
                trait_res.append(line_res)
                expanded_snps = original_data[
                    (original_data["CHR"] == group_df["CHR"].iloc[0])
                    & (original_data["POS"] >= start_pos)
                    & (original_data["POS"] <= end_pos)
                ]

                # Store the interval with the best SNP
                # Collect all SNPs within the region
                for _, snp_row in expanded_snps.iterrows():
                    snp_res = [
                        locus,
                        snp_row["POS"],
                        snp_row["MLOG10P"],
                    ] + snp_row.tolist()
                    trait_res_allsnp.append(snp_res)

    # Convert results to a DataFrame
    columns = [
        "snp_pos",
        "start",
        "end",
        "snp_MLOG10P",
    ] + tiledb_results_pd.columns.tolist()

    trait_res_df = pd.DataFrame(trait_res, columns=columns)

    # Drop specific columns including 'start' and 'end'
    trait_res_df = trait_res_df.drop(columns=["snp_pos", "start", "end"])
    columns = ["locus", "snp_pos", "snp_MLOG10P"] + tiledb_results_pd.columns.tolist()
    columns.remove("S")
    trait_res_allsnp_df = pd.DataFrame(trait_res_allsnp, columns=columns)
    # trait_res_allsnp_df = trait_res_allsnp_df.drop(trait_res_allsnp_df.columns[0], axis=1)
    trait_res_allsnp_df = trait_res_allsnp_df.drop(columns=["snp_pos", "snp_MLOG10P"])

    return [trait_res_df, trait_res_allsnp_df]
