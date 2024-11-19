import pandas as pd


def locus_breaker(
    tiledb_results_pd, pvalue_limit: float = 5, pvalue_sig: float = 5, hole_size: int = 250000
) -> pd.DataFrame:
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
    'CHR': pd.Series(dtype=np.uint8),
    'POS': pd.Series(dtype=np.uint32),
    'SNPID': pd.Series(dtype='object'),
    'EA': pd.Series(dtype='object'),
    'NEA': pd.Series(dtype='object'),
    'EAF': pd.Series(dtype=np.float32),
    'MLOG10P': pd.Series(dtype=np.float64),
    'BETA': pd.Series(dtype=np.float32),
    'SE': pd.Series(dtype=np.float32),
    'TRAITID': pd.Series(dtype='object')
}

    # Convert fmt_LP from list to float
    if tiledb_results_pd.empty:
        print("this region is empty")
        return pd.DataFrame(expected_schema)

    # Filter rows based on the p_limit threshold
    tiledb_results_pd = tiledb_results_pd[tiledb_results_pd["MLOG10P"] > pvalue_limit]

    # If no rows remain after filtering, return an empty DataFrame
    if tiledb_results_pd.empty:
        return pd.DataFrame(expected_schema)

    # Group by 'contig' (chromosome) first, then calculate regions within each chromosome
    trait_res = []

    for contig, chrom_df in tiledb_results_pd.groupby("CHR"):
        # Find regions where gaps between positions exceed hole_size within each chromosome
        gaps = chrom_df["POS"].diff() > hole_size
        group = gaps.cumsum()

        # Group by the identified regions within the chromosome
        for _, group_df in chrom_df.groupby(group):
            if group_df["MLOG10P"].max() > pvalue_sig:
                start_pos = group_df["POS"].min()
                end_pos = group_df["POS"].max()
                best_snp = group_df.loc[group_df["MLOG10P"].idxmax()]

                # Store the interval with the best SNP
                #line_res = [contig, start_pos, end_pos, best_snp["POS"], best_snp["MLOG10P"]] + best_snp.tolist()
                #trait_res.append(line_res)

                # Collect all SNPs within the region
                for _, snp_row in group_df.iterrows():
                    snp_res = [contig, start_pos, end_pos, snp_row["POS"], snp_row["MLOG10P"]] + snp_row.tolist()
                    trait_res.append(snp_res)

    # Convert results to a DataFrame
    columns = ["contig", "start", "end", "snp_pos", "snp_MLOG10P"] + tiledb_results_pd.columns.tolist()
    trait_res_df = pd.DataFrame(trait_res, columns=columns)

    # Drop specific columns including 'start' and 'end'
    trait_res_df = trait_res_df.drop(columns=["snp_pos", "contig", "start", "end"])

    return trait_res_df
