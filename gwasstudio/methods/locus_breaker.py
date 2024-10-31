import pandas as pd
from typing import List


def locus_breaker(
    tiledb_data,
    pvalue_limit: float = 5,
    pvalue_sig: float = 5,
    hole_size: int = 250000,
    chromosome,
    trait_id_list
) -> pd.DataFrame:
    """
    Breaking genome in locus
    Returns a series of parquet files describing the loci created dynamically around significant SNPs.
    :param tiledb_data: TileDBVCF data (default: None)
    :param pvalue_sig: P-value threshold in -log10 format used to create the regions around significant SNPs (default: 5)
    :param pvalue_limit: P-value threshold in -log10 format for loci borders (default: 5)
    :param hole_size: Minimum pair-base distance between SNPs in different loci (default: 250000)
    :return: DataFrame with the loci information
    """
    expected_schema = {
    'contig': pd.Series(dtype='object'),
    'snp_pos': pd.Series(dtype='int64'),
    'snp_fmt_LP': pd.Series(dtype='float64'),
    'alleles': pd.Series(dtype='object'),
    'fmt_ES': pd.Series(dtype='object'),
    'fmt_SE': pd.Series(dtype='object'),
    'sample_name': pd.Series(dtype='object'),
    'pos_end': pd.Series(dtype='int64')
}

    # Convert fmt_LP from list to float
    if tiledb_data.empty:
        print("this region is empty")
        return pd.DataFrame(expected_schema)
    
    tiledb_data["fmt_LP"] = tiledb_data["fmt_LP"].apply(lambda x: float(x[0]))

    # Filter rows based on the p_limit threshold
    tiledb_data = tiledb_data[tiledb_data["fmt_LP"] > pvalue_limit]

    # If no rows remain after filtering, return an empty DataFrame
    if tiledb_data.empty:
        return pd.DataFrame(expected_schema)

    # Group by 'contig' (chromosome) first, then calculate regions within each chromosome
    trait_res = []

    for contig, chrom_df in tiledb_data.groupby("contig"):
        # Find regions where gaps between positions exceed hole_size within each chromosome
        gaps = chrom_df["pos_start"].diff() > hole_size
        group = gaps.cumsum()

        # Group by the identified regions within the chromosome
        for _, group_df in chrom_df.groupby(group):
            if group_df["fmt_LP"].max() > pvalue_sig:
                start_pos = group_df["pos_start"].min()
                end_pos = group_df["pos_start"].max()
                best_snp = group_df.loc[group_df["fmt_LP"].idxmax()]

                # Store the interval with the best SNP
                line_res = [contig, start_pos, end_pos, best_snp["pos_start"], best_snp["fmt_LP"]] + best_snp.tolist()
                trait_res.append(line_res)

                # Collect all SNPs within the region
                for _, snp_row in group_df.iterrows():
                    snp_res = [contig, start_pos, end_pos, snp_row["pos_start"], snp_row["fmt_LP"]] + snp_row.tolist()
                    trait_res.append(snp_res)

    # Convert results to a DataFrame
    columns = ["contig", "start", "end", "snp_pos", "snp_fmt_LP"] + tiledb_data.columns.tolist()
    trait_res_df = pd.DataFrame(trait_res, columns=columns)

    # Drop specific columns including 'start' and 'end'
    trait_res_df = trait_res_df.drop(columns=["pos_start", "fmt_LP", "start", "end"])

    # Remove one of the duplicate 'contig' columns if present
    trait_res_df = trait_res_df.loc[:, ~trait_res_df.columns.duplicated()]

    #columns_attribute_mapping = {v: k for k, v in map_attributes.items() if v in trait_res_df.columns}

    #trait_res_df.rename(columns=columns_attribute_mapping)

    # Rename the columns using the map

    return trait_res_df


# Deprecated for now function to filter for single SNP baed on p-value only
'''
def compute_pval_and_filter_regions(df ,pval = 5e-08):
            """
            Compute and filter for p-value
            Returns a pandas dataframe filtered for a specific threshold.
            :param df: dask dataframe passed through the option map_dask (default: None)
            :param pval: p-value threshold to filter for (default: 5e-08)
            """
            df["BETA"] = df["fmt_ES"].str[0]
            df["SE"] = df["fmt_SE"].str[0]
            df = df.drop(columns=["fmt_ES", "fmt_SE"])
            df["PVAL"] = stats.norm.sf(abs(df["BETA"] / df["SE"])) * 2
            df = df[df["PVAL"] < pval]
            return df

'''
