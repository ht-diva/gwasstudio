import tiledb
import tiledbvcf
import pandas as pd
import pybedtools as pbt
from scipy import stats
from dask_jobqueue import SLURMCluster as Cluster
from dask.distributed import Client

def locus_breaker(tiledb_data, bed_regions, pvalue_sig=5, pvalue_limit=5, hole_size=250000, pvalue_label='fmt_LP', pos_label='pos_start', chr_label = 'contig'):
    """
    Breaking genome in locus
    Returns a series of csv file describing the locus created dynamically around significant SNPs.
    :param tiledb_data: TileDBVCF data (default: None)
    :param bed_regions: Genomic regions to run locus breaker on (default: None)
    :param pvalue_sig: Initla P-value threshold in -log10 format for creating regions to fruther refine later (default: 5)
    :param pvalue_sig: P-value threshold in -log10 format used to create the regions around significant SNPs(default: 5)
    :param hole_size: Space between SNPs to define independent SNPs (default: 250000)
    :param pvalue_label: Name of the column where the -log10(p-value) information is stored (default: fmt_LP)
    :param pos_label: Name of the column where the SNP position is stored (default: pos_start)
    """

    #def process_partition(res, p_sig=4.3, p_limit=5, hole_size=250000, p_label='fmt_LP', pos_label='pos_start'):
    # Convert fmt_LP from list to float
    res['fmt_LP'] = res['fmt_LP'].apply(lambda x: float(x[0]))

    # Filter rows based on the p_limit threshold
    res = res[res[p_label] > p_limit]

    # If no rows remain after filtering, return an empty DataFrame
    if res.empty:
        return pd.DataFrame(columns=['contig', 'start', 'end', 'snp_pos', 'snp_fmt_LP'] + res.columns.tolist())

    # Group by 'contig' (chromosome) first, then calculate regions within each chromosome
    trait_res = []

    for contig, chrom_df in res.groupby(chr_label):
        # Find regions where gaps between positions exceed hole_size within each chromosome
        gaps = chrom_df[pos_label].diff() > hole_size
        group = gaps.cumsum()

        # Group by the identified regions within the chromosome
        for _, group_df in chrom_df.groupby(group):
            if group_df[p_label].max() > p_sig:
                start_pos = group_df[pos_label].min()
                end_pos = group_df[pos_label].max()
                best_snp = group_df.loc[group_df[p_label].idxmax()]

                # Store the interval with the best SNP
                line_res = [contig, start_pos, end_pos, best_snp[pos_label], best_snp[p_label]] + best_snp.tolist()
                trait_res.append(line_res)

                # Collect all SNPs within the region
                for _, snp_row in group_df.iterrows():
                    snp_res = [contig, start_pos, end_pos, snp_row[pos_label], snp_row[p_label]] + snp_row.tolist()
                    trait_res.append(snp_res)

    # Convert results to a DataFrame

    columns = ['contig', 'start', 'end', 'snp_pos', 'snp_fmt_LP'] + res.columns.tolist()
    trait_res_df = pd.DataFrame(trait_res, columns=columns)

    # Drop specific columns including 'start' and 'end'
    trait_res_df = trait_res_df.drop(columns=['pos_start', 'fmt_LP', 'start', 'end'])

    # Remove one of the duplicate 'contig' columns if present
    trait_res_df = trait_res_df.loc[:, ~trait_res_df.columns.duplicated()]

    return trait_res_df








#Deprecated for now function to filter for single SNP baed on p-value only
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
