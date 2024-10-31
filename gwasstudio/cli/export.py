import click
import cloup
from gwasstudio import logger
from gwasstudio.methods.locus_breaker import locus_breaker
from scipy import stats
print("starting export")
help_doc = """
Exports data from a TileDB dataset.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB mandatory options",
    cloup.option("--uri", default="None", help="TileDB dataset URI"),
    cloup.option("--chromosome", default="None", help="The chromosome used for the analysis"),
    cloup.option("--trait_id_file", default="None", help="The chromosome used for the analysis"),
)
@cloup.option_group(
    "Options for Locusbreaker",
    cloup.option("--locusbreaker", default=False, is_flag=True, help="Option to run locusbreaker"),
    cloup.option("--pvalue-sig", default=5.0, help="P-value threshold to use for filtering the data"),
    cloup.option("--pvalue-limit", default=5.0, help="P-value threshold for loci borders"),
    cloup.option("--hole-size", default=250000, help="Minimum pair-base distance between SNPs in different loci (default: 250000)")
)
@cloup.option_group(
    "Options for filtering using a genomic regions or a list of SNPs ids",
    cloup.option("--snp-list", default="None", help="A txt file with a column containing the SNP ids")
)
@click.pass_context
def export(
    ctx,
    uri,
    trait_id_file,
    chromosome,
    output_path,
    pvalue_sig,
    hole_size,
    pvalue_limit,
    hole_size,
    snp_list,
    region
):
    cfg = ctx.obj["cfg"]
    tiledb_unified = tiledb.open(uri, mode="r")
    logger.info("TileDB dataset loaded")
    trait_id_list = open(trait_id_file, "r").read().rstrip().split("\n")
    # Create a mapping of the user selected columns into TileDB
    # If locus_breaker is selected, run locus_breaker
    if locusbreaker:
        print("running locus breaker")
        dask_df = ds.map_dask(
            lambda tiledb_data: locus_breaker(
            tiledb_data,
            pvalue_limit=pvalue_limit,
            pvalue_sig=pvalue_sig,
            hole_size=hole_size,
            chromosome,
            trait_id_list
            ),
            attrs=columns_attribute_mapping.keys()
        )
        logger.info(f"Saving locus-breaker output in {output_path}")
        dask_df.to_csv(output_path)
        return

    # If snp_list is selected, run extract_snp
    if snp_list != "None":
        SNP_list = pd.read_csv(snp_list)
        position_list = SNP_list[["position"]].to_list()
        chromosome_list = SNP_list[["chromosome"]].to_list()
        subset_SNPs = tiledb_s.query(dims=['chromosome','position','trait_id'], attrs=['SNP','beta', 'se', 'freq','alt']).df[chromosome_list, trait_id_list, position_list]
        subset_SNPs["p-value"] = 1 - stats.chi2.cdf((subset_SNPs["beta"]/subset_SNPs["se"])**2, df=1)
        subset_SNPs = subset_SNPs.merge(SNP_list, on = "position")
        filtered_ddf.to_csv(output_path)
        #filtered_ddf.to_parquet(output_path, engine="pyarrow", compression="snappy", schema = None)
        logger.info(f"Saved filtered summary statistics by SNPs in {output_path}")
        exit()

    # If neither locus_breaker nor snp_list is selected, filter the data by regions and samples
    if pvalue_sig:
        subset_SNPs = tiledb_s.query(return_arrow = True, dims=['chromosome','position','trait_id'], attrs=['SNP','beta', 'se', 'freq','alt']).df[chromosome, : ,trait_id_list]
        z_scores = pc.divide(subset_SNPs['beta'], subset_SNPs['se'])

        # Calculate chi-square statistics (z_scores squared)
        chi_square_stats = pc.multiply(z_scores, z_scores)

        # Calculate p-values using scipy for efficiency
        p_values = [1 - stats.chi2.cdf(stat, df=1) for stat in chi_square_stats.to_numpy()]

        # Add p_values as a new column
        subset_SNPs = subset_SNPs.append_column('p_value', pa.array(p_values))

        # Filter the Arrow table based on the p-value threshold
        filtered_data = subset_SNPs.filter(pc.less(subset_SNPs['p_value'], 0.05))

        # Convert back to a table or DataFrame if needed
        filtered_data_df = filtered_data.to_pandas()
        filtered_data_df.to_parquet(output_path, engine="pyarrow", compression="snappy",schema=None)
        logger.info(f"Saving filtered GWAS by regions and samples in {output_path}")
        
