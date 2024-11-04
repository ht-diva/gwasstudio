import click
import cloup
from gwasstudio import logger
from methods.locus_breaker import locus_breaker
import tiledb
from scipy import stats
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds

help_doc = """
Exports data from a TileDB dataset.
"""

@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB mandatory options",
    cloup.option("--uri", default="None", help="TileDB dataset URI"),
    cloup.option("--output_path", default="None", help="The chromosome used for the analysis"),
    cloup.option("--chromosome", default="None", help="The chromosome used for the analysis"),
    cloup.option("--trait_id_file", default="None", help="The chromosome used for the analysis"),
)
@cloup.option_group(
    "Options for Locusbreaker",
    cloup.option("--locusbreaker", default=False, is_flag=True, help="Option to run locusbreaker"),
    cloup.option("--pvalue-sig", default=False, help="P-value threshold to use for filtering the data"),
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
    pvalue_limit,
    hole_size,
    snp_list,
    locusbreaker
    
):
    cfg = ctx.obj["cfg"]
    tiledb_unified = tiledb.open(uri, mode="r")
    logger.info("TileDB dataset loaded")
    trait_id_list = open(trait_id_file, "r").read().rstrip().split("\n")
    # If locus_breaker is selected, run locus_breaker
    if locusbreaker:
        print("running locus breaker")
        subset_SNPs_pd = tiledb_unified.query(dims=['CHR', 'POS', 'TRAITID'], attrs=['SNPID', 'ALLELE0', 'ALLELE1', 'BETA', 'SE', 'EAF', "MLOG10P"]).df[:, :, trait_id_list]
        results_lb = locus_breaker(subset_SNPs_pd)
        logger.info(f"Saving locus-breaker output in {output_path}")
        results_lb.to_csv(output_path, index = False)
        return

    # If snp_list is selected, run extract_snp
    if snp_list != "None":
        SNP_list = pd.read_csv(snp_list, dtype = {"CHR":str, "POS":int, "ALLELE0":str, "ALLELE1":str})
        chromosome_dict = SNP_list.groupby('CHR')['POS'].apply(list).to_dict()
        unique_positions = list(set(pos for positions in chromosome_dict.values() for pos in positions))
        subset_SNPs_arrow = tiledb_unified.query(return_arrow = True, dims=['CHR', 'POS', 'TRAITID'], attrs=['SNPID', 'ALLELE0', 'ALLELE1', 'BETA', 'SE', 'EAF', "MLOG10P"]).df[list(chromosome_dict.keys()), unique_positions, trait_id_list]
        subset_SNPs_pd = subset_SNPs_arrow.to_pandas()
        subset_SNPs_pd['ALLELE0'] = subset_SNPs_pd['ALLELE0'].astype(str)
        subset_SNPs_pd['ALLELE1'] = subset_SNPs_pd['ALLELE1'].astype(str)
        joined = subset_SNPs_pd.merge(SNP_list, on=["CHR", "POS","ALLELE0","ALLELE1"], how="inner")

        if pvalue_sig:
            joined = joined.loc[joined["MLOG10P"]> pvalue_sig]

        joined.to_csv(output_path, index = False)
        logger.info(f"Saved filtered summary statistics by SNPs in {output_path}")
        exit()

    if pvalue_sig:
        subset_SNPs = tiledb_unified.query(cond=f"MLOGP10 > {pvalue_sig}", dims=['CHR','POS','TRAITID'], attrs=['SNPID','ALLELE0','ALLELE1','BETA', 'SE', 'EAF',"MLOG10P"]).df[:, trait_id_list, :]
        subset_SNPs.to_csv(output_path, index = False)
        logger.info(f"Saving filtered GWAS by regions and samples in {output_path}")
        
