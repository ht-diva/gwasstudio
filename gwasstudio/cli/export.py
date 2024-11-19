import click
import cloup
from gwasstudio import logger
from methods.locus_breaker import locus_breaker
from utils import process_write_chunk
import tiledb
from scipy import stats
import pandas as pd
import polars as pl

help_doc = """
Exports data from a TileDB dataset.
"""

@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB mandatory options",
    cloup.option("--uri", default="None", help="TileDB dataset URI"),
    cloup.option("--output_path", default="None", help="The path of the output"),
    cloup.option("--trait_id_file", default="None", help="The trait id used for the analysis"),
    cloup.option("--chr_list", default=False, is_flag=True, help="The chromosome used for the analysis"),
)
@cloup.option_group(
    "Options for Locusbreaker",
    cloup.option("--locusbreaker", default=False, is_flag=True, help="Option to run locusbreaker"),
    cloup.option("--pvalue-sig", default=False, is_flag=True,  help="P-value threshold to use for filtering the data"),
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
    output_path,
    pvalue_sig,
    pvalue_limit,
    hole_size,
    snp_list,
    chr_list,
    locusbreaker
):
    cfg = ctx.obj["cfg"]
    tiledb_unified = tiledb.open(uri, mode="r")
    logger.info("TileDB dataset loaded")
    trait_id_list = open(trait_id_file, "r").read().rstrip().split("\n")
    if chr_list:
        chr_list_str = chr_list.split(",")
        chr_list_int = [int(x) for x in chr_list_str]
    else:
        chr_list_int = slice(None)

    # If locus_breaker is selected, run locus_breaker
    if locusbreaker:
        print("running locus breaker")
        for trait in trait_id_list:
            subset_SNPs_pd = tiledb_unified.query(dims=['CHR', 'POS', 'TRAITID'], attrs=['SNPID', 'EA', 'NEA', 'BETA', 'SE', 'EAF', "MLOG10P"]).df[chr_list_int, :, trait_id_list]
            results_lb = locus_breaker(subset_SNPs_pd)
            logger.info(f"Saving locus-breaker output in {output_path}")
            results_lb.to_csv(f,"{output_path}_{trait}.csv", index = False)
        return

    # If snp_list is selected, run extract_snp
    if snp_list != "None":
        SNP_list = pd.read_csv(snp_list, dtype = {"CHR":str, "POS":int, "EA":str, "NEA":str})
        chromosome_dict = SNP_list.groupby('CHR')['POS'].apply(list).to_dict()
        unique_positions = list(set(pos for positions in chromosome_dict.values() for pos in positions))
        with tiledb_unified as tiledb_iterator:
            tiledb_iterator_query = tiledb_iterator.query(
                return_incomplete=True
            ).df[chr_list_int, unique_positions, trait_id_list]  # Replace with appropriate filters if necessary
            with open(output_path, mode="a") as f:
                for chunk in tiledb_iterator_query:
                    # Convert the chunk to Polars format
                    process_write_chunk(chunk, SNP_list, f)
        
        logger.info(f"Saved filtered summary statistics by SNPs in {output_path}")
        exit()

    if pvalue_sig:
        with tiledb_unified as tiledb_iterator:
            tiledb_iterator_query = tiledb_unified.query(cond=f"MLOGP10 > {pvalue_sig}", dims=['CHR','POS','TRAITID'], attrs=['SNPID','EA','NEA','BETA', 'SE', 'EAF',"MLOG10P"]).df[chr_list_int, :, trait_id_list]
            with open(output_path, mode="a") as f:
                for chunk in tiledb_iterator_query:
                            chunk.to_csv(output_path, index = False)
            logger.info(f"Saving filtered GWAS by regions and samples in {output_path}")
        
