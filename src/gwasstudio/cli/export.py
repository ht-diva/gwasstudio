import click
import cloup
import pandas as pd
import pyarrow.parquet as pq
import tiledb

from gwasstudio import logger
from gwasstudio.methods.compute_pheno_variance import compute_pheno_variance
from gwasstudio.methods.locus_breaker import locus_breaker

help_doc = """
Exports data from a TileDB dataset.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB mandatory options",
    cloup.option("--uri", default=None, help="TileDB dataset URI"),
    cloup.option("--output_path", default="out", help="The path of the output"),
    cloup.option("--trait_id_file", default=None, help="The trait id used for the analysis"),
    cloup.option("--attr", default="BETA,SE,EAF", help="string delimited by comma with the attributes to export"),
)
@cloup.option_group(
    "Options for Locusbreaker",
    cloup.option("--locusbreaker", default=False, is_flag=True, help="Option to run locusbreaker"),
    cloup.option("--pvalue-sig", default=5.0, help="Maximum log p-value threshold within the window"),
    cloup.option("--pvalue-limit", default=3.3, help="Log p-value threshold for loci borders"),
    cloup.option(
        "--hole-size",
        default=250000,
        help="Minimum pair-base distance between SNPs in different loci (default: 250000)",
    ),
)
@cloup.option_group(
    "Options for filtering using a list of SNPs ids",
    cloup.option(
        "--snp-list",
        default=None,
        help="A txt file with a column containing the SNP ids",
    ),
)
@cloup.option_group(
    "Options for filtering using a list of SNPs ids",
    cloup.option(
        "--snp-list",
        default=None,
        help="A txt file with a column containing the SNP ids",
    ),
)
@cloup.option_group(
    "Options for getting the entire sumstats",
    cloup.option(
        "--get-all",
        default=False,
        is_flag=True,
        help="Boolean to get all the sumstats",
    ),
)
@cloup.option_group(
    "Options for filtering specific regions",
    cloup.option(
        "--get-regions",
        default=None,
        help="Bed file with regions to filter",
    ),
    cloup.option(
        "--maf",
        default=None,
        help="MAF filter to apply to each region",
    ),
    cloup.option(
        "--maf",
        default=0.005,
        help="MAF filter to apply to each region",
    ),
    cloup.option(
        "--phenovar",
        default=False,
        is_flag=True,
        help="Boolean to compute phenovariance",
    ),
    cloup.option(
        "--nest",
        default=False,
        is_flag=True,
        help="Estimate effective population size",
    ),
)
@click.pass_context
def export(
    ctx,
    uri,
    trait_id_file,
    attr,
    output_path,
    pvalue_sig,
    pvalue_limit,
    hole_size,
    phenovar,
    nest,
    maf,
    snp_list,
    locusbreaker,
    get_all,
    get_regions,
):
    cfg = ctx.obj["cfg"]
    # batch_size = ctx.obj["batch_size"]
    # client = ctx.obj["client"]
    tiledb_unified = tiledb.open(uri, mode="r", config=cfg)
    logger.info("TileDB dataset loaded")
    trait_id = pd.read_table(trait_id_file)
    trait_id_list = list(trait_id["data_id"])

    # If locus_breaker is selected, run locus_breaker
    if locusbreaker:
        print("running locus breaker")
        for trait in trait_id_list:
            subset_SNPs_pd = tiledb_unified.query(
                cond=f"EAF > {maf} and EAF < {1-maf}",
                dims=["CHR", "POS", "TRAITID"],
                attrs=["SNPID", "BETA", "SE", "EAF", "MLOG10P"],
            ).df[:, :, trait_id_list]
            results_lb_segments, results_lb_intervals = locus_breaker(
                subset_SNPs_pd, hole_size=hole_size, pvalue_sig=pvalue_sig, pvalue_limit=pvalue_limit, phenovar=phenovar
            )
            logger.info(f"Saving locus-breaker output in {output_path} segments and intervals")
            results_lb_segments.to_csv(f"{output_path}_{trait}_segements.csv", index=False)
            results_lb_intervals.to_csv(f"{output_path}_{trait}_intervals.csv", index=False)

        return

    # If snp_list is selected, run extract_snp
    if snp_list:
        SNP_list = pd.read_csv(snp_list, usecols=["CHR", "POS"], dtype={"CHR": str, "POS": int})
        SNP_list = SNP_list[SNP_list["CHR"].astype(str).str.isnumeric()]

        chromosome_dict = SNP_list.groupby("CHR")["POS"].apply(list).to_dict()
        # parallelize by n_workers traits at a time with dask the query on tiledb

        for trait in trait_id_list:
            print(trait)
            for chrom in chromosome_dict.keys():
                chromosomes = int(chrom)
                unique_positions = list(set(chromosome_dict[chrom]))
                # compute batch size traits at time
                tiledb_iterator_query = tiledb_unified.query(
                    dims=["CHR", "POS", "TRAITID"], attrs=attr.split(","), return_arrow=True
                ).df[chromosomes, unique_positions, trait]
                tiledb_iterator_query_df = tiledb_iterator_query.to_pandas()
                tiledb_iterator_query_df.to_csv(f"{output_path}_{trait}", index=False, header=False, mode="a")

            logger.info(f"Saved filtered summary statistics by SNPs in {output_path}_{trait}")

        ctx.obj
        exit()
    if get_all:
        for trait in trait_id_list:
            tiledb_query = tiledb_unified.query(
                dims=["CHR", "POS", "TRAITID"],
                attrs=["SNPID", "BETA", "SE", "EAF", "MLOG10P"],
                return_arrow=True,
            ).df[:, :, trait]
            logger.info(f"Saving all summary statistics in {output_path}")
            pq.write_table(tiledb_query, f"{output_path}_{trait}.parquet", compression="snappy")
    if get_regions:
        bed_region = pd.read_csv(get_regions, sep="\t", header=None)
        bed_region.columns = ["CHR", "START", "END"]
        for trait in trait_id_list:
            df_trait = []
            for index, row in bed_region.iterrows():
                subset_SNPs_pd = tiledb_unified.query(
                    cond=f"EAF > {maf} and EAF < {1-maf}", dims=["CHR", "POS", "TRAITID"], attrs=attr.split(",")
                ).df[row["CHR"], row["START"] : row["END"], trait]
                if phenovar:
                    pv = compute_pheno_variance(subset_SNPs_pd)
                    subset_SNPs_pd["S"] = pv
                    if nest:
                        neff = pv / (
                            (2 * subset_SNPs_pd["EAF"] * (1 - subset_SNPs_pd["EAF"])) * (subset_SNPs_pd["SE"] ^ 2)
                        )
                        subset_SNPs_pd["NEFF"] = neff
                df_trait.append(subset_SNPs_pd)
            df_trait_concat = pd.concat(df_trait)
            logger.info(f"Saving summary statistics in {output_path}")
            pq.write_table(df_trait_concat, f"{output_path}_{trait}.parquet", compression="snappy")
