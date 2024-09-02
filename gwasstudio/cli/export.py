import click
import cloup
import numpy as np
import tiledbvcf
from cloup.constraints import mutually_exclusive
from scipy import stats
import tiledb
from gwasstudio import logger
from  methods import genome_windows
from methods import locus_breaker

help_doc = """
Exports data from a TileDB-VCF dataset.
"""
@cloup.command("export", no_args_is_help=True, help=help_doc)

@cloup.option_group(
    "Options for Locusbreaker",
    
    cloup.option(
        "--locus-breaker", 
        is_flag=True, 
        default=False, 
        help="Flag to use locus breaker"
    ),
    cloup.option(
        "--pvalue_sig",
        default=5,
        help="pvalue threshold to use for filtering the data"
    ),
    cloup.option(
        "--pvalue_limit",
        default=5,
        help="pvalue threshold to use for filtering the data"
    ),
    cloup.option(
        "--chr_label",
        default="contig",
        help="label used for chromosomes"),
    
    cloup.option(
        "--pos_label",
        default="pos_start",
        help="label used for defining the positions of the SNPs in the GWAS data"
    ),
    cloup.option(
        "--pvalue_label",
        default="fmt_LP",
        help="label used for -log10 of p-value in the GWAS data"
    )
)

@cloup.option_group(
    "TileDBVCF options",
    cloup.option(
        "-a",
        "--attrs",
        default=False,
        help="List of attributes to extract, provided as a single string comma separated"
    ),
    cloup.option(
        "-f",
        "--samples-file",
        default=False,
        help="Path of file with 1 sample name per line"
    ),
    cloup.option(
        "-s",
        "--samples",
        default=False,
        help="CSV list of sample names to be read"
    ),
    cloup.option(
        "-g",
        "--genome-version",
        help="Genome version to be used (either hg19 or hg38)",
        default="hg19"
    ),
    cloup.option(
        "-c",
        "--chromosome",
        help="Chromosomes list to use during processing. This can be a list of chromosomes separated by comma (Example: 1,2,3,4)",
        default = "1"
    ),
    cloup.option(
        "--window-size",
        default=50000000,
        help="Widnow size used by tiledbvcf for later queries"
    )
    cloup.option(
        "-s",
        "--sample-partitions",
        help="how many partition to divide the sample list with for computation (default is 1)",
        default=1),

    cloup.option(
        "-r",
        "--region-partitions",
        help="how many partition to divide the region list with for computation (default is 20)",
        default=20
    ),
    cloup.option(
        "-u",
        "--uri",
        default=False,
        help="TileDB-VCF dataset URI"
    ),
    cloup.option(
        "-O",
        "--output-format",
        type=click.Choice(["parquet"]),
        default="parquet",
        help="Export format. Options are: parquet and csv. The default is parquet"
    ),
    cloup.option(
        "-o",
        "--output-path",
        default="output",
        help="The name of the output file"
    )
)

@cloup.option_group(
    "TileDB configurations",
    cloup.option(
        "-b",
        "--mem-budget-mb",
        default=20480,
        help="The memory budget in MB when ingesting the data",
    ),
    cloup.option(
        "--aws-access-key-id", 
        default=None, 
        help="aws access key id"
    ),
    cloup.option(
        "--aws-secret-access-key",
        default=None,
        help="aws access key"
    ),
    cloup.option(
        "--aws-endpoint-override",
        default="https://storage.fht.org:9021",
        help="endpoint where to connect",
    ),
    cloup.option(
        "--aws-use-virtual-addressing", 
        default="false", 
        help="virtual address option"
    ),
    cloup.option(
        "--aws-scheme", 
        default="https", 
        help="type of scheme used at the endpoint"
    ),
    cloup.option(
        "--aws-region", 
        default="",
        help="region where the s3 bucket is located"
    ),
    cloup.option(
        "--aws-verify-ssl",
        default="false",
        help="if ssl verfication is needed"
    ),
)

@click.pass_context
def export(
    attrs,
    mem_budget_mb,
    pvalue_limit,
    pvalue_sig,
    chr_label,
    pos_label,
    pvalue_label,
    window_size,
    output_format,
    output_path,
    genome_version,
    chr_list,
    sample_partitions,
    region_partitions,
    samples,
    uri,
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,
    aws_region,
    aws_verify_ssl,
):
    _attrs = [a.strip() for a in attrs.split(",")]
    cfg = {} 
    cfg["vfs.s3.aws_access_key_id"] = aws_access_key_id
    cfg["vfs.s3.aws_secret_access_key"] = aws_secret_access_key
    cfg["vfs.s3.endpoint_override"] = aws_endpoint_override
    cfg["vfs.s3.use_virtual_addressing"] = aws_use_virtual_addressing
    cfg["vfs.s3.scheme"] = aws_scheme
    cfg["vfs.s3.region"] = aws_region
    cfg["vfs.s3.verify_ssl"] = aws_verify_ssl
    cfg["memory_budget_mb"] = mem_budget_mb

    ds = tiledbvcf.Dataset(uri, mode="r", tiledb_config=cfg)

    samples_list = []
    if samples:
        samples_list = [s.strip() for s in samples.split(",")]
        
    print("functions and library loaded")

    #window size here I used 50000000
    bed_regions_all = genome_windows(style="NCBI", window = window_size, genome=genome_version)
    # Filter bed_regions_all to include chromosomes 1 through 22
    bed_regions = [r for r in bed_regions_all if any(r.startswith(f"{chr_num}:") for chr_num in chr_list.split(","))]

    print("bed regions created")
    if locus_breaker:

        dask_df = ds.map_dask(locus_breaker,
                attrs=_attrs,
                regions=bed_regions,
                samples = samples_list,
                sample_partitions = sample_partitions,
                region_partitions=region_partitions,
                pvalue_limit = pvalue_limit,
                pvalue_sig = pvalue_sig,
                chr_label = chr_label,
                pos_label = pos_label,
                pvalue_label = pvalue_label)
        logger.info(f"Saving locus breaker output in {output_path}")
        if(output_format == "csv"):
            dask_df.to_csv(output_path, header=True, index=False)
        else:
            dask_df.to_parquet(output_path, engine='pyarrow', compression='snappy')
            
    else:
        filtered_ddf = ds.read_dask(attrs=_attrs,
                regions=bed_regions,
                region_partitions=region_partitions,
                samples = samples_list,
                sample_partitions=sample_partitions)
        logger.info(f"Saving filtered GWAS by regions and samples in {output_path}")
        if(output_format == "csv"):
            filtered_ddf.to_csv(output_path, header=True, index=False)
        else:
            filtered_ddf.to_parquet(output_path, engine='pyarrow', compression='snappy')