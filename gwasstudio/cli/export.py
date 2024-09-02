import click
import cloup
import numpy as np
import pandas as pd
import tiledbvcf
from cloup.constraints import mutually_exclusive
from scipy import stats
import tiledb
from gwasstudio import logger
import methods/genome_windows
import methods/locus_breaker

help_doc = """
Exports data from a TileDB-VCF dataset.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Filtering options",
    cloup.option("--pvalue-filter", default=False, help="pvalue threshold to use for filtering the data")
    cloup.option("--window-size", default=False, help="Widnow size used by tiledbvcf for later queries")

)
@cloup.option_group(
    "TileDB options",
    cloup.option("-a", "--attrs", help="List of attributes to extract, provided as a single string comma separated"),
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MB when query a TileDB dataset"),
    cloup.option("-f", "--samples-file", help="Path of file with 1 sample name per line"),
    cloup.option("-s", "--samples", help="CSV list of sample names to be read"),
    cloup.option(
        "-O", "--output-format", type=click.Choice(["csv"]), default="csv", help="Export format. Options are: csv"
    ),
    cloup.option("-o", "--output-path", help="The name of the output file"),
    cloup.option("-g", "--genome-version", help="Genome version to be used (either hg19 or hg38)", default="hg19"),
    cloup.option("-c", "--chromosome", help="Chromosomes to use during processing. This can be a list of chromosomes separated by comma (Example: 1,2,3,4)", default = 1),
    cloup.option("-s", "--sample-partitions", help="how many partition to divide the sample list with for computation (default is 1)", default=1),
    cloup.option("-r", "--region-partitions", help="how many partition to divide the region list with for computation (default is 20)", default=20),
    cloup.option("-u", "--uri", help="TileDB-VCF dataset URI"),
)
@cloup.option_group(
    "TileDB configurations",
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MB when ingesting the data"),
    cloup.option("--aws-access-key-id", default=None, help="aws access key id"),
    cloup.option("--aws-secret-access-key", default=None, help="aws access key"),
    cloup.option("--aws-endpoint-override", default="https://storage.fht.org:9021", help="endpoint where to connect"),
    cloup.option("--aws-use-virtual-addressing", default="false", help="virtual address option"),
    cloup.option("--aws-scheme", default="https", help="type of scheme used at the endpoint"),
    cloup.option("--aws-region", default="", help="region where the s3 bucket is located"),
    cloup.option("--aws-verify-ssl", default="false", help="if ssl verfication is needed")
)
@click.pass_context
def export(
    ctx,
    attrs,
    mem_budget_mb,
    pvalue_filter,
    window_size,
    output_format,
    output_path,
    genome_version,
    chromosome,
    sample_partitions,
    region_partitions,
    samples_file,
    samples,
    uri,
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,
    aws_region,
    aws_verify_ssl

):
    #cfg = tiledbvcf.ReadConfig(
    #    memory_budget_mb=mem_budget_mb)
    _attrs = [a.strip() for a in attrs.split(",")]
    cfg = {} 
    cfg["vfs.s3.aws_access_key_id"] = aws_access_key_id
    cfg["vfs.s3.aws_secret_access_key"] = aws_secret_access_key
    cfg["vfs.s3.endpoint_override"] = aws_endpoint_override
    cfg["vfs.s3.use_virtual_addressing"] = aws_use_virtual_addressing
    cfg["vfs.s3.scheme"] = aws_scheme
    cfg["vfs.s3.region"] = aws_region
    cfg["vfs.s3.verify_ssl"] = aws_verify_ssl
    ds = tiledbvcf.Dataset(uri, mode="r", tiledb_config=cfg)

    samples_list = []
    if samples:
        samples_list = [s.strip() for s in samples.split(",")]


        
        
    print("functions and library loaded")

    #window size here I used 50000000
    bed_regions_all = genome_windows(style="NCBI", window = window_size 50000000)
    # Filter bed_regions_all to include chromosomes 1 through 22
    bed_regions = [r for r in bed_regions_all if any(r.startswith(f"{chr_num}:") for chr_num in chr_list.split(","))]

    print("bed regions created")
    if locus_breaker:

        dask_df = ds.map_dask(process_partition,
                attrs=_attrs,
                regions=bed_regions,
                samples = ,
                sample_partitions = sample_partitions,
                region_partitions=region_partitions)
        logger.info(f"Saving locus breaker output in {output_path}")
        dask_df.to_csv(output_path, header=True, index=False)
            
    else:
        filtered_ddf = ds.read_dask(attrs=_attrs,
                regions=bed_regions,
                region_partitions=region_partitions,
                samples = samples_list,
                sample_partitions=sample_partitions)
        logger.info(f"Saving filtered GWAS by regions and samples in {output_path}")
        filtered_ddf.to_csv(output_path, header=True, index=False)
