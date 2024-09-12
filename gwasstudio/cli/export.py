import click
import cloup
import tiledbvcf
from gwasstudio import logger
from gwasstudio.methods.genome_windows import create_genome_windows
from gwasstudio.methods.locus_breaker import locus_breaker
from gwasstudio.methods.extract_snp import extract_snp


help_doc = """
Exports data from a TileDB-VCF dataset.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)

@cloup.option_group(
    "TileDBVCF options",
    cloup.option(
        "--uri", 
        default="None", 
        help="TileDB-VCF dataset URI"
    ),
    cloup.option(
        "--output-path", 
        default="output", 
        help="The name of the output file"
    ),
    cloup.option(
        "--samples", 
        default="None", 
        help="A path of a txt file containing 1 sample name per line"
    ),
    cloup.option(
        "--genome-version", 
        help="Genome version to be used (either hg19 or hg38)", 
        default="hg19"
    ),
    cloup.option(
        "--columns", 
        default="CHROMOSOME,POSITION,ALLELES,BETA,SE,SAMPLES,LP", 
        help="List of columns to keep, provided as a single string comma separated"
    ),
    # Not yet implemented
    cloup.option(
        "--chromosome",
        help="Chromosomes list to use during processing. This can be a list of chromosomes separated by comma (Example: 1,2,3,4)",
        default="1",
    ),
    cloup.option(
        "--window-size",
        default=50000000,
        help="Widnow size used by tiledbvcf for later queries"
    ),
    cloup.option(
        "--sample-partitions",
        help="how many partition to divide the sample list with for computation (default is 1)",
        default=1,
    ),
    cloup.option(
        "--region-partitions",
        help="how many partition to divide the region list with for computation (default is 20)",
        default=20,
    )
)

@cloup.option_group(
    "Options for Locusbreaker",
    cloup.option(
        "--locusbreaker",
        is_flag=True,
        default=False,
        help="Flag to use locus breaker"
    ),
    cloup.option(
        "--pvalue-sig",
        default=5.0,
        help="P-value threshold to use for filtering the data"
    ),
    cloup.option(
        "--pvalue-limit",
        default=5.0, 
        help="P-value threshold for loci borders"
    ),
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
        default="None",
        help="A csv file with a column containing the SNP ids"
    ),
)

@cloup.option_group(
    "TileDB configurations",
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

def export(
    pvalue_sig,
    pvalue_limit,
    hole_size,
    snp_list,
    window_size,
    output_path,
    genome_version,
    columns,
    chromosome,
    sample_partitions,
    region_partitions,
    locusbreaker,
    samples,
    uri,
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,aws_region,
    aws_verify_ssl):
    cfg = {}
    cfg["vfs.s3.aws_access_key_id"] = aws_access_key_id
    cfg["vfs.s3.aws_secret_access_key"] = aws_secret_access_key
    cfg["vfs.s3.endpoint_override"] = aws_endpoint_override
    cfg["vfs.s3.use_virtual_addressing"] = aws_use_virtual_addressing
    cfg["vfs.s3.scheme"] = aws_scheme
    cfg["vfs.s3.region"] = aws_region
    cfg["vfs.s3.verify_ssl"] = aws_verify_ssl
    ds = tiledbvcf.Dataset(uri, mode="r", tiledb_config=cfg)
    logger.info("TileDBVCF dataset loaded")

    samples_list = []
    if samples:
        samples_file = open(samples, "r").readlines()
        samples_list =  [s.split("\n")[0] for s in samples_file]

    # Create a mapping of the user selected columns into TileDB
    columns_attribute_mapping = {
        "CHROMOSOME": "contig",
        "POSITION": "pos_start",
        "SNP": "id",
        "ALLELES": "alleles",
        "BETA": "fmt_ES",
        "SE": "fmt_SE",
        "LP": "fmt_LP",
        "SAMPLES": "sample_name",
    }
    column_list_select = [columns_attribute_mapping[a] for a in columns.split(",")]
    column_list_select.append("pos_end")

    # Create bed regions for all the genome
    bed_regions_all = create_genome_windows(style="NCBI", window=window_size, genome=genome_version)

    # Filter bed regions by chromosome if selected
    if chromosome:
        bed_regions = [r for r in bed_regions_all if any(r.startswith(f"{chr_num}:") for chr_num in chromosome.split(","))]

    else:
        bed_regions = bed_regions_all
    logger.info("bed regions created")

    # If locus_breaker is selected, run locus_breaker
    if locusbreaker:
        print("running locus breaker")
        dask_df = ds.map_dask(
            lambda tiledb_data: locus_breaker(
            tiledb_data,
            pvalue_limit=pvalue_limit,
            pvalue_sig=pvalue_sig,
            hole_size=hole_size,
            map_attributes=columns_attribute_mapping
            ),
            attrs=column_list_select,
            regions=bed_regions,
            samples=samples_list,
            #sample_partitions=sample_partitions,
            #region_partitions=region_partitions
        )
        logger.info(f"Saving locus-breaker output in {output_path}")
        dask_df.to_csv(output_path)
        exit()

    # If snp_list is selected, run extract_snp
    if snp_list:
        extract_snp(
            tiledb_data=ds,
            snp_file_list=snp_list,
            column_list_select=column_list_select,
            samples_list=samples_list,
            sample_partitions=sample_partitions,
            output_path=output_path,
            map_attributes=columns_attribute_mapping,
        )
        logger.info(f"Saved filtered summary statistics by SNPs in {output_path}")
        exit()

        #tiledb_data_snp.to_parquet(output_path, engine="pyarrow", compression="snappy", schema = None)

    # If neither locus_breaker nor snp_list is selected, filter the data by regions and samples
    else:
        filtered_ddf = ds.read_dask(
            attrs=column_list_select,
            regions=bed_regions,
            region_partitions=region_partitions,
            samples=samples_list,
            sample_partitions=sample_partitions
        )
        logger.info(f"Saving filtered GWAS by regions and samples in {output_path}")
        filtered_ddf.to_parquet(output_path, engine="pyarrow", compression="snappy",schema=None)
        
