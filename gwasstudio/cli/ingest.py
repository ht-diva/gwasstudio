import click
import cloup
import numpy as np
import pandas as pd
import pathlib
import tiledbvcf
from cloup.constraints import mutually_exclusive
from scipy import stats
import tiledb
from gwasstudio import logger

help_doc = """
Ingest GWAS-VCF data in a TileDB-VCF dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--gwas-vcf-path", default=None, help="Path to a folder where all the vcf.gz files to ingest are present"
    )
)
@cloup.option_group(
    "TileDB options",
    cloup.option("-a", "--attrs", help="List of extra attributes to add, provided as a single string comma separated"),
    cloup.option("--tiledb-path-out", default=None, help="s3 path where the tiledb dataset is created")
)
@cloup.option_group(
    "TileDB configurations",
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MB when ingesting the data"),
    cloup.option("-t", "--threads", default=16, help="The number fo threwads used for ingestion"),
    cloup.option("--aws-access-key-id", default=None, help="aws access key id"),
    cloup.option("--aws-secret-access-key", default=None, help="aws access key"),
    cloup.option("--aws-endpoint-override", default="https://storage.fht.org:9021", help="endpoint where to connect"),
    cloup.option("--aws-use-virtual-addressing", default="false", help="virtual address option"),
    cloup.option("--aws-scheme", default="https", help="type of scheme used at the endpoint"),
    cloup.option("--aws-region", default="", help="region where the s3 bucket is located"),
    cloup.option("--aws-verify-ssl", default="false", help="if ssl verfication is needed")
    )

@click.pass_context
def ingest(
    ctx,
    gwas_vcf_path,
    attrs,
    tiledb_vcf_path,
    mem_budget_mb,
    threads,
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,
    aws_region,
    aws_verify_ssl
):
     if ctx.obj["DISTRIBUTE"]:
        pass
    else:
        _attrs = [a.strip() for a in attrs.split(",")]
        cfg = tiledbvcf.ReadConfig(
            memory_budget_mb=mem_budget_mb, 
            vfs.s3.aws_access_key_id=aws_access_key_id, 
            vfs.s3.aws_secret_access_key=aws_secret_access_key,
            vfs.s3.endpoint_override=aws_endpoint_override, 
            vfs.s3.use_virtual_addressing=aws_use_virtual_addressing, 
            vfs.s3.scheme=aws_scheme,
            vfs.s3.region=aws_region,
            vfs.s3.verify_ssl=aws_verify_ssl)
        cfg = tiledb.Config()
        read_cfg = tiledbvcf.ReadConfig(tiledb_config=cfg)
        ds = tiledbvcf.Dataset(tiledb_vcf_path, mode = "w", cfg = read_cfg)
        ds.create_dataset(extra_attrs=[attrs])
        p = pathlib.Path(gwas_vcf_path)
        ds.ingest_samples(threads=threads, sample_uris = [ str(l) for l in list(p.glob("*.gz"))])


