import click
import cloup
import pathlib
import tiledbvcf
import tiledb

help_doc = """
Ingest GWAS-VCF data in a TileDB-VCF dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--gwas-vcf-path",
        default=None,
        help="Path to a folder where all the vcf.gz files to ingest are present",
    ),
    cloup.option(
        "-a",
        "--attrs",
        help="List of extra attributes to add, provided as a single string comma separated",
    ),
    cloup.option(
        "-to",
        "--tiledb-path-out",
        default=None,
        help="s3 path where the tiledb dataset is created",
    ),
)
@cloup.option_group(
    "TileDB configurations",
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MiB when ingesting the data"),
    cloup.option("-tr", "--threads", default=16, help="The number fo threads used for ingestion"),
    cloup.option("--aws-access-key-id", default=None, help="aws access key id"),
    cloup.option("--aws-secret-access-key", default=None, help="aws access key"),
    cloup.option("--aws-endpoint-override", default="https://storage.fht.org:9021", help="endpoint where to connect"),
    cloup.option("--aws-use-virtual-addressing", default="false", help="virtual address option"),
    cloup.option("--aws-scheme", default="https", help="type of scheme used at the endpoint"),
    cloup.option("--aws-region", default="", help="region where the s3 bucket is located"),
    cloup.option("--aws-verify-ssl", default="false", help="if ssl verification is needed"),
)
@click.pass_context
def ingest(
    ctx,
    gwas_vcf_path,
    attrs,
    tiledb_path_out,
    mem_budget_mb,
    threads,
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,
    aws_region,
    aws_verify_ssl,
):
    if ctx.obj["DISTRIBUTE"]:
        pass
    else:
        _attrs = [a.strip() for a in attrs.split(",")]
        cfg = tiledb.Config(
            {
                "vfs.s3.aws_access_key_id": aws_access_key_id,
                "vfs.s3.aws_secret_access_key": aws_secret_access_key,
                "vfs.s3.endpoint_override": aws_endpoint_override,
                "vfs.s3.use_virtual_addressing": aws_use_virtual_addressing,
                "vfs.s3.scheme": aws_scheme,
                "vfs.s3.region": aws_region,
                "vfs.s3.verify_ssl": aws_verify_ssl,
            }
        )
        read_cfg = tiledbvcf.ReadConfig(tiledb_config=cfg)
        # vfs = tiledb.VFS(config=cfg)
        # if (vfs.is_dir(tiledb_path_out)):
        #    print(f"Deleting existing array '{tiledb_path_out}'")
        #    vfs.remove_dir(tiledb_path_out)
        #    print("Done.")
        ds = tiledbvcf.Dataset(tiledb_path_out, mode="w", cfg=read_cfg)
        ds.create_dataset(extra_attrs=[attrs])
        p = pathlib.Path(gwas_vcf_path)
        ds.ingest_samples(
            total_memory_budget_mb=mem_budget_mb,
            threads=threads,
            sample_uris=[str(file) for file in list(p.glob("*.gz"))],
        )
