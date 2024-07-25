import click
import cloup
import tiledbvcf

help_doc = """
Get some information about your dataset, such as the sample names, the attributes you can query
"""


@cloup.command("query", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB options",
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MB when query a TileDB dataset"),
    cloup.option(
        "-i",
        "--information",
        type=click.Choice(["attributes", "samples"]),
        default="samples",
        help="The type of information that you would like to retrieve from the data set",
    ),
    cloup.option("-u", "--uri", help="TileDB-VCF dataset URI"),
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
def query(
    ctx, 
    mem_budget_mb, 
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,
    aws_region,
    aws_verify_ssl,
    information, 
    uri):
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=mem_budget_mb)
    cfg = tiledb.Config({
            "vfs.s3.aws_access_key_id":aws_access_key_id,
            "vfs.s3.aws_secret_access_key":aws_secret_access_key,
            "vfs.s3.endpoint_override":aws_endpoint_override,
            "vfs.s3.use_virtual_addressing":aws_use_virtual_addressing,
            "vfs.s3.scheme":aws_scheme,
            "vfs.s3.region":aws_region,
            "vfs.s3.verify_ssl":aws_verify_ssl
        })
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    if information == "samples":
        for s in ds.samples():
            print(s)
    else:
        print(ds.attributes())
