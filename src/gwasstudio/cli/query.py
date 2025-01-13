import click
import cloup
import tiledbvcf

help_doc = """
Get some information about your dataset, such as the sample names, the attributes you can query
"""


@cloup.command("query", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB options",
    cloup.option(
        "-i",
        "--information",
        type=click.Choice(["attributes", "samples"]),
        default="samples",
        help="The type of information that you would like to retrieve from the data set",
    ),
    cloup.option("-u", "--uri", help="TileDB-VCF dataset URI"),
)
@click.pass_context
def query(
    ctx,
    information,
    uri,
):
    cfg = ctx.obj["cfg"]
    ds = tiledbvcf.Dataset(uri, mode="r", tiledb_config=cfg)
    if information == "samples":
        for s in ds.samples():
            print(s)
    else:
        print(ds.attributes())
