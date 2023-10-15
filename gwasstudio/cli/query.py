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
@click.pass_context
def query(ctx, mem_budget_mb, information, uri):
    cfg = tiledbvcf.ReadConfig(memory_budget_mb=mem_budget_mb)
    ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)
    if information == "samples":
        print(ds.samples())
    else:
        print(ds.attributes())
        print(ds.attributes(attr_type="builtin"))
