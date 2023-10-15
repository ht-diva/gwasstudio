import click
import cloup
import pandas as pd
import tiledbvcf

from gwasstudio import logger

help_doc = """
Exports data from a TileDB-VCF dataset.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "TileDB options",
    cloup.option("-a", "--attrs", help="List of attributes to extract, provided as a single string comma separated"),
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MB when query a TileDB dataset"),
    cloup.option("-f", "--samples-file", help="Path to file with 1 sample name per line"),
    cloup.option(
        "-O", "--output-format", type=click.Choice(["csv"]), default="csv", help="Export format. Options are: csv"
    ),
    cloup.option("-o", "--output-path", help="The name of the output file"),
    cloup.option("-R", "--regions-file", help="File containing regions (BED format)"),
    cloup.option("-u", "--uri", help="TileDB-VCF dataset URI"),
)
@click.pass_context
def export(ctx, attrs, mem_budget_mb, output_format, output_path, regions_file, samples_file, uri):
    if ctx.obj["DISTRIBUTE"]:
        pass
    else:
        _attrs = [a.strip() for a in attrs.split(",")]
        cfg = tiledbvcf.ReadConfig(memory_budget_mb=mem_budget_mb)
        ds = tiledbvcf.Dataset(uri, mode="r", cfg=cfg)

        frames = []
        for batch in ds.read_iter(attrs=_attrs, bed_file=regions_file, samples_file=samples_file):
            batch["BETA"] = batch["fmt_ES"].str[0]
            batch["SE"] = batch["fmt_SE"].str[0]
            batch["LP"] = batch["fmt_LP"].str[0]
            batch = batch.drop(columns=["fmt_ES", "fmt_SE", "fmt_LP"])
            frames.append(batch)
        df = pd.concat(frames, axis=1)
        completed = ds.read_completed()
        logger.info("Reading the data set completed: {}".format(completed))

        if output_format == "csv":
            logger.info(f"Saving Dataframe in {output_path}")
            df.to_csv(output_path, sep="\t", index=False)
