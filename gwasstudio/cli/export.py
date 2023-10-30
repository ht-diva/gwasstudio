import click
import cloup
import numpy as np
import pandas as pd
import tiledbvcf
from cloup.constraints import mutually_exclusive
from scipy import stats

from gwasstudio import logger

help_doc = """
Exports data from a TileDB-VCF dataset.
"""


@cloup.command("export", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Filtering options",
    cloup.option(
        "--mlog10p-le", default=None, help="Filter by the mlog10p value less than or equal to the number given"
    ),
    cloup.option(
        "--mlog10p-ge", default=None, help="Filter by the mlog10p value greater than or equal to the number given"
    ),
    cloup.option("--mlog10p-min", is_flag=True, help="Filter by the mlog10p minimum value"),
    constraint=mutually_exclusive,
)
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
def export(
    ctx,
    attrs,
    mem_budget_mb,
    mlog10p_le,
    mlog10p_ge,
    mlog10p_min,
    output_format,
    output_path,
    regions_file,
    samples_file,
    uri,
):
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
        df = pd.concat(frames, axis=0)
        df["MLOG10P"] = -np.log10(stats.norm.sf(abs(df["BETA"] / df["SE"])) * 2)
        completed = ds.read_completed()
        logger.info("Reading the data set completed: {}".format(completed))

        if mlog10p_le:
            df = df.loc[df.LP.le(mlog10p_le) | np.isclose(df.LP, mlog10p_le)]
        elif mlog10p_ge:
            df = df.loc[df.LP.ge(mlog10p_ge) | np.isclose(df.LP, mlog10p_ge)]
        elif mlog10p_min:
            df = df.loc[df.LP.min()]

        if output_format == "csv":
            logger.info(f"Saving Dataframe in {output_path}")
            df.to_csv(output_path, sep="\t", index=False)
