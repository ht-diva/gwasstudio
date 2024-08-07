import click
import cloup
import numpy as np
import pandas as pd
import tiledbvcf
from cloup.constraints import mutually_exclusive
from scipy import stats
import tiledb
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
    cloup.option("--mlog10p-max", is_flag=True, help="Get the mlog10p maximum value for each sample"),
    constraint=mutually_exclusive,
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
    cloup.option("-R", "--regions-file", help="File containing regions (BED format)"),
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
    mlog10p_le,
    mlog10p_ge,
    mlog10p_max,
    output_format,
    output_path,
    regions_file,
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
    if ctx.obj["DISTRIBUTE"]:
        pass
    else:
        _attrs = [a.strip() for a in attrs.split(",")]
        cfg = tiledbvcf.ReadConfig(
            memory_budget_mb=mem_budget_mb)
        cfg = tiledb.Config({
            "vfs.s3.aws_access_key_id":aws_access_key_id,
            "vfs.s3.aws_secret_access_key":aws_secret_access_key,
            "vfs.s3.endpoint_override":aws_endpoint_override,
            "vfs.s3.use_virtual_addressing":aws_use_virtual_addressing,
            "vfs.s3.scheme":aws_scheme,
            "vfs.s3.region":aws_region,
            "vfs.s3.verify_ssl":aws_verify_ssl
        })

        cfg = tiledb.Config()
        read_cfg = tiledbvcf.ReadConfig(tiledb_config=cfg)

        ds = tiledbvcf.Dataset(uri, mode="r", cfg=read_cfg)

        frames = []
        results = {}
        samples_list = []
        if samples:
            samples_list = [s.strip() for s in samples.split(",")]
        for batch in ds.read_iter(attrs=_attrs, bed_file=regions_file, samples_file=samples_file, samples=samples_list):
            batch["BETA"] = batch["fmt_ES"].str[0]
            batch["SE"] = batch["fmt_SE"].str[0]
            batch["LP"] = batch["fmt_LP"].str[0]
            batch = batch.drop(columns=["fmt_ES", "fmt_SE", "fmt_LP"])
            batch["MLOG10P"] = -np.log10(stats.norm.sf(abs(batch["BETA"] / batch["SE"])) * 2)

            if mlog10p_max:
                by_sample_name = batch.groupby(["sample_name"]).MLOG10P.idxmax()
                for i in by_sample_name:
                    values = {
                        "id": batch.iloc[i].id,
                        "sample_name": batch.iloc[i].sample_name,
                        "mlog10p": batch.iloc[i].MLOG10P,
                    }
                    results[batch.iloc[i].sample_name] = values
            else:
                frames.append(batch)

        if mlog10p_max:
            print(results)
            f = open(output_path, "w")
            header = "software_model\tID\tmax_minusLog10P\n"
            f.write(header)
            lines = ["{}\t{}\t{}\n".format(v["sample_name"], v["id"], v["mlog10p"]) for v in results.values()]
            f.writelines(lines)
            f.close()
        else:
            df = pd.concat(frames, axis=0)
            completed = ds.read_completed()
            logger.info("Reading the data set completed: {}".format(completed))

            if mlog10p_le:
                df = df.loc[df.LP.le(mlog10p_le) | np.isclose(df.LP, mlog10p_le)]
            elif mlog10p_ge:
                df = df.loc[df.LP.ge(mlog10p_ge) | np.isclose(df.LP, mlog10p_ge)]

            if output_format == "csv":
                logger.info(f"Saving Dataframe in {output_path}")
                df.to_csv(output_path, sep="\t", index=False)
