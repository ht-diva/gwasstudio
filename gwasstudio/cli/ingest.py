import click
import cloup
import pathlib
import tiledbvcf

help_doc = """
Ingest GWAS-VCF data in a TileDB-VCF dataset.
"""


@cloup.command("ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--gwas-vcf-path",
        default=None,
        help="Path to a folder where all the vcf.gz files to ingest are stored",
    ),
    cloup.option(
        "-a",
        "--attrs",
        help="List of extra attributes to add, provided as a single string comma separated",
    ),
    cloup.option(
        "-to",
        "--tiledb-out-path",
        default=None,
        help="s3 path for the tiledb dataset",
    ),
)
@cloup.option_group(
    "TileDB configurations",
    cloup.option("-b", "--mem-budget-mb", default=20480, help="The memory budget in MiB when ingesting the data"),
    cloup.option("-tr", "--threads", default=16, help="The number fo threads used for ingestion"),
)
@click.pass_context
def ingest(ctx, gwas_vcf_path, attrs, tiledb_out_path, mem_budget_mb, threads):
    if ctx.obj["DISTRIBUTE"]:
        pass
    else:
        _attrs = [a.strip() for a in attrs.split(",")]
        cfg = ctx.obj["cfg"]
        # vfs = tiledb.VFS(config=cfg)
        # if (vfs.is_dir(tiledb_path_out)):
        #    print(f"Deleting existing array '{tiledb_path_out}'")
        #    vfs.remove_dir(tiledb_path_out)
        #    print("Done.")
        ds = tiledbvcf.Dataset(tiledb_out_path, mode="w", tiledb_config=cfg)
        ds.create_dataset(extra_attrs=[_attrs])
        p = pathlib.Path(gwas_vcf_path)
        ds.ingest_samples(
            total_memory_budget_mb=mem_budget_mb,
            threads=threads,
            sample_uris=[str(file) for file in list(p.glob("*.gz"))],
        )
