import click
import cloup

from gwasstudio import __appname__, __version__, context_settings, log_file, logger
from gwasstudio.cli.export import export
from gwasstudio.cli.info import info
from gwasstudio.cli.ingest import ingest
from gwasstudio.cli.metadata.ingest import meta_ingest
from gwasstudio.cli.metadata.query import meta_query
from gwasstudio.cli.metadata.view import meta_view
from gwasstudio.dask_client import DaskClient as Client
from dask.distributed import LocalCluster



@cloup.group(
    name="main",
    help="GWASStudio",
    no_args_is_help=True,
    context_settings=context_settings,
)
@click.version_option(version=__version__)
@cloup.option("-q", "--quiet", default=False, is_flag=True, help="Set log verbosity")
@cloup.option_group(
    "Dask options",
    cloup.option(
        "--distribute",
        default=False,
        is_flag=True,
        help="Distribute the load to a Dask cluster",
    ),
    cloup.option("--local", is_flag = True, default=True, help="If the cluster needs to be kept local"),
    cloup.option("--local_workers", help="Number of workers for local cluster", default=1),
    cloup.option("--local_threads", help="Threads per worker for local cluster", default=4),
    cloup.option("--local_memory", help="Memory per worker for local cluster", default="12GB"),
    cloup.option("--minimum_workers", help="Minimum amount of running workers", default=10),
    cloup.option("--maximum_workers", help="Maximum amount of running workers", default=100),
    cloup.option("--memory_workers", help="Memory amount per worker", default="12GB"),
    cloup.option("--cpu_workers", help="CPU numbers per worker", default=6),
)
@cloup.option_group(
    "MongoDB configuration",
    cloup.option("--mongo-uri", default=None, help="Specify a MongoDB uri if it is different from localhost"),
)
@cloup.option_group(
    "TileDB configuration",
    cloup.option("--aws-access-key-id", default="None", help="aws access key id"),
    cloup.option("--aws-secret-access-key", default="None", help="aws access key"),
    cloup.option(
        "--aws-endpoint-override",
        default="https://storage.fht.org:9021",
        help="endpoint where to connect",
    ),
    cloup.option("--aws-use-virtual-addressing", default="false", help="virtual address option"),
    cloup.option("--aws-scheme", default="https", help="type of scheme used at the endpoint"),
    cloup.option("--aws-region", default="", help="region where the s3 bucket is located"),
    cloup.option("--aws-verify-ssl", default="false", help="if ssl verification is needed"),
)
@click.pass_context
def cli_init(
    ctx,
    aws_access_key_id,
    aws_secret_access_key,
    aws_endpoint_override,
    aws_use_virtual_addressing,
    aws_scheme,
    aws_region,
    aws_verify_ssl,
    distribute,
    local,
    minimum_workers,
    maximum_workers,
    memory_workers,
    local_workers,
    local_threads,
    local_memory,
    cpu_workers,
    mongo_uri,
    quiet,
):
    if quiet:
        logger.add(log_file, level="INFO", retention="30 days")
    else:
        logger.add(log_file, level="DEBUG", retention="30 days")

    cfg = {
        "vfs.s3.aws_access_key_id": aws_access_key_id,
        "vfs.s3.aws_secret_access_key": aws_secret_access_key,
        "vfs.s3.endpoint_override": aws_endpoint_override,
        "vfs.s3.use_virtual_addressing": aws_use_virtual_addressing,
        "vfs.s3.scheme": aws_scheme,
        "vfs.s3.region": aws_region,
        "vfs.s3.verify_ssl": aws_verify_ssl,
        "sm.dedup_coords": "true",
    }

    ctx.ensure_object(dict)
    ctx.obj["cfg"] = cfg
    ctx.obj["DISTRIBUTE"] = distribute

    if distribute:
        if local:
            client = LocalCluster(
                n_workers=local_workers,
                threads_per_worker=local_threads,
                memory_limit=local_memory,
                dashboard_address=":8787",
            ).get_client()
            ctx.obj["client"] = client
            ctx.obj["batch_size"] = local_workers
        else:
            client = Client(
                minimum_workers=minimum_workers,
                maximum_workers=maximum_workers,
                memory_workers=memory_workers,
                cpu_workers=cpu_workers,
            ).get_client()
            ctx.obj["client"] = client
            ctx.obj["batch_size"] = minimum_workers
            # logger.info("Dask dashboard available at {}".format(client.get_dashboard()))
    else:
        ctx.obj["batch_size"] = 1

    ctx.obj["mongo_uri"] = mongo_uri


def main():
    cli_init.add_command(info)
    # cli_init.add_command(query)
    cli_init.add_command(export)
    cli_init.add_command(ingest)
    cli_init.add_command(meta_ingest)
    cli_init.add_command(meta_view)
    cli_init.add_command(meta_query)
    logger.info("{} started".format(__appname__.capitalize()))

    cli_init(obj={})


if __name__ == "__main__":
    main()
