import click
import cloup

from gwasstudio import __appname__, __version__, context_settings, log_file, logger
from gwasstudio.cli.export import export
from gwasstudio.cli.info import info
from gwasstudio.cli.ingest import ingest
from gwasstudio.cli.metadata.ingest import meta_ingest
from gwasstudio.cli.metadata.query import meta_query
from gwasstudio.cli.metadata.view import meta_view
from gwasstudio.dask_client import DaskCluster as Cluster


@cloup.group(
    name="main",
    help="GWASStudio",
    no_args_is_help=True,
    context_settings=context_settings,
)
@click.version_option(version=__version__)
@cloup.option("-q", "--quiet", default=False, is_flag=True, help="Set log verbosity")
@cloup.option_group(
    "Dask options HPC or Gateway",
    cloup.option(
        "--dask-distribute",
        default=False,
        is_flag=True,
        help="Distribute the load to a Dask cluster on a HPC that use SLURM",
    ),
    cloup.option("--minimum_workers", default=10, help="Minimum amount of running workers"),
    cloup.option("--maximum_workers", default=100, help="Maximum amount of running workers"),
    cloup.option("--memory_workers", default="12GB", help="Memory amount per worker"),
    cloup.option("--cpu_workers", help="CPU numbers per worker", default=6),
    cloup.option("--address", help="address in case a gateway is available", default=1),
)
@cloup.option_group(
    "Dask options Local",
    cloup.option("--local_workers", default=4, help="Number of workers for local cluster"),
    cloup.option("--local_threads", default=1, help="Threads per worker for local cluster"),
    cloup.option("--local_memory", default="2GB", help="Memory per worker for local cluster"),
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
    dask_distribute,
    minimum_workers,
    maximum_workers,
    memory_workers,
    local_workers,
    local_threads,
    local_memory,
    cpu_workers,
    address,
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
    ctx.obj["mongo_uri"] = mongo_uri

    cluster = Cluster(
        minimum_workers=minimum_workers,
        maximum_workers=maximum_workers,
        memory_workers=memory_workers,
        cpu_workers=cpu_workers,
        address=address,
        local_workers=local_workers,
        local_threads=local_threads,
        local_memory=local_memory,
    )
    client = cluster.get_client()
    type_cluster = cluster.get_type_cluster()
    if dask_distribute:
        ctx.obj["batch_size"] = minimum_workers
    else:
        ctx.obj["batch_size"] = local_workers

    ctx.obj["client"] = client
    ctx.obj["type_cluster"] = type_cluster
    ctx.call_on_close(cluster.shutdown)


def main():
    cli_init.add_command(info)
    cli_init.add_command(export)
    cli_init.add_command(ingest)
    cli_init.add_command(meta_ingest)
    cli_init.add_command(meta_view)
    cli_init.add_command(meta_query)
    logger.info("{} started".format(__appname__.capitalize()))

    cli_init(obj={})


if __name__ == "__main__":
    main()
