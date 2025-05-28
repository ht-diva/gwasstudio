import sys

import click
import cloup

from gwasstudio import __appname__, __version__, context_settings, log_file, logger
from gwasstudio.cli.export import export
from gwasstudio.cli.info import info
from gwasstudio.cli.ingest import ingest
from gwasstudio.cli.metadata.query import meta_query
from gwasstudio.utils.mongo_manager import mongo_deployment_types


def configure_logging(stdout, verbosity, _logger):
    """
    Configure logging behavior based on stdout flag and verbosity level.

    Args:
        stdout (bool): Flag indicating whether to log to stdout or not.
        verbosity (str): Level of verbosity, can be 'quiet', 'normal' or 'loud'.
        _logger: Logger instance to configure.

    Returns:
        None

    Notes:
        This function configures the logging behavior based on the provided parameters.
        It sets the log level and output target accordingly. If stdout is True,
        logs are written to stdout, otherwise they are written to a file at `log_file`.
        The verbosity parameter determines the log level as follows:
            - 'quiet': Log level set to ERROR
            - 'normal': Log level set to INFO
            - 'loud': Log level set to DEBUG

    """
    target = sys.stdout if stdout else log_file
    loglevel = {"quiet": "ERROR", "normal": "INFO", "loud": "DEBUG"}.get(verbosity, "INFO")

    kwargs = {"level": loglevel}
    if target == sys.stdout:
        fmt = "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <yellow>{level: <8}</yellow> | <level>{message}</level>"
        kwargs["format"] = fmt
    else:
        kwargs["retention"] = "30 days"
    _logger.add(target, **kwargs)


@cloup.group(
    name="main",
    help="GWASStudio",
    no_args_is_help=True,
    context_settings=context_settings,
)
@click.version_option(version=__version__)
@cloup.option("--verbosity", type=click.Choice(["quiet", "normal", "loud"]), default="normal", help="Set log verbosity")
@cloup.option("--stdout", is_flag=True, default=False, help="Print logs to the stdout")
@cloup.option_group(
    "Deployment options",
    cloup.option(
        "--dask-deployment",
        type=click.Choice(["local", "gateway", "slurm", "none"]),
        default="local",
        help="Specify the deployment environment for the Dask cluster",
    ),
    cloup.option(
        "--mongo-deployment",
        type=click.Choice(mongo_deployment_types),
        default="embedded",
        help="Specify the deployment environment for the MongoDB server",
    ),
)
@cloup.option_group(
    "Dask remote cluster options - SLurm HPC or a Dask gateway setup",
    cloup.option("--minimum-workers", default=10, help="Minimum amount of running workers"),
    cloup.option("--walltime", default="72:00:00", help="Walltime for each worker"),
    cloup.option("--maximum-workers", default=100, help="Maximum amount of running workers"),
    cloup.option("--memory-workers", default="12GB", help="Memory amount per worker"),
    cloup.option("--cpu-workers", help="CPU numbers per worker", default=1),
    cloup.option("--address", default=None, help="Dask gateway address"),
)
@cloup.option_group(
    "Dask local cluster options",
    cloup.option("--local-workers", default=2, help="Number of workers for local cluster"),
    cloup.option("--local-threads", default=2, help="Threads per worker for local cluster"),
    cloup.option("--local-memory", default="4GB", help="Memory per worker for local cluster"),
)
@cloup.option_group(
    "MongoDB options",
    cloup.option("--mongo-uri", default=None, help="Specify a MongoDB uri if it is different from localhost"),
)
@cloup.option_group(
    "S3 options",
    cloup.option("--aws-access-key-id", default="None", help="S3 access key id"),
    cloup.option("--aws-secret-access-key", default="None", help="S3 access key"),
    cloup.option(
        "--aws-endpoint-override",
        default="https://storage.fht.org:9021",
        help="S3 endpoint where to connect",
    ),
    cloup.option("--aws-use-virtual-addressing", default="false", help="S3 use virtual address option"),
    cloup.option("--aws-scheme", default="https", help="type of scheme used at the S3 endpoint"),
    cloup.option("--aws-region", default="", help="region where the S3 bucket is located"),
    cloup.option("--aws-verify-ssl", default="false", help="enable SSL verification"),
)
@cloup.option_group(
    "Vault options",
    cloup.option(
        "--vault-auth", type=click.Choice(["basic", "oidc"]), default="basic", help="Vault authentication mechanism"
    ),
    cloup.option("--vault-mount-point", default="secret", help="The path the secret engine was mounted on."),
    cloup.option("--vault-path", default=None, help="Vault path to access"),
    cloup.option("--vault-token", default=None, help="Access token for the vault"),
    cloup.option("--vault-url", default=None, help="Vault server URL"),
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
    dask_deployment,
    walltime,
    minimum_workers,
    maximum_workers,
    memory_workers,
    local_workers,
    local_threads,
    local_memory,
    cpu_workers,
    address,
    mongo_uri,
    mongo_deployment,
    verbosity,
    stdout,
    vault_auth,
    vault_mount_point,
    vault_path,
    vault_token,
    vault_url,
):
    configure_logging(stdout, verbosity, logger)
    logger.info("{} started".format(__appname__.capitalize()))

    ctx.ensure_object(dict)
    ctx.obj["mongo"] = {"uri": mongo_uri, "deployment": mongo_deployment}

    ctx.obj["vault"] = {
        "auth": vault_auth,
        "mount_point": vault_mount_point,
        "path": vault_path,
        "token": vault_token,
        "url": vault_url,
    }

    ctx.obj["tiledb"] = {
        "vfs.s3.aws_access_key_id": aws_access_key_id,
        "vfs.s3.aws_secret_access_key": aws_secret_access_key,
        "vfs.s3.endpoint_override": aws_endpoint_override,
        "vfs.s3.use_virtual_addressing": aws_use_virtual_addressing,
        "vfs.s3.scheme": aws_scheme,
        "vfs.s3.region": aws_region,
        "vfs.s3.verify_ssl": aws_verify_ssl,
        "sm.dedup_coords": "true",
    }

    batch_sizes = {"gateway": minimum_workers, "slurm": maximum_workers - minimum_workers, "local": local_workers}
    ctx.obj["dask"] = {
        "deployment": dask_deployment,
        "batch_size": batch_sizes.get(dask_deployment, None),
        "minimum_workers": minimum_workers,
        "maximum_workers": maximum_workers,
        "memory_workers": memory_workers,
        "cpu_workers": cpu_workers,
        "address": address,
        "local_workers": local_workers,
        "local_threads": local_threads,
        "local_memory": local_memory,
        "walltime": walltime,
    }


def main():
    cli_init.add_command(info)
    cli_init.add_command(export)
    cli_init.add_command(ingest)
    cli_init.add_command(meta_query)

    cli_init(obj={})


if __name__ == "__main__":
    main()
