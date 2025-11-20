import sys

import click
import cloup

from gwasstudio import __appname__, __version__, context_settings, log_file, logger
from gwasstudio.cli import list_projects, info, ingest, export, query_metadata
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
        type=click.Choice(["local", "gateway", "slurm"]),
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
    "Dask cluster options",
    cloup.option("--address", default=None, help="Dask gateway address (only for remote cluster config)"),
    cloup.option("--image", default=None, help="Dask gateway image"),
    cloup.option("--cores-per-worker", default=2, help="CPU cores per worker"),
    cloup.option("--job-script-prologue", default=[], help="Commands to add to script before launching worker."),
    cloup.option(
        "--interface", default=None, help="Specify the high-performance network interface if available (e.g. ib0)"
    ),
    cloup.option(
        "--local-directory", default=None, help="Fast local directory for Dask workers. Usually /scratch or $TMPDIR"
    ),
    cloup.option("--memory-per-worker", default="4GiB", help="Memory per worker (e.g. 36GiB)"),
    cloup.option("--python", default=None, help="Python executable used to launch Dask workers."),
    cloup.option("--walltime", default="12:00:00", help="Walltime for each worker (only for remote cluster config)"),
    cloup.option("--workers", default=2, help="Number of Dask workers to start"),
    cloup.option("--batch-size", default=0, help="Number of tasks per batch (0 for no batching)"),
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
        default=None,
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
    batch_size,
    address,
    image,
    workers,
    cores_per_worker,
    memory_per_worker,
    interface,
    walltime,
    job_script_prologue,
    python,
    local_directory,
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
        "vfs.s3.connect_timeout_ms": 30000,  # 30 seconds
        "vfs.s3.request_timeout_ms": 300000,  # 5 minutes
    }

    ctx.obj["dask"] = {
        "deployment": dask_deployment,
        "batch_size": batch_size,  # s.get(dask_deployment, None),
        "workers": workers,
        "cores_per_worker": cores_per_worker,
        "memory_per_worker": memory_per_worker,
        "interface": interface,
        "address": address,
        "image": image,
        "walltime": walltime,
        "job_script_prologue": job_script_prologue,
        "python": python,
        "local_directory": local_directory,
    }


def main():
    cli_init.add_command(info)
    cli_init.add_command(export)
    cli_init.add_command(ingest)
    cli_init.add_command(query_metadata)
    cli_init.add_command(list_projects)

    cli_init(obj={})


if __name__ == "__main__":
    main()
