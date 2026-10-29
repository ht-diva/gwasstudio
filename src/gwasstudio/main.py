import importlib
import sys

import click
import cloup

from gwasstudio import __appname__, __version__, context_settings, log_file, logger

# Lazy subcommand registry: name → (module, attribute, short help).
# Modules are imported only when a command is actually invoked, so lightweight
# commands (``list``, ``meta-query``, ``info``) don't pay the import cost of
# the Dask/TileDB/Pandas stack used by ``export`` and ``ingest``.
_LAZY_COMMANDS = {
    "export": (
        "gwasstudio.cli.export",
        "export",
        "Export summary statistics from TileDB datasets with various filtering options.",
    ),
    "info": ("gwasstudio.cli.info", "info", "Show GWASStudio details"),
    "ingest": ("gwasstudio.cli.ingest", "ingest", "Ingest data in a TileDB-unified dataset."),
    "list": (
        "gwasstudio.cli.list",
        "list_projects",
        "List every category → project → study hierarchy stored in the MongoDB.",
    ),
    "meta-query": (
        "gwasstudio.cli.metadata.query",
        "query_metadata",
        "Query metadata records from MongoDB using GWASStudio core.",
    ),
}


class _LazyCommandGroup(cloup.Group):
    """``cloup.Group`` whose subcommands are imported on first use.

    Placeholders (created by :func:`_placeholder`) carry the command name and
    help text for ``--help`` output; the real command object is imported the
    first time the command is resolved via :meth:`get_command`.
    """

    def get_command(self, ctx, cmd_name):
        cmd = super().get_command(ctx, cmd_name)
        if cmd is None:
            return None
        load = getattr(cmd, "_lazy_loader", None)
        if load is None:
            return cmd
        real = load()
        # Swap the placeholder(s) for the real command so later lookups (and
        # ``group.commands``) see the fully-built command.
        for name, registered in list(self.commands.items()):
            if registered is cmd:
                self.commands[name] = real
        return real


def _lazy_group(*args, **kwargs):
    """Drop-in replacement for ``cloup.group`` that builds a :class:`_LazyCommandGroup`.

    Mirrors :func:`click.command`'s decorator semantics: the option/argument
    decorators applied below the group decorator stow their parameters on
    ``f.__click_params__`` and they must be handed to the command class.
    """

    def decorator(f):
        params = kwargs.pop("params", None) or []
        decorator_params = getattr(f, "__click_params__", None)
        if decorator_params is not None:
            del f.__click_params__
            params.extend(reversed(decorator_params))
        if kwargs.get("help") is None:
            kwargs["help"] = f.__doc__
        cmd = _LazyCommandGroup(*args, callback=f, params=params, **kwargs)
        cmd.__doc__ = f.__doc__
        return cmd

    return decorator


def _placeholder(name: str, help_text: str, loader) -> click.Command:
    """A stand-in command that shows the right name and help until the real one is imported."""
    cmd = click.Command(name, help=help_text)
    cmd._lazy_loader = loader
    return cmd


def _register_lazy_commands(group):
    """Register all subcommands of ``group`` as lazy placeholders."""
    for name, (module, attr, help_text) in _LAZY_COMMANDS.items():

        def _load(module=module, attr=attr):
            return getattr(importlib.import_module(module), attr)

        group.add_command(_placeholder(name, help_text, _load))


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


@_lazy_group(
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
)
@cloup.option_group(
    "Dask cluster options",
    cloup.option("--gw-address", default=None, help="Dask gateway address (only for remote cluster config)"),
    cloup.option("--gw-image", default=None, help="Dask gateway image"),
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
    cloup.option(
        "--workers",
        default=2,
        help="Number of Dask workers to start (0 = run in-process without a Dask cluster)",
    ),
    cloup.option("--batch-size", default=0, help="Number of tasks per batch (0 for no batching)"),
)
@cloup.option_group(
    "MongoDB options",
    cloup.option("--mongo-uri", default=None, help="Specify a MongoDB uri if it is different from localhost"),
)
@cloup.option_group(
    "S3 options",
    cloup.option("--aws-access-key-id", default=None, help="S3 access key id"),
    cloup.option("--aws-secret-access-key", default=None, help="S3 access key"),
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
    gw_address,
    gw_image,
    workers,
    cores_per_worker,
    memory_per_worker,
    interface,
    walltime,
    job_script_prologue,
    python,
    local_directory,
    mongo_uri,
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
    ctx.obj["mongo"] = {"uri": mongo_uri}
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
        "gw_address": gw_address,
        "gw_image": gw_image,
        "walltime": walltime,
        "job_script_prologue": job_script_prologue,
        "python": python,
        "local_directory": local_directory,
    }


# Register the subcommands up-front as lightweight placeholders; the real
# command objects are imported only when a command is actually invoked.
_register_lazy_commands(cli_init)


def main():
    cli_init(obj={})


if __name__ == "__main__":
    main()
