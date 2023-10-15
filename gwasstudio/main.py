import click
import cloup

from gwasstudio import __appname__, __version__, context_settings, log_file, logger
from gwasstudio.cli.export import export
from gwasstudio.dask_client import DaskClient as Client


@cloup.group(name="main", help="GWASStudio", no_args_is_help=True, context_settings=context_settings)
@click.version_option(version=__version__)
@cloup.option("-q", "--quiet", default=False, is_flag=True, help="Set log verbosity")
@cloup.option_group(
    "Dask options",
    cloup.option("--distribute", default=False, is_flag=True, help="Distribute the load to a Dask cluster"),
    cloup.option("--minimum_workers", help="Minimum amount of running workers", default=10),
    cloup.option("--maximum_workers", help="Maximum amount of running workers", default=100),
)
@click.pass_context
def cli_init(ctx, distribute, minimum_workers, maximum_workers, quiet):
    if quiet:
        logger.add(log_file, level="INFO", retention="30 days")
    else:
        logger.add(log_file, level="DEBUG", retention="30 days")
    logger.info("{} started".format(__appname__.capitalize()))
    if distribute:
        client = Client(minimum_workers=minimum_workers, maximum_workers=maximum_workers)
        logger.info("Dask dashboard available at {}".format(client.get_dashboard()))
    ctx.ensure_object(dict)
    ctx.obj["DISTRIBUTE"] = distribute


def main():
    cli_init.add_command(export)
    cli_init(obj={})


if __name__ == "__main__":
    main()
