import click
import cloup

from gwasstudio import __appname__, __version__, config_dir, data_dir, log_dir

help_doc = """
Show GWASStudio details
"""


@cloup.command("info", no_args_is_help=False, help=help_doc)
def info():
    click.echo("{}, version {}\n".format(__appname__.capitalize(), __version__))

    paths = {"config dir": config_dir, "data dir": data_dir, "log dir": log_dir}
    click.echo("Paths: ")
    for k, v in paths.items():
        click.echo("  {}: {}".format(k, v))
    click.echo("\n")
