import cloup
from gwasstudio import __appname__, __version__, config_dir, log_file

help_doc = """
Show GWASStudio details
"""


@cloup.command("info", no_args_is_help=False, help=help_doc)
def info():
    print("{}, version {}\n".format(__appname__.capitalize(), __version__))

    paths = {"config dir": config_dir, "log file": log_file}
    print("Paths: ")
    for k, v in paths.items():
        print("  {}: {}".format(k, v))
    print("\n")
