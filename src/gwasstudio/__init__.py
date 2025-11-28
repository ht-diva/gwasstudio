import importlib.metadata
import tempfile
from pathlib import Path

import pandas as pd
from cloup import Context, HelpFormatter, HelpTheme, Style
from loguru import logger as a_logger
from platformdirs import user_config_dir, user_data_dir, user_log_dir

# https://pandas.pydata.org/pandas-docs/stable/user_guide/copy_on_write.html#
pd.options.mode.copy_on_write = True

__all__ = [
    "__appname__",
    "__version__",
    "context_settings",
    "config_dir",
    "config_filename",
    "data_dir",
    "log_file",
    "logger",
    "mongo_db_path",
    "mongo_db_logpath",
]

__appname__ = __name__

try:
    __version__ = importlib.metadata.version(__appname__)
except importlib.metadata.PackageNotFoundError:
    __version__ = "unknown"

default_log_dir = Path(user_log_dir(__appname__))
try:
    default_log_dir.mkdir(parents=True, exist_ok=True)
    test_file = default_log_dir / ".write_test"
    test_file.write_text("test")
    test_file.unlink()
    dir_writable = True
except Exception:
    dir_writable = False

if dir_writable:
    log_dir = Path(user_log_dir(__appname__))
    config_dir = Path(user_config_dir(__appname__))
    data_dir = Path(user_data_dir(__appname__)) / "data"
else:
    tmp_root = Path(tempfile.gettempdir())
    base_dir = tmp_root / f"{__appname__}_tmp"
    log_dir = base_dir / "log"
    config_dir = base_dir / "config"
    data_dir = base_dir / "data"

log_dir.mkdir(parents=True, exist_ok=True)
config_dir.mkdir(parents=True, exist_ok=True)
data_dir.mkdir(parents=True, exist_ok=True)

log_file = log_dir / "gwasstudio.log"
config_filename = "config.yaml"
mongo_db_path = data_dir / "mongo_db"
mongo_db_logpath = log_dir / "mongod.log"

mongo_db_path.mkdir(parents=True, exist_ok=True)

# Check the docs for all available arguments of HelpFormatter and HelpTheme.
formatter_settings = HelpFormatter.settings(
    theme=HelpTheme(
        invoked_command=Style(fg="red"),
        heading=Style(fg="bright_white", bold=True),
        constraint=Style(fg="magenta"),
        col1=Style(fg="yellow"),
    )
)

context_settings = Context.settings(formatter_settings=formatter_settings)

logger = a_logger
logger.remove()
