import importlib.metadata
import os
from pathlib import Path

from cloup import Context, HelpFormatter, HelpTheme, Style
from loguru import logger as a_logger
from platformdirs import PlatformDirs

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

# Determine whether $HOME is writable
# if not, use /tmp
home_path = Path.home()
home_writable = os.access(home_path, os.W_OK)

if home_writable:
    dirs = PlatformDirs(__appname__)
else:
    tmp_base = Path("/tmp") / __appname__
    dirs = PlatformDirs(
        __appname__,
        roaming=False,
        ensure_exists=False
    )
    dirs.user_log_path = str(tmp_base / "log")
    dirs.user_config_path = str(tmp_base / "config")
    dirs.user_data_path = str(tmp_base / "data")

log_dir = Path(dirs.user_log_path)
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / "gwasstudio.log"

config_dir = Path(dirs.user_config_path)
config_dir.mkdir(parents=True, exist_ok=True)
config_filename = "config.yaml"

data_dir = Path(dirs.user_data_path)
data_dir.mkdir(parents=True, exist_ok=True)

mongo_db_path = data_dir / "mongo_db"
mongo_db_path.mkdir(parents=True, exist_ok=True)
mongo_db_logpath = log_dir / "mongod.log"

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
