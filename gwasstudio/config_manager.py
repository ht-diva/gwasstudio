from importlib.resources import files
from pathlib import Path
from shutil import copyfile

from comoda.yaml import load

from gwasstudio import __appname__, config_dir, config_filename, logger


class ConfigurationManager:
    def __init__(self, **kwargs):
        def copy_config_file_from_package(dst):
            package_name = ".".join([__appname__, "config"])
            _from_package = files(package_name).joinpath(config_filename)
            copyfile(_from_package, dst)

        # Check if a custom config file is provided
        custom_config_file = kwargs.get("cf")
        if custom_config_file:
            configuration_file = Path(custom_config_file)
            if not configuration_file.exists():
                msg = f"{configuration_file} file not found. Please check the path"
                logger.error(msg)
                exit(msg)
        # If no custom config is provided, use the default one
        else:
            # Create configuration file from default if needed
            configuration_file = Path(config_dir, config_filename)
            if not configuration_file.exists():
                configuration_file.parent.mkdir(parents=True, exist_ok=True)
                logger.warning(
                    "Copying default config file from {} package " "resource to {}".format(
                        __appname__, configuration_file
                    )
                )
                copy_config_file_from_package(configuration_file)
                logger.warning(f"Configuration file has default values! Update them in {configuration_file}")

        logger.debug(f"Reading configuration from {configuration_file}")
        c = load(configuration_file)

        # Get database connection settings from kwargs, if not present, use config
        mdb_connection = c.get("mdbc", {})
        self.mdbc_db = kwargs.get("db", mdb_connection.get("db"))
        self.mdbc_uri = kwargs.get("uri", mdb_connection.get("uri"))

        self.data_category_list = c.get("data_category", [])
        self.project_list = c.get("project", [])
        self.ancestry_list = c.get("ancestry", [])
        self.build_list = c.get("build", [])

    @property
    def get_mdbc_db(self):
        return self.mdbc_db

    @property
    def get_mdbc_uri(self):
        return self.mdbc_uri

    @property
    def get_data_category_list(self):
        return self.data_category_list

    @property
    def get_project_list(self):
        return self.project_list

    @property
    def get_ancestry_list(self):
        return self.ancestry_list

    @property
    def get_build_list(self):
        return self.build_list
