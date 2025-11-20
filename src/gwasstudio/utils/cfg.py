from typing import Dict

from gwasstudio import logger
from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.utils.vault import get_config_from_vault


def get_mongo_deployment(ctx: object) -> str:
    """Retrieve MongoDB deployment from command line options."""
    mongo_deployment = ctx.obj.get("mongo").get("deployment")
    return mongo_deployment


def get_mongo_uri(ctx: object) -> str:
    """Retrieve MongoDB URI from Vault or command line options."""

    vault_options = ctx.obj.get("vault")
    mongo_config = get_config_from_vault("mongo", vault_options)

    return mongo_config.get("uri") or ctx.obj.get("mongo").get("uri")


def get_tiledb_config(ctx: object) -> Dict[str, str]:
    """
    Retrieve the combined TileDB configuration from VFS and SM configurations.

    Args:
        ctx (object): The context object containing configuration options.

    Returns:
        Dict[str, str]: The combined TileDB configuration.
    """
    try:
        vfs_config = get_tiledb_vfs_config(ctx)
        sm_config = get_tiledb_sm_config()
        return vfs_config | sm_config
    except Exception as e:
        logger.error(f"Failed to retrieve TileDB configuration: {e}")
        return {}


def get_tiledb_vfs_config(ctx: object) -> Dict[str, str]:
    """
    Retrieve TileDB VFS configuration from Vault or command line options.

    Args:
        ctx (object): The context object containing configuration options.

    Returns:
        Dict[str, str]: The TileDB VFS configuration.
    """
    try:
        vault_options = ctx.obj.get("vault")
        tiledb_cfg = get_config_from_vault("tiledb", vault_options)
        return tiledb_cfg or ctx.obj.get("tiledb", {})
    except Exception as e:
        logger.error(f"Failed to retrieve TileDB VFS configuration: {e}")
        return {}


def get_tiledb_sm_config() -> Dict[str, str]:
    """
    Retrieve TileDB SM configuration from the configuration file.

    Returns:
        Dict[str, str]: The TileDB SM configuration.
    """
    try:
        cm = ConfigurationManager()
        return cm.tiledb_sm_config
    except Exception as e:
        logger.error(f"Failed to retrieve TileDB SM configuration: {e}")
        return {}


def get_dask_config(ctx: object):
    """Retrieve Dask configuration from command line options."""
    return ctx.obj.get("dask")


def get_dask_batch_size(ctx: object, capacity_mode: bool = False) -> int:
    workers = get_dask_config(ctx).get("workers")
    cores_per_worker = get_dask_config(ctx).get("cores_per_worker")
    # When capacity_mode is True we return total worker capacity,
    # otherwise we fall back to the configured batch size.
    return workers * cores_per_worker if capacity_mode else get_dask_config(ctx).get("batch_size")


def get_dask_deployment(ctx: object) -> str:
    return get_dask_config(ctx).get("deployment")
