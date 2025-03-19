from typing import Dict

from gwasstudio.utils.vault import get_config_from_vault


def get_mongo_uri(ctx: object) -> str:
    """Retrieve MongoDB URI from Vault or command line options."""

    vault_options = ctx.obj.get("vault")
    mongo_config = get_config_from_vault("mongo", vault_options)

    return mongo_config.get("uri") or ctx.obj.get("mongo").get("uri")


def get_tiledb_config(ctx: object) -> Dict[str, str]:
    """Retrieve TileDB configuration from Vault or command line options."""

    vault_options = ctx.obj.get("vault")
    tiledb_cfg = get_config_from_vault("tiledb", vault_options)

    return tiledb_cfg or ctx.obj.get("tiledb")
