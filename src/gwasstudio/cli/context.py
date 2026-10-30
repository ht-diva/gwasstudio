"""
GWASStudio CLI context helpers
=====================================================

Lightweight configuration helpers shared by CLI commands.

This module deliberately avoids heavy third-party imports (pandas, numpy,
TileDB, Dask, boto3) so that commands which only need a ``GWASStudioConfig``
(e.g. ``list``, ``meta-query``, ``info``) keep a small import footprint.
The heavyweight data-processing helpers live in :mod:`gwasstudio.cli.utils`
and :mod:`gwasstudio.cli.table_io`.
"""

import urllib.parse
from pathlib import Path
from typing import Any

import yaml

from gwasstudio import logger
from gwasstudio.core.config import (
    DaskConfig,
    GWASStudioConfig,
    MongoConfig,
    S3Config,
    TileDBConfig,
    VaultConfig,
)
from gwasstudio.core.exceptions import InvalidInputError


def mongo_conn_info(config: "GWASStudioConfig") -> tuple[str | None, str | None]:
    """
    Get MongoDB connection URI and database name from GWASStudioConfig or Vault.

    Priority:
    1. Direct URI from config.mongo.uri
    2. URI from Vault (if configured)

    Args:
        config: GWASStudio configuration object.

    Returns:
        tuple[str | None, str | None]: (connection_uri, database_name)
    """
    # Try direct URI first
    if uri := getattr(config.mongo, "uri", None):
        _, _, db_name = parse_uri(uri)
        return uri, db_name.replace("/", "")

    # Fall back to Vault if configured
    vault = getattr(config, "vault", None)
    if vault and all(getattr(vault, attr) for attr in ("path", "token", "url")):
        try:
            from hvac.exceptions import Forbidden, VaultError

            from gwasstudio.utils.vault import get_config_from_vault

            mongo_config = get_config_from_vault("mongo", vault)
            if uri := mongo_config.get("uri"):
                _, _, db_name = parse_uri(uri)
                return uri, db_name.replace("/", "")
        except ImportError as e:
            logger.opt(exception=True).error("Failed to import Vault client library - please ensure hvac is installed")
            raise
        except Forbidden as e:
            logger.opt(exception=True).error(
                "Vault access forbidden - check your Vault token and permissions. "
                "Verify the token has access to path '{}'".format(getattr(vault, "path", "unknown"))
            )
            raise
        except VaultError as e:
            logger.opt(exception=True).error("Vault operation failed - check Vault server status and configuration")
            raise
        except Exception as e:
            logger.opt(exception=True).error("Unexpected error while retrieving MongoDB URI from Vault")
            raise

    return None, None


def _resolve_s3_config(ctx) -> dict:
    """Resolve the effective TileDB/S3 (``vfs.s3.*``) configuration.

    The ``tiledb`` section of the Vault secret is a complete, coherent S3
    config (endpoint, credentials, region, scheme, …) and should be used as the
    default — exactly like the MongoDB URI is pulled from Vault by
    :func:`mongo_conn_info`. Explicit ``--aws-*`` CLI flags (i.e. any value the
    user changed from its default) override the corresponding Vault key.

    Precedence:
      1. Explicit CLI ``--aws-*`` value (differs from the CLI default) — wins.
      2. The ``tiledb`` section of the Vault secret (when Vault is configured).
      3. The CLI default.

    Returns a plain dict keyed by ``vfs.s3.*`` (suitable for TileDB's VFS config).
    Unset values are omitted so TileDB / boto3 fall back to standard ``AWS_*``
    environment variables instead of being handed the literal string ``"None"``.
    """
    cli_s3 = dict(ctx.obj.get("tiledb", {}))

    # CLI defaults as declared in ``main.py``; a value equal to its default is
    # treated as "not provided by the user".
    defaults = {
        "vfs.s3.aws_access_key_id": None,
        "vfs.s3.aws_secret_access_key": None,
        "vfs.s3.endpoint_override": None,
        "vfs.s3.use_virtual_addressing": "false",
        "vfs.s3.scheme": "https",
        "vfs.s3.region": "",
        "vfs.s3.verify_ssl": "false",
    }
    explicit = {k: v for k, v in cli_s3.items() if v != defaults.get(k)}

    resolved = cli_s3

    # If the user did not explicitly supply the core S3 identity, pull the full
    # ``tiledb`` section from Vault (mirrors how the MongoDB URI is resolved).
    if not (explicit.get("vfs.s3.aws_access_key_id") and explicit.get("vfs.s3.endpoint_override")):
        vault = ctx.obj.get("vault", {})
        if all([vault.get("path"), vault.get("token"), vault.get("url")]):
            try:
                from gwasstudio.utils.vault import get_config_from_vault

                vault_s3 = get_config_from_vault("tiledb", VaultConfig(**vault)) or {}
            except Exception as e:  # noqa: BLE001 - never let optional Vault S3 config break the run
                logger.warning(f"Failed to load 'tiledb' config from Vault: {e}")
                vault_s3 = {}
            if vault_s3:
                # Vault is the base; any explicitly-set CLI flag overrides that key.
                resolved = {**vault_s3, **explicit}

    # Omit unset values so downstream libraries fall back to AWS_* env vars.
    return {k: v for k, v in resolved.items() if v is not None}


def create_config_from_context(ctx) -> GWASStudioConfig:
    """
    Create a GWASStudioConfig object from the Click context.

    Extracts configuration from the Click context (ctx.obj) and creates a
    standardized GWASStudioConfig object for use by core modules.

    Args:
        ctx: Click context object.

    Returns:
        GWASStudioConfig: Configuration object for GWASStudio core.
    """
    # Extract configuration from context
    mongo_config = ctx.obj.get("mongo", {})
    dask_config = ctx.obj.get("dask", {})
    s3_config = _resolve_s3_config(ctx)
    vault_config = ctx.obj.get("vault", {})

    # Create GWASStudioConfig with extracted values
    config = GWASStudioConfig(
        dask=DaskConfig(
            deployment=dask_config.get("deployment", "local"),
            workers=dask_config.get("workers", 2),
            cores_per_worker=dask_config.get("cores_per_worker", 2),
            memory_per_worker=dask_config.get("memory_per_worker", "4GiB"),
            walltime=dask_config.get("walltime", "12:00:00"),
            batch_size=dask_config.get("batch_size", 0),
            interface=dask_config.get("interface"),
            gw_address=dask_config.get("gw_address"),
            gw_image=dask_config.get("gw_image"),
            job_script_prologue=dask_config.get("job_script_prologue", []),
            python=dask_config.get("python"),
            local_directory=Path(dask_config.get("local_directory")) if dask_config.get("local_directory") else None,
        ),
        mongo=MongoConfig(uri=mongo_config.get("uri"), db_name="gwasstudio"),
        s3=S3Config(
            aws_access_key_id=s3_config.get("vfs.s3.aws_access_key_id"),
            aws_secret_access_key=s3_config.get("vfs.s3.aws_secret_access_key"),
            endpoint_override=s3_config.get("vfs.s3.endpoint_override"),
            use_virtual_addressing=s3_config.get("vfs.s3.use_virtual_addressing", False),
            scheme=s3_config.get("vfs.s3.scheme", "https"),
            region=s3_config.get("vfs.s3.region", ""),
            verify_ssl=s3_config.get("vfs.s3.verify_ssl", False),
            connect_timeout_ms=s3_config.get("vfs.s3.connect_timeout_ms", 30000),
            request_timeout_ms=s3_config.get("vfs.s3.request_timeout_ms", 300000),
        ),
        vault=VaultConfig(
            auth=vault_config.get("auth", "basic"),
            mount_point=vault_config.get("mount_point", "secret"),
            path=vault_config.get("path"),
            token=vault_config.get("token"),
            url=vault_config.get("url"),
        ),
        tiledb=TileDBConfig(
            vfs_config=s3_config,  # Reuse S3 config for TileDB,
            # TileDB configuration
            # https://cloud.tiledb.com/academy/structure/arrays/tutorials/basics/configuration/index.html
            sm_config={"sm.dedup_coords": "false"},
        ),
    )

    # Update mongo config with resolved connection info
    if (uri := mongo_conn_info(config)[0]) is not None:
        config.mongo.uri = uri
        config.mongo.db_name = mongo_conn_info(config)[1]

    return config


def parse_uri(uri: str) -> tuple[str, str, str]:
    try:
        parsed = urllib.parse.urlparse(uri)
        scheme, netloc, path = parsed.scheme, parsed.netloc, parsed.path
        if scheme in ["s3", "https"]:
            path = path.strip("/")
        return scheme, netloc, path
    except ValueError as e:
        raise ValueError(f"Invalid URI: {uri}") from e


def load_yaml_file(file_path: str) -> dict[str, Any]:
    """
    Load and parse a YAML file.

    Args:
        file_path: Path to the YAML file.

    Returns:
        Dict[str, Any]: Parsed YAML content.

    Raises:
        InvalidInputError: If the file doesn't exist or is invalid YAML.
    """
    try:
        logger.info(f"Processing {file_path}")
        with open(file_path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise InvalidInputError(f"Input file not found: {file_path}")
    except yaml.YAMLError as e:
        raise InvalidInputError(f"Invalid YAML in input file: {str(e)}")
