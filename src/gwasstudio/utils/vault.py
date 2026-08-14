"""
HashiCorp Vault integration for GWASStudio.

Uses VaultConfig dataclass from core.config for type-safe configuration.
"""

from hvac import Client
from hvac.exceptions import InvalidPath

from gwasstudio import logger
from gwasstudio.core.config import VaultConfig


def create_vault_client(vault_config: VaultConfig) -> Client | None:
    """
    Create a Vault client from VaultConfig.

    Args:
        vault_config: Vault configuration dataclass.

    Returns:
        hvac.Client or None if config is incomplete or connection fails.
    """
    if not all([vault_config.path, vault_config.token, vault_config.url]):
        logger.warning("Vault configuration incomplete: missing path, token, or url")
        return None

    client = Client(url=vault_config.url)

    # Validate the Vault URL is reachable
    try:
        # This will raise an exception if the URL is unreachable
        # or returns a non-Vault response (like a 403 HTML page)
        client.sys.read_health_status()
    except Exception as e:
        logger.error(f"Failed to connect to Vault at {vault_config.url}: {e}")
        return None

    try:
        match vault_config.auth:
            case "basic":
                client.token = vault_config.token
            case "oidc":
                client.sys.enable_auth_method(
                    method_type="oidc",
                )
    except Exception as e:
        logger.error(f"Failed to authenticate with Vault: {e}")
        return None

    return client


def get_secret_from_vault(vault_client: Client, vault_config: VaultConfig) -> dict:
    """
    Retrieve configuration dictionary from Vault.

    Args:
        vault_client: Connected Vault client.
        vault_config: Vault configuration with path and mount_point.

    Returns:
        dict: secret dictionary from Vault, or empty dict on error.
    """
    try:
        read_response = vault_client.secrets.kv.read_secret_version(
            path=vault_config.path,
            mount_point=vault_config.mount_point,
        )
        if not read_response:
            logger.warning(
                f"Vault returned empty response for path={vault_config.path}, mount_point={vault_config.mount_point}"
            )
            return {}

        if "data" not in read_response:
            logger.warning(
                f"Vault response missing 'data' key for path={vault_config.path}, mount_point={vault_config.mount_point}"
            )
            return {}

        data = read_response["data"]
        if "data" not in data:
            logger.warning(
                f"Vault response['data'] missing 'data' key for path={vault_config.path}, mount_point={vault_config.mount_point}"
            )
            return {}

        return data["data"]
    except InvalidPath as e:
        logger.error(f"Vault path not found: {vault_config.path} at mount_point={vault_config.mount_point}: {e}")
        return {}
    except Exception as e:
        logger.error(
            f"Failed to read secret from Vault at path={vault_config.path}, mount_point={vault_config.mount_point}: {e}"
        )
        return {}


def get_config_from_vault(
    vault_label: str,
    vault_config: VaultConfig,
) -> dict:
    """
    Retrieve configuration data from HashiCorp Vault.

    Args:
        vault_label: Key within the Vault secret to extract.
        vault_config: Complete Vault configuration dataclass.

    Returns:
        dict: Extracted configuration, or empty dict if unavailable.
    """
    vault_client = create_vault_client(vault_config)
    if not vault_client:
        logger.warning(f"Failed to create Vault client for {vault_config.url}")
        return {}

    config = get_secret_from_vault(vault_client, vault_config)

    if not config:
        logger.warning(
            f"No secret retrieved from Vault at path={vault_config.path}, mount_point={vault_config.mount_point}"
        )
        return {}

    if vault_label not in config:
        logger.warning(
            f"Key '{vault_label}' not found in Vault secret at path={vault_config.path}. Available keys: {list(config.keys())}"
        )
        return {}

    value = config.get(vault_label)
    if not value:
        logger.warning(
            f"Key '{vault_label}' exists but has empty/null value in Vault secret at path={vault_config.path}"
        )
        return {}

    return value
