"""
HashiCorp Vault integration for GWASStudio.

Uses VaultConfig dataclass from core.config for type-safe configuration.
"""

from hvac import Client

from gwasstudio.core.config import VaultConfig


def create_vault_client(vault_config: VaultConfig) -> Client | None:
    """
    Create a Vault client from VaultConfig.

    Args:
        vault_config: Vault configuration dataclass.

    Returns:
        hvac.Client or None if config is incomplete.
    """
    if not all([vault_config.path, vault_config.token, vault_config.url]):
        return None

    client = Client(url=vault_config.url)
    match vault_config.auth:
        case "basic":
            client.token = vault_config.token
        case "oidc":
            client.sys.enable_auth_method(
                method_type="oidc",
            )

    return client


def get_secret_from_vault(vault_client: Client, vault_config: VaultConfig) -> dict:
    """
    Retrieve configuration dictionary from Vault.

    Args:
        vault_client: Connected Vault client.
        vault_config: Vault configuration with path and mount_point.

    Returns:
        dict: secret dictionary from Vault.
    """
    read_response = vault_client.secrets.kv.read_secret_version(
        path=vault_config.path,
        mount_point=vault_config.mount_point,
    )
    return read_response["data"]["data"]


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
    if not (vault_client := create_vault_client(vault_config)):
        return {}

    config = get_secret_from_vault(vault_client, vault_config)
    return config.get(vault_label, {})
