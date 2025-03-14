from typing import Dict

from hvac import Client


def create_vault_client(vault_options: dict) -> Client:
    """
    Create a Vault client.

    Args:
        vault_options (dict): Vault options.

    Returns:
        Client: Vault client.
    """
    if not all([vault_options.get("path", False), vault_options.get("token", False), vault_options.get("url", False)]):
        return None

    client = Client(url=vault_options["url"])
    match vault_options["auth"]:
        case "basic":
            client.token = vault_options["token"]
        case "oidc":
            client.sys.enable_auth_method(
                method_type="oidc",
            )

    return client


def get_secret_from_vault(vault_client: Client, vault_path: str) -> dict:
    """
    Retrieve configuration dictionary from Vault.

    Args:
        vault_client (Client): Vault client.

    Returns:
        dict: secret dictionary.
    """

    read_response = vault_client.secrets.kv.read_secret_version(path=vault_path, raise_on_deleted_version=True)

    return read_response["data"]["data"]


def get_config_from_vault(vault_label: str, vault_options: Dict[str, str]) -> Dict:
    """Retrieve configuration data from HashiCorp Vault."""

    if not (vault_client := create_vault_client(vault_options)):
        return {}

    path = vault_options.get("path")
    config = get_secret_from_vault(vault_client, path)
    return config.get(vault_label, {})
