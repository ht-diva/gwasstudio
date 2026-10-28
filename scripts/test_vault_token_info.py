#!/usr/bin/env python3
"""
Simple script to test Vault token entity information extraction.

This script validates that we can extract user identity information from a Vault token,
which is the foundation for the Vault-only token-based authorization strategy.

Usage:
    python scripts/test_vault_token_info.py --vault-url https://vault.example.com \
        --vault-token s.xxxxxxxxxxxx

Requirements:
    pip install hvac
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any

from hvac import Client
from hvac.exceptions import Forbidden, InvalidPath, Unauthorized


def create_vault_client(vault_url: str, vault_token: str) -> Client:
    """Create a Vault client with the given URL and token."""
    client = Client(url=vault_url)
    client.token = vault_token
    return client


def get_token_info(client: Client) -> dict[str, Any]:
    """Extract information from the Vault token."""
    try:
        # Get token details including accessor, display_name, etc.
        token_data = client.lookup_token()
        if not token_data or not token_data.get("data"):
            return {"error": "No token data returned"}

        return token_data["data"]
    except (Unauthorized, Forbidden) as e:
        return {"error": f"Token validation failed: {str(e)}"}
    except Exception as e:
        return {"error": f"Failed to get token info: {str(e)}"}


def get_entity_info(client: Client, entity_id: str | None) -> dict[str, Any]:
    """Extract entity information including groups."""
    if not entity_id:
        return {"groups": []}

    try:
        entity_data = client.sys.read_entity(entity_id=entity_id)
        if not entity_data or not entity_data.get("data"):
            return {"groups": []}

        data = entity_data["data"]
        groups_data = data.get("groups", {}).get("data", {})

        # Extract group names
        groups = []
        if isinstance(groups_data, dict):
            groups = list(groups_data.keys())

        return {
            "entity_id": entity_id,
            "groups": groups,
            "metadata": data.get("metadata", {}),
            "name": data.get("name"),
        }
    except (InvalidPath, Forbidden) as e:
        return {"error": f"Failed to get entity info: {str(e)}", "groups": []}
    except Exception as e:
        return {"error": f"Unexpected error getting entity info: {str(e)}", "groups": []}


def get_token_policies(client: Client) -> list[str]:
    """Get the list of policies associated with the token."""
    try:
        token_info = client.lookup_token()
        if not token_info or not token_info.get("data"):
            return []
        return token_info["data"].get("policies", [])
    except Exception:
        return []


def _epoch_seconds(value: Any) -> float | None:
    """Best-effort conversion of a value to epoch seconds (float), or None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def human_readable_timestamp(value: Any) -> str:
    """Format a Vault epoch timestamp as a human-readable UTC date-time."""
    epoch = _epoch_seconds(value)
    if epoch is None:
        return str(value) if value is not None else "N/A"
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


def human_readable_duration(seconds: Any) -> str:
    """Format a number of seconds as a compact human-readable duration."""
    s = _epoch_seconds(seconds)
    if s is None:
        return str(seconds) if seconds is not None else "N/A"
    if s <= 0:
        return "no TTL (infinite)"
    total = int(s)
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def format_output(info: dict[str, Any]) -> str:
    """Format the output for display."""
    lines = []

    # Basic token info
    lines.append("=" * 60)
    lines.append("VAULT TOKEN INFORMATION")
    lines.append("=" * 60)

    if "error" in info:
        lines.append(f"Error: {info['error']}")
        return "\n".join(lines)

    # Token section
    lines.append("\n[TOKEN]")
    lines.append(f"  Accessor:      {info.get('accessor', 'N/A')}")
    lines.append(f"  Display Name:  {info.get('display_name', 'N/A')}")
    lines.append(f"  Entity ID:     {info.get('entity_id', 'N/A')}")
    lines.append(f"  Token Type:    {info.get('token_type', 'N/A')}")

    # Human-readable dates / durations
    created_epoch = _epoch_seconds(info.get("creation_time"))
    lines.append(f"  Created:       {human_readable_timestamp(info.get('creation_time'))}")
    creation_ttl = _epoch_seconds(info.get("creation_ttl"))
    if created_epoch is not None and creation_ttl and creation_ttl > 0:
        lines.append(f"  Expires:       {human_readable_timestamp(created_epoch + creation_ttl)}")
    lines.append(f"  TTL (left):    {human_readable_duration(info.get('ttl'))}")

    # Policies
    policies = info.get("policies", [])
    lines.append(f"\n[POLICIES] ({len(policies)} total)")
    for policy in policies:
        lines.append(f"  - {policy}")

    # Entity section
    entity = info.get("entity", {})
    if entity:
        lines.append("\n[ENTITY]")
        lines.append(f"  Entity ID:     {entity.get('entity_id', 'N/A')}")
        lines.append(f"  Name:          {entity.get('name', 'N/A')}")
        lines.append(f"  Metadata:      {json.dumps(entity.get('metadata', {}), indent=2)}")

        groups = entity.get("groups", [])
        lines.append(f"\n  Groups ({len(groups)} total):")
        for group in groups:
            lines.append(f"    - {group}")
    else:
        lines.append("\n[ENTITY]")
        lines.append("  No entity information available")

    lines.append("\n" + "=" * 60)

    return "\n".join(lines)


def get_secret_from_vault(vault_client: Client, vault_path: str, vault_mount_point: str) -> dict:
    """
    Retrieve configuration dictionary from Vault.

    Args:
        vault_client: Connected Vault client.
        vault_config: Vault configuration with path and mount_point.

    Returns:
        dict: secret dictionary from Vault.
    """
    try:
        read_response = vault_client.secrets.kv.read_secret_version(
            path=vault_path,
            mount_point=vault_mount_point,
        )
        if not read_response:
            print(
                f"[DEBUG] Vault response is None/empty for path={vault_path}, mount_point={vault_mount_point}",
                file=sys.stderr,
            )
            return {}

        if "data" not in read_response:
            print(f"[DEBUG] Vault response missing 'data' key. Full response: {read_response}", file=sys.stderr)
            return {}

        data = read_response["data"]
        if "data" not in data:
            print(f"[DEBUG] Vault response['data'] missing 'data' key. Full response: {read_response}", file=sys.stderr)
            return {}

        return data["data"]
    except Exception as e:
        print(f"[DEBUG] Exception reading secret from Vault: {type(e).__name__}: {e}", file=sys.stderr)
        return {}


def get_config_from_vault(vault_client: Client, vault_label: str, vault_path: str, vault_mount_point: str) -> dict:
    """
    Retrieve configuration data from HashiCorp Vault.

    Args:
        vault_label: Key within the Vault secret to extract.
        vault_config: Complete Vault configuration dataclass.

    Returns:
        dict: Extracted configuration, or empty dict if unavailable.
    """
    config = get_secret_from_vault(vault_client, vault_path, vault_mount_point)

    if not config:
        print(f"[DEBUG] Secret is empty for path={vault_path}, mount_point={vault_mount_point}", file=sys.stderr)
        return {}

    print(f"[DEBUG] Retrieved secret keys: {list(config.keys())}", file=sys.stderr)

    if vault_label not in config:
        print(
            f"[DEBUG] Key '{vault_label}' not found in secret. Available keys: {list(config.keys())}", file=sys.stderr
        )
        return {}

    value = config.get(vault_label)
    print(f"[DEBUG] Found '{vault_label}' = {value}", file=sys.stderr)

    return value if value else {}


def main():
    parser = argparse.ArgumentParser(description="Test Vault token entity information extraction")
    parser.add_argument(
        "--vault-url",
        required=True,
        help="Vault server URL (e.g., https://vault.example.com:8200)",
    )
    parser.add_argument(
        "--vault-token",
        required=True,
        help="Vault token (s.xxxxxxxxxxxx)",
    )
    parser.add_argument(
        "--vault-mount-point",
        required=True,
    )
    parser.add_argument(
        "--vault-path",
        required=True,
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output in JSON format",
    )
    parser.add_argument(
        "--only-groups",
        action="store_true",
        help="Output only the groups list",
    )

    args = parser.parse_args()

    # Create Vault client
    try:
        client = create_vault_client(args.vault_url, args.vault_token)
    except Exception as e:
        print(f"Error creating Vault client: {e}", file=sys.stderr)
        sys.exit(1)

    # Get token info
    token_info = get_token_info(client)
    if "error" in token_info:
        print(f"Error: {token_info['error']}", file=sys.stderr)
        sys.exit(1)

    # Get entity info
    entity_id = token_info.get("entity_id")
    entity_info = get_entity_info(client, entity_id)

    # Get policies
    policies = get_token_policies(client)

    # Combine all information
    info = {
        **token_info,
        "policies": policies,
        "entity": entity_info,
    }

    # Output
    if args.json:
        print(json.dumps(info, indent=2))
    elif args.only_groups:
        groups = info.get("entity", {}).get("groups", [])
        print("\n".join(groups))
    else:
        print(format_output(info))

    print("\n[DEBUG] Attempting to retrieve mongo_uri from Vault...", file=sys.stderr)
    label = get_config_from_vault(client, "mongo", args.vault_path, args.vault_mount_point)


if __name__ == "__main__":
    main()
