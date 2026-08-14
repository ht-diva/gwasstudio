import unittest
from unittest.mock import Mock, patch

from hvac import Client
from hvac.exceptions import InvalidPath

from gwasstudio.core.config import VaultConfig
from gwasstudio.utils.vault import create_vault_client, get_config_from_vault, get_secret_from_vault


class TestVaultFunctions(unittest.TestCase):
    def _make_vault_config(self, **overrides) -> VaultConfig:
        base = VaultConfig(
            auth=overrides.pop("auth", "basic"),
            mount_point=overrides.pop("mount_point", "secret"),
            path=overrides.pop("path", "path"),
            token=overrides.pop("token", "token"),
            url=overrides.pop("url", "url"),
        )
        # Override any subset
        for k, v in overrides.items():
            if k == "mount_point":
                continue  # already set
            setattr(base, k, v)
        return base

    def test_create_vault_client(self):
        """Test that a Vault client is created successfully when all required options are provided."""
        vault_config = self._make_vault_config(auth="basic")
        with patch.object(Client, "sys") as mock_sys:
            mock_sys.read_health_status.return_value = {"status": "ok"}
            client = create_vault_client(vault_config)
            self.assertIsInstance(client, Client)
            mock_sys.read_health_status.assert_called_once()

    def test_create_vault_client_missing_options(self):
        """Test that None is returned when not all required options are provided."""
        vault_config = VaultConfig(path="path", token="token")  # missing url
        client = create_vault_client(vault_config)
        self.assertIsNone(client)

    def test_create_vault_client_oidc_auth(self):
        """Test that the enable_auth_method method is called when the auth type is 'oidc'."""
        vault_config = self._make_vault_config(auth="oidc")
        with patch.object(Client, "sys") as mock_sys:
            mock_sys.read_health_status.return_value = {"status": "ok"}
            client = create_vault_client(vault_config)
            self.assertIsInstance(client, Client)
            mock_sys.enable_auth_method.assert_called_once_with(method_type="oidc")

    def test_get_secret_from_vault(self):
        """Test that a secret is retrieved successfully from Vault."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.return_value = {"data": {"data": {"key": "value"}}}
        secret = get_secret_from_vault(vault_client, VaultConfig(path="path", mount_point="secret"))
        self.assertEqual(secret, {"key": "value"})

    def test_get_secret_from_vault_returns_empty_on_exception(self):
        """Test that an empty dict is returned when read_secret_version raises an exception."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.side_effect = Exception("Test exception")
        secret = get_secret_from_vault(vault_client, VaultConfig(path="path", mount_point="secret"))
        self.assertEqual(secret, {})

    def test_get_secret_from_vault_returns_empty_on_invalid_path(self):
        """Test that an empty dict is returned when the path is invalid."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.side_effect = InvalidPath("Path not found")
        secret = get_secret_from_vault(vault_client, VaultConfig(path="invalid/path", mount_point="secret"))
        self.assertEqual(secret, {})

    def test_get_secret_from_vault_empty_response(self):
        """Test that an empty dict is returned when Vault returns None."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.return_value = None
        secret = get_secret_from_vault(vault_client, VaultConfig(path="path", mount_point="secret"))
        self.assertEqual(secret, {})

    def test_get_secret_from_vault_missing_data_key(self):
        """Test that an empty dict is returned when response is missing 'data' key."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.return_value = {"status": "ok"}
        secret = get_secret_from_vault(vault_client, VaultConfig(path="path", mount_point="secret"))
        self.assertEqual(secret, {})

    def test_get_secret_from_vault_missing_nested_data_key(self):
        """Test that an empty dict is returned when response['data'] is missing 'data' key."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.return_value = {"data": {"status": "ok"}}
        secret = get_secret_from_vault(vault_client, VaultConfig(path="path", mount_point="secret"))
        self.assertEqual(secret, {})

    def test_get_config_from_vault(self):
        """Test that a configuration is retrieved successfully from Vault."""
        vault_label = "label"
        vault_config = self._make_vault_config()
        with patch.object(Client, "sys") as mock_sys, patch.object(Client, "secrets") as mock_secrets:
            mock_sys.read_health_status.return_value = {"status": "ok"}
            mock_secrets.kv.read_secret_version.return_value = {"data": {"data": {vault_label: {"key": "value"}}}}
            config = get_config_from_vault(vault_label, vault_config)
            self.assertEqual(config, {"key": "value"})

    def test_get_config_from_vault_empty(self):
        """Test that an empty configuration is returned when the secret does not contain the specified label."""
        vault_label = "label"
        vault_config = self._make_vault_config()
        with patch.object(Client, "sys") as mock_sys, patch.object(Client, "secrets") as mock_secrets:
            mock_sys.read_health_status.return_value = {"status": "ok"}
            mock_secrets.kv.read_secret_version.return_value = {"data": {"data": {}}}
            config = get_config_from_vault(vault_label, vault_config)
            self.assertEqual(config, {})

    def test_get_config_from_vault_create_vault_client_fails(self):
        """Test that an empty configuration is returned when the Vault client cannot be created."""
        vault_label = "label"
        vault_config = VaultConfig(path="path", token="token")  # missing url -> client creation fails
        config = get_config_from_vault(vault_label, vault_config)
        self.assertEqual(config, {})

    def test_get_config_from_vault_label_not_found(self):
        """Test that an empty dict is returned when the label is not in the secret."""
        vault_label = "nonexistent"
        vault_config = self._make_vault_config()
        with patch.object(Client, "sys") as mock_sys, patch.object(Client, "secrets") as mock_secrets:
            mock_sys.read_health_status.return_value = {"status": "ok"}
            mock_secrets.kv.read_secret_version.return_value = {"data": {"data": {"other_key": "value"}}}
            config = get_config_from_vault(vault_label, vault_config)
            self.assertEqual(config, {})

    def test_get_config_from_vault_empty_label_value(self):
        """Test that an empty dict is returned when the label exists but has null/empty value."""
        vault_label = "empty_label"
        vault_config = self._make_vault_config()
        with patch.object(Client, "sys") as mock_sys, patch.object(Client, "secrets") as mock_secrets:
            mock_sys.read_health_status.return_value = {"status": "ok"}
            mock_secrets.kv.read_secret_version.return_value = {"data": {"data": {vault_label: None}}}
            config = get_config_from_vault(vault_label, vault_config)
            self.assertEqual(config, {})

    def test_get_config_from_vault_health_check_fails(self):
        """Test that an empty dict is returned when the health check fails."""
        vault_label = "label"
        vault_config = self._make_vault_config()
        with patch.object(Client, "sys") as mock_sys:
            mock_sys.read_health_status.side_effect = Exception("Connection refused")
            config = get_config_from_vault(vault_label, vault_config)
            self.assertEqual(config, {})

    def test_create_vault_client_health_check_fails(self):
        """Test that None is returned when health check fails."""
        vault_config = self._make_vault_config()
        with patch.object(Client, "sys") as mock_sys:
            mock_sys.read_health_status.side_effect = Exception("403 Forbidden")
            client = create_vault_client(vault_config)
            self.assertIsNone(client)
