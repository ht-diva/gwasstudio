import unittest
from unittest.mock import Mock, patch

from hvac import Client

from gwasstudio.utils.vault import create_vault_client, get_secret_from_vault, get_config_from_vault


class TestVaultFunctions(unittest.TestCase):
    def test_create_vault_client(self):
        """Test that a Vault client is created successfully when all required options are provided."""
        vault_options = {"path": "path", "token": "token", "url": "url", "auth": "basic"}
        client = create_vault_client(vault_options)
        self.assertIsInstance(client, Client)

    def test_create_vault_client_missing_options(self):
        """Test that None is returned when not all required options are provided."""
        vault_options = {"path": "path", "token": "token"}
        client = create_vault_client(vault_options)
        self.assertIsNone(client)

    def test_create_vault_client_oidc_auth(self):
        """Test that the enable_auth_method method is called when the auth type is "oidc"."""
        vault_options = {"path": "path", "token": "token", "url": "url", "auth": "oidc"}
        with patch.object(Client, "sys") as mock_sys:
            client = create_vault_client(vault_options)
            self.assertIsInstance(client, Client)
            mock_sys.enable_auth_method.assert_called_once_with(method_type="oidc")

    def test_get_secret_from_vault(self):
        """Test that a secret is retrieved successfully from Vault."""
        vault_client = Mock()
        vault_client.secrets.kv.read_secret_version.return_value = {"data": {"data": {"key": "value"}}}
        secret = get_secret_from_vault(vault_client, {"path": "path"})
        self.assertEqual(secret, {"key": "value"})

    def test_get_secret_from_vault_raise_on_deleted_version(self):
        """Test that an exception is raised when the read_secret_version method fails."""
        vault_client = Mock()
        vault_client.kv.read_secret_version.side_effect = Exception("Test exception")
        with self.assertRaises(Exception):
            get_secret_from_vault(vault_client, "path")

    def test_get_config_from_vault(self):
        """Test that a configuration is retrieved successfully from Vault."""
        vault_label = "label"
        vault_options = {"path": "path", "token": "token", "url": "url", "auth": "basic"}
        with patch.object(Client, "secrets") as mock_secrets:
            mock_secrets.kv.read_secret_version.return_value = {"data": {"data": {vault_label: {"key": "value"}}}}
            config = get_config_from_vault(vault_label, vault_options)
            self.assertEqual(config, {"key": "value"})

    def test_get_config_from_vault_empty(self):
        """Test that an empty configuration is returned when the secret does not contain the specified label."""
        vault_label = "label"
        vault_options = {"path": "path", "token": "token", "url": "url", "auth": "basic"}
        with patch.object(Client, "secrets") as mock_secrets:
            mock_secrets.kv.read_secret_version.return_value = {"data": {"data": {}}}
            config = get_config_from_vault(vault_label, vault_options)
            self.assertEqual(config, {})

    def test_get_config_from_vault_create_vault_client_fails(self):
        """Test that an empty configuration is returned when the Vault client cannot be created."""
        vault_label = "label"
        vault_options = {"path": "path", "token": "token"}
        config = get_config_from_vault(vault_label, vault_options)
        self.assertEqual(config, {})
