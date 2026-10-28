"""Unit tests for gwasstudio.cli.utils S3/Vault config resolution."""

from types import SimpleNamespace
from unittest.mock import patch

from gwasstudio.cli.utils import _resolve_s3_config

# Matches the CLI defaults now declared in ``main.py`` (real ``None`` for unset
# credentials/endpoint, so unset values fall back to AWS_* env vars).
CLI_S3_DEFAULTS = {
    "vfs.s3.aws_access_key_id": None,
    "vfs.s3.aws_secret_access_key": None,
    "vfs.s3.endpoint_override": None,
    "vfs.s3.use_virtual_addressing": "false",
    "vfs.s3.scheme": "https",
    "vfs.s3.region": "",
    "vfs.s3.verify_ssl": "false",
}

VAULT_S3 = {
    "vfs.s3.aws_access_key_id": "RO-KEY",
    "vfs.s3.aws_secret_access_key": "RO-SECRET",
    "vfs.s3.endpoint_override": "https://storage.fht.org:9021",
    "vfs.s3.region": "",
    "vfs.s3.scheme": "https",
    "vfs.s3.use_virtual_addressing": "false",
    "vfs.s3.verify_ssl": "false",
}


def _ctx(tiledb=None, vault=None):
    return SimpleNamespace(obj={"tiledb": tiledb or dict(CLI_S3_DEFAULTS), "vault": vault or {}})


class TestResolveS3Config:
    def test_defaults_returned_when_no_vault(self):
        """Without Vault, unset credentials/endpoint are omitted (env-var fallback)."""
        result = _resolve_s3_config(_ctx())
        assert result.get("vfs.s3.aws_access_key_id") is None
        assert result.get("vfs.s3.endpoint_override") is None
        assert "vfs.s3.aws_access_key_id" not in result  # None values are dropped
        # Non-None defaults are preserved
        assert result["vfs.s3.scheme"] == "https"
        assert result["vfs.s3.use_virtual_addressing"] == "false"

    def test_vault_tiledb_section_used_when_no_cli_flags(self):
        """No explicit CLI flags → the Vault 'tiledb' section is used."""
        vault = {"auth": "basic", "mount_point": "secret", "path": "p", "token": "t", "url": "u"}
        with patch("gwasstudio.utils.vault.get_config_from_vault", return_value=dict(VAULT_S3)) as mock_get:
            result = _resolve_s3_config(_ctx(vault=vault))
        mock_get.assert_called_once()
        assert result["vfs.s3.endpoint_override"] == "https://storage.fht.org:9021"
        assert result["vfs.s3.aws_access_key_id"] == "RO-KEY"
        assert result["vfs.s3.aws_secret_access_key"] == "RO-SECRET"

    def test_explicit_cli_flag_overrides_vault_key(self):
        """An explicitly-set CLI flag wins over the matching Vault key."""
        vault = {"auth": "basic", "mount_point": "secret", "path": "p", "token": "t", "url": "u"}
        tiledb = dict(CLI_S3_DEFAULTS)
        tiledb["vfs.s3.endpoint_override"] = "https://other.example:9000"
        with patch("gwasstudio.utils.vault.get_config_from_vault", return_value=dict(VAULT_S3)):
            result = _resolve_s3_config(_ctx(tiledb=tiledb, vault=vault))
        assert result["vfs.s3.endpoint_override"] == "https://other.example:9000"
        # Other keys still come from Vault
        assert result["vfs.s3.aws_access_key_id"] == "RO-KEY"

    def test_explicit_key_and_endpoint_skip_vault(self):
        """If the CLI supplies both key and endpoint, Vault is not consulted."""
        vault = {"auth": "basic", "mount_point": "secret", "path": "p", "token": "t", "url": "u"}
        tiledb = dict(CLI_S3_DEFAULTS)
        tiledb["vfs.s3.aws_access_key_id"] = "CLI-KEY"
        tiledb["vfs.s3.aws_secret_access_key"] = "CLI-SECRET"
        tiledb["vfs.s3.endpoint_override"] = "https://cli.example:9000"
        with patch("gwasstudio.utils.vault.get_config_from_vault") as mock_get:
            result = _resolve_s3_config(_ctx(tiledb=tiledb, vault=vault))
        mock_get.assert_not_called()
        assert result["vfs.s3.aws_access_key_id"] == "CLI-KEY"
        assert result["vfs.s3.endpoint_override"] == "https://cli.example:9000"

    def test_vault_incomplete_returns_cli_defaults(self):
        """Vault configured but missing token → fall back to CLI defaults."""
        vault = {"path": "p", "url": "u"}  # no token
        result = _resolve_s3_config(_ctx(vault=vault))
        assert result.get("vfs.s3.aws_access_key_id") is None
        assert "vfs.s3.aws_access_key_id" not in result

    def test_vault_empty_tiledb_returns_cli_defaults(self):
        """Vault present but no 'tiledb' section → unset values omitted."""
        vault = {"auth": "basic", "mount_point": "secret", "path": "p", "token": "t", "url": "u"}
        with patch("gwasstudio.utils.vault.get_config_from_vault", return_value={}):
            result = _resolve_s3_config(_ctx(vault=vault))
        assert result.get("vfs.s3.aws_access_key_id") is None
        assert "vfs.s3.aws_access_key_id" not in result

    def test_vault_error_falls_back_to_cli(self):
        """A Vault read failure never breaks the run; unset values are omitted."""
        vault = {"auth": "basic", "mount_point": "secret", "path": "p", "token": "t", "url": "u"}
        with patch("gwasstudio.utils.vault.get_config_from_vault", side_effect=RuntimeError("boom")):
            result = _resolve_s3_config(_ctx(vault=vault))
        assert result.get("vfs.s3.endpoint_override") is None
        assert "vfs.s3.endpoint_override" not in result

    def test_none_values_are_filtered_from_result(self):
        """None-valued keys must never be handed to TileDB (env-var fallback)."""
        result = _resolve_s3_config(_ctx())
        assert all(v is not None for v in result.values())
