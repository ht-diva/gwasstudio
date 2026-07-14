"""
Unit tests for GWASStudio authorization module.

Tests cover:
- VaultUserContext creation and parsing
- AuthorizationService access checks
- Access level handling
- Integration with MongoDB metadata
"""

from unittest.mock import MagicMock, patch

import pytest

from gwasstudio.core.authorization import (
    AccessLevel,
    AuthorizationService,
    VaultUserContext,
)
from gwasstudio.core.config import AuthConfig, GWASStudioConfig, VaultConfig
from gwasstudio.core.exceptions import AuthenticationError, PermissionError

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_vault_config():
    """Create a mock Vault configuration with a token."""
    return VaultConfig(
        token="test-token-123",
        url="https://vault.test",
        path="secret/gwasstudio",
        auth="basic",
    )


@pytest.fixture
def mock_gwas_config(mock_vault_config):
    """Create a GWASStudioConfig with Vault and Auth configuration."""
    return GWASStudioConfig(
        vault=mock_vault_config,
        auth=AuthConfig(
            enabled=True,
            default_access_level=AccessLevel.PROTECTED.value,
            allow_anonymous_public=False,
        ),
    )


@pytest.fixture
def mock_vault_client():
    """Create a mock hvac.Client that simulates Vault responses."""
    client = MagicMock()

    # Mock token lookup response with display_name pattern
    client.lookup_token.return_value = {
        "data": {
            "accessor": "dkE5rNWmE6rCyg41gTB0WJT2",
            "display_name": "token-GWASStudio-gianmauro-cuccuru-fht-org",
            "entity_id": None,
            "token_type": "service",
            "ttl": 31535310,
            "creation_time": "2024-01-15T10:30:00Z",
            "policies": ["computationalbiobank-reader", "default"],
        }
    }

    return client


@pytest.fixture
def mock_vault_client_no_token():
    """Mock client that raises Unauthorized."""
    client = MagicMock()
    from hvac.exceptions import Unauthorized

    client.lookup_token.side_effect = Unauthorized("Invalid token")
    return client


@pytest.fixture
def mock_vault_client_no_display_name():
    """Mock client with no display_name pattern."""
    client = MagicMock()
    client.lookup_token.return_value = {
        "data": {
            "accessor": "test-accessor",
            "display_name": "random-token-name",
            "policies": ["default"],
        }
    }
    return client


@pytest.fixture
def mock_mongo_storage():
    """Create a mock MongoDB storage that returns test metadata."""
    storage = MagicMock()

    def mock_query_metadata(query, **kwargs):
        """Return different metadata based on query."""
        data_id = query.get("data_id")

        if data_id == "public_dataset":
            return [{"data_id": "public_dataset", "access_level": "public"}]
        elif data_id == "protected_dataset":
            return [{"data_id": "protected_dataset", "access_level": "protected"}]
        elif data_id == "restricted_dataset":
            return [
                {
                    "data_id": "restricted_dataset",
                    "access_level": "restricted",
                    "allowed_users": ["gianmauro-cuccuru"],
                    "allowed_token_accessors": ["dkE5rNWmE6rCyg41gTB0WJT2"],
                    "allowed_policies": ["computationalbiobank-reader"],
                }
            ]
        elif data_id == "restricted_policies_only":
            return [
                {
                    "data_id": "restricted_policies_only",
                    "access_level": "restricted",
                    "allowed_policies": ["other-policy"],
                }
            ]
        elif data_id == "nonexistent":
            return []
        else:
            return []

    storage.query_metadata = mock_query_metadata
    return storage


# =============================================================================
# VaultUserContext Tests
# =============================================================================


class TestVaultUserContext:
    """Tests for VaultUserContext class."""

    def test_from_vault_config_extracts_username_and_org(self, mock_vault_config, mock_vault_client):
        """Test that username and org are extracted from display_name pattern."""
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client):
            ctx = VaultUserContext.from_vault_config(mock_vault_config)

            assert ctx.is_authenticated
            assert ctx.token_accessor == "dkE5rNWmE6rCyg41gTB0WJT2"
            assert ctx.display_name == "token-GWASStudio-gianmauro-cuccuru-fht-org"
            assert ctx.username == "gianmauro-cuccuru"
            assert ctx.organization == "fht-org"
            assert "computationalbiobank-reader" in ctx.token_policies
            assert "default" in ctx.token_policies

    def test_from_vault_config_no_token(self):
        """Test handling of missing token."""
        config = VaultConfig(token=None)
        ctx = VaultUserContext.from_vault_config(config)

        assert not ctx.is_authenticated
        assert ctx.token_accessor is None
        assert ctx.username is None

    def test_from_vault_config_invalid_token(self, mock_vault_config, mock_vault_client_no_token):
        """Test handling of invalid token."""
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client_no_token):
            with pytest.raises(AuthenticationError) as exc_info:
                VaultUserContext.from_vault_config(mock_vault_config)

            assert "Vault token validation failed" in str(exc_info.value)

    def test_from_vault_config_no_pattern(self, mock_vault_config, mock_vault_client_no_display_name):
        """Test handling of display_name without expected pattern."""
        mock_vault_config.token = "test-token"
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client_no_display_name):
            ctx = VaultUserContext.from_vault_config(mock_vault_config)

            assert ctx.username is None
            assert ctx.organization is None
            assert ctx.display_name == "random-token-name"

    def test_anonymous_context(self):
        """Test anonymous user context."""
        ctx = VaultUserContext.anonymous()

        assert not ctx.is_authenticated
        assert ctx.token_accessor is None
        assert ctx.username is None

    def test_to_dict(self, mock_vault_config, mock_vault_client):
        """Test conversion to dictionary."""
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client):
            ctx = VaultUserContext.from_vault_config(mock_vault_config)
            d = ctx.to_dict()

            assert d["token_accessor"] == "dkE5rNWmE6rCyg41gTB0WJT2"
            assert d["username"] == "gianmauro-cuccuru"
            assert d["organization"] == "fht-org"
            assert d["is_authenticated"] is True


# =============================================================================
# AuthorizationService Tests
# =============================================================================


class TestAuthorizationService:
    """Tests for AuthorizationService class."""

    def test_check_access_public_dataset_allowed(self, mock_gwas_config, mock_mongo_storage):
        """Test that PUBLIC datasets are accessible with valid token."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                token_accessor="test",
                is_authenticated=True,
            )

            assert auth.check_access(data_id="public_dataset") is True

    def test_check_access_public_dataset_anonymous_denied(self, mock_gwas_config, mock_mongo_storage):
        """Test that PUBLIC datasets are NOT accessible to anonymous users by default."""
        config = GWASStudioConfig(
            vault=VaultConfig(token=None),
            auth=AuthConfig(allow_anonymous_public=False),
        )
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(config)
            auth._user_context = VaultUserContext.anonymous()

            assert auth.check_access(data_id="public_dataset") is False

    def test_check_access_public_dataset_anonymous_allowed(self, mock_gwas_config, mock_mongo_storage):
        """Test that PUBLIC datasets ARE accessible to anonymous users when configured."""
        config = GWASStudioConfig(
            vault=VaultConfig(token=None),
            auth=AuthConfig(allow_anonymous_public=True),
        )
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(config)
            auth._user_context = VaultUserContext.anonymous()

            assert auth.check_access(data_id="public_dataset") is True

    def test_check_access_protected_dataset_authenticated(self, mock_gwas_config, mock_mongo_storage):
        """Test that PROTECTED datasets are accessible to authenticated users."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                token_accessor="test",
                is_authenticated=True,
            )

            assert auth.check_access(data_id="protected_dataset") is True

    def test_check_access_protected_dataset_anonymous_denied(self, mock_gwas_config, mock_mongo_storage):
        """Test that PROTECTED datasets are NOT accessible to anonymous users."""
        config = GWASStudioConfig(
            vault=VaultConfig(token=None),
            auth=AuthConfig(enabled=True),
        )
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(config)
            auth._user_context = VaultUserContext.anonymous()

            assert auth.check_access(data_id="protected_dataset") is False

    def test_check_access_restricted_by_username(self, mock_gwas_config, mock_mongo_storage):
        """Test RESTRICTED access by username."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                token_accessor="dkE5rNWmE6rCyg41gTB0WJT2",
                username="gianmauro-cuccuru",
                organization="fht-org",
                token_policies=["computationalbiobank-reader"],
                is_authenticated=True,
            )

            assert auth.check_access(data_id="restricted_dataset") is True

    def test_check_access_restricted_by_accessor(self, mock_gwas_config, mock_mongo_storage):
        """Test RESTRICTED access by token accessor."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                token_accessor="dkE5rNWmE6rCyg41gTB0WJT2",
                is_authenticated=True,
            )

            assert auth.check_access(data_id="restricted_dataset") is True

    def test_check_access_restricted_by_policy(self, mock_gwas_config, mock_mongo_storage):
        """Test RESTRICTED access by policy."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                token_accessor="some-other-accessor",
                username="other-user",
                token_policies=["computationalbiobank-reader"],
                is_authenticated=True,
            )

            assert auth.check_access(data_id="restricted_dataset") is True

    def test_check_access_restricted_denied(self, mock_gwas_config, mock_mongo_storage):
        """Test RESTRICTED access denied when no permissions match."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                token_accessor="other-accessor",
                username="other-user",
                token_policies=["some-other-policy"],
                is_authenticated=True,
            )

            assert auth.check_access(data_id="restricted_policies_only") is False

    def test_check_access_dataset_not_found(self, mock_gwas_config, mock_mongo_storage):
        """Test that PermissionError is raised for nonexistent dataset."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(
                is_authenticated=True,
            )

            with pytest.raises(PermissionError) as exc_info:
                auth.check_access(data_id="nonexistent")

            assert "Dataset not found" in str(exc_info.value)

    def test_check_access_no_identifier(self, mock_gwas_config):
        """Test that PermissionError is raised when no identifier is provided."""
        with patch("gwasstudio.core.authorization.MongoDBStorage"):
            auth = AuthorizationService(mock_gwas_config)
            auth._user_context = VaultUserContext(is_authenticated=True)

            with pytest.raises(PermissionError) as exc_info:
                auth.check_access()

            assert "Must specify" in str(exc_info.value)

    def test_auth_disabled_allows_all(self, mock_gwas_config, mock_mongo_storage):
        """Test that all access is allowed when auth is disabled."""
        config = GWASStudioConfig(
            vault=VaultConfig(token="test"),
            auth=AuthConfig(enabled=False),
        )
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(config)
            auth._user_context = VaultUserContext(is_authenticated=False)

            # All datasets should be accessible when auth is disabled
            assert auth.check_access(data_id="restricted_dataset") is True
            assert auth.check_access(data_id="public_dataset") is True

    def test_get_dataset_access_level(self, mock_gwas_config, mock_mongo_storage):
        """Test retrieving access level for a dataset."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)

            assert auth.get_dataset_access_level("public_dataset") == "public"
            assert auth.get_dataset_access_level("protected_dataset") == "protected"
            assert auth.get_dataset_access_level("restricted_dataset") == "restricted"

    def test_get_dataset_access_level_not_found(self, mock_gwas_config, mock_mongo_storage):
        """Test PermissionError for nonexistent dataset access level."""
        with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo_storage):
            auth = AuthorizationService(mock_gwas_config)

            with pytest.raises(PermissionError):
                auth.get_dataset_access_level("nonexistent")

    def test_validate_token_valid(self, mock_gwas_config, mock_vault_client):
        """Test token validation with valid token."""
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client):
            auth = AuthorizationService(mock_gwas_config)
            assert auth.validate_token() is True

    def test_validate_token_invalid(self, mock_gwas_config, mock_vault_client_no_token):
        """Test token validation with invalid token."""
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client_no_token):
            auth = AuthorizationService(mock_gwas_config)
            assert auth.validate_token() is False

    def test_validate_token_no_token(self):
        """Test token validation with no token configured."""
        config = GWASStudioConfig(
            vault=VaultConfig(token=None),
        )
        auth = AuthorizationService(config)
        assert auth.validate_token() is False


# =============================================================================
# AccessLevel Tests
# =============================================================================


class TestAccessLevel:
    """Tests for AccessLevel enum."""

    def test_access_level_values(self):
        """Test AccessLevel enum values."""
        assert AccessLevel.PUBLIC.value == "public"
        assert AccessLevel.PROTECTED.value == "protected"
        assert AccessLevel.RESTRICTED.value == "restricted"

    def test_access_level_iteration(self):
        """Test iterating over AccessLevel."""
        levels = list(AccessLevel)
        assert len(levels) == 3
        assert AccessLevel.PUBLIC in levels
        assert AccessLevel.PROTECTED in levels
        assert AccessLevel.RESTRICTED in levels


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration-style tests with mocked components."""

    def test_full_authorization_flow(self, mock_vault_config, mock_vault_client):
        """Test complete authorization flow from token to access decision."""
        # Mock MongoDB to return restricted dataset
        mock_mongo = MagicMock()
        mock_mongo.query_metadata.return_value = [
            {
                "data_id": "test_dataset",
                "access_level": "restricted",
                "allowed_policies": ["computationalbiobank-reader"],
            }
        ]

        config = GWASStudioConfig(
            vault=mock_vault_config,
            auth=AuthConfig(enabled=True),
        )

        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client):
            with patch("gwasstudio.core.authorization.MongoDBStorage", return_value=mock_mongo):
                auth = AuthorizationService(config)

                # This should work because token has computationalbiobank-reader policy
                assert auth.check_access(data_id="test_dataset") is True

    def test_username_extraction_known_org(self, mock_vault_config, mock_vault_client):
        """Test username extraction with known organization pattern."""
        # This test verifies that tokens with known orgs (like fht-org) are parsed correctly
        with patch("gwasstudio.core.authorization.create_vault_client", return_value=mock_vault_client):
            ctx = VaultUserContext.from_vault_config(mock_vault_config)
            assert ctx.username == "gianmauro-cuccuru"
            assert ctx.organization == "fht-org"
