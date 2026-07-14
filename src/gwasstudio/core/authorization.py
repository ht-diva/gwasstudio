"""
GWASStudio Authorization Module
===============================

Vault-only token-based authorization for TileDB dataset access control.

This module provides authorization services that use Vault tokens as the sole
authentication mechanism. It extracts user identity from Vault token introspection
and checks against MongoDB metadata to determine access permissions.

Key concepts:
- Vault tokens are the only authentication method (no username/password)
- Token accessor is the primary stable identifier
- Token display_name can be parsed for username extraction
- Token policies enable role-based access control
- MongoDB metadata stores dataset-level permissions
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from hvac import Client
from hvac.exceptions import Forbidden, InvalidPath, Unauthorized

from gwasstudio.core.config import AuthConfig, GWASStudioConfig, VaultConfig
from gwasstudio.core.exceptions import AuthenticationError, PermissionError
from gwasstudio.core.storage.mongodb import MongoDBStorage
from gwasstudio.utils.vault import create_vault_client


class AccessLevel(str, Enum):
    """Access levels for datasets."""

    PUBLIC = "public"
    PROTECTED = "protected"
    RESTRICTED = "restricted"


@dataclass
class VaultUserContext:
    """
    User context derived from a Vault token.

    This represents the authenticated user's identity extracted from Vault
    token introspection. It serves as the foundation for all authorization
    decisions in GWASStudio.

    Attributes:
        token_accessor: Stable, unique token identifier (primary ID)
        display_name: Human-readable token name (may contain username)
        username: Extracted username from display_name pattern
        organization: Extracted organization from display_name pattern
        token_policies: List of Vault policies assigned to the token
        is_authenticated: True if token is valid and verified
        raw_display_name: Original display_name from Vault (unparsed)
    """

    token_accessor: str | None = None
    display_name: str | None = None
    username: str | None = None
    organization: str | None = None
    token_policies: list[str] = field(default_factory=list)
    is_authenticated: bool = False
    raw_display_name: str | None = None

    @classmethod
    def from_vault_config(cls, vault_config: VaultConfig) -> VaultUserContext:
        """
        Create a VaultUserContext by introspecting the Vault token from config.

        Args:
            vault_config: GWASStudio Vault configuration containing token

        Returns:
            VaultUserContext populated from Vault token information

        Raises:
            AuthenticationError: If token is invalid or cannot be validated
        """
        if not vault_config.token:
            return cls(is_authenticated=False)

        client = create_vault_client(vault_config)
        if client is None:
            return cls(is_authenticated=False)

        try:
            # Get token information
            token_data = client.lookup_token()
            if not token_data or not token_data.get("data"):
                raise AuthenticationError("Vault token lookup returned no data")

            data = token_data["data"]
            accessor = data.get("accessor")
            display_name = data.get("display_name")
            policies = data.get("policies", [])

            # Extract username and organization from display_name
            # Pattern: token-GWASStudio-{username}-{organization}
            # Note: Both username and organization can contain hyphens, making parsing ambiguous.
            # We use the convention that the organization is the LAST segment (after the last hyphen).
            # This means:
            #   - "token-GWASStudio-alice-fht-org" -> username="alice", org="fht-org"
            #   - "token-GWASStudio-bob-org" -> username="bob", org="org"
            #   - "token-GWASStudio-charlie" -> username="charlie", org=None
            # For tokens like "token-GWASStudio-name-surname-fht-org", this gives:
            #   - username="name-surname", org="fht-org" (if we split at first hyphen from end)
            # But since "fht-org" contains a hyphen, we actually get:
            #   - username="name-surname-fht", org="org"
            # To handle this, we check for known organization patterns.
            username = None
            organization = None
            if display_name and display_name.startswith("token-GWASStudio-"):
                remainder = display_name[len("token-GWASStudio-") :]
                if remainder:
                    # Try to parse with known organizations
                    # If remainder ends with a known org pattern, extract it
                    known_orgs = ["fht-org"]  # Add more as needed
                    for org in known_orgs:
                        if remainder.endswith(f"-{org}"):
                            username = remainder[: -(len(org) + 1)]  # +1 for the hyphen
                            organization = org
                            break
                    else:
                        # No known org found, use last hyphen split
                        last_hyphen_idx = remainder.rfind("-")
                        if last_hyphen_idx > 0:
                            username = remainder[:last_hyphen_idx]
                            organization = remainder[last_hyphen_idx + 1 :]
                        else:
                            username = remainder

            return cls(
                token_accessor=accessor,
                display_name=display_name,
                username=username,
                organization=organization,
                token_policies=policies,
                is_authenticated=True,
                raw_display_name=display_name,
            )
        except (Unauthorized, Forbidden) as e:
            raise AuthenticationError(f"Vault token validation failed: {str(e)}")
        except Exception as e:
            raise AuthenticationError(f"Failed to extract user context from Vault: {str(e)}")

    @classmethod
    def anonymous(cls) -> VaultUserContext:
        """Create an anonymous (unauthenticated) user context."""
        return cls(is_authenticated=False)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/audit purposes."""
        return {
            "token_accessor": self.token_accessor,
            "display_name": self.display_name,
            "username": self.username,
            "organization": self.organization,
            "token_policies": self.token_policies,
            "is_authenticated": self.is_authenticated,
        }


class AuthorizationService:
    """
    Service for checking authorization to access TileDB datasets.

    This service uses Vault token introspection to determine the user's identity
    and checks against MongoDB metadata to decide if access should be granted.

    The authorization model supports three mechanisms:
    1. Policy-based: Users with specific Vault policies can access datasets
    2. Username-based: Specific usernames (from display_name) can access datasets
    3. Accessor-based: Specific token accessors can access datasets

    Example usage:
        >>> config = GWASStudioConfig()
        >>> auth = AuthorizationService(config)
        >>> user_context = auth.get_user_context()
        >>> if auth.check_access(data_id="dataset_001", user_context=user_context):
        ...     # Grant access
        ...     pass
    """

    def __init__(self, config: GWASStudioConfig):
        """
        Initialize the authorization service.

        Args:
            config: GWASStudio configuration
        """
        self.config = config
        self._vault_client: Client | None = None
        self._user_context: VaultUserContext | None = None
        self._mongo = MongoDBStorage(config)

    def get_user_context(self) -> VaultUserContext:
        """
        Get the Vault user context from the configuration.

        Caches the result for the lifetime of the service instance.

        Returns:
            VaultUserContext with the authenticated user's identity

        Raises:
            AuthenticationError: If token is invalid
        """
        if self._user_context is None:
            self._user_context = VaultUserContext.from_vault_config(self.config.vault)
        return self._user_context

    def get_vault_client(self) -> Client | None:
        """
        Get a Vault client for the configured token.

        Returns:
            hvac.Client or None if configuration is incomplete
        """
        if self._vault_client is None:
            self._vault_client = create_vault_client(self.config.vault)
        return self._vault_client

    def validate_token(self) -> bool:
        """
        Validate the Vault token from configuration.

        Returns:
            True if token is valid, False otherwise
        """
        try:
            ctx = self.get_user_context()
            return ctx.is_authenticated
        except AuthenticationError:
            return False

    def check_access(
        self,
        data_id: str | None = None,
        project: str | None = None,
        study: str | None = None,
        user_context: VaultUserContext | None = None,
    ) -> bool:
        """
        Check if the user can access a dataset.

        The check follows this priority:
        1. If auth is disabled, return True
        2. If dataset is PUBLIC and anonymous access is allowed, return True
        3. If no valid user context, return False (except for PUBLIC with anonymous allowed)
        4. Check dataset-specific permissions (policies, username, accessor)

        Args:
            data_id: Specific dataset identifier (most specific)
            project: Project name (less specific)
            study: Study name (less specific)
            user_context: Vault user context (auto-created if None)

        Returns:
            bool: True if access is permitted

        Raises:
            PermissionError: If access is explicitly denied (e.g., dataset not found)
        """
        # If authorization is disabled, allow all access
        if not self.config.auth.enabled:
            return True

        # Get or use provided user context
        ctx = user_context or self.get_user_context()

        # Build query - prefer data_id, then project+study, then project
        query = {}
        if data_id:
            query["data_id"] = data_id
        elif project and study:
            query = {"project": project, "study": study}
        elif project:
            query = {"project": project}
        else:
            raise PermissionError("Must specify data_id, or project+study, or project")

        # Get metadata from MongoDB
        metadata_list = self._mongo.query_metadata(query, limit=1)
        if not metadata_list:
            raise PermissionError(f"Dataset not found: {query}")

        metadata = metadata_list[0]
        access_level = metadata.get("access_level", self.config.auth.default_access_level)

        # Handle PUBLIC datasets
        if access_level == AccessLevel.PUBLIC.value:
            if not ctx.is_authenticated:
                return self.config.auth.allow_anonymous_public
            return True

        # For PROTECTED and RESTRICTED, user must be authenticated
        if not ctx.is_authenticated:
            return False

        # PROTECTED: Any authenticated user with valid token
        if access_level == AccessLevel.PROTECTED.value:
            return True

        # RESTRICTED: Check specific permissions
        if access_level == AccessLevel.RESTRICTED.value:
            return self._check_restricted_access(metadata, ctx)

        # Default deny for unknown access levels
        return False

    def _check_restricted_access(self, metadata: dict[str, Any], user_context: VaultUserContext) -> bool:
        """
        Check if user has permission for a restricted dataset.

        Checks in order of preference:
        1. Token accessor (most reliable)
        2. Username (extracted from display_name)
        3. Vault policies

        Args:
            metadata: Dataset metadata from MongoDB
            user_context: Vault user context

        Returns:
            bool: True if user has permission
        """
        # Check by token accessor (most reliable)
        if self.config.auth.use_accessor:
            token_accessor = user_context.token_accessor
            allowed_accessors = metadata.get("allowed_token_accessors", [])
            if token_accessor and token_accessor in allowed_accessors:
                return True

        # Check by username (extracted from display_name)
        if self.config.auth.use_username:
            username = user_context.username
            allowed_users = metadata.get("allowed_users", [])
            if username and username in allowed_users:
                return True

        # Check by Vault policies
        if self.config.auth.use_policies:
            user_policies = set(user_context.token_policies)
            allowed_policies = set(metadata.get("allowed_policies", []))
            if user_policies & allowed_policies:  # Any intersection
                return True

        # No matching permission found
        return False

    def get_dataset_access_level(self, data_id: str) -> str:
        """
        Get the access level for a specific dataset.

        Args:
            data_id: Dataset identifier

        Returns:
            str: Access level (public, protected, restricted)

        Raises:
            PermissionError: If dataset is not found
        """
        metadata_list = self._mongo.query_metadata({"data_id": data_id}, limit=1)
        if metadata_list:
            return metadata_list[0].get("access_level", self.config.auth.default_access_level)
        raise PermissionError(f"Dataset not found: {data_id}")

    def list_accessible_datasets(
        self,
        user_context: VaultUserContext | None = None,
        query: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all datasets accessible by the current user.

        Args:
            user_context: Vault user context (auto-created if None)
            query: Optional MongoDB query to filter datasets

        Returns:
            list: List of accessible dataset metadata
        """
        ctx = user_context or self.get_user_context()
        query = query or {}

        # Get all matching datasets
        all_datasets = self._mongo.query_metadata(query)

        # Filter by access
        accessible = []
        for dataset in all_datasets:
            try:
                # Create a temporary query with just data_id
                if self.check_access(data_id=dataset.get("data_id"), user_context=ctx):
                    accessible.append(dataset)
            except PermissionError:
                continue

        return accessible
