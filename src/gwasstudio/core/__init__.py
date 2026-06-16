"""
GWASStudio Core Module
======================

Public surface for the core package.  Only stable, external-facing
APIs are re-exported here; internal helpers live in their submodules.
"""

# ── configuration ──────────────────────────────────────────────────────
from gwasstudio.core.config import (
    DaskConfig,
    GWASStudioConfig,
    MongoConfig,
    S3Config,
    TileDBConfig,
    VaultConfig,
    get_dask_batch_size,
    get_dask_deployment,
    get_tiledb_config,
)
from gwasstudio.core.enums import MetadataEnum

# ── exceptions ─────────────────────────────────────────────────────────
from gwasstudio.core.exceptions import (
    AuthenticationError,
    ClusterError,
    ConfigurationError,
    DaskError,
    GWASStudioError,
    IngestionError,
    InvalidConfigError,
    InvalidInputError,
    PermissionError,
    VaultError,
)

# ── public helpers ─────────────────────────────────────────────────────
from gwasstudio.core.hashing import Hashing

# ── stable core APIs ───────────────────────────────────────────────────
from gwasstudio.core.ingestion import ingest_metadata
from gwasstudio.core.query import InvalidQueryFieldError, QueryError, list_projects

# ── backward-compatible aliases ────────────────────────────────────────
from gwasstudio.core.storage.base import StorageError

__all__ = [
    # Configuration
    "GWASStudioConfig",
    "DaskConfig",
    "MongoConfig",
    "S3Config",
    "TileDBConfig",
    "VaultConfig",
    get_dask_batch_size,
    get_dask_deployment,
    get_tiledb_config,
    # Exceptions
    "GWASStudioError",
    "IngestionError",
    "InvalidInputError",
    "QueryError",
    "InvalidQueryFieldError",
    "ConfigurationError",
    "InvalidConfigError",
    "DaskError",
    "ClusterError",
    "AuthenticationError",
    "VaultError",
    "PermissionError",
    # Helpers
    "Hashing",
    # Core APIs
    "ingest_metadata",
    "list_projects",
    "MetadataEnum",
    # Backward-compatible
    "StorageError",
]
