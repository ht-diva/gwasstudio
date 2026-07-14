"""
GWASStudio Configuration Module
===============================

This module defines the configuration classes for GWASStudio core functionality.
It centralizes all configuration options for Dask, MongoDB, S3, Vault, and TileDB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def get_dask_batch_size(config: GWASStudioConfig, capacity_mode: bool = False) -> int:
    """
    Get the Dask batch size. When capacity_mode is true, return the total worker capacity.

    Args:
        config: GWASStudioConfig containing Dask settings.
        capacity_mode: If True, use capacity-based batch sizing (workers * cores_per_worker).

    Returns:
        int: Batch size for Dask operations.
    """
    workers = config.dask.workers
    cores_per_worker = config.dask.cores_per_worker
    return workers * cores_per_worker if capacity_mode else config.dask.batch_size


def get_dask_deployment(config: GWASStudioConfig) -> str:
    """
    Get Dask deployment type from GWASStudioConfig.

    Args:
        config: GWASStudioConfig containing Dask settings.

    Returns:
        str: Dask deployment type ("local", "gateway", "slurm").
    """
    return config.dask.deployment


def get_tiledb_config(config: GWASStudioConfig, prefix: str | None = None) -> dict[str, Any]:
    """
    Get TileDB configuration from GWASStudioConfig.

    Args:
        config: GWASStudio configuration object.
        prefix: Optional prefix to filter keys ('vfs' or 'sm').
                If None, returns merged vfs_config and sm_config.
                If 'vfs', returns only vfs_config.
                If 'sm', returns only sm_config.

    Returns:
        dict: TileDB configuration dictionary.
    """
    vfs_config = config.tiledb.vfs_config
    sm_config = config.tiledb.sm_config

    if prefix is None:
        return vfs_config | sm_config
    elif prefix == "vfs":
        return vfs_config
    elif prefix == "sm":
        return sm_config
    else:
        # Return filtered dict based on key prefix
        combined = vfs_config | sm_config
        return {k: v for k, v in combined.items() if k.startswith(prefix)}


@dataclass
class DaskConfig:
    """Configuration for Dask distributed computing."""

    deployment: str = "local"  # local, gateway, slurm
    workers: int = 2
    cores_per_worker: int = 2
    memory_per_worker: str = "4GiB"
    interface: str | None = None  # e.g., ib0
    gw_address: str | None = None  # Dask gateway address
    gw_image: str | None = None  # Dask gateway image
    walltime: str = "12:00:00"  # Walltime for each worker (SLURM)
    job_script_prologue: list[str] = field(default_factory=list)  # Commands to add to script before launching worker
    python: str | None = None  # Python executable used to launch Dask workers
    local_directory: Path | None = None  # Fast local directory for Dask workers
    batch_size: int = 0  # Number of tasks per batch (0 for no batching)


@dataclass
class MongoConfig:
    """Configuration for MongoDB storage."""

    uri: str | None = None  # MongoDB connection URI
    db_name: str = "gwasstudio"
    collection: str = "metadata"
    log_path: Path | None = None  # Path for MongoDB logs
    data_path: Path | None = None  # Path for MongoDB data (embedded)


@dataclass
class S3Config:
    """Configuration for S3 storage."""

    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    endpoint_override: str | None = None
    use_virtual_addressing: bool = False
    scheme: str = "https"
    region: str = ""
    verify_ssl: bool = False
    connect_timeout_ms: int = 30000  # 30 seconds
    request_timeout_ms: int = 300000  # 5 minutes


@dataclass
class VaultConfig:
    """Configuration for HashiCorp Vault."""

    auth: str = "basic"  # basic, oidc
    mount_point: str = "secret"
    path: str | None = None
    token: str | None = None
    url: str | None = None


@dataclass
class TileDBConfig:
    """Configuration for TileDB storage."""

    vfs_config: dict[str, Any] = field(default_factory=dict)
    sm_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class AuthConfig:
    """
    Configuration for GWASStudio authorization.

    Attributes:
        enabled: Whether authorization checks are enforced
        default_access_level: Default access level for new datasets
        allow_anonymous_public: Allow unauthenticated access to PUBLIC datasets
        use_policies: Enable policy-based authorization
        use_username: Enable username-based authorization (from display_name)
        use_accessor: Enable token accessor-based authorization
    """

    enabled: bool = True
    default_access_level: str = "protected"
    allow_anonymous_public: bool = True
    use_policies: bool = True
    use_username: bool = True
    use_accessor: bool = True


@dataclass
class GWASStudioConfig:
    """
    Main configuration class for GWASStudio.

    This class centralizes all configuration options for GWASStudio,
    including Dask, MongoDB, S3, Vault, and TileDB settings.

    Attributes:
        dask: Configuration for Dask distributed computing.
        mongo: Configuration for MongoDB storage.
        s3: Configuration for S3 storage.
        vault: Configuration for HashiCorp Vault.
        tiledb: Configuration for TileDB storage.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        log_file: Path to the log file.
        data_dir: Path to the data directory.
        config_dir: Path to the configuration directory.
    """

    dask: DaskConfig = field(default_factory=DaskConfig)
    mongo: MongoConfig = field(default_factory=MongoConfig)
    s3: S3Config = field(default_factory=S3Config)
    vault: VaultConfig = field(default_factory=VaultConfig)
    tiledb: TileDBConfig = field(default_factory=TileDBConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)

    # Logging and paths
    log_level: str = "INFO"
    log_file: Path | None = None
    data_dir: Path | None = None
    config_dir: Path | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the configuration to a dictionary."""
        return {
            "dask": self.dask.__dict__,
            "mongo": self.mongo.__dict__,
            "s3": self.s3.__dict__,
            "vault": self.vault.__dict__,
            "tiledb": self.tiledb.__dict__,
            "auth": self.auth.__dict__,
            "log_level": self.log_level,
            "log_file": str(self.log_file) if self.log_file else None,
            "data_dir": str(self.data_dir) if self.data_dir else None,
            "config_dir": str(self.config_dir) if self.config_dir else None,
        }

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "GWASStudioConfig":
        """Create a GWASStudioConfig from a dictionary."""
        return cls(
            dask=DaskConfig(**config_dict.get("dask", {})),
            mongo=MongoConfig(**config_dict.get("mongo", {})),
            s3=S3Config(**config_dict.get("s3", {})),
            vault=VaultConfig(**config_dict.get("vault", {})),
            tiledb=TileDBConfig(**config_dict.get("tiledb", {})),
            auth=AuthConfig(**config_dict.get("auth", {})),
            log_level=config_dict.get("log_level", "INFO"),
            log_file=Path(config_dict["log_file"]) if config_dict.get("log_file") else None,
            data_dir=Path(config_dict["data_dir"]) if config_dict.get("data_dir") else None,
            config_dir=Path(config_dict["config_dir"]) if config_dict.get("config_dir") else None,
        )
