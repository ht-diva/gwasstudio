"""
GWASStudio Configuration Module
===============================

This module defines the configuration classes for GWASStudio core functionality.
It centralizes all configuration options for Dask, MongoDB, S3, Vault, and TileDB.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


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
    db_name: str = "datahub"
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
            log_level=config_dict.get("log_level", "INFO"),
            log_file=Path(config_dict["log_file"]) if config_dict.get("log_file") else None,
            data_dir=Path(config_dict["data_dir"]) if config_dict.get("data_dir") else None,
            config_dir=Path(config_dict["config_dir"]) if config_dict.get("config_dir") else None,
        )
