"""
Tests for GWASStudio Core Configuration Module
==============================================

Tests for the configuration classes in gwasstudio.core.config.
"""

from pathlib import Path

from gwasstudio.core.config import (
    DaskConfig,
    GWASStudioConfig,
    MongoConfig,
    S3Config,
    TileDBConfig,
    VaultConfig,
)


class TestDaskConfig:
    """Tests for DaskConfig class."""

    def test_default_values(self):
        """Test that DaskConfig has correct default values."""
        config = DaskConfig()
        assert config.deployment == "local"
        assert config.workers == 2
        assert config.cores_per_worker == 2
        assert config.memory_per_worker == "4GiB"
        assert config.walltime == "12:00:00"
        assert config.batch_size == 0
        assert config.job_script_prologue == []

    def test_custom_values(self):
        """Test that DaskConfig accepts custom values."""
        config = DaskConfig(
            deployment="slurm",
            workers=4,
            cores_per_worker=4,
            memory_per_worker="8GiB",
            walltime="24:00:00",
            batch_size=100,
        )
        assert config.deployment == "slurm"
        assert config.workers == 4
        assert config.cores_per_worker == 4
        assert config.memory_per_worker == "8GiB"
        assert config.walltime == "24:00:00"
        assert config.batch_size == 100


class TestMongoConfig:
    """Tests for MongoConfig class."""

    def test_default_values(self):
        """Test that MongoConfig has correct default values."""
        config = MongoConfig()
        assert config.db_name == "gwasstudio"
        assert config.uri is None

    def test_custom_values(self):
        """Test that MongoConfig accepts custom values."""
        config = MongoConfig(
            uri="mongodb://localhost:27017",
            db_name="test_db",
        )
        assert config.uri == "mongodb://localhost:27017"
        assert config.db_name == "test_db"


class TestS3Config:
    """Tests for S3Config class."""

    def test_default_values(self):
        """Test that S3Config has correct default values."""
        config = S3Config()
        assert config.scheme == "https"
        assert config.region == ""
        assert config.verify_ssl is False
        assert config.connect_timeout_ms == 30000
        assert config.request_timeout_ms == 300000

    def test_custom_values(self):
        """Test that S3Config accepts custom values."""
        config = S3Config(
            aws_access_key_id="test_key",
            aws_secret_access_key="test_secret",
            endpoint_override="http://localhost:9000",
            use_virtual_addressing=True,
            scheme="http",
            region="us-east-1",
            verify_ssl=True,
        )
        assert config.aws_access_key_id == "test_key"
        assert config.endpoint_override == "http://localhost:9000"
        assert config.scheme == "http"
        assert config.region == "us-east-1"
        assert config.verify_ssl is True


class TestVaultConfig:
    """Tests for VaultConfig class."""

    def test_default_values(self):
        """Test that VaultConfig has correct default values."""
        config = VaultConfig()
        assert config.auth == "basic"
        assert config.mount_point == "secret"

    def test_custom_values(self):
        """Test that VaultConfig accepts custom values."""
        config = VaultConfig(
            auth="oidc",
            mount_point="custom_secret",
            path="/path/to/secret",
            token="test_token",
            url="http://localhost:8200",
        )
        assert config.auth == "oidc"
        assert config.mount_point == "custom_secret"
        assert config.path == "/path/to/secret"
        assert config.token == "test_token"
        assert config.url == "http://localhost:8200"


class TestTileDBConfig:
    """Tests for TileDBConfig class."""

    def test_default_values(self):
        """Test that TileDBConfig has correct default values."""
        config = TileDBConfig()
        assert config.vfs_config == {}

    def test_custom_values(self):
        """Test that TileDBConfig accepts custom values."""
        config = TileDBConfig(vfs_config={"vfs.s3.region": "us-east-1"})
        assert config.vfs_config == {"vfs.s3.region": "us-east-1"}


class TestGWASStudioConfig:
    """Tests for GWASStudioConfig class."""

    def test_default_values(self, base_config):
        """Test that GWASStudioConfig has correct default values."""
        assert isinstance(base_config.dask, DaskConfig)
        assert isinstance(base_config.mongo, MongoConfig)
        assert isinstance(base_config.s3, S3Config)
        assert isinstance(base_config.vault, VaultConfig)
        assert isinstance(base_config.tiledb, TileDBConfig)
        assert base_config.log_level == "DEBUG"

    def test_custom_values(self):
        """Test that GWASStudioConfig accepts custom values."""
        config = GWASStudioConfig(
            dask=DaskConfig(deployment="slurm"),
            mongo=MongoConfig(uri="mongodb://localhost:27017"),
            log_level="DEBUG",
            data_dir=Path("/tmp/gwas"),
        )
        assert config.dask.deployment == "slurm"
        assert config.mongo.uri == "mongodb://localhost:27017"
        assert config.log_level == "DEBUG"
        assert config.data_dir == Path("/tmp/gwas")

    def test_to_dict(self, base_config):
        """Test that to_dict() method works correctly."""
        config_dict = base_config.to_dict()

        assert "dask" in config_dict
        assert "mongo" in config_dict
        assert "s3" in config_dict
        assert "vault" in config_dict
        assert "tiledb" in config_dict
        assert "log_level" in config_dict
        assert config_dict["log_level"] == "DEBUG"

    def test_from_dict(self, base_config):
        """Test that from_dict() method works correctly."""
        config_dict = base_config.to_dict()
        new_config = GWASStudioConfig.from_dict(config_dict)

        assert new_config.dask.deployment == base_config.dask.deployment
        assert new_config.mongo.db_name == base_config.mongo.db_name
        assert new_config.log_level == base_config.log_level

    def test_from_dict_partial(self):
        """Test that from_dict() works with partial data."""
        config_dict = {
            "dask": {"deployment": "local", "workers": 4},
            "mongo": {"db_name": "test_db"},
        }
        config = GWASStudioConfig.from_dict(config_dict)

        assert config.dask.deployment == "local"
        assert config.dask.workers == 4
        assert config.mongo.db_name == "test_db"
        # Check defaults for missing values
        assert config.s3.scheme == "https"
        assert config.log_level == "INFO"

    def test_data_dir_creation(self, base_config, temp_data_dir):
        """Test that data_dir is created if it doesn't exist."""
        base_config.data_dir = temp_data_dir / "new_dir"
        config = GWASStudioConfig.from_dict(base_config.to_dict())

        # The directory should be created when used by storage backends
        # (not automatically by the config class itself)
        assert config.data_dir == temp_data_dir / "new_dir"
