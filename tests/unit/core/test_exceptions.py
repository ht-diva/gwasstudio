"""
Tests for GWASStudio Core Exceptions Module
===========================================

Tests for the custom exceptions in gwasstudio.core.exceptions.
"""

import pytest

from gwasstudio.core.exceptions import (
    AuthenticationError,
    ClusterError,
    ConfigurationError,
    DaskError,
    ExportError,
    GWASStudioError,
    IngestionError,
    InvalidConfigError,
    InvalidDataError,
    InvalidExportError,
    InvalidInputError,
    InvalidQueryError,
    MongoDBError,
    PermissionError,
    PlottingError,
    ProjectNotFoundError,
    QueryError,
    StorageError,
    TileDBError,
    VaultError,
)


class TestGWASStudioError:
    """Tests for GWASStudioError base class."""

    def test_base_exception(self):
        """Test that GWASStudioError can be instantiated."""
        error = GWASStudioError("Test error message")
        assert str(error) == "Test error message"
        assert error.message == "Test error message"
        assert error.code == 500
        assert error.details == {}

    def test_exception_with_code(self):
        """Test that GWASStudioError accepts a custom code."""
        error = GWASStudioError("Test error", code=400)
        assert error.code == 400

    def test_exception_with_details(self):
        """Test that GWASStudioError accepts custom details."""
        error = GWASStudioError("Test error", code=400, details={"key": "value", "project_id": "123"})
        assert error.details == {"key": "value", "project_id": "123"}

    def test_to_dict(self):
        """Test that to_dict() method works correctly."""
        error = GWASStudioError("Test error", code=404, details={"id": "123"})
        result = error.to_dict()

        assert result == {
            "error": {
                "message": "Test error",
                "code": 404,
                "details": {"id": "123"},
            }
        }


class TestIngestionError:
    """Tests for IngestionError class."""

    def test_ingestion_error(self):
        """Test that IngestionError inherits from GWASStudioError."""
        error = IngestionError("Ingestion failed")
        assert isinstance(error, GWASStudioError)
        assert isinstance(error, IngestionError)


class TestInvalidInputError:
    """Tests for InvalidInputError class."""

    def test_invalid_input_error(self):
        """Test that InvalidInputError has correct default code."""
        error = InvalidInputError("Invalid input file")
        assert isinstance(error, IngestionError)
        assert error.code == 400

    def test_invalid_input_error_with_details(self):
        """Test that InvalidInputError can include details."""
        error = InvalidInputError("Invalid input", details={"file": "test.parquet", "reason": "missing columns"})
        assert error.details == {"file": "test.parquet", "reason": "missing columns"}


class TestStorageError:
    """Tests for StorageError class."""

    def test_storage_error(self):
        """Test that StorageError can be instantiated."""
        error = StorageError("Storage error occurred")
        assert isinstance(error, GWASStudioError)


class TestTileDBError:
    """Tests for TileDBError class."""

    def test_tiledb_error(self):
        """Test that TileDBError inherits from StorageError."""
        error = TileDBError("TileDB error")
        assert isinstance(error, StorageError)
        assert isinstance(error, TileDBError)


class TestMongoDBError:
    """Tests for MongoDBError class."""

    def test_mongodb_error(self):
        """Test that MongoDBError inherits from StorageError."""
        error = MongoDBError("MongoDB error")
        assert isinstance(error, StorageError)
        assert isinstance(error, MongoDBError)


class TestQueryError:
    """Tests for QueryError class."""

    def test_query_error(self):
        """Test that QueryError can be instantiated."""
        error = QueryError("Query failed")
        assert isinstance(error, GWASStudioError)


class TestProjectNotFoundError:
    """Tests for ProjectNotFoundError class."""

    def test_project_not_found_error(self):
        """Test that ProjectNotFoundError has correct default code."""
        error = ProjectNotFoundError("Project not found", project_id="123")
        assert isinstance(error, QueryError)
        assert error.code == 404
        assert "project_id" in error.details
        assert error.details["project_id"] == "123"

    def test_project_not_found_error_without_id(self):
        """Test that ProjectNotFoundError works without project_id."""
        error = ProjectNotFoundError("Project not found")
        assert error.code == 404
        assert "project_id" not in error.details


class TestInvalidQueryError:
    """Tests for InvalidQueryError class."""

    def test_invalid_query_error(self):
        """Test that InvalidQueryError has correct default code."""
        error = InvalidQueryError("Invalid query parameters")
        assert isinstance(error, QueryError)
        assert error.code == 400


class TestExportError:
    """Tests for ExportError class."""

    def test_export_error(self):
        """Test that ExportError can be instantiated."""
        error = ExportError("Export failed")
        assert isinstance(error, GWASStudioError)


class TestInvalidExportError:
    """Tests for InvalidExportError class."""

    def test_invalid_export_error(self):
        """Test that InvalidExportError has correct default code."""
        error = InvalidExportError("Invalid export format")
        assert isinstance(error, ExportError)
        assert error.code == 400


class TestPlottingError:
    """Tests for PlottingError class."""

    def test_plotting_error(self):
        """Test that PlottingError can be instantiated."""
        error = PlottingError("Plotting failed")
        assert isinstance(error, GWASStudioError)


class TestInvalidDataError:
    """Tests for InvalidDataError class."""

    def test_invalid_data_error(self):
        """Test that InvalidDataError has correct default code."""
        error = InvalidDataError("Invalid data for plot")
        assert isinstance(error, PlottingError)
        assert error.code == 400


class TestConfigurationError:
    """Tests for ConfigurationError class."""

    def test_configuration_error(self):
        """Test that ConfigurationError can be instantiated."""
        error = ConfigurationError("Invalid configuration")
        assert isinstance(error, GWASStudioError)


class TestInvalidConfigError:
    """Tests for InvalidConfigError class."""

    def test_invalid_config_error(self):
        """Test that InvalidConfigError has correct default code."""
        error = InvalidConfigError("Invalid config value")
        assert isinstance(error, ConfigurationError)
        assert error.code == 400


class TestDaskError:
    """Tests for DaskError class."""

    def test_dask_error(self):
        """Test that DaskError can be instantiated."""
        error = DaskError("Dask error")
        assert isinstance(error, GWASStudioError)


class TestClusterError:
    """Tests for ClusterError class."""

    def test_cluster_error(self):
        """Test that ClusterError inherits from DaskError."""
        error = ClusterError("Cluster error")
        assert isinstance(error, DaskError)
        assert isinstance(error, ClusterError)


class TestAuthenticationError:
    """Tests for AuthenticationError class."""

    def test_authentication_error(self):
        """Test that AuthenticationError has correct default code."""
        error = AuthenticationError("Authentication failed")
        assert isinstance(error, GWASStudioError)
        assert error.code == 401


class TestVaultError:
    """Tests for VaultError class."""

    def test_vault_error(self):
        """Test that VaultError inherits from AuthenticationError."""
        error = VaultError("Vault error")
        assert isinstance(error, AuthenticationError)
        assert isinstance(error, VaultError)


class TestPermissionError:
    """Tests for PermissionError class."""

    def test_permission_error(self):
        """Test that PermissionError has correct default code."""
        error = PermissionError("Permission denied")
        assert isinstance(error, GWASStudioError)
        assert error.code == 403


class TestExceptionHierarchy:
    """Tests for the exception hierarchy."""

    def test_all_exceptions_inherit_from_base(self):
        """Test that all exceptions inherit from GWASStudioError."""
        exceptions = [
            IngestionError,
            InvalidInputError,
            StorageError,
            TileDBError,
            MongoDBError,
            QueryError,
            ProjectNotFoundError,
            InvalidQueryError,
            ExportError,
            InvalidExportError,
            PlottingError,
            InvalidDataError,
            ConfigurationError,
            InvalidConfigError,
            DaskError,
            ClusterError,
            AuthenticationError,
            VaultError,
            PermissionError,
        ]

        for exc_class in exceptions:
            exc = exc_class("Test message")
            assert isinstance(exc, GWASStudioError)

    def test_exception_catching(self):
        """Test that exceptions can be caught by their base class."""
        with pytest.raises(GWASStudioError):
            raise IngestionError("Test error")

        with pytest.raises(IngestionError):
            raise InvalidInputError("Test error")

        with pytest.raises(QueryError):
            raise ProjectNotFoundError("Test error", project_id="123")

        with pytest.raises(AuthenticationError):
            raise VaultError("Test error")
