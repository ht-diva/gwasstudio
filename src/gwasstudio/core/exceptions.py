"""
GWASStudio Core Exceptions Module
==================================

This module defines custom exceptions for GWASStudio core functionality.
All exceptions inherit from GWASStudioError for easy catching.
"""


class GWASStudioError(Exception):
    """Base exception class for GWASStudio errors."""

    def __init__(self, message: str, code: int = 500, details: dict = None):
        """
        Initialize a GWASStudio error.

        Args:
            message: Error message.
            code: Error code (HTTP-like, for API consistency).
            details: Additional error details.
        """
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def to_dict(self) -> dict:
        """Convert the exception to a dictionary for API responses."""
        return {
            "error": {
                "message": self.message,
                "code": self.code,
                "details": self.details,
            }
        }


# --- Ingestion Errors ---
class IngestionError(GWASStudioError):
    """Base exception for data ingestion errors."""

    pass


class InvalidInputError(IngestionError):
    """Exception raised for invalid input files or data."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=400, details=details)


class StorageError(GWASStudioError):
    """Base exception for storage-related errors."""

    pass


class TileDBError(StorageError):
    """Exception raised for TileDB-specific errors."""

    pass


class MongoDBError(StorageError):
    """Exception raised for MongoDB-specific errors."""

    pass


# --- Query Errors ---
class QueryError(GWASStudioError):
    """Base exception for data query errors."""

    pass


class ProjectNotFoundError(QueryError):
    """Exception raised when a project is not found."""

    def __init__(self, message: str, project_id: str = None, details: dict = None):
        details = details or {}
        if project_id:
            details["project_id"] = project_id
        super().__init__(message, code=404, details=details)


class InvalidQueryError(QueryError):
    """Exception raised for invalid query parameters."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=400, details=details)


# --- Export Errors ---
class ExportError(GWASStudioError):
    """Base exception for data export errors."""

    pass


class InvalidExportError(ExportError):
    """Exception raised for invalid export parameters."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=400, details=details)


# --- Plotting Errors ---
class PlottingError(GWASStudioError):
    """Base exception for plotting errors."""

    pass


class InvalidDataError(PlottingError):
    """Exception raised for invalid data in plotting functions."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=400, details=details)


# --- Configuration Errors ---
class ConfigurationError(GWASStudioError):
    """Base exception for configuration errors."""

    pass


class InvalidConfigError(ConfigurationError):
    """Exception raised for invalid configuration."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=400, details=details)


# --- Dask Errors ---
class DaskError(GWASStudioError):
    """Base exception for Dask-related errors."""

    pass


class ClusterError(DaskError):
    """Exception raised for Dask cluster errors."""

    pass


# --- Authentication Errors ---
class AuthenticationError(GWASStudioError):
    """Base exception for authentication errors."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=401, details=details)


class VaultError(AuthenticationError):
    """Exception raised for Vault-related errors."""

    pass


# --- Permission Errors ---
class PermissionError(GWASStudioError):
    """Base exception for permission errors."""

    def __init__(self, message: str, details: dict = None):
        super().__init__(message, code=403, details=details)
