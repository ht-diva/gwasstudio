"""
GWASStudio Core Module
======================

This is the main __init__.py for the GWASStudio core module.
It exposes all public APIs for the core functionality.
"""

# Import configuration classes
from gwasstudio.core.config import (
    GWASStudioConfig,
    DaskConfig,
    MongoConfig,
    S3Config,
    VaultConfig,
    TileDBConfig,
)

#
# # Import storage backends
from gwasstudio.core.storage import (
    StorageBackend,
    #     TileDBStorage,
    MongoDBStorage,
    #     S3Storage,
    StorageError,
)

#
# # Import core functionality
# from gwasstudio.core.ingestion import (
#     ingest_summary_stats,
#     ingest_metadata,
#     IngestionError,
#     InvalidInputError,
#     StorageError as IngestionStorageError,
# )
#
from gwasstudio.core.query import (
    #     query_metadata,
    #     query_data,
    #     query_data_stream,
    list_projects,
    QueryError,
    #     ProjectNotFoundError,
    #     InvalidQueryError,
)
#
# from gwasstudio.core.export import (
#     export_data,
#     export_data_stream,
#     export_region_data,
#     export_metadata,
#     ExportError,
#     InvalidExportError,
# )
#
# from gwasstudio.core.plotting import (
#     generate_manhattan_plot,
#     generate_qq_plot,
#     generate_meta_analysis_plot,
#     PlottingError,
#     InvalidDataError,
# )

# Import exceptions
from gwasstudio.core.exceptions import (
    GWASStudioError,
    ConfigurationError,
    InvalidConfigError,
    DaskError,
    ClusterError,
    AuthenticationError,
    VaultError,
    PermissionError,
    IngestionError,
    InvalidInputError,
)

# Re-export storage errors for backward compatibility
StorageError = StorageError

# Define public API
__all__ = [
    # Configuration
    "GWASStudioConfig",
    "DaskConfig",
    "MongoConfig",
    "S3Config",
    "VaultConfig",
    "TileDBConfig",
    # Storage
    "StorageBackend",
    # "TileDBStorage",
    "MongoDBStorage",
    # "S3Storage",
    # Ingestion
    # "ingest_summary_stats",
    # "ingest_metadata",
    # Query
    # "query_metadata",
    # "query_data",
    # "query_data_stream",
    "list_projects",
    # Export
    # "export_data",
    # "export_data_stream",
    # "export_region_data",
    # "export_metadata",
    # Plotting
    # "generate_manhattan_plot",
    # "generate_qq_plot",
    # "generate_meta_analysis_plot",
    # Exceptions
    "GWASStudioError",
    "IngestionError",
    "InvalidInputError",
    "StorageError",
    "QueryError",
    # "ProjectNotFoundError",
    # "InvalidQueryError",
    # "ExportError",
    # "InvalidExportError",
    # "PlottingError",
    # "InvalidDataError",
    "ConfigurationError",
    "InvalidConfigError",
    "DaskError",
    "ClusterError",
    "AuthenticationError",
    "VaultError",
    "PermissionError",
]
