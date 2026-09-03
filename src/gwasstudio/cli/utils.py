"""
GWASStudio CLI Utilities
=====================================================

This module provides utility functions for the CLI that work with GWASStudioConfig.
These functions are designed to be used with the new core configuration system.
"""

import gzip
import pathlib
import urllib.parse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import tiledb
import yaml
import re

from gwasstudio import logger
from gwasstudio.core import (
    DaskConfig,
    GWASStudioConfig,
    Hashing,
    InvalidInputError,
    MetadataEnum,
    MongoConfig,
    S3Config,
    TileDBConfig,
    VaultConfig,
)


def mongo_conn_info(config: "GWASStudioConfig") -> tuple[str | None, str | None]:
    """
    Get MongoDB connection URI and database name from GWASStudioConfig or Vault.

    Priority:
    1. Direct URI from config.mongo.uri
    2. URI from Vault (if configured)

    Args:
        config: GWASStudio configuration object.

    Returns:
        tuple[str | None, str | None]: (connection_uri, database_name)
    """
    # Try direct URI first
    if uri := getattr(config.mongo, "uri", None):
        _, _, db_name = parse_uri(uri)
        return uri, db_name.replace("/", "")

    # Fall back to Vault if configured
    vault = getattr(config, "vault", None)
    if vault and all(getattr(vault, attr) for attr in ("path", "token", "url")):
        try:
            from hvac.exceptions import Forbidden, VaultError

            from gwasstudio.utils.vault import get_config_from_vault

            mongo_config = get_config_from_vault("mongo", vault)
            if uri := mongo_config.get("uri"):
                _, _, db_name = parse_uri(uri)
                return uri, db_name.replace("/", "")
        except ImportError as e:
            logger.opt(exception=True).error("Failed to import Vault client library - please ensure hvac is installed")
            raise
        except Forbidden as e:
            logger.opt(exception=True).error(
                "Vault access forbidden - check your Vault token and permissions. "
                "Verify the token has access to path '{}'".format(getattr(vault, "path", "unknown"))
            )
            raise
        except VaultError as e:
            logger.opt(exception=True).error("Vault operation failed - check Vault server status and configuration")
            raise
        except Exception as e:
            logger.opt(exception=True).error("Unexpected error while retrieving MongoDB URI from Vault")
            raise

    return None, None


def create_config_from_context(ctx) -> GWASStudioConfig:
    """
    Create a GWASStudioConfig object from the Click context.

    Extracts configuration from the Click context (ctx.obj) and creates a
    standardized GWASStudioConfig object for use by core modules.

    Args:
        ctx: Click context object.

    Returns:
        GWASStudioConfig: Configuration object for GWASStudio core.
    """
    # Extract configuration from context
    mongo_config = ctx.obj.get("mongo", {})
    dask_config = ctx.obj.get("dask", {})
    s3_config = ctx.obj.get("tiledb", {})
    vault_config = ctx.obj.get("vault", {})

    # Create GWASStudioConfig with extracted values
    config = GWASStudioConfig(
        dask=DaskConfig(
            deployment=dask_config.get("deployment", "local"),
            workers=dask_config.get("workers", 2),
            cores_per_worker=dask_config.get("cores_per_worker", 2),
            memory_per_worker=dask_config.get("memory_per_worker", "4GiB"),
            walltime=dask_config.get("walltime", "12:00:00"),
            batch_size=dask_config.get("batch_size", 0),
            interface=dask_config.get("interface"),
            gw_address=dask_config.get("gw_address"),
            gw_image=dask_config.get("gw_image"),
            job_script_prologue=dask_config.get("job_script_prologue", []),
            python=dask_config.get("python"),
            local_directory=Path(dask_config.get("local_directory")) if dask_config.get("local_directory") else None,
        ),
        mongo=MongoConfig(uri=mongo_config.get("uri"), db_name="gwasstudio"),
        s3=S3Config(
            aws_access_key_id=s3_config.get("vfs.s3.aws_access_key_id"),
            aws_secret_access_key=s3_config.get("vfs.s3.aws_secret_access_key"),
            endpoint_override=s3_config.get("vfs.s3.endpoint_override"),
            use_virtual_addressing=s3_config.get("vfs.s3.use_virtual_addressing", False),
            scheme=s3_config.get("vfs.s3.scheme", "https"),
            region=s3_config.get("vfs.s3.region", ""),
            verify_ssl=s3_config.get("vfs.s3.verify_ssl", False),
            connect_timeout_ms=s3_config.get("vfs.s3.connect_timeout_ms", 30000),
            request_timeout_ms=s3_config.get("vfs.s3.request_timeout_ms", 300000),
        ),
        vault=VaultConfig(
            auth=vault_config.get("auth", "basic"),
            mount_point=vault_config.get("mount_point", "secret"),
            path=vault_config.get("path"),
            token=vault_config.get("token"),
            url=vault_config.get("url"),
        ),
        tiledb=TileDBConfig(
            vfs_config=s3_config,  # Reuse S3 config for TileDB,
            # TileDB configuration
            # https://cloud.tiledb.com/academy/structure/arrays/tutorials/basics/configuration/index.html
            sm_config={"sm.dedup_coords": "false"},
        ),
    )

    # Update mongo config with resolved connection info
    if (uri := mongo_conn_info(config)[0]) is not None:
        config.mongo.uri = uri
        config.mongo.db_name = mongo_conn_info(config)[1]

    return config


def parse_uri(uri: str) -> tuple[str, str, str]:
    try:
        parsed = urllib.parse.urlparse(uri)
        scheme, netloc, path = parsed.scheme, parsed.netloc, parsed.path
        if scheme in ["s3", "https"]:
            path = path.strip("/")
        return scheme, netloc, path
    except ValueError as e:
        raise ValueError(f"Invalid URI: {uri}") from e


def load_metadata(file_path: Path, delimiter: str = "\t") -> pd.DataFrame:
    """Load metadata from a file in tabular format."""
    try:
        logger.info(f"Processing {file_path}")
        return pd.read_csv(file_path, sep=delimiter, dtype=MetadataEnum.get_all_dtypes_dict(), dtype_backend="pyarrow")
    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
        raise ValueError("File not found")
    except pd.errors.EmptyDataError:
        logger.error("No data found in the file. Please check the file content.")
        raise ValueError("No data found in the file")
    except pd.errors.ParserError:
        logger.error("Error parsing the file. Please check the file format.")
        raise ValueError("Error parsing the file")


# def ingest_metadata(df: pd.DataFrame, mongo_uri: str = None) -> None:
#     """Ingest data into the MongoDB collection."""
#
#     def _document_generator(df):
#         for row in df.itertuples(index=False):
#             yield process_metadata_dict(row)
#
#     logger.info("Starting metadata ingestion")
#     rows = len(df.axes[0])
#     processed_rows = 0
#     logger.info(f"{rows} documents to ingest")
#
#     # Helper that creates and saves a single document
#     def _save_document(doc):
#         obj = EnhancedDataProfile(uri=mongo_uri, **doc)
#         obj.save()
#         return 1  # count of one processed row
#
#     for document in _document_generator(df):
#         processed_rows += _save_document(document)
#
#         # Print the row counter every 100 rows
#         if processed_rows % 100 == 0:
#             logger.info(f"{processed_rows} documents processed")


# def ingest_metadata_bulk(df: pd.DataFrame, mongo_uri: str = None, batch_size: int = 1000) -> None:
#     """Ingest data into the MongoDB collection in bulk using generator pattern."""
#
#     def _document_generator(df):
#         for row in df.itertuples(index=False):
#             yield process_metadata_dict(row)
#
#     logger.info("Starting bulk metadata ingestion")
#     rows = len(df.axes[0])
#     logger.info(f"{rows} documents to ingest")
#
#     # Process in batches to avoid memory issues
#     processed = 0
#
#     batch = []
#     for i, document in enumerate(_document_generator(df), 1):
#         batch.append(document)
#
#         print(batch)
#
#         if i % batch_size == 0:
#             result = EnhancedDataProfile.bulk_create(batch, mongo_uri, batch_size=batch_size)
#             if "invalid_documents" in result:
#                 for invalid in result["invalid_documents"]:
#                     logger.error(f"Invalid document: {invalid}")
#             processed += result["total"]
#             logger.info(f"Processed batch: {processed}/{rows} documents")
#             batch = []
#
#     # Process remaining documents
#     if batch:
#         result = EnhancedDataProfile.bulk_create(batch, mongo_uri, batch_size=batch_size)
#         if "invalid_documents" in result:
#             for invalid in result["invalid_documents"]:
#                 logger.error(f"Invalid document: {invalid}")
#
#         processed += result["total"]
#         logger.info(f"Processed final batch: {processed}/{rows} documents")
#
#     logger.info(f"Bulk ingestion complete: {processed} documents processed")


def _get_valid_metadata_columns() -> list[str]:
    """
    Get the list of valid metadata fields from MetadataEnum.

    Returns:
        List[str]: List of valid metadata field names.
    """
    return [field.get_value() for field in MetadataEnum]


def validate_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Validate that the DataFrame contains all required columns and no invalid columns.

    Args:
        df: pandas DataFrame containing metadata to validate

    Returns:
        pandas DataFrame: The input DataFrame if validation passes

    Raises:
        InvalidInputError: If required columns are missing or invalid columns are present
    """
    # Validate columns using core exception
    required_columns = MetadataEnum.required_fields()
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise InvalidInputError(f"Missing column(s) in the input file: {', '.join(missing_cols)}")

    valid_columns = _get_valid_metadata_columns()
    invalid_columns = []
    for column in df.columns:
        if column not in valid_columns:
            invalid_columns.append(column)

    if invalid_columns:
        raise InvalidInputError(f"Invalid column(s) in the input file: {', '.join(invalid_columns)}")

    return df


def load_yaml_file(file_path: str) -> dict[str, Any]:
    """
    Load and parse a YAML file.

    Args:
        file_path: Path to the YAML file.

    Returns:
        Dict[str, Any]: Parsed YAML content.

    Raises:
        InvalidInputError: If the file doesn't exist or is invalid YAML.
    """
    try:
        logger.info(f"Processing {file_path}")
        with open(file_path, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise InvalidInputError(f"Input file not found: {file_path}")
    except yaml.YAMLError as e:
        raise InvalidInputError(f"Invalid YAML in input file: {str(e)}")


def write_table(
    df: pd.DataFrame,
    where: str,
    logger: object,
    compression: bool = True,
    file_format: str = "parquet",
    log_msg: str = "none",
    **kwargs,
):
    """
    Writes the given DataFrame to a specified location on disk in the desired format. Three file
    formats are supported: "parquet", "csv.gz" and "csv". The function handles file compression for
    "parquet" format. Logs a custom or default message indicating the status.

    :param logger: The logger object used for logging messages.
    :param df: The pandas DataFrame to be saved.
    :param where: Destination file path, without extension, where the file should be saved.
    :param compression: Compression flag indicating whether to compress the file.
    :param file_format: File format to save the data, either "parquet", "csv.gz", or "csv". Default is "parquet".
    :param log_msg: Custom log message. If "none", a default message will be logged. Default is "none".
    :param kwargs: Any additional keyword arguments to be passed to the underlying `to_parquet` or
        `to_csv` pandas methods.
    :return: None
    """
    # Check if format is valid
    if file_format not in ["parquet", "csv.gz", "csv"]:
        raise ValueError("Format must be either 'parquet', 'csv.gz', or 'csv'")

    # Sanitize filename by replacing whitespaces
    file_name = re.sub(r"\s+", "_", Path(where).name.strip())

    # Create the full path by joining the output directory and sanitized filename with extension
    output_path = Path(where).with_name(f"{file_name}.{file_format}")

    msg = log_msg if log_msg != "none" else f"Saving DataFrame to {output_path}"
    logger.info(msg)

    if df.empty:
        logger.warning(f"DataFrame is empty while writing to {output_path}")

    if file_format == "parquet":
        compression_to_use = "snappy" if compression else None
        df.to_parquet(output_path, compression=compression_to_use, **kwargs)
    elif file_format == "csv.gz":
        compression_to_use = {"method": "gzip", "compresslevel": 1, "mtime": 1} if compression else None
        df.to_csv(output_path, compression=compression_to_use, **kwargs)
    else:
        df.to_csv(output_path, **kwargs)


def write_if_not_empty(
    df: pd.DataFrame,
    where: str,
    logger: object,
    compression: bool = True,
    file_format: str = "parquet",
    log_msg: str = "none",
    **kwargs,
):
    """
    Write the DataFrame only if it is not empty.
    Returns None if the DataFrame is empty.
    """
    if df is None or df.empty:
        return None

    return write_table(
        df=df,
        where=where,
        logger=logger,
        compression=compression,
        file_format=file_format,
        log_msg=log_msg,
        **kwargs,
    )


# def process_and_ingest(file_path: str, uri: str, cfg: dict, ingest_pval: bool) -> None:
#     """
#     Process a single file and ingest it in a TileDB
#
#     Args:
#         file_path (str): The path where the file to ingest is stored
#         uri (str): The path where the TileDB is stored.
#         cfg (dict): A configuration dictionary to use for connecting to S3.
#     """
#
#     def read_sumstat_file(file_path, ingest_pval=False):
#         file_path = pathlib.Path(file_path)
#         required_cols = ["CHR", "POS", "EA", "NEA", "EAF", "SE", "BETA"]
#         types = {
#             "CHR": np.uint8,
#             "POS": np.uint32,
#             "EA": str,
#             "NEA": str,
#             "EAF": np.float32,
#             "SE": np.float32,
#             "BETA": np.float32,
#         }
#         if ingest_pval:
#             required_cols.append("MLOG10P")
#             types["MLOG10P"] = np.float32
#
#         suffix = file_path.suffix.lower()
#
#         if suffix == ".parquet":
#             parquet_file = pd.read_parquet(file_path, columns=required_cols)
#             missing_cols = [col for col in required_cols if col not in parquet_file.columns]
#             if missing_cols:
#                 raise ValueError(f"Missing required columns in parquet file: {missing_cols}")
#             df = parquet_file
#         elif suffix == ".gz":
#             with gzip.open(file_path, "rt") as f:
#                 header = f.readline().strip().split("\t")
#                 missing_cols = [col for col in required_cols if col not in header]
#                 if missing_cols:
#                     raise ValueError(f"Missing required columns in tsv.gz file: {missing_cols}")
#             df = pd.read_csv(file_path, compression="gzip", sep="\t", usecols=required_cols)
#         else:
#             raise ValueError("Unsupported file format. Only .parquet and .tsv.gz are supported.")
#
#         # Apply type conversion after reading
#         for col, dtype in types.items():
#             if col in df.columns:
#                 df[col] = df[col].astype(dtype)
#
#         return df
#
#     df = read_sumstat_file(file_path, ingest_pval)
#
#     # Add trait_id based on the checksum_dict
#     hg = Hashing()
#     df["TRAITID"] = hg.compute_hash(file_path)
#     # Store the processed data in TileDB
#     ctx = tiledb.Ctx(tiledb.Config(cfg))
#     tiledb.from_pandas(
#         uri=uri,
#         dataframe=df,
#         index_dims=["CHR", "TRAITID", "POS"],
#         mode="append",
#         ctx=ctx,
#     )


def process_and_ingest(file_path: str, uri: str, cfg: dict, additional_columns: list[str] = None) -> None:
    """
    Process a single file and ingest it into a TileDB array.

    Args:
        file_path: Path to the input file (supports .parquet and .tsv.gz)
        uri: Path where the TileDB array will be stored
        cfg: Configuration dictionary for S3 connection
        additional_columns: List of additional column names to include (default: None)
    """

    def read_sumstat_file(file_path: str, additional_columns: list[str]) -> pd.DataFrame:
        """Read and validate summary statistics file."""
        file_path = pathlib.Path(file_path)
        required_cols = {"CHR", "POS", "EA", "NEA", "EAF", "SE", "BETA"}
        dtype_spec = {
            "CHR": np.uint8,
            "POS": np.uint32,
            "EA": str,
            "NEA": str,
            "EAF": np.float32,
            "SE": np.float32,
            "BETA": np.float32,
        }

        # Handle additional columns
        if additional_columns:
            required_cols.update(additional_columns)
            for col in additional_columns:
                if col == "MLOG10P":
                    dtype_spec[col] = np.float32
                elif col == "N":
                    dtype_spec[col] = np.uint32
                # Add more column type specifications as needed

        suffix = file_path.suffix.lower()

        try:
            if suffix == ".parquet":
                return _read_parquet(file_path, required_cols, dtype_spec)
            elif suffix == ".gz":
                return _read_tsv_gz(file_path, required_cols, dtype_spec)
            raise ValueError("Unsupported file format. Only .parquet and .tsv.gz are supported.")
        except Exception as e:
            raise ValueError(f"Error reading file {file_path}: {str(e)}") from e

    def _read_parquet(file_path: pathlib.Path, required_cols: set, dtype_spec: dict) -> pd.DataFrame:
        """Read parquet file with validation."""
        df = pd.read_parquet(file_path, columns=list(required_cols))
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns in parquet file: {missing_cols}")
        return _apply_dtypes(df, dtype_spec)

    def _read_tsv_gz(file_path: pathlib.Path, required_cols: set, dtype_spec: dict) -> pd.DataFrame:
        """Read gzipped TSV file with validation."""
        with gzip.open(file_path, "rt") as f:
            header = set(f.readline().strip().split("\t"))
            missing_cols = required_cols - header
            if missing_cols:
                raise ValueError(f"Missing required columns in tsv.gz file: {missing_cols}")
        return _apply_dtypes(
            pd.read_csv(file_path, compression="gzip", sep="\t", usecols=list(required_cols)), dtype_spec
        )

    def _apply_dtypes(df: pd.DataFrame, dtype_spec: dict) -> pd.DataFrame:
        """Apply dtype conversions to dataframe."""
        return df.astype({col: dtype for col, dtype in dtype_spec.items() if col in df.columns})

    # Main processing
    df = read_sumstat_file(file_path, additional_columns or [])
    df["TRAITID"] = Hashing().compute_hash(file_path)

    # Store in TileDB
    ctx = tiledb.Ctx(tiledb.Config(cfg))
    tiledb.from_pandas(
        uri=uri,
        dataframe=df,
        index_dims=["CHR", "TRAITID", "POS"],
        mode="append",
        ctx=ctx,
    )
