"""
GWASStudio CLI Utilities
=====================================================

This module provides utility functions for the CLI that work with GWASStudioConfig.
These functions are designed to be used with the new core configuration system.

Lightweight helpers (:func:`create_config_from_context`, :func:`mongo_conn_info`,
:func:`load_yaml_file`, :func:`parse_uri`) live in :mod:`gwasstudio.cli.context`
and table writers (:func:`write_table`, :func:`write_if_not_empty`) live in
:mod:`gwasstudio.cli.table_io`; they are re-exported here for backward
compatibility.
"""

import gzip
import pathlib
from pathlib import Path

import numpy as np
import pandas as pd
import tiledb

from gwasstudio import logger
from gwasstudio.cli.context import (  # noqa: F401
    _resolve_s3_config,
    create_config_from_context,
    load_yaml_file,
    mongo_conn_info,
    parse_uri,
)
from gwasstudio.cli.table_io import (  # noqa: F401
    write_if_not_empty,
    write_table,
)
from gwasstudio.core import Hashing, InvalidInputError, MetadataEnum, StorageError


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
        logger.debug(f"Processing file: {file_path}")
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
                return _read_parquet(file_path, required_cols, dtype_spec, file_path)
            elif suffix == ".gz":
                return _read_tsv_gz(file_path, required_cols, dtype_spec, file_path)
            raise ValueError("Unsupported file format. Only .parquet and .tsv.gz are supported.")
        except Exception as e:
            raise ValueError(f"Error reading file {file_path}: {str(e)}") from e

    def _read_parquet(
        file_path: pathlib.Path, required_cols: set, dtype_spec: dict, origin_path: pathlib.Path
    ) -> pd.DataFrame:
        """Read parquet file with validation."""
        df = pd.read_parquet(file_path, columns=list(required_cols))
        missing_cols = required_cols - set(df.columns)
        if missing_cols:
            raise ValueError(f"Missing required columns in parquet file: {missing_cols}")
        return _apply_dtypes(df, dtype_spec, origin_path)

    def _read_tsv_gz(
        file_path: pathlib.Path, required_cols: set, dtype_spec: dict, origin_path: pathlib.Path
    ) -> pd.DataFrame:
        """Read gzipped TSV file with validation."""
        with gzip.open(file_path, "rt") as f:
            header = set(f.readline().strip().split("\t"))
            missing_cols = required_cols - header
            if missing_cols:
                raise ValueError(f"Missing required columns in tsv.gz file: {missing_cols}")
        return _apply_dtypes(
            pd.read_csv(file_path, compression="gzip", sep="\t", usecols=list(required_cols)), dtype_spec, origin_path
        )

    def _apply_dtypes(df: pd.DataFrame, dtype_spec: dict, file_path: pathlib.Path) -> pd.DataFrame:
        """Apply dtype conversions to dataframe."""
        cols_to_convert = {col: dtype for col, dtype in dtype_spec.items() if col in df.columns}
        if not cols_to_convert:
            return df
        try:
            return df.astype(cols_to_convert)
        except (ValueError, TypeError) as e:
            # Provide detailed error context
            for col, dtype in cols_to_convert.items():
                unique_values = df[col].dropna().unique()[:5]  # Sample first 5 unique values
                if df[col].dtype != dtype:
                    raise ValueError(
                        f"Failed to convert column '{col}' to {dtype} in file {file_path}. "
                        f"Found values: {list(unique_values)}. Error: {e}"
                    ) from e
            raise ValueError(f"Failed to apply dtypes in file {file_path}: {e}") from e

    # Main processing
    df = read_sumstat_file(file_path, additional_columns or [])
    df["TRAITID"] = Hashing().compute_hash(file_path)

    # Store in TileDB
    try:
        ctx = tiledb.Ctx(tiledb.Config(cfg))
        tiledb.from_pandas(
            uri=uri,
            dataframe=df,
            index_dims=["CHR", "TRAITID", "POS"],
            mode="append",
            ctx=ctx,
        )
    except Exception as e:
        raise StorageError(f"Failed to write file {file_path} to TileDB at {uri}: {str(e)}") from e
