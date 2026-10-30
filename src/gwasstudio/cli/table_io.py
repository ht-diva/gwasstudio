"""
GWASStudio CLI table output helpers
=====================================================

DataFrame-to-file writers shared by CLI commands. Kept separate from
:mod:`gwasstudio.cli.utils` so that commands which only write tables
(e.g. ``meta-query``) don't import TileDB.
"""

import re
from pathlib import Path

import pandas as pd


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
