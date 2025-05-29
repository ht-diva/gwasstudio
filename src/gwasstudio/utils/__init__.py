"""
Generic Utilities
=================
Generic utilities used by other modules.
"""

import pathlib
import random
import string
import urllib.parse
from typing import Any, Dict
from scipy import stats
import numpy as np
import pandas as pd
import tiledb

from gwasstudio.utils.hashing import Hashing

    
def check_file_exists(input_file: str, logger: object) -> bool:
    """
    Check if a file exists and log the appropriate message.

    Parameters:
    search_file (str): The path to the file to check.

    Returns:
    bool: True if the file exists, False otherwise.
    """
    msg_ok = f"Processing {input_file}"
    msg_err = f"{input_file} does not exist; exiting"

    if pathlib.Path(input_file).exists():
        logger.info(msg_ok)
        return True
    else:
        logger.error(msg_err)
        return False


def find_item(obj: Dict, key: str) -> Any:
    """
    Recursively search for a specific key in a dictionary.

    Args:
        obj (Dict): The dictionary to search in.
        key (str): The key to search for.

    Returns:
        Any: The value associated with the key if found, otherwise None.

    Notes:
        This function searches for the key in the dictionary and its nested dictionaries.
        If the key is found, the function returns the associated value.
        If the key is not found, the function returns None.

    """
    if key in obj:
        return obj[key]
    else:
        for k, v in obj.items():
            if isinstance(v, dict):
                result = find_item(v, key)
                if result is not None:
                    return result
        return None


def generate_random_word(length: int) -> str:
    """
    Generates a random word consisting of lowercase letters.

    Args:
        length (int): The number of characters to include in the generated word.

    Returns:
        str: A random word of the specified length.
    """
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


def lower_and_replace(text: str) -> str:
    """
    Replaces spaces in the input string with underscores and converts it to lowercase.

    Args:
        text (str): The input string to be modified.

    Returns:
        str: The modified string with spaces replaced by underscores and converted to lowercase.
    """
    return f"{text.lower().replace(' ', '_')}"


def parse_uri(uri: str) -> tuple[str, str, str]:
    try:
        parsed = urllib.parse.urlparse(uri)
        scheme, netloc, path = parsed.scheme, parsed.netloc, parsed.path
        if scheme in ["s3", "https"]:
            path = path.strip("/")
        return scheme, netloc, path
    except ValueError as e:
        raise ValueError(f"Invalid URI: {uri}") from e


# Define the TileDB array schema with SNP, gene, and population dimensions
def create_tiledb_schema(uri: str, cfg: dict, use_pval = False) -> None:
    """
    Create an empty schema for TileDB.

    Args:
        uri (str): The path where the TileDB will be stored.
        cfg (dict): A configuration dictionary to use for connecting to S3.
    """
    chrom_domain = (1, 24)
    pos_domain = (1, 250000000)
    
    dom = tiledb.Domain(
        tiledb.Dim(name="CHR", domain=chrom_domain, dtype=np.uint8, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
        tiledb.Dim(name="TRAITID", dtype="ascii", var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
        tiledb.Dim(name="POS", domain=pos_domain, dtype=np.uint32, var=False,filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])),
    )
    attr = [
        tiledb.Attr(
            name="BETA",
            dtype=np.float32,
            var=False,
            filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
        ),
        tiledb.Attr(
            name="SE",
            dtype=np.float32,
            var=False,
            filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
        ),
        tiledb.Attr(
            name="EAF",
            dtype=np.float32,
            var=False,
            filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
        ),
        tiledb.Attr(
            name="EA",
            dtype=str,
            var=False,
            filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
        ),
        tiledb.Attr(
            name="NEA",
            dtype=str,
            var=False,
            filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
        ),
    ]

    if use_pval:
        attr.append(
            tiledb.Attr(
                name="MLOG10P",
                dtype=np.float32,
                var=False,
                filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
            )
        )
        schema = tiledb.ArraySchema(
            domain=dom,
            sparse=True,
            allows_duplicates=True,
            attrs= attr
    )
    ctx = tiledb.Ctx(cfg)
    tiledb.Array.create(uri, schema, ctx=ctx)


def process_and_ingest(file_path: str, uri: str, cfg: dict, use_pval = False) -> None:
    """
    Process a single file and ingest it in a TileDB

    Args:
        file_path (str): The path where the file to ingest is stored
        uri (str): The path where the TileDB is stored.
        cfg (dict): A configuration dictionary to use for connecting to S3.
    """

    # Read file with Dask
    col_df = ["CHR", "POS", "EAF", "SE", "BETA", "EA", "NEA", "MLOG10P"]
    dtype_col = {
            "CHR": np.uint8,
            "POS": np.uint32,
            "EAF": np.float32,
            "EA": str,
            "NEA": str,
            "SE": np.float32,
            "BETA": np.float32,
            "MLOG10P": np.float32,
        }
    if use_pval:
        col_df.append("MLOG10P")
        dtype_col["MLOG10P"] = np.float32

    df = pd.read_csv(
            file_path,
            compression="gzip",
            sep="\t",
            usecols=col_df,
            dtype=dtype_col
        )
    # Add trait_id based on the checksum_dict
    hg = Hashing()
    df["TRAITID"] = hg.compute_hash(fpath=file_path)
    # Store the processed data in TileDB
    ctx = tiledb.Ctx(cfg)
    tiledb.from_pandas(
        uri=uri,
        dataframe=df,
        index_dims=["CHR", "POS", "TRAITID"],
        mode="append",
        ctx=ctx,
    )


def write_table(
    df: pd.DataFrame,
    where: str,
    logger: object,
    compression: str = "snappy",
    file_format: str = "parquet",
    log_msg: str = "none",
    **kwargs,
):
    """
    Writes the given DataFrame to a specified location on disk in the desired format. Two file
    formats are supported: "parquet" and "csv". The function handles file compression for
    "parquet" format. Logs a custom or default message indicating the status.

    :param engine:
    :param logger: The logger object used for logging messages.
    :param df: The pandas DataFrame to be saved.
    :param where: Destination file path, without extension, where the file should be saved.
    :param compression: Compression type for parquet files. Default is "snappy".
    :param file_format: File format to save the data, either "parquet" or "csv". Default is "parquet".
    :param log_msg: Custom log message. If "none", a default message will be logged. Default is "none".
    :param kwargs: Any additional keyword arguments to be passed to the underlying `to_parquet` or
        `to_csv` pandas methods.
    :return: None
    """
    # Check if format is valid
    if file_format not in ["parquet", "csv"]:
        raise ValueError("Format must be either 'parquet' or 'csv'")

    # Set the output filename based on the provided format and extension
    file_extension = "." + file_format

    # Create the full path by joining the output directory and filename with extension
    output_path = f"{where}{file_extension}"

    msg = log_msg if log_msg != "none" else f"Saving DataFrame to {output_path}"
    logger.info(msg)

    if file_format == "parquet":
        df.to_parquet(output_path, compression=compression, **kwargs)
    elif file_format == "csv":
        df.to_csv(output_path, **kwargs)

def get_log_p_value_from_z(z_score: float) -> float:
    """
    Calculate the p-value from a z-score.

    Args:
        z_score (float): The z-score value.

    Returns:
        float: The p-value corresponding to the z-score.
    """
    # Use the cumulative distribution function (CDF) for the normal distribution
    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
    log10_p = np.float32(-np.log10(p_value))
    return log10_p