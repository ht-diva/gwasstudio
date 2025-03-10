"""
Generic Utilities
=================
Generic utilities used by other modules.
"""

import hashlib
import pathlib
import random
import string
import urllib.parse
from typing import Any, Dict

import numpy as np
import pandas as pd
import polars as pl
import tiledb

DEFAULT_BUFSIZE = 4096


def compute_sha256(fpath: object = None, st: object = None) -> str:
    """
    Computes file or string hash using sha256 algorithm.

    Args:
        fpath (str): Path to a file for which to compute the hash.
        st (str): String for which to compute the hash.

    Returns:
        str: The SHA-256 hash of the input as a hexadecimal string, or None if neither input is provided.
    """
    algorithm = "sha256"

    def _compute_hash(input_):
        if isinstance(input_, pathlib.Path):
            return compute_file_hashing(algorithm, input_)
        elif isinstance(input_, str):
            return compute_string_hash(algorithm, input_)
        else:
            raise ValueError("Unsupported input type")

    if fpath is not None and st is not None:
        raise ValueError("Cannot provide both file path and string")
    elif fpath is not None:
        return _compute_hash(pathlib.Path(fpath))
    elif st is not None:
        return _compute_hash(st)
    else:
        return None


def compute_file_hashing(algorithm: str, path: pathlib.Path, bufsize: int = DEFAULT_BUFSIZE) -> str:
    """
    Computes the hash of a file using the algorithm function

    Args:
        algorithm (str): The name of the hashing algorithm.
        path: The path to the file for which to compute the hash.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash.

    """
    # with open(path, "rb") as fp:
    #   return hashlib.file_digest(fp, algorithm).hexdigest()
    digest = hashlib.new(algorithm)
    with open(path, "rb") as fp:
        s = fp.read(bufsize)
        while s:
            digest.update(s)
            s = fp.read(bufsize)
    return digest.hexdigest()


def compute_string_hash(algorithm: str, st: str) -> str:
    """
    Computes the hash of a string using the algorithm function.

    Args:
        algorithm (str): The name of the hashing algorithm.
        st: The string for which to compute the hash.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash.
    """
    h = hashlib.new(algorithm)
    h.update(st.encode("ascii"))
    return h.hexdigest()


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
def create_tiledb_schema(uri: str, cfg: dict):
    """
    Create an empty schema for TileDB.

    Args:
        uri (str): The path where the TileDB will be stored.
        cfg (dict): A configuration dictionary to use for connecting to S3.
    """
    chrom_domain = (1, 24)
    pos_domain = (1, 3000000000)
    dom = tiledb.Domain(
        tiledb.Dim(name="CHR", domain=chrom_domain, dtype=np.uint8, var=False),
        tiledb.Dim(name="POS", domain=pos_domain, dtype=np.uint32, var=False),
        tiledb.Dim(name="TRAITID", dtype="ascii", var=False),
    )
    schema = tiledb.ArraySchema(
        domain=dom,
        sparse=True,
        allows_duplicates=True,
        attrs=[
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
                name="MLOG10P",
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
                name="SNPID",
                dtype="ascii",
                var=True,
                filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)]),
            ),
        ],
    )
    ctx = tiledb.Ctx(cfg)
    tiledb.Array.create(uri, schema, ctx=ctx)


def process_and_ingest(file_path: str, uri: str, cfg: dict) -> None:
    """
    Process a single file and ingest it in a TileDB

    Args:
        file_path (str): The path where the file to ingest is stored
        uri (str): The path where the TileDB is stored.
        cfg (dict): A configuration dictionary to use for connecting to S3.
    """

    # Read file with Dask
    df = pd.read_csv(
        file_path,
        compression="gzip",
        sep="\t",
        usecols=["CHR", "POS", "SNPID", "EAF", "SE", "BETA", "MLOG10P"],
        dtype={
            "CHR": np.uint8,
            "POS": np.uint32,
            "SNPID": str,
            "EAF": np.float32,
            "SE": np.float32,
            "BETA": np.float32,
            "MLOG10P": np.float32,
        },
    )
    # Add trait_id based on the checksum_dict
    # file_name = file_path.split('/')[-1]
    df["TRAITID"] = compute_sha256(fpath=file_path)
    dtype_tbd = {
        "CHR": np.uint8,
        "POS": np.uint32,
        "SNPID": str,
        "EAF": np.float32,
        "SE": np.float32,
        "BETA": np.float32,
        "MLOG10P": np.float32,
        "TRAITID": str,
    }
    # Store the processed data in TileDB
    ctx = tiledb.Ctx(cfg)
    tiledb.from_pandas(
        uri=uri,
        dataframe=df,
        index_dims=["CHR", "POS", "TRAITID"],
        mode="append",
        column_types=dtype_tbd,
        ctx=ctx,
    )

"""
def process_write_chunk(chunk, SNP_list, file_stream):
    SNP_list_polars = pl.DataFrame(SNP_list)
    chunk_polars = pl.DataFrame(chunk)
    SNP_list_polars = SNP_list_polars.with_columns([pl.col("POS").cast(pl.UInt32)])
    chunk_polars = chunk_polars.with_columns(
        pl.col("SNPID").str.split_exact(":", 4)
        .struct.rename_fields(["chr",'pos','EA','NEA'])
        .alias("fields")
    ).unnest('fields')
    
    # Perform the join operation with Polars
    subset_SNPs_merge = chunk_polars.join(SNP_list_polars, on=["CHR", "POS", "NEA", "EA"], how="inner")
    subset_SNPs_merge = subset_SNPs_merge.select('CHR','POS','TRAITID','SNPID','BETA','SE')
    # Append the merged chunk to CSV
    subset_SNPs_merge.write_csv(file_stream)
"""
