"""
Generic Utilities
=================
Generic utilities used by other modules.
"""

import hashlib
import pathlib
import random
import string

import numpy as np
import pandas as pd
import polars as pl
import tiledb

DEFAULT_BUFSIZE = 4096


def compute_sha256(fpath=None, st=None):
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
    Computes the SHA-256 hash of a file.

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
    Computes the SHA-256 hash of a string.

    Args:
        algorithm (str): The name of the hashing algorithm.
        st: The string for which to compute the hash.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash.
    """
    h = hashlib.new(algorithm)
    h.update(st.encode("ascii"))
    return h.hexdigest()


def generate_random_word(length: int) -> str:
    """
    Generates a random word consisting of lowercase letters.

    Args:
        length (int): The number of characters to include in the generated word.

    Returns:
        str: A random word of the specified length.
    """
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))


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
        tiledb.Dim(name="chrom", domain=chrom_domain, dtype=np.uint8, var=False),
        tiledb.Dim(name="pos", domain=pos_domain, dtype=np.uint32, var=False),
        tiledb.Dim(name="trait_id", dtype=np.dtype("S64"), var=True),
    )
    schema = tiledb.ArraySchema(
        domain=dom,
        sparse=True,
        allows_duplicates=True,
        attrs=[
            tiledb.Attr(
                name="beta", dtype=np.float32, var=False, filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])
            ),
            tiledb.Attr(
                name="se", dtype=np.float32, var=False, filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])
            ),
            tiledb.Attr(
                name="freq", dtype=np.float32, var=False, filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])
            ),
            tiledb.Attr(
                name="alt", dtype=np.dtype("S5"), var=True, filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])
            ),
            tiledb.Attr(
                name="SNP", dtype=np.dtype("S20"), var=True, filters=tiledb.FilterList([tiledb.ZstdFilter(level=5)])
            ),
        ],
    )
    tiledb.Array.create(uri, schema, ctx=cfg)


def process_and_ingest(
    file_path: str, uri, dict_type: dict, renaming_columns: dict, attributes_columns: list, cfg: dict
):
    """
    Process a single file and ingest it in a TileDB

    Args:
        file_path (str): The path where the file to ingest is stored
        uri (str): The path where the TileDB is stored.
        dict_type(str)
        cfg (dict): A configuration dictionary to use for connecting to S3.
    """

    # Read file with Dask
    df = pd.read_csv(
        file_path,
        compression="gzip",
        sep="\t",
        usecols=attributes_columns,
        # usecols=["Chrom", "Pos", "Name", "effectAllele", "Beta", "SE", "ImpMAF"]
    )
    sha256 = compute_sha256(file_path)
    # Rename columns and modify 'chrom' field
    df = df.rename(columns=renaming_columns)
    df["chrom"] = df["chrom"].str.replace("chr", "")
    df["chrom"] = df["chrom"].str.replace("X", "23")
    df["chrom"] = df["chrom"].str.replace("Y", "24")
    # Add trait_id based on the checksum_dict
    # file_name = file_path.split("/")[-1]
    df["trait_id"] = sha256

    # Store the processed data in TileDB
    tiledb.from_pandas(
        uri=uri, dataframe=df, index_dims=["chrom", "pos", "trait_id"], mode="append", column_types=dict_type, ctx=cfg
    )


def process_write_chunk(chunk, SNP_list, file_stream):
    SNP_list_polars = pl.DataFrame(SNP_list)
    chunk_polars = pl.DataFrame(chunk)
    SNP_list_polars = SNP_list_polars.with_columns([pl.col("POS").cast(pl.UInt32)])
    chunk_polars = chunk_polars.with_columns(
        [pl.col("ALLELE0").cast(pl.Utf8), pl.col("ALLELE1").cast(pl.Utf8), pl.col("SNPID").cast(pl.Utf8)]
    )
    # Perform the join operation with Polars
    subset_SNPs_merge = chunk_polars.join(SNP_list_polars, on=["CHR", "POS", "ALLELE0", "ALLELE1"], how="inner")
    # Append the merged chunk to CSV
    subset_SNPs_merge.write_csv(file_stream)
