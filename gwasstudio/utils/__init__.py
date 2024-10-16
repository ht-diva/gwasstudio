"""
Generic Utilities
=================
Generic utilities used by other modules.
"""

import hashlib
import pathlib
import string
import random

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
