import hashlib
import pathlib

from gwasstudio.config_manager import ConfigurationManager

DEFAULT_BUFSIZE = 4096


class Hashing:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Hashing, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, "initialized"):
            cm = ConfigurationManager()
            self.algorithm = cm.hash_algorithm
            self.length = cm.hash_length
            self.initialized = True

    @property
    def hash_length(self):
        return self.length

    def compute_hash(self, fpath: str = None, st: str = None) -> str | None:
        """
        Computes file or string hash using the algorithm set in the class.
        Notes:
            - If `fpath` is provided, the hash is computed based on the filename and file content.
            - If `st` is provided, the hash is computed based on the string content.
            - If neither `fpath` nor `st` is provided, the function returns None.

        Args:
            fpath (str): Path to a file for which to compute the hash.
            st (str): String for which to compute the hash.
        Returns:
            str: The hash of the input as a hexadecimal string, or None if neither input is provided.
        """
        match (fpath, st):
            case (None, None):
                return None
            case (None, _):
                hash_value = self.compute_string_hash(st)
            case (_, None):
                # Convert the file path to a Path object
                path = pathlib.Path(fpath)
                # Compute the hash of the filename
                filename_hash = self.compute_string_hash(path.name)
                # Compute the hash of the file content
                file_content_hash = self.compute_file_hash(path)
                # Bind the filename hash, and the file content hash
                hash_value = self.compute_string_hash(filename_hash + file_content_hash)
            case _:
                raise ValueError("Cannot provide both file path and string")

        return hash_value if self.length is None else hash_value[: self.length] if hash_value else None

    def compute_file_hash(self, path: pathlib.Path, bufsize: int = DEFAULT_BUFSIZE) -> str:
        """
        Computes the hash of a file using the algorithm function

        Args:
            path: The path to the file for which to compute the hash.
            bufsize (int): The size of the buffer to use when reading the file.

        Returns:
            str: The hexadecimal representation of the hash.
        """
        digest = hashlib.new(self.algorithm)
        with open(path, "rb") as fp:
            s = fp.read(bufsize)
            while s:
                digest.update(s)
                s = fp.read(bufsize)
        return digest.hexdigest()

    def compute_string_hash(self, st: str) -> str:
        """
        Computes the hash of a string using the algorithm function.

        Args:
            st: The string for which to compute the hash.

        Returns:
            str: The hexadecimal representation of the hash.
        """
        h = hashlib.new(self.algorithm)
        h.update(st.encode("ascii"))
        return h.hexdigest()
