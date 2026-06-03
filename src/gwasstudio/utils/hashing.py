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

    def compute_file_hash(
        self, file_path: pathlib.Path, bufsize: int = DEFAULT_BUFSIZE, method: str = "balanced"
    ) -> str:
        """Generate a hash for a file by reading strategic portions of its content.

        The function can use the methods:
        full - read the entire file
        balanced - read chunks from the start, middle, and end of the file

        This function creates a hash by reading:
        1. The file header (beginning)
        2. A middle chunk (center)
        3. The file footer (end)

        The function automatically adjusts read sizes for small files to prevent
        reading beyond file boundaries.

        Args:
            file_path: Path to the file (string or Path object)
            bufsize: Minimum size (bytes) to read from file
            method: strategy for reading the file

        Returns:
            str: The hexadecimal representation of the hash.
        """
        path = pathlib.Path(file_path)
        file_size = path.stat().st_size
        digest = hashlib.new(self.algorithm)

        if method == "full":
            with open(file_path, "rb") as f:
                while chunk := f.read(bufsize):
                    digest.update(chunk)
        else:
            with path.open("rb") as f:
                # 1. Read header (adjust size if file is small)
                header_size = min(bufsize, file_size)
                header = f.read(header_size)
                digest.update(header)

                # 2. Read middle chunk (adjust position if file is small)
                if file_size > header_size + bufsize:
                    middle_pos = max(header_size, (file_size // 2) - (bufsize // 2))
                    f.seek(middle_pos)
                    middle_size = min(bufsize, file_size - middle_pos)
                    middle = f.read(middle_size)
                    digest.update(middle)

                # 3. Read footer (adjust size if file is small)
                if file_size > header_size + bufsize:
                    f.seek(-bufsize, 2)
                    footer_size = min(bufsize, file_size)
                    footer = f.read(footer_size)
                    digest.update(footer)

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
