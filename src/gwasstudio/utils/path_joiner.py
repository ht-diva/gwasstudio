"""
Utility for joining filesystem paths and S3 URIs.

Typical usage
-------------
# >>> from path_joiner import join_path
# >>> join_path("/tmp/data", "destination")
'/tmp/data/destination'

# >>> join_path("s3://my-bucket/data", "destination")
's3://my-bucket/data/destination'

The function works with any number of extra components:

# >>> join_path("s3://my-bucket", "a", "b", "c")
's3://my-bucket/a/b/c'

It also copes with stray slashes, empty components and
different OS path conventions.
"""

from pathlib import PurePosixPath, Path
from urllib.parse import urlparse, urlunparse


def _is_uri(s: str) -> bool:
    """
    Very small helper – returns ``True`` if *s* looks like a URI
    (i.e. it has a non‑empty scheme part).

    We deliberately treat only ``scheme://`` as a URI; a leading ``/``
    is considered a plain local path.
    """
    return bool(urlparse(s).scheme)


def _join_posix(base: str, *parts: str) -> str:
    """
    Join *base* and *parts* using POSIX semantics (forward slash).
    This is the logic we want for S3 keys and for any path that
    already uses forward slashes (including Unix paths on Windows).

    ``PurePosixPath`` never touches the actual filesystem – it simply
    manipulates strings.
    """
    p = PurePosixPath(base)
    for part in parts:
        p = p / part
    return str(p)


def join_path(base: str, *parts: str) -> str:
    """
    Join a base location with one or more path components.

    Parameters
    ----------
    base:
        Either a plain filesystem path (e.g. ``/a/b`` or ``C:\\a\\b``) or
        an S3 URI such as ``s3://my-bucket/a/b``.
    *parts:
        Additional components to append to *base*.  Empty strings or
        strings consisting only of slashes are ignored.

    Returns
    -------
    str
        The combined path/URI.

    Raises
    ------
    ValueError
        If *base* is an empty string.

    Examples
    --------
    >>> join_path("/tmp", "data", "dest")
    '/tmp/data/dest'

    >>> join_path("s3://my-bucket", "folder", "file.txt")
    's3://my-bucket/folder/file.txt'
    """
    if not base:
        raise ValueError("base path must be a non‑empty string")

    # Normalise the extra parts – strip leading/trailing slashes,
    # drop empty entries.
    clean_parts = [p.strip("/") for p in parts if p and p.strip("/")]

    # ------------------------------------------------------------------
    # 1️⃣  S3‑style URIs (or any URI with a scheme) – treat everything
    #     after the scheme:// as a POSIX path.
    # ------------------------------------------------------------------
    if _is_uri(base):
        parsed = urlparse(base)

        # Preserve the original scheme (e.g. "s3") and netloc (bucket name)
        # but join the path part with the extra components using POSIX logic.
        joined_path = _join_posix(parsed.path, *clean_parts)

        # ``urlunparse`` expects a 6‑tuple; we keep the original query,
        # fragment, params, etc. unchanged.
        rebuilt = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                joined_path,
                parsed.params,
                parsed.query,
                parsed.fragment,
            )
        )
        return rebuilt

    # ------------------------------------------------------------------
    # 2️⃣  Plain filesystem path.
    # ------------------------------------------------------------------
    # On Windows we must respect the native separator.  ``Path`` does that.
    # If the base path already uses forward slashes we still want the
    # correct OS‑specific behaviour, so we convert it to a ``Path`` first.
    p = Path(base)

    for part in clean_parts:
        p = p / part

    # ``Path`` will automatically use ``os.sep`` for the current platform.
    # For consistency with the examples we return a string.
    return str(p)
