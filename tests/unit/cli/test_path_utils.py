# ------------------------------------------------------------
# file: test_path_joiner.py
# ------------------------------------------------------------
"""
Pytest suite for the ``join_path`` function defined in
path_joiner.py.

The tests cover:

* plain POSIX and Windows filesystem paths,
* S3 URIs (including bucket‑only URIs),
* the “file://” scheme,
* handling of stray leading/trailing slashes,
* empty/None components (they should be ignored),
* error handling for an empty base string.
"""

import os
import sys

import pytest

# Import the function we are testing.
# Adjust the import if your module lives in a package.
from gwasstudio.cli.path_utils import compose_tiledb_uri, join_path


# ----------------------------------------------------------------------
# Helper – compute the native‑separator version of a POSIX path.
# ----------------------------------------------------------------------
def native_path(posix_path: str) -> str:
    """
    Convert a POSIX style path (using forward slashes) to the
    current OS's native representation.  On Unix this is a no‑op,
    on Windows it turns '/' into '\\'.
    """
    return posix_path.replace("/", os.sep)


# ----------------------------------------------------------------------
# 1️⃣  Filesystem‑only tests
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "base,parts,expected",
    [
        # Simple POSIX join
        ("/tmp", ["data"], "/tmp/data"),
        ("/tmp/", ["data"], "/tmp/data"),
        ("/tmp", ["data", "sub"], "/tmp/data/sub"),
        # Leading/trailing slashes on parts must be stripped
        ("/tmp", ["/data/"], "/tmp/data"),
        ("/tmp/", ["/data/", "/sub/"], "/tmp/data/sub"),
        # Empty strings are ignored
        ("/tmp", ["", "data"], "/tmp/data"),
        ("/tmp", ["data", ""], "/tmp/data"),
        # Windows native paths – only run on Windows to avoid false failures
        pytest.param(
            r"C:\Users\bob",
            ["docs", "report.txt"],
            r"C:\Users\bob\docs\report.txt",
            marks=pytest.mark.skipif(
                not sys.platform.startswith("win"),
                reason="Windows‑specific path test",
            ),
        ),
        # Mixed forward‑slash input on Windows – should still end up native
        pytest.param(
            r"C:/temp",
            ["folder", "file.txt"],
            r"C:\temp\folder\file.txt",
            marks=pytest.mark.skipif(
                not sys.platform.startswith("win"),
                reason="Windows‑specific path test",
            ),
        ),
    ],
)
def test_filesystem_paths(base, parts, expected):
    assert join_path(base, *parts) == expected


# ----------------------------------------------------------------------
# 2️⃣  S3‑style URI tests
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "base,parts,expected",
    [
        # Bucket only + one component
        ("s3://my-bucket", ["data"], "s3://my-bucket/data"),
        # Bucket + prefix + many components
        ("s3://my-bucket/prefix", ["a", "b", "c"], "s3://my-bucket/prefix/a/b/c"),
        # Trailing/leading slashes on parts must be stripped
        ("s3://my-bucket/prefix/", ["/a/", "/b/"], "s3://my-bucket/prefix/a/b"),
        # Empty parts are ignored
        ("s3://my-bucket", ["", "x"], "s3://my-bucket/x"),
        # Preserve query/fragment if they exist
        (
            "s3://my-bucket/prefix?versionId=123#section",
            ["file.txt"],
            "s3://my-bucket/prefix/file.txt?versionId=123#section",
        ),
    ],
)
def test_s3_uris(base, parts, expected):
    assert join_path(base, *parts) == expected


# ----------------------------------------------------------------------
# 3️⃣  Other URI schemes (file://, http://, etc.)
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "base,parts,expected",
    [
        ("file:///var/log", ["app.log"], "file:///var/log/app.log"),
        ("http://example.com/api", ["v1", "users"], "http://example.com/api/v1/users"),
        # Even though we mainly care about S3, the logic works for any scheme.
    ],
)
def test_generic_uris(base, parts, expected):
    assert join_path(base, *parts) == expected


# ----------------------------------------------------------------------
# 4️⃣  Edge‑case / error handling
# ----------------------------------------------------------------------
def test_empty_base_raises():
    """An empty base string should raise a ValueError."""
    with pytest.raises(ValueError):
        join_path("", "anything")


def test_none_parts_are_ignored():
    """
    ``None`` (or any falsy value) in *parts* should be treated like an empty
    string and simply ignored.
    """
    assert join_path("/tmp", None, "data", "", "sub") == "/tmp/data/sub"


# ----------------------------------------------------------------------
# 5️⃣  Idempotency – calling join_path on an already‑joined result
# ----------------------------------------------------------------------
def test_idempotent_join():
    """
    ``join_path`` should be safe to call repeatedly; the result of a join
    is a valid *base* for a subsequent call.
    """
    first = join_path("s3://bucket/folder", "sub1")
    second = join_path(first, "sub2")
    assert second == "s3://bucket/folder/sub1/sub2"


# ----------------------------------------------------------------------
# 6️⃣  Mixed‑type inputs (bytes → str conversion is not supported)
# ----------------------------------------------------------------------
def test_non_string_input():
    """Only strings are accepted – other types raise a TypeError."""
    with pytest.raises(TypeError):
        # ``bytes`` does not have ``strip`` – our implementation will
        # raise an AttributeError before any custom check.
        join_path(b"s3://bucket", "key")


# ----------------------------------------------------------------------
# 7️⃣  compose_tiledb_uri tests
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "base_uri,group_name_parts,expected_group_name,expected_uri",
    [
        # Simple S3 URI with single group part
        ("s3://my-bucket/dataset", ("group1",), "group1", "s3://my-bucket/dataset/group1"),
        # S3 URI with multiple group parts
        ("s3://my-bucket/dataset", ("group1", "group2"), "group1_group2", "s3://my-bucket/dataset/group1_group2"),
        # File URI with single group part
        ("file:///data", ("study1",), "study1", "file:///data/study1"),
        # File URI with multiple group parts
        ("file:///data", ("study1", "batch2"), "study1_batch2", "file:///data/study1_batch2"),
        # Local path with group parts
        ("/tmp/data", ("group1", "group2"), "group1_group2", "/tmp/data/group1_group2"),
        # S3 bucket only
        ("s3://my-bucket", ("dataset",), "dataset", "s3://my-bucket/dataset"),
    ],
)
def test_compose_tiledb_uri(base_uri, group_name_parts, expected_group_name, expected_uri):
    """Test that compose_tiledb_uri correctly joins base URI with group name parts."""
    group_name, tiledb_uri = compose_tiledb_uri(base_uri, group_name_parts)
    assert group_name == expected_group_name
    assert tiledb_uri == expected_uri


def test_compose_tiledb_uri_with_logger(caplog):
    """Test that compose_tiledb_uri logs the processing message when logger is provided."""
    import logging

    logger = logging.getLogger("test_logger")
    base_uri = "s3://my-bucket/dataset"
    group_name_parts = ("group1", "group2")

    with caplog.at_level(logging.INFO, logger="test_logger"):
        group_name, tiledb_uri = compose_tiledb_uri(base_uri, group_name_parts, logger)

    assert group_name == "group1_group2"
    assert tiledb_uri == "s3://my-bucket/dataset/group1_group2"
    assert "Processing the group group1_group2" in caplog.text


def test_compose_tiledb_uri_without_logger():
    """Test that compose_tiledb_uri works without a logger (no logging output)."""
    base_uri = "s3://my-bucket/dataset"
    group_name_parts = ("group1",)
    group_name, tiledb_uri = compose_tiledb_uri(base_uri, group_name_parts, None)
    assert group_name == "group1"
    assert tiledb_uri == "s3://my-bucket/dataset/group1"
