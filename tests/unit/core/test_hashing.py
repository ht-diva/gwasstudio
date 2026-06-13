"""
Tests for GWASStudio Core Hashing Module
=========================================

Tests for the Hashing class in gwasstudio.core.hashing.
Covers singleton behavior, file hashing, string hashing,
truncation, edge cases, and deterministic output.
"""

import hashlib
import pathlib
import tempfile

import pytest

from gwasstudio.core.hashing import (
    DEFAULT_BUFSIZE,
    HASH_ALGORITHM,
    HASH_LENGTH,
    Hashing,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_hashing_class():
    """Reset the Hashing singleton between tests."""
    # Force reset by removing the cached instance
    Hashing._instance = None
    yield Hashing
    # Cleanup
    Hashing._instance = None


@pytest.fixture
def hashing(clean_hashing_class):
    """Provide a fresh Hashing instance."""
    Hashing._instance = None
    return Hashing()


@pytest.fixture
def temp_file_small():
    """Create a small temporary file (< bufsize, < 2*bufsize)."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        f.write("Hello, this is a small file.")
    p = pathlib.Path(f.name)
    yield p
    p.unlink(missing_ok=True)


@pytest.fixture
def temp_file_exact_bufsize():
    """Create a temporary file exactly bufsize bytes."""
    content = "A" * DEFAULT_BUFSIZE
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        f.write(content)
    p = pathlib.Path(f.name)
    yield p
    p.unlink(missing_ok=True)


@pytest.fixture
def temp_file_large():
    """Create a temporary file significantly larger than bufsize."""
    content = "B" * (DEFAULT_BUFSIZE * 4 + 1234)
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        f.write(content)
    p = pathlib.Path(f.name)
    yield p
    p.unlink(missing_ok=True)


@pytest.fixture
def temp_file_empty():
    """Create an empty temporary file."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
        pass
    p = pathlib.Path(f.name)
    yield p
    p.unlink(missing_ok=True)


@pytest.fixture
def temp_file_binary():
    """Create a temporary file with binary-like content."""
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".dat") as f:
        f.write(bytes(range(256)) * 16)
    p = pathlib.Path(f.name)
    yield p
    p.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Singleton / Class behavior
# ---------------------------------------------------------------------------


class TestSingleton:
    """Tests for Hashing singleton behavior."""

    def test_singleton_returns_same_instance(self, clean_hashing_class):
        """Multiple calls should return the same instance."""
        instance1 = clean_hashing_class()
        instance2 = clean_hashing_class()
        assert instance1 is instance2

    def test_reset_clears_singleton(self, clean_hashing_class):
        """Manually clearing _instance gives a new instance."""
        instance1 = clean_hashing_class()
        clean_hashing_class._instance = None
        instance2 = clean_hashing_class()
        assert instance1 is not instance2

    def test_instance_has_attributes(self, hashing):
        """Hashing instance should have expected attributes."""
        assert hasattr(hashing, "algorithm")
        assert hasattr(hashing, "length")
        assert hasattr(hashing, "hash_length")
        assert hasattr(hashing, "compute_hash")
        assert hasattr(hashing, "compute_file_hash")
        assert hasattr(hashing, "compute_string_hash")


# ---------------------------------------------------------------------------
# Initialization defaults
# ---------------------------------------------------------------------------


class TestInitialization:
    """Tests for Hashing initialization."""

    def test_default_algorithm(self, hashing):
        """Default hash algorithm should be SHA-256."""
        assert hashing.algorithm == HASH_ALGORITHM
        assert hashing.algorithm == "sha256"

    def test_default_length(self, hashing):
        """Default hash length should be 10."""
        assert hashing.length == HASH_LENGTH
        assert hashing.hash_length == HASH_LENGTH


# ---------------------------------------------------------------------------
# compute_hash — common cases
# ---------------------------------------------------------------------------


class TestComputeHash:
    """Tests for compute_hash method."""

    def test_neither_arg_returns_none(self, hashing):
        """Calling with no arguments should return None."""
        assert hashing.compute_hash() is None

    def test_both_args_raises(self, hashing):
        """Calling with both fpath and st should raise ValueError."""
        with pytest.raises(ValueError, match="Cannot provide both file path and string"):
            hashing.compute_hash(fpath="/tmp/x", st="hello")

    def test_string_input_returns_truncated(self, hashing):
        """String input should return truncated hash."""
        result = hashing.compute_hash(st="test string")
        assert result is not None
        assert len(result) == hashing.hash_length
        assert isinstance(result, str)

    def test_file_input_returns_truncated(self, hashing, temp_file_small):
        """File input should return truncated hash."""
        result = hashing.compute_hash(fpath=str(temp_file_small))
        assert result is not None
        assert len(result) == hashing.hash_length
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# compute_string_hash
# ---------------------------------------------------------------------------


class TestComputeStringHash:
    """Tests for compute_string_hash method."""

    def test_deterministic(self, hashing):
        """Same string should always produce the same hash."""
        h1 = hashing.compute_string_hash("hello world")
        h2 = hashing.compute_string_hash("hello world")
        assert h1 == h2

    def test_different_strings_different_hashes(self, hashing):
        """Different strings should produce different hashes."""
        h1 = hashing.compute_string_hash("hello")
        h2 = hashing.compute_string_hash("world")
        assert h1 != h2

    def test_returns_full_sha256(self, hashing):
        """Should return a full SHA-256 hex digest (64 chars)."""
        result = hashing.compute_string_hash("test")
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_ascii_encoding(self, hashing):
        """Should encode string as ASCII."""
        result = hashing.compute_string_hash("ascii only")
        expected = hashlib.new("sha256", "ascii only".encode("ascii")).hexdigest()
        assert result == expected

    def test_empty_string(self, hashing):
        """Empty string should produce a valid hash."""
        result = hashing.compute_string_hash("")
        expected = hashlib.new("sha256", b"").hexdigest()
        assert result == expected

    def test_unicode_raises_or_handles(self, hashing):
        """Unicode strings may raise on ASCII encoding or produce a hash."""
        try:
            result = hashing.compute_string_hash("café")
            # If it doesn't raise, it should still be 64 hex chars
            assert len(result) == 64
        except UnicodeEncodeError:
            # Or it may raise — both are acceptable behaviors
            pass

    def test_long_string(self, hashing):
        """Long string should produce a valid hash."""
        long_str = "x" * 100_000
        result = hashing.compute_string_hash(long_str)
        expected = hashlib.new("sha256", long_str.encode("ascii")).hexdigest()
        assert result == expected


# ---------------------------------------------------------------------------
# compute_file_hash
# ---------------------------------------------------------------------------


class TestComputeFileHash:
    """Tests for compute_file_hash method."""

    def test_deterministic(self, hashing, temp_file_small):
        """Same file should always produce the same hash."""
        h1 = hashing.compute_file_hash(temp_file_small)
        h2 = hashing.compute_file_hash(temp_file_small)
        assert h1 == h2

    def test_different_content_different_hash(self, hashing, temp_file_small, temp_file_large):
        """Different files should produce different hashes."""
        h1 = hashing.compute_file_hash(temp_file_small)
        h2 = hashing.compute_file_hash(temp_file_large)
        assert h1 != h2

    def test_balanced_method(self, hashing, temp_file_large):
        """Default 'balanced' method should return 64-char hex digest."""
        result = hashing.compute_file_hash(temp_file_large, method="balanced")
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_full_method(self, hashing, temp_file_large):
        """'full' method should return 64-char hex digest."""
        result = hashing.compute_file_hash(temp_file_large, method="full")
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_full_vs_balanced_differ_for_large_files(self, hashing, temp_file_large):
        """'full' and 'balanced' should differ for large files."""
        h_full = hashing.compute_file_hash(temp_file_large, method="full")
        h_balanced = hashing.compute_file_hash(temp_file_large, method="balanced")
        # Since the file is much larger than bufsize, these should differ
        assert h_full != h_balanced

    def test_small_file_balanced(self, hashing, temp_file_small):
        """Balanced method should handle files smaller than bufsize."""
        result = hashing.compute_file_hash(temp_file_small, method="balanced")
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_small_file_full(self, hashing, temp_file_small):
        """Full method should handle files smaller than bufsize."""
        result = hashing.compute_file_hash(temp_file_small, method="full")
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_full_vs_balanced_same_for_small_files(self, hashing, temp_file_small):
        """'full' and 'balanced' should be the same for files <= bufsize."""
        h_full = hashing.compute_file_hash(temp_file_small, method="full")
        h_balanced = hashing.compute_file_hash(temp_file_small, method="balanced")
        assert h_full == h_balanced

    def test_empty_file(self, hashing, temp_file_empty):
        """Empty file should produce a valid hash."""
        result = hashing.compute_file_hash(temp_file_empty)
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_binary_file(self, hashing, temp_file_binary):
        """Binary file should produce a valid hash."""
        result = hashing.compute_file_hash(temp_file_binary)
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_exact_bufsize_file(self, hashing, temp_file_exact_bufsize):
        """File exactly bufsize bytes should produce a valid hash."""
        result = hashing.compute_file_hash(temp_file_exact_bufsize)
        assert len(result) == 64
        assert set(result) <= set("0123456789abcdef")

    def test_pathlib_path(self, hashing, temp_file_small):
        """Should accept a pathlib.Path object."""
        result = hashing.compute_file_hash(pathlib.Path(temp_file_small))
        assert len(result) == 64

    def test_custom_bufsize(self, hashing, temp_file_large):
        """Custom bufsize should not raise."""
        result = hashing.compute_file_hash(temp_file_large, bufsize=8192, method="full")
        assert len(result) == 64

    def test_custom_bufsize_balanced(self, hashing, temp_file_large):
        """Custom bufsize with balanced method should not raise."""
        result = hashing.compute_file_hash(temp_file_large, bufsize=8192, method="balanced")
        assert len(result) == 64


# ---------------------------------------------------------------------------
# compute_hash — integration
# ---------------------------------------------------------------------------


class TestComputeHashIntegration:
    """Tests for compute_hash end-to-end behavior."""

    def test_file_hash_includes_filename(self, hashing, temp_file_small):
        """compute_hash(fpath=...) should include the filename in the hash."""
        import pathlib as _p

        # Directly compute what the hash should be:
        # hash(filename_hash + file_content_hash)
        hg_direct = Hashing()
        path = _p.Path(temp_file_small)
        filename_hash = hg_direct.compute_string_hash(path.name)
        file_content_hash = hg_direct.compute_file_hash(path)
        expected_full = hg_direct.compute_string_hash(filename_hash + file_content_hash)
        expected_short = expected_full[: hashing.hash_length]

        result = hashing.compute_hash(fpath=str(temp_file_small))
        assert result == expected_short

    def test_string_hash_equals_compute_string_hash(self, hashing):
        """compute_hash(st=...) should equal compute_string_hash(...)."""
        result_computed = hashing.compute_hash(st="test value")
        result_direct = hashing.compute_string_hash("test value")
        assert result_computed == result_direct[: hashing.hash_length]

    def test_file_hash_differs_string_hash(self, hashing, temp_file_small):
        """Hashing a file and its content as a string should differ."""
        # File content
        content = temp_file_small.read_text()
        h_file = hashing.compute_hash(fpath=str(temp_file_small))
        h_string = hashing.compute_hash(st=content)
        # The file hash includes the filename, so they should differ
        assert h_file != h_string


# ---------------------------------------------------------------------------
# Determinism and correctness
# ---------------------------------------------------------------------------


class TestDeterminismAndCorrectness:
    """Tests for hash determinism and correctness."""

    def test_multiple_files_produce_unique_hashes(self, hashing, tmp_path):
        """Each unique file should produce a unique hash."""
        hashes = set()
        for i in range(10):
            fpath = tmp_path / f"file_{i}.txt"
            fpath.write_text(f"unique content {i}")
            h = hashing.compute_hash(fpath=str(fpath))
            hashes.add(h)
        assert len(hashes) == 10

    def test_repeated_calls_same_result(self, hashing, temp_file_large):
        """Repeated calls on the same file should return the same result."""
        results = [hashing.compute_hash(fpath=str(temp_file_large)) for _ in range(100)]
        assert len(set(results)) == 1

    def test_hash_is_hex(self, hashing):
        """Hash output should be valid hex string."""
        result = hashing.compute_hash(st="hex check")
        assert set(result) <= set("0123456789abcdef")

    def test_hash_length_is_correct(self, hashing):
        """Truncated hash should match hash_length."""
        result = hashing.compute_hash(st="length check")
        assert len(result) == HASH_LENGTH

    def test_hash_length_property(self, hashing):
        """hash_length property should match instance.length."""
        assert hashing.hash_length == hashing.length


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Tests for edge cases and unusual inputs."""

    def test_none_fpath_st(self, hashing):
        """Both None should return None."""
        assert hashing.compute_hash(fpath=None, st=None) is None

    def test_empty_string(self, hashing):
        """Empty string should produce a valid hash."""
        result = hashing.compute_hash(st="")
        assert result is not None
        assert len(result) == HASH_LENGTH

    def test_whitespace_string(self, hashing):
        """String with only whitespace should produce a valid hash."""
        result = hashing.compute_hash(st="   \t\n  ")
        assert result is not None
        assert len(result) == HASH_LENGTH

    def test_special_characters(self, hashing):
        """Special characters should be handled."""
        special = "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        result = hashing.compute_hash(st=special)
        assert result is not None
        assert len(result) == HASH_LENGTH

    def test_numeric_string(self, hashing):
        """Numeric string should produce a valid hash."""
        result = hashing.compute_hash(st="1234567890")
        assert result is not None
        assert len(result) == HASH_LENGTH

    def test_newline_string(self, hashing):
        """String with newlines should produce a valid hash."""
        result = hashing.compute_hash(st="line1\nline2\nline3")
        assert result is not None
        assert len(result) == HASH_LENGTH

    def test_very_long_string(self, hashing):
        """Very long string should not raise."""
        result = hashing.compute_hash(st="L" * 1_000_000)
        assert result is not None
        assert len(result) == HASH_LENGTH
