"""
Integration test fixtures for GWASStudio.
"""

import os
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

DEFAULT_PORT = 27018
DEFAULT_TIMEOUT = 10


def _is_mongo_ready(host: str = "localhost", port: int = DEFAULT_PORT, timeout: int = DEFAULT_TIMEOUT) -> bool:
    """Check if MongoDB is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            result = subprocess.run(
                ["mongostat", "--host", f"{host}:{port}", "-n", "1"],
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if result.returncode == 0 and "insert" in result.stdout:
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass
        time.sleep(1)
    return False


@pytest.fixture(scope="session")
def mongod(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Start a local MongoDB instance for integration tests.

    Yields the MongoDB URI (e.g. 'mongodb://localhost:27018/test_integration').
    """
    db_path = tmp_path_factory.mktemp("mongo_db")
    log_path = Path("tests/integration/mongod_test.log")
    pid_path = Path("tests/integration/mongod_test.pid")

    db_path.mkdir(exist_ok=True)

    proc = subprocess.Popen(
        [
            "mongod",
            "--dbpath",
            str(db_path),
            "--logpath",
            str(log_path),
            "--logappend",
            "--port",
            str(DEFAULT_PORT),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        assert _is_mongo_ready(port=DEFAULT_PORT, timeout=DEFAULT_TIMEOUT), "MongoDB did not start in time"
        uri = f"mongodb://localhost:{DEFAULT_PORT}/test_integration"
        yield uri
    finally:
        proc.terminate()
        proc.wait(timeout=5)
        pid_path.unlink(missing_ok=True)
        log_path.unlink(missing_ok=True)
