"""
MongoDB Test Utilities for GWASStudio
=====================================

This module provides utilities for starting and stopping embedded MongoDB
instances during testing. It is intended for use in test scripts only.

Usage:
    from mongo_test_utils import manage_mongo, MongoDBManager

    # As a context manager
    with manage_mongo(db_path="/tmp/mongo_test", log_path="/tmp/mongod_test.log"):
        # MongoDB is running here
        ...
    # MongoDB is stopped here

    # Or manually
    manager = MongoDBManager(db_path="/tmp/mongo_test", log_path="/tmp/mongod_test.log")
    manager.start()
    # ... use MongoDB
    manager.stop()

CLI Usage:
    # Start MongoDB
    python scripts/mongo_test_utils.py start --port 27018

    # Stop MongoDB
    python scripts/mongo_test_utils.py stop

    # Check status
    python scripts/mongo_test_utils.py status
"""

import os
import signal
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

# Default paths - can be overridden via environment variables
DEFAULT_DB_PATH = os.environ.get("MONGO_DB_PATH", str(Path("data") / "mongo_db"))
DEFAULT_LOG_PATH = os.environ.get("MONGO_LOG_PATH", str(Path("logs") / "mongod.log"))
DEFAULT_PORT = int(os.environ.get("MONGO_PORT", "27018"))
DEFAULT_TIMEOUT = int(os.environ.get("MONGO_TIMEOUT", "5"))
DEFAULT_PID_FILE = os.environ.get("MONGO_PID_FILE", str(Path("logs") / "mongod.pid"))


class MongoDBManager:
    """
    Manage the lifecycle of an embedded MongoDB server process.

    This class provides methods to start and stop a MongoDB server process,
    which is useful for testing environments where an embedded MongoDB
    instance is needed.

    Args:
        dbpath: Path to the MongoDB database directory. Defaults to
                environment variable MONGO_DB_PATH or "data/mongo_db".
        logpath: Path to the MongoDB log file. Defaults to
                 environment variable MONGO_LOG_PATH or "logs/mongod.log".
        port: Port for MongoDB server. Defaults to environment variable
              MONGO_PORT or 27018.
        timeout: Timeout in seconds for starting MongoDB. Defaults to
                 environment variable MONGO_TIMEOUT or 5.
        pid_file: Path to write the process PID. Defaults to
                  environment variable MONGO_PID_FILE or "logs/mongod.pid".
    """

    def __init__(
        self,
        dbpath: str = DEFAULT_DB_PATH,
        logpath: str = DEFAULT_LOG_PATH,
        port: int = DEFAULT_PORT,
        timeout: int = DEFAULT_TIMEOUT,
        pid_file: str = DEFAULT_PID_FILE,
    ):
        self.dbpath = dbpath
        self.process = None
        self.logpath = logpath
        self.host = "localhost"
        self.port = port
        self.timeout = timeout
        self.pid_file = pid_file

    def _write_pid(self) -> None:
        """Write the process PID to the PID file."""
        if self.process and self.process.pid:
            Path(self.pid_file).parent.mkdir(parents=True, exist_ok=True)
            with open(self.pid_file, "w") as f:
                f.write(str(self.process.pid))

    def _read_pid(self) -> Optional[int]:
        """Read the process PID from the PID file."""
        pid_file = Path(self.pid_file)
        if pid_file.exists():
            with open(pid_file, "r") as f:
                try:
                    return int(f.read().strip())
                except ValueError:
                    return None
        return None

    def _remove_pid(self) -> None:
        """Remove the PID file."""
        pid_file = Path(self.pid_file)
        if pid_file.exists():
            pid_file.unlink()

    def start(self) -> None:
        """
        Start the MongoDB server.

        Creates the database directory if it doesn't exist, then starts
        mongod as a subprocess. Waits for the server to be ready to
        accept connections. Writes the PID to the PID file.

        Raises:
            Exception: If the MongoDB server fails to start within the timeout.
        """
        import logging

        logger = logging.getLogger(__name__)

        try:
            # Ensure database directory exists
            db_dir = Path(self.dbpath)
            db_dir.mkdir(parents=True, exist_ok=True)

            # Ensure log directory exists
            log_dir = Path(self.logpath).parent
            log_dir.mkdir(parents=True, exist_ok=True)

            # Start the MongoDB server
            self.process = subprocess.Popen(
                [
                    "mongod",
                    "--dbpath",
                    self.dbpath,
                    "--logpath",
                    self.logpath,
                    "--logappend",
                    "--port",
                    str(self.port),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.debug(f"Attempting to start embedded MongoDB server (PID: {self.process.pid})...")

            # Write PID to file
            self._write_pid()

            # Check if the server is running
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                return_code = self.process.poll()
                if return_code is not None:
                    logger.error("MongoDB server stopped unexpectedly.")
                    self._remove_pid()
                    raise Exception(f"MongoDB server stopped unexpectedly with return code {return_code}")

                # Check if the server is ready to accept connections
                try:
                    result = subprocess.run(
                        ["mongostat", "--host", f"{self.host}:{self.port}", "-n", "1"],
                        capture_output=True,
                        text=True,
                        timeout=self.timeout,
                    )
                    if result.returncode == 0 and "insert" in result.stdout:
                        logger.info(f"MongoDB server on {self.host}:{self.port} is running and ready.")
                        return
                except subprocess.TimeoutExpired:
                    logger.info("mongostat command timed out. Server may be down.")
                    pass

                time.sleep(1)

            # If we reach here, the server did not start within the timeout period
            error_msg = "MongoDB server did not start within the timeout period."
            logger.error(error_msg)
            self._remove_pid()
            if self.process and self.process.poll() is None:
                self.stop()
            raise Exception(error_msg)

        except Exception as e:
            logger.error(f"Failed to start MongoDB server: {e}")
            self._remove_pid()
            raise

    def stop(self) -> None:
        """
        Stop the MongoDB server.

        Terminates the mongod process and waits for it to stop.
        Removes the PID file.

        Raises:
            Exception: If the MongoDB server fails to stop.
        """
        import logging

        logger = logging.getLogger(__name__)

        try:
            if self.process and self.process.poll() is None:
                self.process.terminate()
                self.process.wait()
                self.process = None
                logger.info("MongoDB server stopped.")
            self._remove_pid()
        except Exception as e:
            logger.error(f"Failed to stop MongoDB server: {e}")
            raise

    def __del__(self) -> None:
        """
        Destructor to ensure the MongoDB server is stopped when the object is deleted.
        """
        if self.process and self.process.poll() is None:
            try:
                self.stop()
            except Exception:
                pass  # Ignore errors during cleanup


def stop_mongo_by_pid(pid_file: str = DEFAULT_PID_FILE) -> bool:
    """
    Stop a MongoDB server using the PID from a PID file.

    Args:
        pid_file: Path to the file containing the process PID.

    Returns:
        bool: True if the process was stopped, False if no process was found.
    """
    import logging

    logger = logging.getLogger(__name__)

    pid = None
    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        logger.warning(f"PID file {pid_file} not found or invalid.")
        return False

    if pid is None:
        return False

    # Check if process exists
    try:
        os.kill(pid, 0)  # Signal 0 checks if process exists
    except OSError:
        # Process doesn't exist, remove PID file and return
        logger.warning(f"Process {pid} does not exist.")
        try:
            Path(pid_file).unlink()
        except FileNotFoundError:
            pass
        return False

    # Try graceful shutdown first
    try:
        logger.info(f"Stopping MongoDB server (PID: {pid})...")
        os.kill(pid, signal.SIGTERM)
        # Wait a bit for graceful shutdown
        time.sleep(2)
        # Check if still running
        try:
            os.kill(pid, 0)
            # Still running, force kill
            logger.info(f"MongoDB server (PID: {pid}) did not stop gracefully, forcing...")
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass  # Process stopped
    except OSError as e:
        logger.error(f"Failed to stop MongoDB process {pid}: {e}")
        return False

    # Remove PID file
    try:
        Path(pid_file).unlink()
        logger.info(f"MongoDB server (PID: {pid}) stopped. PID file removed.")
    except FileNotFoundError:
        pass

    return True


def is_mongo_running(pid_file: str = DEFAULT_PID_FILE) -> bool:
    """
    Check if a MongoDB server is running.

    Args:
        pid_file: Path to the file containing the process PID.

    Returns:
        bool: True if MongoDB is running, False otherwise.
    """
    import logging

    logger = logging.getLogger(__name__)

    try:
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return False

    try:
        os.kill(pid, 0)  # Signal 0 checks if process exists
        return True
    except OSError:
        # Process doesn't exist, remove stale PID file
        logger.warning(f"Stale PID file {pid_file} with non-existent PID {pid}.")
        try:
            Path(pid_file).unlink()
        except FileNotFoundError:
            pass
        return False


@contextmanager
def manage_mongo(
    dbpath: Optional[str] = None,
    logpath: Optional[str] = None,
    port: Optional[int] = None,
    timeout: Optional[int] = None,
    pid_file: Optional[str] = None,
):
    """
    Context manager to handle the lifecycle of a MongoDB server.

    This is useful for test scripts that need an embedded MongoDB instance.
    The MongoDB server is automatically started when entering the context
    and stopped when exiting.

    Args:
        dbpath: Path to the MongoDB database directory.
        logpath: Path to the MongoDB log file.
        port: Port for MongoDB server.
        timeout: Timeout in seconds for starting MongoDB.
        pid_file: Path to write the process PID.

    Yields:
        None - the MongoDB server is running during the context

    Raises:
        Exception: If an error occurs during MongoDB server management.

    Example:
        with manage_mongo(dbpath="/tmp/test_mongo", port=27019):
            # MongoDB is running on localhost:27019
            client = MongoClient("mongodb://localhost:27019")
            # ... do work
        # MongoDB is stopped
    """
    import logging

    logger = logging.getLogger(__name__)

    # Use provided values or defaults
    actual_dbpath = dbpath if dbpath is not None else DEFAULT_DB_PATH
    actual_logpath = logpath if logpath is not None else DEFAULT_LOG_PATH
    actual_port = port if port is not None else DEFAULT_PORT
    actual_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    actual_pid_file = pid_file if pid_file is not None else DEFAULT_PID_FILE

    # Determine if we should use embedded mode
    embedded_mongo = actual_port == 27018

    if embedded_mongo:
        logger.debug(f"Starting embedded MongoDB on port {actual_port}")
        mdb = MongoDBManager(
            dbpath=actual_dbpath,
            logpath=actual_logpath,
            port=actual_port,
            timeout=actual_timeout,
            pid_file=actual_pid_file,
        )
        mdb.start()
    else:
        mdb = None
        logger.debug("Using external MongoDB (non-default port)")

    try:
        yield
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        if mdb is not None and embedded_mongo:
            mdb.stop()


def start_mongo(
    dbpath: Optional[str] = None,
    logpath: Optional[str] = None,
    port: Optional[int] = None,
    timeout: Optional[int] = None,
    pid_file: Optional[str] = None,
) -> MongoDBManager:
    """
    Start a MongoDB server and return the manager.

    Args:
        dbpath: Path to the MongoDB database directory.
        logpath: Path to the MongoDB log file.
        port: Port for MongoDB server.
        timeout: Timeout in seconds for starting MongoDB.
        pid_file: Path to write the process PID.

    Returns:
        MongoDBManager: The started MongoDB manager instance.

    Example:
        manager = start_mongo(port=27019)
        try:
            # ... do work
        finally:
            manager.stop()
    """
    actual_dbpath = dbpath if dbpath is not None else DEFAULT_DB_PATH
    actual_logpath = logpath if logpath is not None else DEFAULT_LOG_PATH
    actual_port = port if port is not None else DEFAULT_PORT
    actual_timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    actual_pid_file = pid_file if pid_file is not None else DEFAULT_PID_FILE

    manager = MongoDBManager(
        dbpath=actual_dbpath,
        logpath=actual_logpath,
        port=actual_port,
        timeout=actual_timeout,
        pid_file=actual_pid_file,
    )
    manager.start()
    return manager


def stop_mongo(manager: MongoDBManager) -> None:
    """
    Stop a MongoDB server using the manager instance.

    Args:
        manager: The MongoDBManager instance to stop.
    """
    manager.stop()


def get_pid_from_file(pid_file: str = DEFAULT_PID_FILE) -> Optional[int]:
    """
    Get the PID from a PID file.

    Args:
        pid_file: Path to the file containing the process PID.

    Returns:
        int or None: The process PID, or None if not found/invalid.
    """
    try:
        with open(pid_file, "r") as f:
            return int(f.read().strip())
    except (FileNotFoundError, ValueError):
        return None


if __name__ == "__main__":
    # Allow running as a script for manual testing
    import argparse
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Manage embedded MongoDB for testing")
    subparsers = parser.add_subparsers(dest="command")

    # Start command
    start_parser = subparsers.add_parser("start", help="Start embedded MongoDB")
    start_parser.add_argument("--dbpath", default=None, help="Database path")
    start_parser.add_argument("--logpath", default=None, help="Log file path")
    start_parser.add_argument("--port", type=int, default=None, help="MongoDB port")
    start_parser.add_argument("--timeout", type=int, default=None, help="Startup timeout")
    start_parser.add_argument("--pid-file", default=None, help="PID file path")
    start_parser.add_argument("--wait", action="store_true", help="Wait for user to press Enter")

    # Stop command
    stop_parser = subparsers.add_parser("stop", help="Stop embedded MongoDB")
    stop_parser.add_argument("--pid-file", default=None, help="PID file path")

    # Status command
    status_parser = subparsers.add_parser("status", help="Check MongoDB status")
    status_parser.add_argument("--pid-file", default=None, help="PID file path")

    args = parser.parse_args()

    if args.command == "start":
        manager = start_mongo(
            dbpath=args.dbpath,
            logpath=args.logpath,
            port=args.port,
            timeout=args.timeout,
            pid_file=args.pid_file,
        )
        actual_port = args.port if args.port is not None else DEFAULT_PORT
        actual_pid_file = args.pid_file if args.pid_file is not None else DEFAULT_PID_FILE
        print(f"MongoDB started on localhost:{actual_port}")
        print(f"PID: {manager.process.pid}")
        print(f"PID file: {actual_pid_file}")
        if args.wait:
            input("Press Enter to stop MongoDB...")
            manager.stop()

    elif args.command == "stop":
        actual_pid_file = args.pid_file if args.pid_file is not None else DEFAULT_PID_FILE
        if stop_mongo_by_pid(actual_pid_file):
            print(f"MongoDB stopped. PID file: {actual_pid_file}")
        else:
            print(f"MongoDB not running or PID file not found: {actual_pid_file}")
            exit(1)

    elif args.command == "status":
        actual_pid_file = args.pid_file if args.pid_file is not None else DEFAULT_PID_FILE
        if is_mongo_running(actual_pid_file):
            pid = get_pid_from_file(actual_pid_file)
            print(f"MongoDB is running (PID: {pid})")
        else:
            print("MongoDB is not running")
            exit(1)

    else:
        parser.print_help()
