import subprocess
import time
from contextlib import contextmanager

from gwasstudio import logger, mongo_db_path, mongo_db_logpath
from gwasstudio.utils.cfg import get_mongo_deployment, get_mongo_uri

mongo_deployment_types = ["embedded", "standalone"]


@contextmanager
def manage_mongo(ctx):
    """
    Context manager to handle the lifecycle of MongoDB server.

    Args:
        ctx: The context object containing configuration details.

    Yields:
        None

    Raises:
        Exception: If an error occurs during the MongoDB server management.
    """
    embedded_mongo = (get_mongo_deployment(ctx) == "embedded") and (
        (get_mongo_uri(ctx) is None) or ("localhost:27018" in get_mongo_uri(ctx))
    )
    logger.debug(f"Embedded MongoDB: {embedded_mongo}")
    mdb = MongoDBManager()
    if embedded_mongo:
        mdb.start()
    try:
        yield
    except Exception as e:
        logger.error(f"Error occurred: {e}")
        raise
    finally:
        if embedded_mongo:
            mdb.stop()


class MongoDBManager:
    """
    Initialize the embedded MongoDBManager with the given database path and log path.

    Args:
        dbpath (str): The path to the MongoDB database.
        logpath (str): The path to the MongoDB log file.
        port (int): The port on which the MongoDB server will run. Default is 27018.
        timeout (int): The timeout period for starting the MongoDB server. Default is 5 seconds.
    """

    def __init__(self, dbpath=mongo_db_path, logpath=mongo_db_logpath, port=27018, timeout=5):
        self.dbpath = dbpath
        self.process = None
        self.logpath = logpath
        self.host = "localhost"
        self.port = port
        self.timeout = timeout

    def start(self):
        """
        Start the MongoDB server.

        Raises:
            Exception: If the MongoDB server fails to start.
        """
        try:
            # Start the MongoDB server
            self.process = subprocess.Popen(
                ["mongod", "--dbpath", self.dbpath, "--logpath", self.logpath, "--logappend", "--port", str(self.port)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.debug("Attempting to start embedded MongoDB server...")

            # Check if the server is running
            # Wait for the server to start
            start_time = time.time()
            while time.time() - start_time < self.timeout:
                return_code = self.process.poll()
                if return_code is not None:
                    logger.error("MongoDB server stopped unexpectedly.")
                    break

                # Check if the server is ready to accept connections
                try:
                    # Attempt to connect to the MongoDB server
                    # Run mongostat with one iteration (-n 1) and timeout
                    result = subprocess.run(
                        ["mongostat", "--host", f"{self.host}:{self.port}", "-n", "1"],
                        capture_output=True,
                        text=True,
                        timeout=self.timeout,
                    )
                    # If mongostat returns output containing stats, server is up
                    if result.returncode == 0 and "insert" in result.stdout:
                        logger.info(
                            f"MongoDB server on {self.host}:{self.port} is running and ready to accept connections."
                        )
                        break
                except subprocess.TimeoutExpired:
                    logger.info("mongostat command timed out. Server may be down or unreachable.")
                    break

                # Sleep for a short period before checking again
                time.sleep(1)

                # If we reach here, the server did not start within the timeout period
                logger.error("MongoDB server did not start within the timeout period.")

        except Exception as e:
            logger.error(f"Failed to start MongoDB server: {e}")

    def stop(self):
        """
        Stop the MongoDB server.

        Raises:
            Exception: If the MongoDB server fails to stop.
        """
        try:
            # Stop the MongoDB server
            self.process.terminate()
            self.process.wait()
            self.process = None
            logger.info("MongoDB server stopped.")
        except Exception as e:
            logger.error(f"Failed to stop MongoDB server: {e}")

    def __del__(self):
        """
        Destructor to ensure the MongoDB server is stopped when the object is deleted.
        """
        if self.process and self.process.poll() is None:
            self.stop()
