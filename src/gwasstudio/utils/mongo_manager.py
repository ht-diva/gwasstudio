import subprocess
import time
from contextlib import contextmanager

from gwasstudio import logger, mongo_db_path, mongo_db_logpath
from gwasstudio.utils.cfg import get_mongo_deployment, get_mongo_uri

mongo_deployment_types = ["embedded", "standalone"]


@contextmanager
def manage_mongo(ctx):
    embedded_mongo = (get_mongo_deployment(ctx) == "embedded") and (get_mongo_uri(ctx) is None)
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
    def __init__(self, dbpath=mongo_db_path, logpath=mongo_db_logpath):
        self.dbpath = dbpath
        self.process = None
        self.logpath = logpath

    def start(self):
        try:
            # Start the MongoDB server
            self.process = subprocess.Popen(
                ["mongod", "--dbpath", self.dbpath, "--logpath", self.logpath, "--logappend"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            logger.debug("Attempting to start embedded MongoDB server...")

            # Check if the server is running
            while True:
                return_code = self.process.poll()
                if return_code is not None:
                    logger.error("MongoDB server stopped unexpectedly.")
                    break

                # Check if the server is ready to accept connections
                try:
                    # Attempt to connect to the MongoDB server
                    subprocess.run(
                        ["mongosh", "--eval", "db.runCommand({ping: 1})"],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                    logger.info("MongoDB server is running and ready to accept connections.")
                    break
                except subprocess.CalledProcessError:
                    # Server is not ready yet, wait and try again
                    time.sleep(1)

        except Exception as e:
            logger.error(f"Failed to start MongoDB server: {e}")

    def stop(self):
        try:
            # Stop the MongoDB server
            self.process.terminate()
            self.process.wait()
            self.process = None
            logger.info("MongoDB server stopped.")
        except Exception as e:
            logger.error(f"Failed to stop MongoDB server: {e}")

    def __del__(self):
        if self.process and self.process.poll() is None:
            self.stop()
