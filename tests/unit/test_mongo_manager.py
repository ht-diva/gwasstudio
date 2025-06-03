import subprocess
import unittest
from unittest.mock import patch, MagicMock

from gwasstudio.utils.mongo_manager import MongoDBManager


class TestMongoDBManager(unittest.TestCase):
    @patch("gwasstudio.utils.mongo_manager.subprocess.Popen")
    @patch("gwasstudio.utils.mongo_manager.subprocess.run")
    @patch("gwasstudio.utils.mongo_manager.logger")
    def test_start(self, mock_logger, mock_subprocess_run, mock_subprocess_popen):
        # Mock the Popen and run methods
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_subprocess_popen.return_value = mock_process
        mock_subprocess_run.return_value = MagicMock()

        # Create an instance of MongoDBManager
        manager = MongoDBManager(dbpath="/tmp/mongo_test", logpath="/tmp/mongo_test.log")

        # Start the MongoDB server
        manager.start()

        # Assert that the Popen method was called with the correct arguments
        mock_subprocess_popen.assert_called_once_with(
            [
                "mongod",
                "--dbpath",
                "/tmp/mongo_test",
                "--logpath",
                "/tmp/mongo_test.log",
                "--logappend",
                "--port",
                "27018",
                "--bind_ip_all",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Assert that the logger was called to indicate the server is running
        # mock_logger.info.assert_called_with("MongoDB server on localhost:27018 is running and ready to accept connections.")

    @patch("gwasstudio.utils.mongo_manager.subprocess.Popen")
    @patch("gwasstudio.utils.mongo_manager.logger")
    def test_stop(self, mock_logger, mock_subprocess_popen):
        # Mock the Popen method
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_subprocess_popen.return_value = mock_process

        # Create an instance of MongoDBManager
        manager = MongoDBManager(dbpath="/tmp/mongo_test")

        # Start the MongoDB server
        manager.start()

        # Stop the MongoDB server
        manager.stop()

        # Assert that the terminate and wait methods were called
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once()

        # Assert that the logger was called to indicate the server is stopped
        mock_logger.info.assert_called_with("MongoDB server stopped.")

    @patch("gwasstudio.utils.mongo_manager.subprocess.Popen")
    @patch("gwasstudio.utils.mongo_manager.logger")
    def test_del(self, mock_logger, mock_subprocess_popen):
        # Mock the Popen method
        mock_process = MagicMock()
        mock_process.poll.return_value = None
        mock_subprocess_popen.return_value = mock_process

        # Create an instance of MongoDBManager
        manager = MongoDBManager(dbpath="/tmp/mongo_test")

        # Start the MongoDB server
        manager.start()

        # Explicitly call the __del__ method
        manager.__del__()

        # Assert that the terminate and wait methods were called
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once()

        # Assert that the logger was called to indicate the server is stopped
        mock_logger.info.assert_called_with("MongoDB server stopped.")
