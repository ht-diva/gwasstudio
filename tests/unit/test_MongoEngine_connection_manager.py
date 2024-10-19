import unittest


from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.connection_manager import MongoEngineConnectionManager, get_mec, get_mec_from_config


class TestMongoEngineConnectionManager(unittest.TestCase):
    def test_get_mec(self):
        cm = ConfigurationManager()
        mec = get_mec(uri="test_uri")
        self.assertEqual(mec.uri, "test_uri")

        mec = get_mec()
        self.assertEqual(mec.uri, cm.get_mdbc_uri)

    def test_get_mec_from_config(self):
        cm = ConfigurationManager()
        mec = get_mec_from_config()
        self.assertEqual(mec.uri, cm.get_mdbc_uri)

    def test_init(self):
        mec = MongoEngineConnectionManager(uri="test_uri")
        self.assertEqual(mec.uri, "test_uri")

    # def test_enter(self):
    #
    #     with patch.object(connect, 'host') as mock_connect:
    #         mec = MongoEngineConnectionManager(uri="test_uri")
    #         mec.__enter__()
    #         mock_connect.assert_called_once_with(host="test_uri")

    # def test_exit(self):
    #     mec = MongoEngineConnectionManager(db="test_db", uri="test_uri")
    #     with patch.object(disconnect) as mock_disconnect:
    #         mec.__exit__(None, None, None)
    #         mock_disconnect.assert_called_once()
