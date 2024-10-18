"""
Manager to handle Mongoengine's connection
"""

from mongoengine import connect, disconnect

from gwasstudio.config_manager import ConfigurationManager


def get_mec(db=None, uri=None):
    return MongoEngineConnectionManager(
        db=db,
        uri=uri,
    )


def get_mec_from_config():
    return MongoEngineConnectionManager()


class MongoEngineConnectionManager:
    def __init__(self, **kwargs):
        cm = ConfigurationManager()
        self.db = kwargs.get("db", cm.get_mdbc_db)
        self.uri = kwargs.get("uri", cm.get_mdbc_uri)

    def __enter__(self):
        connect(self.db, host=self.uri)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        disconnect()
