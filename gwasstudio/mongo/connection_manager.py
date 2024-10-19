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
        if "db" in kwargs and kwargs["db"] is None:
            self.db = cm.get_mdbc_db
        else:
            self.db = kwargs.get("db")

        if "uri" in kwargs and kwargs["uri"] is None:
            self.uri = cm.get_mdbc_uri
        else:
            self.uri = kwargs.get("uri")

    def __enter__(self):
        connect(self.db, host=self.uri)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        disconnect()
