"""
Manager to handle Mongoengine's connection
"""

from mongoengine import connect, disconnect

from gwasstudio.config_manager import ConfigurationManager


def get_mec(uri=None):
    return MongoEngineConnectionManager(
        uri=uri,
    )


def get_mec_from_config():
    return MongoEngineConnectionManager()


class MongoEngineConnectionManager:
    def __init__(self, **kwargs):
        cm = ConfigurationManager()

        if kwargs.get("uri") is None:
            self.uri = cm.get_mdbc_uri
        else:
            self.uri = kwargs.get("uri")

    def __enter__(self):
        connect(host=self.uri)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        disconnect()
