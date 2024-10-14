import unittest

import mongomock
from mongoengine import connect, disconnect, get_connection
from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile


class TestEnhancedDataProfile(unittest.TestCase):
    def setUp(self) -> None:
        self.mec = get_connection()
        self.cm = ConfigurationManager()

    def tearDown(self) -> None:
        if DataProfile.objects().first():
            DataProfile.objects().first().delete()

    @classmethod
    def setUpClass(cls):
        connect(
            "mongoenginetest",
            host="mongodb://localhost",
            mongo_client_class=mongomock.MongoClient,
            uuidRepresentation="standard",
        )

    @classmethod
    def tearDownClass(cls):
        disconnect()

    def test_pk_offline(self):
        """
        If obj is not mapped, then obj.pk is None
        """
        obj = EnhancedDataProfile()
        assert obj.pk is None

    def test_pk_online(self):
        """
        If obj is mapped, then obj.pk is not None
        """
        project = self.cm.get_project_list[0]
        data_id = "1ce4b858d04eeef0090de34c29d6042c7d1fc0e65a889dd9c44e11a5459eb3df"
        obj = EnhancedDataProfile(project=project, data_id=data_id, mec=self.mec)
        obj.save()
        assert obj.pk is not None

    def test_unique_key(self):
        project = self.cm.get_project_list[0]
        data_id = "1ce4b858d04eeef0090de34c29d6042c7d1fc0e65a889dd9c44e11a5459eb3df"
        obj = EnhancedDataProfile(project=project, data_id=data_id, mec=self.mec)
        self.assertEqual(obj.unique_key, f"{obj.mdb_obj.project}:{obj.mdb_obj.data_id}")
