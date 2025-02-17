import unittest

import mongomock
from mongoengine import connect, disconnect, get_connection

from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import generate_random_word, compute_sha256


class TestEnhancedDataProfile(unittest.TestCase):
    def setUp(self) -> None:
        self.mec = get_connection()
        self.cm = ConfigurationManager()

        self.data_id = compute_sha256(st=generate_random_word(64))
        self.study = generate_random_word(250)

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
        study = self.study
        data_id = self.data_id
        obj = EnhancedDataProfile(project=project, study=study, data_id=data_id, mec=self.mec)
        obj.save()
        assert obj.pk is not None

    def test_unique_key(self):
        project = self.cm.get_project_list[0]
        study = self.study
        data_id = self.data_id
        obj = EnhancedDataProfile(project=project, study=study, data_id=data_id, mec=self.mec)
        self.assertEqual(obj.unique_key, f"{obj.mdb_obj.project}:{obj.mdb_obj.study}:{obj.mdb_obj.data_id}")

    def test_is_connected(self):
        """
        return False if mec is None else True
        """
        project = self.cm.get_project_list[0]
        data_id = self.data_id

        obj = EnhancedDataProfile(project=project, data_id=data_id, mec=None)
        self.assertFalse(obj.is_connected)
        obj = EnhancedDataProfile(project=project, data_id=data_id)
        self.assertTrue(obj.is_connected)
        obj = EnhancedDataProfile(project=project, data_id=data_id, mec=self.mec)
        self.assertTrue(obj.is_connected)
