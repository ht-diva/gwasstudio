import json
import unittest

import mongomock
from mongoengine import connect, disconnect, get_connection

from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import generate_random_word
from gwasstudio.utils.hashing import Hashing


class TestDataProfile(unittest.TestCase):
    def setUp(self) -> None:
        self.mec = get_connection()
        self.cm = ConfigurationManager()
        self.data_id = Hashing().compute_hash(st=generate_random_word(64))
        self.study = generate_random_word(250)
        self.project = generate_random_word(250)

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

    def test_DataProfile_fields(self):
        kwargs = {
            "project": self.project,
            "study": self.study,
            "data_id": self.data_id,
            "trait": '{"code": "example1", "feature": "example2", "tissue": "example3"}',
            "category": "pQTL",
            "tags": ["tag1", "tag2"],
            "total": '{"samples": "10", "total_cases": "5"}',
            "population": ["NR"],
            "build": "GRCh38",
            "notes": '{"note1": ["value1", "value2"], "note2": "value3"}',
        }

        obj = EnhancedDataProfile(mec=self.mec, **kwargs)
        obj.save()

        JSON_results = {
            "trait": json.loads(kwargs.get("trait")),
            "total": json.loads(kwargs.get("total")),
            "notes": json.loads(kwargs.get("notes")),
        }

        from_mongo = DataProfile.objects(
            project=kwargs.get("project"), study=kwargs.get("study"), data_id=kwargs.get("data_id")
        ).first()
        self.assertEqual(from_mongo.project, kwargs.get("project"))
        self.assertEqual(from_mongo.study, kwargs.get("study"))
        self.assertEqual(from_mongo.data_id, kwargs.get("data_id"))
        self.assertEqual(from_mongo.trait, JSON_results.get("trait"))
        self.assertEqual(from_mongo.category.value, kwargs.get("category"))
        self.assertEqual(from_mongo.tags, kwargs.get("tags"))
        self.assertEqual(from_mongo.total, JSON_results.get("total"))
        self.assertEqual(from_mongo.population, kwargs.get("population"))
        self.assertEqual(from_mongo.build.value, kwargs.get("build"))
        self.assertEqual(from_mongo.notes, JSON_results.get("notes"))
