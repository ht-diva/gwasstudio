import mongomock
import unittest


from mongoengine import connect, disconnect, get_connection
from gwasstudio.utils import compute_sha256, generate_random_word
from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile


class TestDataProfile(unittest.TestCase):
    def setUp(self) -> None:
        self.mec = get_connection()
        self.cm = ConfigurationManager()

        self.data_id = compute_sha256(st=generate_random_word(64))

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
            "project": self.cm.get_project_list[0],
            "data_id": self.data_id,
            "trait_desc": '{"code": "example1", "feature": "example2, "tissue": "example3"}',
            "category": "pQTL",
            "tags": ["tag1", "tag2"],
            "total_samples": 10,
            "total_cases": 5,
            "population": "NR",
            "build": "GRCh38",
        }

        obj = EnhancedDataProfile(mec=self.mec, **kwargs)
        obj.save()

        from_mongo = DataProfile.objects(project=kwargs.get("project"), data_id=kwargs.get("data_id")).first()
        self.assertEqual(from_mongo.project.value, kwargs.get("project"))
        self.assertEqual(from_mongo.data_id, kwargs.get("data_id"))
        self.assertEqual(from_mongo.trait_desc, kwargs.get("trait_desc"))
        self.assertEqual(from_mongo.category.value, kwargs.get("category"))
        self.assertEqual(from_mongo.tags, kwargs.get("tags"))
        self.assertEqual(from_mongo.total_samples, kwargs.get("total_samples"))
        self.assertEqual(from_mongo.total_cases, kwargs.get("total_cases"))
        self.assertEqual(from_mongo.population.value, kwargs.get("population"))
        self.assertEqual(from_mongo.build.value, kwargs.get("build"))
