import unittest

import mongomock
from mongoengine import connect, disconnect, get_connection

from gwasstudio.mongo.models import EnhancedDataProfile, Ancestry, Build


class TestEnhancedDataProfileQuery(unittest.TestCase):
    def setUp(self) -> None:
        self.mec = get_connection()
        self.profile1 = EnhancedDataProfile(
            mec=self.mec,
            project="project1",
            data_id="data_id1",
            trait_desc="trait_desc1",
            total_samples=10,
            total_cases=5,
            population=Ancestry.EUROPEAN,
            references=[],
            build=Build.GRCH37,
        )
        self.profile1.save()
        self.profile2 = EnhancedDataProfile(
            mec=self.mec,
            project="project1",
            data_id="data_id2",
            trait_desc="trait_desc2",
            total_samples=20,
            total_cases=10,
            population=Ancestry.ICELANDIC,
            references=[],
            build=Build.GRCH38,
        )
        self.profile2.save()
        self.profile3 = EnhancedDataProfile(
            mec=self.mec,
            project="project2",
            data_id="data_id1",
            trait_desc="trait_desc1",
            total_samples=30,
            total_cases=15,
            population=Ancestry.EUROPEAN,
            references=[],
            build=Build.GRCH37,
        )
        self.profile3.save()

    def tearDown(self) -> None:
        self.profile1.delete()
        self.profile2.delete()
        self.profile3.delete()

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

    def test_query_by_trait_desc(self):
        profiles = EnhancedDataProfile(mec=self.mec).query(trait_desc="trait_desc1")
        assert len(profiles) == 2
        self.assertIn(self.profile1.view(), profiles)
        self.assertIn(self.profile3.view(), profiles)

    def test_query_by_project_and_data_id(self):
        profile = EnhancedDataProfile(mec=self.mec).query(project="project1", data_id="data_id1")
        self.assertEqual(1, len(profile))
        self.assertEqual(self.profile1.view(), profile[0])

    def test_query_by_population(self):
        profiles = EnhancedDataProfile(mec=self.mec).query(population=Ancestry.ICELANDIC)
        self.assertEqual(len(profiles), 1)
        self.assertIn(self.profile2.view(), profiles)

    def test_query_by_build(self):
        profiles = EnhancedDataProfile(mec=self.mec).query(build=Build.GRCH37)
        self.assertEqual(len(profiles), 2)
        self.assertIn(self.profile1.view(), profiles)
        self.assertIn(self.profile3.view(), profiles)