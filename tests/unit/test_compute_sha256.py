import unittest

import mongomock
from mongoengine import connect, disconnect

from gwasstudio.utils import compute_hash, compute_sha256


class TestComputesha256(unittest.TestCase):
    def setUp(self) -> None:
        pass
        # self.mec = get_connection()
        # self.cm = ConfigurationManager()
        #
        # self.data_id = compute_sha256(st=generate_random_word(64))

    def tearDown(self) -> None:
        pass
        # if DataProfile.objects().first():
        #     DataProfile.objects().first().delete()

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

    def test_compute_sha256(self):
        path = "tests/data/file.bin"
        checksum = "875617088a4f08e5d836b8629f6bf16d9bc5bf4b1c43b8520af0bcb3d4814a62"

        digest = compute_sha256(fpath=path)
        assert digest == checksum

    def test_compute_hash(self):
        path = "tests/data/file.bin"
        checksum = "875617088a4f08e5d836b8629f6bf16d9bc5bf4b1c43b8520af0bcb3d4814a62"
        length = 10

        digest = compute_hash(fpath=path, length=length)
        assert digest == checksum[:length]

        digest = compute_hash(length=length)
        self.assertIsNone(digest)
