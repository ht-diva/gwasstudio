import os
import tempfile
import unittest

from gwasstudio.utils.hashing import Hashing


class TestHashing(unittest.TestCase):
    def setUp(self):
        self.hashing = Hashing()
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.temp_file.write(b"test data")
        self.temp_file.close()

    def tearDown(self):
        os.remove(self.temp_file.name)
        del self.hashing

    def test_compute_hash_file(self):
        hash_value = self.hashing.compute_hash(fpath=self.temp_file.name)
        self.assertEqual(len(hash_value), self.hashing.hash_length)
        self.assertIsInstance(hash_value, str)

    def test_compute_hash_string(self):
        hash_value = self.hashing.compute_hash(st="test data")
        self.assertEqual(len(hash_value), self.hashing.hash_length)
        self.assertIsInstance(hash_value, str)

    def test_compute_hash_none(self):
        hash_value = self.hashing.compute_hash()
        self.assertIsNone(hash_value)

    def test_compute_hash_both(self):
        with self.assertRaises(ValueError):
            self.hashing.compute_hash(fpath=self.temp_file.name, st="test data")

    def test_singleton(self):
        another_hashing = Hashing()
        self.assertIs(self.hashing, another_hashing)
