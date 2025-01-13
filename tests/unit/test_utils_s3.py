import unittest

from gwasstudio.utils.s3 import parse_uri


class TestParseUri(unittest.TestCase):
    def test_valid_s3_uri(self):
        uri = "s3://bucket/key"
        scheme, netloc, path = parse_uri(uri)
        self.assertEqual(scheme, "s3")
        self.assertEqual(netloc, "bucket")
        self.assertEqual(path, "key")

    def test_valid_https_uri(self):
        uri = "https://example.com/path"
        scheme, netloc, path = parse_uri(uri)
        self.assertEqual(scheme, "https")
        self.assertEqual(netloc, "example.com")
        self.assertEqual(path, "path")

    def test_valid_file_uri(self):
        uri = "file://root/path"
        scheme, netloc, path = parse_uri(uri)
        self.assertEqual(scheme, "file")
        self.assertEqual(netloc, "root")
        self.assertEqual(path, "path")
