import unittest

from gwasstudio.utils import find_item, parse_uri


class TestFindItemFunction(unittest.TestCase):
    def test_key_found_in_root(self):
        obj = {"a": 1, "b": 2}
        self.assertEqual(find_item(obj, "a"), 1)

    def test_key_not_found_in_root(self):
        obj = {"a": 1, "b": 2}
        self.assertIsNone(find_item(obj, "c"))

    def test_key_found_in_nested_dict(self):
        obj = {"a": 1, "b": {"c": 3, "d": 4}}
        self.assertEqual(find_item(obj, "c"), 3)

    def test_key_not_found_in_nested_dict(self):
        obj = {"a": 1, "b": {"c": 3, "d": 4}}
        self.assertIsNone(find_item(obj, "e"))

    def test_key_found_in_deeply_nested_dict(self):
        obj = {"a": 1, "b": {"c": 3, "d": {"e": 5, "f": 6}}}
        self.assertEqual(find_item(obj, "e"), 5)

    def test_key_not_found_in_deeply_nested_dict(self):
        obj = {"a": 1, "b": {"c": 3, "d": {"e": 5, "f": 6}}}
        self.assertIsNone(find_item(obj, "g"))

    def test_empty_dict(self):
        obj = {}
        self.assertIsNone(find_item(obj, "a"))

    def test_none_input(self):
        with self.assertRaises(TypeError):
            find_item(None, "a")

    def test_non_dict_input(self):
        with self.assertRaises(AttributeError):
            find_item("a", "b")


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
        uri = "file:///root/path"
        scheme, netloc, path = parse_uri(uri)
        self.assertEqual(scheme, "file")
        self.assertEqual(netloc, "")
        self.assertEqual(path, "/root/path")
