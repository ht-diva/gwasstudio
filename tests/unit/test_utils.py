import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from gwasstudio.utils import find_item, parse_uri, write_table


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

    def test_valid_file_uri_bis(self):
        uri = "/root/path"
        scheme, netloc, path = parse_uri(uri)
        self.assertEqual(scheme, "")
        self.assertEqual(netloc, "")
        self.assertEqual(path, "/root/path")


class TestWriteTable(unittest.TestCase):
    def setUp(self):
        self.logger = MagicMock()
        self.df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})

    def test_write_parquet(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = Path(tmpdirname) / "test_output"
            write_table(self.df, str(output_path), self.logger, file_format="parquet")
            self.assertTrue(output_path.with_suffix(".parquet").exists())
            self.logger.info.assert_called_with(f"Saving DataFrame to {output_path.with_suffix('.parquet')}")

    def test_write_csv(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = Path(tmpdirname) / "test_output"
            write_table(self.df, str(output_path), self.logger, file_format="csv")
            self.assertTrue(output_path.with_suffix(".csv").exists())
            self.logger.info.assert_called_with(f"Saving DataFrame to {output_path.with_suffix('.csv')}")

    def test_custom_log_message(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = Path(tmpdirname) / "test_output"
            custom_msg = "Custom log message"
            write_table(self.df, str(output_path), self.logger, file_format="parquet", log_msg=custom_msg)
            self.assertTrue(output_path.with_suffix(".parquet").exists())
            self.logger.info.assert_called_with(custom_msg)

    def test_invalid_format(self):
        with tempfile.TemporaryDirectory() as tmpdirname:
            output_path = Path(tmpdirname) / "test_output"
            with self.assertRaises(ValueError):
                write_table(self.df, str(output_path), self.logger, file_format="invalid_format")
