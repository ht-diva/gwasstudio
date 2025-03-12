import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ruamel.yaml import YAML

from gwasstudio.cli.metadata.utils import load_search_topics


class TestLoadSearchTopics(unittest.TestCase):
    def setUp(self):
        self.yml = YAML(typ="safe")
        self.test_file = Path("tests/test_metadata.yaml")
        self.test_data = {"trait_desc": [{"a": "1"}, {"b": "2"}]}
        self.yml.dump(self.test_data, self.test_file)

    def tearDown(self):
        if self.test_file.exists():
            self.test_file.unlink()

    @patch("pathlib.Path.exists", new_callable=MagicMock)
    def test_loads_search_topics(self, mock_exists):
        mock_exists.return_value = True
        result = load_search_topics(str(self.test_file))
        self.assertIsInstance(result, dict)
        self.assertDictEqual(self.test_data, result)
        # mock_exists.assert_called_once_with(str(self.test_file))

    @patch("pathlib.Path.exists", new_callable=MagicMock)
    def test_returns_none_if_file_doesnt_exist(self, mock_exists):
        mock_exists.return_value = False
        result = load_search_topics(str(self.test_file.name))
        self.assertIsNone(result)
        # mock_exists.assert_called_once_with(str(self.test_file))
