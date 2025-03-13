import unittest
from pathlib import Path

from ruamel.yaml import YAML

from gwasstudio.cli.metadata.utils import load_search_topics


class TestLoadSearchTopics(unittest.TestCase):
    def setUp(self):
        self.yml = YAML(typ="safe")
        self.test_file = Path("tests/test_metadata.yaml")

    def test_load_search_topics_file_exists(self):
        """Loading search topics from a file that exists and contains valid YAML data."""
        # Create a temporary YAML file
        with open(self.test_file, "w") as file:
            self.yml.dump(
                {
                    "project": "Test Project",
                    "study": "Test Study",
                    "category": "Test Category",
                    "data_id": "Test Data ID",
                    "output": ["field1", "field2"],
                },
                file,
            )

        # Load search topics from the file
        topics, output_fields = load_search_topics(str(self.test_file))
        print(topics, output_fields)
        # Check if the topics and output fields are loaded correctly
        self.assertEqual(
            topics,
            {"project": "test_project", "study": "test_study", "category": "Test Category", "data_id": "Test Data ID"},
        )
        self.assertEqual(output_fields, ["project", "study", "category", "data_id", "field1", "field2"])

        # Remove the temporary file
        Path(self.test_file).unlink()

    def test_load_search_topics_file_does_not_exist(self):
        """Loading search topics from a file that does not exist."""
        # Load search topics from a non-existent file
        topics, output_fields = load_search_topics("non_existent_file.yaml")

        # Check if the function returns None for both topics and output fields
        self.assertIsNone(topics)
        self.assertIsNone(output_fields)

    def test_load_search_topics_file_is_empty(self):
        """Loading search topics from an empty file."""
        # Create a temporary empty YAML file
        with open("test.yaml", "w") as file:
            file.write("")

        # Load search topics from the empty file
        topics, output_fields = load_search_topics("test.yaml")

        # Check if the function returns None for both topics and output fields
        self.assertIsNone(topics)
        self.assertIsNone(output_fields)

        # Remove the temporary file
        Path("test.yaml").unlink()

    def test_load_search_topics_file_is_invalid(self):
        """Loading search topics from a file that contains invalid YAML data."""
        # Create a temporary invalid YAML file
        with open("test.yaml", "w") as file:
            file.write("Invalid YAML")

        # Load search topics from the invalid file
        with self.assertRaises(ValueError):
            load_search_topics("test.yaml")

        # Remove the temporary file
        Path("test.yaml").unlink()
