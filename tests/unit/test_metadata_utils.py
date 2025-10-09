import json
import unittest
from pathlib import Path

import pandas as pd
from ruamel.yaml import YAML

from gwasstudio.utils import generate_random_word
from gwasstudio.utils import lower_and_replace
from gwasstudio.utils.enums import MetadataEnum
from gwasstudio.utils.hashing import Hashing
from gwasstudio.utils.metadata import load_search_topics, load_metadata, process_row


class TestLoadSearchTopics(unittest.TestCase):
    def setUp(self):
        self.hg = Hashing()
        self.yml = YAML(typ="safe")
        self.test_file = Path("tests/test_metadata.yaml")
        self.test_dataset = Path("tests/test_dataset.txt")
        with open(self.test_dataset, "w") as f:
            f.write(generate_random_word(250))
        self.delimiter = "\t"
        self.test_file = Path("tests/test_metadata.tsv")
        with open(self.test_file, "w") as f:
            f.write("project\tstudy\tfile_path\tcategory\nvalue1\tvalue2\ttests/test_dataset.txt\tGWAS")

    def tearDown(self):
        del self.hg
        if self.test_file.exists():
            self.test_file.unlink()
        if self.test_dataset.exists():
            self.test_dataset.unlink()

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
        self.assertEqual(
            output_fields,
            ["project", "study", "category", "data_id", MetadataEnum.get_source_id_field(), "field1", "field2"],
        )

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

    def test_load_metadata(self):
        # Load the data
        df = load_metadata(self.test_file, delimiter="\t")

        # Check the loaded data.
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1, 4))
        self.assertEqual(df.columns.tolist(), ["project", "study", "file_path", "category"])

    def test_load_data_file_not_found(self):
        # Try to load a non-existent file
        with self.assertRaises(ValueError):
            load_metadata("non_existent_file.csv", delimiter="\t")

    def test_process_row(self):
        # Create a test row
        test_row = pd.Series(
            {
                "project": "project1",
                "study": "study1",
                "file_path": self.test_dataset,
                "category": "category1",
                "trait_subkey1": "value1",
                "notes_subkey2": "value2",
                "total_subkey3": "value3",
                "key4_subkey4": "value4",
            }
        )

        # Process the row
        metadata = process_row(test_row)

        # Check the processed metadata
        self.assertEqual(metadata["project"], lower_and_replace("project1"))
        self.assertEqual(metadata["study"], lower_and_replace("study1"))
        self.assertEqual(metadata["data_id"], self.hg.compute_hash(fpath=self.test_dataset))
        self.assertEqual(metadata["category"], "category1")
        self.assertEqual(json.loads(metadata["trait"]), {"subkey1": "value1"})
        self.assertEqual(json.loads(metadata["notes"]), {"subkey2": "value2"})
        self.assertEqual(json.loads(metadata["total"]), {"subkey3": "value3"})
        self.assertEqual(metadata["key4_subkey4"], "value4")

    def test_process_row_no_nested_keys(self):
        row = pd.Series(
            {
                "project": "Test Project",
                "study": "Test Study",
                "file_path": self.test_dataset,
                "other_field": "other_value",
            }
        )
        metadata = process_row(row)

        # Check if the nested key handling is skipped
        self.assertNotIn("json_field", metadata)
        self.assertEqual(metadata["other_field"], "other_value")
