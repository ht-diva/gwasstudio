import json
import unittest
from pathlib import Path

import pandas as pd

from gwasstudio.cli.metadata.utils import load_metadata, process_row
from gwasstudio.utils import generate_random_word, lower_and_replace, compute_sha256


class TestIngestFunctionality(unittest.TestCase):
    def setUp(self):
        self.test_dataset = Path("tests/test_dataset.txt")
        with open(self.test_dataset, "w") as f:
            f.write(generate_random_word(250))
        self.delimiter = "\t"
        self.test_file = Path("tests/test_metadata.tsv")
        with open(self.test_file, "w") as f:
            f.write("project\tstudy\tfile_path\tcategory\nvalue1\tvalue2\ttests/test_dataset.txt\tGWAS")

    def tearDown(self):
        if self.test_file.exists():
            self.test_file.unlink()
        if self.test_dataset.exists():
            self.test_dataset.unlink()

    def test_load_data(self):
        # Load the data
        df = load_metadata(self.test_file, delimiter="\t")

        # Check the loaded data
        self.assertEqual(df.shape, (1, 4))
        self.assertEqual(df.columns.tolist(), ["project", "study", "file_path", "category"])

    def test_load_data_file_not_found(self):
        # Try to load a non-existent file
        with self.assertRaises(SystemExit):
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
        self.assertEqual(metadata["data_id"], compute_sha256(fpath=self.test_dataset))
        self.assertEqual(metadata["category"], "category1")
        self.assertEqual(json.loads(metadata["trait"]), {"subkey1": "value1"})
        self.assertEqual(json.loads(metadata["notes"]), {"subkey2": "value2"})
        self.assertEqual(json.loads(metadata["total"]), {"subkey3": "value3"})
        self.assertEqual(metadata["key4_subkey4"], "value4")
