import unittest

import pandas as pd

from gwasstudio.cli.export import create_output_prefix_dict
from gwasstudio.utils.enums import MetadataEnum


class TestCreateOutputPrefixDict(unittest.TestCase):
    def setUp(self) -> None:
        self.source_id_field = MetadataEnum.get_source_id_field()

    def test_source_id_column_exists_and_has_no_nans(self):
        """Test when source_id_column exists and has no NaNs."""
        data = {
            "data_id": pd.Series([101, 102, 103], dtype="string"),
            "notes_source_id": pd.Series([456, 789, 104], dtype="string"),
        }
        df = pd.DataFrame(data)
        output_prefix = "output"

        result = create_output_prefix_dict(df, output_prefix, source_id_column=self.source_id_field)

        self.assertEqual(result, {"101": "output_456", "102": "output_789", "103": "output_104"})

    def test_source_id_column_has_nans(self):
        """Test when source_id_column has some NaNs."""
        data = {
            "data_id": pd.Series([101, 102, 103], dtype="string"),
            "notes_source_id": pd.Series([456, None, 104], dtype="string"),
        }

        df = pd.DataFrame(data)
        output_prefix = "output"

        result = create_output_prefix_dict(df, output_prefix, source_id_column=self.source_id_field)

        self.assertEqual(result, {"101": "output_456", "102": "output_102", "103": "output_104"})

    def test_source_id_column_does_not_exist(self):
        """Test when source_id_column is not present in the DataFrame."""
        data = {"data_id": pd.Series([101, 102, 103], dtype="string")}
        df = pd.DataFrame(data)
        output_prefix = "output"

        result = create_output_prefix_dict(df, output_prefix, source_id_column=self.source_id_field)

        self.assertEqual(result, {"101": "output_101", "102": "output_102", "103": "output_103"})

    def test_empty_dataframe(self):
        """Test empty DataFrame returns an empty dictionary."""
        df = pd.DataFrame(columns=["data_id", "notes_source_id"])
        output_prefix = "output"

        result = create_output_prefix_dict(df, output_prefix, source_id_column=self.source_id_field)

        self.assertEqual(result, {})
