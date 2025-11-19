import unittest

import numpy as np
import pandas as pd

from gwasstudio.methods.dataframe import (
    _get_log_p_value_from_z,
    _build_snpid,
    _check_required_columns,
    process_dataframe,
)


class TestDataframeHelpers(unittest.TestCase):
    def test_get_log_p_value_from_z(self):
        z_scores = np.array([0, 1, 2, 3])
        expected_log10_p_values = np.array([0.0, 0.498516, 1.341986, 2.568669])
        log10_p_values = _get_log_p_value_from_z(z_scores)
        np.testing.assert_almost_equal(log10_p_values, expected_log10_p_values, decimal=6)

    def test_build_snpid(self):
        data = {"CHR": [1, 2, 3], "POS": [100, 200, 300], "EA": ["A", "T", "G"], "NEA": ["T", "A", "C"]}
        df = pd.DataFrame(data)
        expected_snpid_series = pd.Series(["1:100:A:T", "2:200:T:A", "3:300:G:C"])
        snpid_series = _build_snpid(df)
        pd.testing.assert_series_equal(snpid_series, expected_snpid_series)

    def test_build_snpid_missing_columns(self):
        data = {"CHR": [1, 2, 3], "POS": [100, 200, 300], "EA": ["A", "T", "G"]}
        df = pd.DataFrame(data)
        with self.assertRaises(KeyError) as cm:
            match = "'Missing required columns in DataFrame: NEA'"
            _build_snpid(df)
        self.assertEqual(str(cm.exception), match)

    def test_check_required_columns(self):
        """Test the _check_required_columns function."""
        df = pd.DataFrame({"CHR": [1], "POS": [1000]})

        with self.assertRaises(KeyError) as cm:
            match = "'Missing required columns in DataFrame: EA, NEA'"
            _check_required_columns({"CHR", "POS", "EA", "NEA"}, df)
        self.assertEqual(str(cm.exception), match)


class TestProcessDataFrame(unittest.TestCase):
    def setUp(self):
        self.data = {
            "BETA": [0.1, 0.3],
            "SE": [0.05, 0.15],
            "CHR": ["1", "2"],
            "POS": [1000, 2000],
            "EA": ["A", "T"],
            "NEA": ["C", "G"],
            "TRAITID": ["trait1", "trait2"],
        }
        self.df = pd.DataFrame(self.data)

    def test_process_dataframe_with_mlog10p(self):
        processed_df = process_dataframe(self.df, drop_tid=True)
        self.assertIn("MLOG10P", processed_df.columns)
        np.testing.assert_array_almost_equal(processed_df["MLOG10P"], [1.3419861, 1.3419861])

    def test_process_dataframe_with_snpid(self):
        processed_df = process_dataframe(self.df, drop_tid=True)
        self.assertIn("SNIPID", processed_df.columns)
        np.testing.assert_array_equal(processed_df["SNIPID"], ["1:1000:A:C", "2:2000:T:G"])

    def test_process_dataframe_drop_tid(self):
        processed_df = process_dataframe(self.df, drop_tid=True)
        self.assertNotIn("TRAITID", processed_df.columns)

    def test_process_dataframe_without_drop_tid(self):
        processed_df = process_dataframe(self.df, drop_tid=False)
        self.assertIn("TRAITID", processed_df.columns)

    def test_missing_columns(self):
        df = pd.DataFrame({"BETA": [1.96], "SE": [1], "CHR": [1]})

        with self.assertRaises(KeyError):
            process_dataframe(df)
