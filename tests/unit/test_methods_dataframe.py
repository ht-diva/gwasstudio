import unittest

import numpy as np
import pandas as pd

from gwasstudio.methods.dataframe import _get_log_p_value_from_z, _build_snpid, process_dataframe


class TestUtils(unittest.TestCase):
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
        with self.assertRaises(KeyError):
            _build_snpid(df)

    def test_process_dataframe(self):
        data = {
            "BETA": [0.1, 0.2, 0.3],
            "SE": [0.05, 0.1, 0.15],
            "CHR": [1, 2, 3],
            "POS": [100, 200, 300],
            "EA": ["A", "T", "G"],
            "NEA": ["T", "A", "C"],
            "TRAITID": [1, 2, 3],
        }
        df = pd.DataFrame(data)
        attributes = ["MLOG10P", "SNIPID"]
        processed_df = process_dataframe(df, attributes, drop_tid=True)

        expected_data = {
            "BETA": [0.1, 0.2, 0.3],
            "SE": [0.05, 0.1, 0.15],
            "CHR": [1, 2, 3],
            "POS": [100, 200, 300],
            "EA": ["A", "T", "G"],
            "NEA": ["T", "A", "C"],
            "MLOG10P": [1.3419861, 1.3419861, 1.3419861],
            "SNIPID": ["1:100:A:T", "2:200:T:A", "3:300:G:C"],
        }
        expected_df = pd.DataFrame(expected_data)
        expected_df["MLOG10P"] = expected_df["MLOG10P"].astype(np.float32)
        pd.testing.assert_frame_equal(processed_df, expected_df)
