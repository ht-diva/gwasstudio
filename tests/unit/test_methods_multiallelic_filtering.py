import unittest

import pandas as pd

from gwasstudio.methods.multiallelic_filtering import keep_best_multiallelic_variant


class TestKeepBestMultiallelicVariant(unittest.TestCase):
    def test_keeps_biallelic_variant_with_highest_maf(self):
        """Keep the biallelic variant with the highest MAF."""
        df = pd.DataFrame(
            {
                "CHR": ["1", "1", "2"],
                "POS": [1000, 1000, 2000],
                "EA": ["A", "A", "T"],
                "NEA": ["C", "G", "G"],
                "EAF": [0.995, 0.792, 0.007],
            }
        )
        df_filtered = keep_best_multiallelic_variant(df)
        # MAF values at 1:1000 are:
        # A/C: min(0.995, 0.005) = 0.005
        # A/G: min(0.792, 0.208) = 0.208
        expected_snpids = ["1:1000:A:G", "2:2000:T:G"]
        self.assertListEqual(df_filtered["SNPID"].tolist(), expected_snpids)

    def test_prefers_valid_biallelic_snv(self):
        """Ignore a non-biallelic variant even when it has a higher MAF."""
        df = pd.DataFrame(
            {
                "CHR": ["1", "1", "2"],
                "POS": [1000, 1000, 2000],
                "EA": ["A", "AC", "T"],
                "NEA": ["C", "G", "G"],
                "EAF": [0.995, 0.792, 0.007],
            }
        )
        df_filtered = keep_best_multiallelic_variant(df)
        expected_snpids = ["1:1000:A:C", "2:2000:T:G"]
        self.assertListEqual(df_filtered["SNPID"].tolist(), expected_snpids)

    def test_removes_locus_without_valid_biallelic_snv(self):
        """Remove a multiallelic locus containing no valid biallelic SNV."""
        df = pd.DataFrame(
            {
                "CHR": ["1", "1", "2"],
                "POS": [1000, 1000, 2000],
                "EA": ["AC", "AC", "T"],
                "NEA": ["C", "G", "G"],
                "EAF": [0.995, 0.792, 0.007],
            }
        )
        df_filtered = keep_best_multiallelic_variant(df)
        expected_snpids = ["2:2000:T:G"]
        self.assertListEqual(df_filtered["SNPID"].tolist(), expected_snpids)

    def test_preserves_ordinary_loci(self):
        """Preserve rows at loci containing only one allele pair."""
        df = pd.DataFrame(
            {
                "CHR": ["1", "2"],
                "POS": [1000, 2000],
                "EA": ["A", "AC"],
                "NEA": ["G", "A"],
                "EAF": [0.20, 0.30],
            }
        )
        df_filtered = keep_best_multiallelic_variant(df)
        expected_snpids = ["1:1000:A:G", "2:2000:AC:A"]
        self.assertListEqual(df_filtered["SNPID"].tolist(), expected_snpids)

    def test_removes_temporary_maf_column(self):
        """Do not include the temporary _MAF column in the result."""
        df = pd.DataFrame(
            {
                "CHR": ["1", "1"],
                "POS": [1000, 1000],
                "EA": ["A", "A"],
                "NEA": ["C", "G"],
                "EAF": [0.10, 0.20],
            }
        )
        df_filtered = keep_best_multiallelic_variant(df)
        self.assertNotIn("_MAF", df_filtered.columns)

    def test_builds_snpid_when_missing(self):
        """Build SNPID when it is absent from the input."""
        df = pd.DataFrame(
            {
                "CHR": ["1"],
                "POS": [1000],
                "EA": ["A"],
                "NEA": ["G"],
                "EAF": [0.10],
            }
        )
        df_filtered = keep_best_multiallelic_variant(df)
        self.assertIn("SNPID", df_filtered.columns)
        self.assertEqual(df_filtered.iloc[0]["SNPID"], "1:1000:A:G")

    def test_missing_required_column(self):
        """Raise KeyError when a required column is missing."""
        df = pd.DataFrame(
            {
                "CHR": ["1"],
                "POS": [1000],
                "EA": ["A"],
                "NEA": ["G"],
                # EAF is missing.
            }
        )
        with self.assertRaises(KeyError):
            keep_best_multiallelic_variant(df)
