import os
import tempfile
import unittest

from gwasstudio.utils.io import read_to_bed, _is_path


class TestReadToBed(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test files
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Remove the temporary directory
        for f in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, f))
        os.rmdir(self.test_dir)

    def test_bed_format(self):
        # Test standard chromosomes (e.g., chr1, chr2)
        data = """chr1\t100\t200
chr2\t300\t400"""
        fp = os.path.join(self.test_dir, "test.bed")
        with open(fp, "w") as f:
            f.write(data)
        df = read_to_bed(fp)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1, 2])

    def test_is_path_inline_regions(self):
        self.assertFalse(_is_path("1,100,200;2,300,400"))

    def test_is_path_inline_snps(self):
        self.assertFalse(_is_path("1,100;2,300"))

    def test_inline_regions(self):
        # Test regions inline string (e.g., CHR,START,END;CHR,START,END)
        fp_string = "1,100,200;2,300,400"
        df = read_to_bed(fp_string)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1, 2])
        self.assertListEqual(df["START"].tolist(), [100, 300])
        self.assertListEqual(df["END"].tolist(), [200, 400])

    def test_inline_snps(self):
        # Test SNPs inline string (e.g., CHR,POS;CHR,POS)
        fp_string = "1,100;2,300"
        df = read_to_bed(fp_string)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1, 2])
        self.assertListEqual(df["START"].tolist(), [100, 300])
        self.assertListEqual(df["END"].tolist(), [101, 301])

    def test_bed_format_xy_chromosomes(self):
        # Test X and Y chromosomes
        data = """chrX\t100\t200
chrY\t300\t400"""
        fp = os.path.join(self.test_dir, "test.bed")
        with open(fp, "w") as f:
            f.write(data)
        df = read_to_bed(fp)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [23, 24])

    def test_bed_format_mixed_chromosomes(self):
        # Test mixed chromosomes (e.g., chr1, X, Y)
        data = """chr1\t100\t200
X\t300\t400
Y\t500\t600"""
        fp = os.path.join(self.test_dir, "test.bed")
        with open(fp, "w") as f:
            f.write(data)
        df = read_to_bed(fp)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1, 23, 24])

    def test_bed_format_non_numeric_chr(self):
        # Test BED format with non-numeric CHR (should be removed)
        data = """chr1\t100\t200
chrM\t300\t400"""
        fp = os.path.join(self.test_dir, "test.bed")
        with open(fp, "w") as f:
            f.write(data)

        df = read_to_bed(fp)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1])
        self.assertListEqual(df["START"].tolist(), [100])
        self.assertListEqual(df["END"].tolist(), [200])

    def test_snp_list_format_non_numeric_chr(self):
        # Test SNP list with non-numeric CHR (should be removed)
        data = """CHR,POS
chr1,100
chrM,300"""
        fp = os.path.join(self.test_dir, "test_snp.csv")
        with open(fp, "w") as f:
            f.write(data)

        df = read_to_bed(fp)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1])
        self.assertListEqual(df["START"].tolist(), [100])
        self.assertListEqual(df["END"].tolist(), [101])

    def test_snp_list_format(self):
        # Test SNP list format
        data = """CHR,POS
chr1,100
chr2,300
chrX,500
chrY,700"""
        fp = os.path.join(self.test_dir, "test_snp.csv")
        with open(fp, "w") as f:
            f.write(data)

        df = read_to_bed(fp)
        self.assertIsNotNone(df)
        self.assertListEqual(df["CHR"].tolist(), [1, 2, 23, 24])
        self.assertListEqual(df["START"].tolist(), [100, 300, 500, 700])
        self.assertListEqual(df["END"].tolist(), [101, 301, 501, 701])

    def test_invalid_file(self):
        # Test invalid file (e.g., non-BED format)
        fp = os.path.join(self.test_dir, "invalid.bed")
        with open(fp, "w") as f:
            f.write("not_a_bed_file")
        with self.assertRaises(ValueError):
            read_to_bed(fp)
