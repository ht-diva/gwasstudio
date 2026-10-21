import os
import tempfile
import unittest

from gwasstudio.cli.region_io import _is_path, read_to_bed


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


class TestReadToBedRegionBuffer(unittest.TestCase):
    """--region-buffer behavior: extend region rows, keep SNP rows, preserve originals."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        for f in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, f))
        os.rmdir(self.test_dir)

    def _write_bed(self, data, name="regions.bed"):
        fp = os.path.join(self.test_dir, name)
        with open(fp, "w") as f:
            f.write(data)
        return fp

    def test_bed_regions_extended(self):
        fp = self._write_bed("chr1\t1000\t2000\nchr2\t3000\t4000")
        df = read_to_bed(fp, region_buffer=500)
        self.assertListEqual(df["START"].tolist(), [500, 2500])
        self.assertListEqual(df["END"].tolist(), [2500, 4500])
        # Original boundaries are preserved for downstream region semantics.
        self.assertListEqual(df["START_ORIG"].tolist(), [1000, 3000])
        self.assertListEqual(df["END_ORIG"].tolist(), [2000, 4000])

    def test_bed_clipped_at_chromosome_start(self):
        fp = self._write_bed("chr1\t100\t200")
        df = read_to_bed(fp, region_buffer=500)
        self.assertEqual(df["START"].iloc[0], 1)
        self.assertEqual(df["END"].iloc[0], 700)
        self.assertEqual(df["START_ORIG"].iloc[0], 100)

    def test_zero_buffer_is_noop(self):
        fp = self._write_bed("chr1\t1000\t2000")
        df = read_to_bed(fp, region_buffer=0)
        self.assertListEqual(df["START"].tolist(), [1000])
        self.assertListEqual(df["END"].tolist(), [2000])
        self.assertNotIn("START_ORIG", df.columns)
        self.assertNotIn("END_ORIG", df.columns)

    def test_inline_regions_extended(self):
        df = read_to_bed("1,1000,2000;2,300,400", region_buffer=100)
        self.assertListEqual(df["START"].tolist(), [900, 200])
        self.assertListEqual(df["END"].tolist(), [2100, 500])
        self.assertListEqual(df["START_ORIG"].tolist(), [1000, 300])

    def test_snp_rows_unaffected(self):
        # SNP rows (END == START + 1) must not be extended.
        fp = self._write_bed("CHR,POS\nchr1,100\nchr2,300\n", name="snps.csv")
        df = read_to_bed(fp, region_buffer=500)
        self.assertListEqual(df["START"].tolist(), [100, 300])
        self.assertListEqual(df["END"].tolist(), [101, 301])
        self.assertListEqual(df["START_ORIG"].tolist(), [100, 300])

    def test_mixed_snp_and_region_rows(self):
        # A single-region BED row is extended; a 1-bp "region" (SNP-like) is not.
        fp = self._write_bed("chr1\t100\t101\nchr2\t1000\t2000")
        df = read_to_bed(fp, region_buffer=500)
        self.assertListEqual(df["START"].tolist(), [100, 500])
        self.assertListEqual(df["END"].tolist(), [101, 2500])
