import shutil
import tempfile
import unittest

import tiledb

from gwasstudio.utils.tdb_schema import TileDBSchemaCreator, DimensionEnum


class TestTileDBSchemaCreator(unittest.TestCase):
    def setUp(self):
        self.uri = tempfile.mkdtemp()
        self.cfg = {"vfs.s3.region": "us-west-2"}
        self.ingest_pval = True
        self.creator = TileDBSchemaCreator(self.uri, self.cfg, self.ingest_pval)

    def tearDown(self):
        shutil.rmtree(self.uri)

    def test_initialization(self):
        self.assertEqual(self.creator.uri, self.uri)
        self.assertEqual(self.creator.cfg, self.cfg)
        self.assertEqual(self.creator.ingest_pval, self.ingest_pval)

    def test_create_schema(self):
        self.creator.create_schema()

        with tiledb.open(self.uri, ctx=tiledb.Ctx(self.cfg)) as array:
            schema = array.schema

            # Check dimensions
            self.assertEqual(schema.domain.ndim, 3)
            self.assertEqual(schema.domain.dim(0).name, DimensionEnum.DIM1.value)
            self.assertEqual(schema.domain.dim(1).name, DimensionEnum.DIM2.value)
            self.assertEqual(schema.domain.dim(2).name, DimensionEnum.DIM3.value)

            # Check attributes
            self.assertEqual(len(schema.attr_names), 6)
            self.assertEqual(schema.attr(0).name, "BETA")
            self.assertEqual(schema.attr(1).name, "SE")
            self.assertEqual(schema.attr(2).name, "EAF")
            self.assertEqual(schema.attr(3).name, "EA")
            self.assertEqual(schema.attr(4).name, "NEA")
            self.assertEqual(schema.attr(5).name, "MLOG10P")

    def test_get_dimension_names(self):
        expected_names = (DimensionEnum.DIM1.value, DimensionEnum.DIM2.value, DimensionEnum.DIM3.value)
        self.assertEqual(DimensionEnum.get_names(), expected_names)
