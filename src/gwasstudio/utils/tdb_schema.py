from typing import Dict, Any, List

import tiledb

from gwasstudio.utils.datatypes import DataType
from gwasstudio.utils.enums import BaseEnum


class AttributeEnum(BaseEnum):
    BETA = ("BETA", DataType.FLOAT32_NP)
    SE = ("SE", DataType.FLOAT32_NP)
    EAF = ("EAF", DataType.FLOAT32_NP)
    EA = ("EA", DataType.STRING)
    NEA = ("NEA", DataType.STRING)
    MLOG10P = ("MLOG10P", DataType.FLOAT32_NP)


class DimensionEnum(BaseEnum):
    DIM1 = ("CHR", DataType.UINT8_NP)
    DIM2 = ("TRAITID", DataType.ASCII)
    DIM3 = ("POS", DataType.UINT32_NP)


class TileDBSchemaCreator:
    DEFAULT_FILTER = tiledb.FilterList([tiledb.ZstdFilter(level=5)])
    CHROM_DOMAIN = (1, 24)
    POS_DOMAIN = (1, 250000000)

    def __init__(
        self,
        uri: str,
        cfg: Dict[str, Any],
        ingest_pval: bool,
        attribute_enum: BaseEnum = AttributeEnum,
        dimension_enum: BaseEnum = DimensionEnum,
    ):
        """
        Initialize the TileDBSchemaCreator with the given parameters.

        Args:
            uri (str): The path where the TileDB array will be stored.
            cfg (Dict[str, Any]): A configuration dictionary for connecting to S3.
            ingest_pval (bool): Flag to indicate whether to include the MLOG10P attribute.
        """
        self.uri = uri
        self.cfg = cfg
        self.ingest_pval = ingest_pval
        self.attribute_enum = attribute_enum
        self.dimension_enum = dimension_enum

    def _create_dimensions(self) -> tiledb.Domain:
        """
        Create the dimensions for the TileDB schema.

        Returns:
            tiledb.Domain: The domain containing the dimensions.
        """
        return tiledb.Domain(
            tiledb.Dim(
                name=self.dimension_enum.DIM1.get_value(),
                domain=self.CHROM_DOMAIN,
                dtype=self.dimension_enum.DIM1.get_dtype(),
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Dim(
                name=self.dimension_enum.DIM2.get_value(),
                dtype=self.dimension_enum.DIM2.get_dtype(),
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Dim(
                name=self.dimension_enum.DIM3.get_value(),
                domain=self.POS_DOMAIN,
                dtype=self.dimension_enum.DIM3.get_dtype(),
                filters=self.DEFAULT_FILTER,
            ),
        )

    def _create_attributes(self) -> List[tiledb.Attr]:
        """
        Create the attributes for the TileDB schema.

        Returns:
            List[tiledb.Attr]: The list of attributes.
        """
        attributes_list = [
            self.attribute_enum.BETA,
            self.attribute_enum.SE,
            self.attribute_enum.EAF,
            self.attribute_enum.EA,
            self.attribute_enum.NEA,
        ]
        if self.ingest_pval:
            attributes_list.append(self.attribute_enum.MLOG10P)

        attributes = [
            tiledb.Attr(
                name=attr.get_value(),
                dtype=attr.get_dtype(),
                filters=self.DEFAULT_FILTER,
            )
            for attr in attributes_list
        ]

        return attributes

    def create_schema(self) -> None:
        """
        Create an empty schema for TileDB.
        """
        domain = self._create_dimensions()
        attributes = self._create_attributes()

        schema = tiledb.ArraySchema(
            domain=domain,
            sparse=True,
            allows_duplicates=True,
            attrs=attributes,
        )

        try:
            ctx = tiledb.Ctx(self.cfg)
            tiledb.Array.create(self.uri, schema, ctx=ctx)
        except Exception as e:
            raise RuntimeError(f"Failed to create TileDB schema: {e}")
