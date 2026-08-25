from pathlib import Path
from typing import Any

import tiledb

from gwasstudio.core.enums import BaseEnum, DataType


class AttributeEnum(BaseEnum):
    BETA = ("BETA", DataType.FLOAT32_NP)
    SE = ("SE", DataType.FLOAT32_NP)
    EAF = ("EAF", DataType.FLOAT32_NP)
    EA = ("EA", DataType.STRING)
    NEA = ("NEA", DataType.STRING)
    MLOG10P = ("MLOG10P", DataType.FLOAT32_NP)
    N = ("N", DataType.UINT32_NP)


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
        cfg: dict[str, Any],
        additional_attributes: list[str],
        attribute_enum: BaseEnum = AttributeEnum,
        dimension_enum: BaseEnum = DimensionEnum,
    ):
        """
        Initialize the TileDBSchemaCreator with the given parameters.

        Args:
            uri (str): The path where the TileDB array will be stored.
            cfg (Dict[str, Any]): A configuration dictionary for connecting to S3.
            additional_attributes (list): string list of attributes to add
        """
        self.uri = uri
        self.cfg = cfg
        self.additional_attributes = additional_attributes
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

    def _create_attributes(self) -> list[tiledb.Attr]:
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
        if self.additional_attributes:
            if "MLOG10P" in self.additional_attributes:
                attributes_list.append(self.attribute_enum.MLOG10P)
            if "N" in self.additional_attributes:
                attributes_list.append(self.attribute_enum.N)

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
            # Ensure parent directory exists for local filesystem URIs
            from gwasstudio.cli.utils import parse_uri

            scheme, _, path = parse_uri(self.uri)
            if not scheme or scheme == "file":
                array_path = Path(path)
                array_path.parent.mkdir(parents=True, exist_ok=True)

            tiledb.Array.create(self.uri, schema, ctx=ctx)
        except Exception as e:
            raise RuntimeError(f"Failed to create TileDB schema: {e}")
