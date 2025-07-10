from enum import Enum
from typing import Dict, Any, List, Tuple

import numpy as np
import tiledb


class AttributeName(Enum):
    BETA = "BETA"
    SE = "SE"
    EAF = "EAF"
    EA = "EA"
    NEA = "NEA"
    MLOG10P = "MLOG10P"

    @classmethod
    def get_attribute_names(cls) -> Tuple[str, ...]:
        """
        Return a tuple with the attribute names.

        Returns:
            Tuple[str, ...]: A tuple containing the attribute names.
        """
        return tuple(member.value for member in cls)


class DimensionName(Enum):
    DIM1 = "CHR"
    DIM2 = "TRAITID"
    DIM3 = "POS"

    @classmethod
    def get_dimension_names(cls) -> Tuple[str, ...]:
        """
        Return a tuple with the dimension names.

        Returns:
            Tuple[str, ...]: A tuple containing the dimension names.
        """
        return tuple(member.value for member in cls)


class TileDBSchemaCreator:
    DEFAULT_FILTER = tiledb.FilterList([tiledb.ZstdFilter(level=5)])
    CHROM_DOMAIN = (1, 24)
    POS_DOMAIN = (1, 250000000)

    def __init__(self, uri: str, cfg: Dict[str, Any], ingest_pval: bool):
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

    def _create_dimensions(self) -> tiledb.Domain:
        """
        Create the dimensions for the TileDB schema.

        Returns:
            tiledb.Domain: The domain containing the dimensions.
        """
        return tiledb.Domain(
            tiledb.Dim(
                name=DimensionName.DIM1.value,
                domain=self.CHROM_DOMAIN,
                dtype=np.uint8,
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Dim(
                name=DimensionName.DIM2.value,
                dtype="ascii",
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Dim(
                name=DimensionName.DIM3.value,
                domain=self.POS_DOMAIN,
                dtype=np.uint32,
                filters=self.DEFAULT_FILTER,
            ),
        )

    def _create_attributes(self) -> List[tiledb.Attr]:
        """
        Create the attributes for the TileDB schema.

        Returns:
            List[tiledb.Attr]: The list of attributes.
        """
        attributes = [
            tiledb.Attr(
                name=AttributeName.BETA.value,
                dtype=np.float32,
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Attr(
                name=AttributeName.SE.value,
                dtype=np.float32,
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Attr(
                name=AttributeName.EAF.value,
                dtype=np.float32,
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Attr(
                name=AttributeName.EA.value,
                dtype=str,
                filters=self.DEFAULT_FILTER,
            ),
            tiledb.Attr(
                name=AttributeName.NEA.value,
                dtype=str,
                filters=self.DEFAULT_FILTER,
            ),
        ]

        if self.ingest_pval:
            attributes.append(
                tiledb.Attr(
                    name=AttributeName.MLOG10P.value,
                    dtype=np.float32,
                    filters=self.DEFAULT_FILTER,
                )
            )

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
