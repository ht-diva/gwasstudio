from enum import Enum
from typing import Any, Tuple

from gwasstudio.utils.datatypes import DataType


class BaseEnum(Enum):
    def __init__(self, value, dtype):
        self._value_ = value
        self.dtype = dtype

    def get_value(self) -> str:
        """
        Return the value of the enum member.

        Returns:
            str: The value of the enum member.
        """
        return self._value_

    def get_dtype(self) -> Any:
        """
        Return the data type of the enum member.

        Returns:
            Any: The data type of the enum member.
        """
        return self.dtype.value

    @classmethod
    def get_names(cls) -> Tuple[str, ...]:
        """
        Return a tuple with the dimension names.

        Returns:
            Tuple[str, ...]: A tuple containing the dimension names.
        """
        return tuple(member.get_value() for member in cls)

    @classmethod
    def get_all_dtypes_dict(cls) -> dict:
        return {member.get_value(): member.get_dtype() for member in cls}


class MetadataEnum(BaseEnum):
    PROJECT = ("project", DataType.CATEGORY)
    STUDY = ("study", DataType.CATEGORY)
    FILE_PATH = ("file_path", DataType.STRING_PA)
    CATEGORY = ("category", DataType.CATEGORY)
    DATA_ID = ("data_id", DataType.STRING_PA)
    BUILD = ("build", DataType.CATEGORY)
    CONSORTIUM = ("notes_consortium", DataType.STRING_PA)
    SEX = ("notes_sex", DataType.CATEGORY)
    SOURCE_ID = ("notes_source_id", DataType.STRING_PA)
    SAMPLES = ("total_samples", DataType.UINT64_PA)
    CASES = ("total_cases", DataType.UINT64_PA)
    CONTROLS = ("total_controls", DataType.UINT64_PA)
    HEALTHCARE_TAXONOMY = ("trait_code", DataType.STRING_PA)
    DESCRIPTION = ("trait_desc", DataType.STRING_PA)
    GENE_IDS = ("trait_gene_ids", DataType.STRING_PA)
    PROTEIN_IDS = ("trait_protein_ids", DataType.STRING_PA)
    SOMALOGIC_ID = ("trait_seqid", DataType.STRING_PA)
    TISSUE = ("trait_tissue", DataType.CATEGORY)
    UNIT = ("trait_unit", DataType.STRING_PA)

    @classmethod
    def required_fields(cls):
        """It returns a list of required fields for ingestion"""
        return [
            cls.PROJECT.get_value(),
            cls.STUDY.get_value(),
            cls.FILE_PATH.get_value(),
            cls.CATEGORY.get_value(),
        ]

    @classmethod
    def get_source_id_field(cls):
        """It returns the metadata field name that store source ids"""
        return cls.SOURCE_ID.get_value()

    @classmethod
    def get_tiledb_grouping_fields(cls):
        """It returns a list of fields to be used for grouping in TileDB"""
        return [cls.PROJECT.get_value(), cls.STUDY.get_value()]
