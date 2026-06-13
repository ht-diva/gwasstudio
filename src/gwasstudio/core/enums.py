from enum import Enum
from typing import Any, Tuple

import numpy as np


class DataType(Enum):
    ASCII = "ascii"
    STRING = "str"
    FLOAT32_NP = np.float32
    UINT8_NP = np.uint8
    UINT32_NP = np.uint32
    STRING_PA = "string[pyarrow]"
    INT64_PA = "Int64[pyarrow]"
    UINT64_PA = "UInt64[pyarrow]"
    UINT16_PA = "UInt16[pyarrow]"
    CATEGORY = "category"
    FLOAT_PA = "Float64[pyarrow]"


# Mapping from full descriptions to ancestry codes for normalization
ANCESTRY_DESCRIPTION_TO_CODE = {
    "Aboriginal Australian": "AUS",
    "African American or Afro-Caribbean": "AFA",
    "African unspecified": "AFR",
    "Asian unspecified": "ASN",
    "Central Asian": "CAS",
    "East Asian": "EAS",
    "European": "EUR",
    "Greater Middle Eastern": "MDE",
    "Middle Eastern, North African, or Persian": "MDE",
    "Hispanic or Latin American": "AMR",
    "Not reported / unknown": "NR",
    "Native American": "NAM",
    "Oceanian": "OCE",
    "Other": "OTH",
    "Other admixed ancestry": "ADM",
    "South Asian": "SAS",
    "South East Asian": "SEA",
    "Sub-Saharan African": "SAF",
}

# Reverse mapping for validation
ANCESTRY_CODE_TO_DESCRIPTION = {v: k for k, v in ANCESTRY_DESCRIPTION_TO_CODE.items()}


class AncestryEnum(str, Enum):
    """
    Ancestry group codes based on NCBI PMC5815218.

    Reference: https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5815218/table/Tab1/
    """

    AUS = "AUS"  # Aboriginal Australian
    AFA = "AFA"  # African American or Afro-Caribbean
    AFR = "AFR"  # African unspecified
    ASN = "ASN"  # Asian unspecified
    CAS = "CAS"  # Central Asian
    EAS = "EAS"  # East Asian
    EUR = "EUR"  # European
    MDE = "MDE"  # Greater Middle Eastern (Middle Eastern, North African, or Persian)
    AMR = "AMR"  # Hispanic or Latin American
    NR = "NR"  # Not reported / unknown
    NAM = "NAM"  # Native American
    OCE = "OCE"  # Oceanian
    OTH = "OTH"  # Other
    ADM = "ADM"  # Other admixed ancestry
    SAS = "SAS"  # South Asian
    SEA = "SEA"  # South East Asian
    SAF = "SAF"  # Sub-Saharan African

    @classmethod
    def get_values(cls) -> list[str]:
        """Return list of valid ancestry codes."""
        return [member.value for member in cls]

    @classmethod
    def normalize(cls, value: str) -> str:
        """
        Normalize a population value to its standard code.
        Accepts either the code (e.g., 'EUR') or full description (e.g., 'European').

        Args:
            value: Population value to normalize

        Returns:
            Standard ancestry code

        Raises:
            ValueError: If value is not a valid code or description
        """
        if not value:
            return value

        # Check if it's already a valid code
        if value in ANCESTRY_CODE_TO_DESCRIPTION:
            return value

        # Check if it's a valid description
        if value in ANCESTRY_DESCRIPTION_TO_CODE:
            return ANCESTRY_DESCRIPTION_TO_CODE[value]

        # Check with case-insensitive matching
        value_upper = value.upper()
        for code in cls.get_values():
            if code == value_upper:
                return code

        for desc, code in ANCESTRY_DESCRIPTION_TO_CODE.items():
            if desc.upper() == value_upper:
                return code

        raise ValueError(
            f"Invalid population value '{value}'. "
            f"Valid codes: {', '.join(cls.get_values())}. "
            f"Valid descriptions: {', '.join(sorted(ANCESTRY_DESCRIPTION_TO_CODE.keys())[:5])}..."
        )


class DataCategoryEnum(str, Enum):
    """
    Data category codes for GWASStudio.
    """

    GWAS = "GWAS"
    PQTL = "pQTL"
    EQTL = "eQTL"

    @classmethod
    def get_values(cls) -> list[str]:
        """Return list of valid data category codes."""
        return [member.value for member in cls]

    @classmethod
    def validate(cls, value: str) -> str:
        """
        Validate a data category value (case-sensitive).

        Args:
            value: Data category value to validate

        Returns:
            The validated value (same as input if valid)

        Raises:
            ValueError: If value is not a valid data category
        """
        if not value:
            return value

        # Check if it's a valid code (case-sensitive)
        if value in cls.get_values():
            return value

        raise ValueError(f"Invalid data_category value '{value}'. Valid values are: {', '.join(cls.get_values())}")


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
    POPULATION = ("population", DataType.STRING_PA)

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
    def required_output_fields(cls):
        """It returns a list of required output fields for the meta-query"""
        return [
            cls.PROJECT.get_value(),
            cls.STUDY.get_value(),
            cls.CATEGORY.get_value(),
            cls.DATA_ID.get_value(),
            cls.SOURCE_ID.get_value(),
        ]

    @classmethod
    def get_source_id_field(cls):
        """It returns the metadata field name that store source ids"""
        return cls.SOURCE_ID.get_value()

    @classmethod
    def get_tiledb_grouping_fields(cls):
        """It returns a list of fields to be used for grouping in TileDB"""
        return [cls.PROJECT.get_value(), cls.STUDY.get_value()]
