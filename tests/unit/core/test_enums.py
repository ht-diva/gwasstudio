"""
Tests for GWASStudio Core Enums Module
======================================

Tests for the enums and data classes in gwasstudio.core.enums.
"""

import pytest

from gwasstudio.core.enums import (
    DataType,
    MetadataEnum,
    OntologyID,
    OntologyNamespace,
)


class TestOntologyNamespace:
    """Tests for OntologyNamespace enum."""

    def test_enum_values(self):
        """Test that OntologyNamespace has expected values."""
        assert OntologyNamespace.EFO.value == "EFO"
        assert OntologyNamespace.UBERON.value == "UBERON"
        assert OntologyNamespace.ICD10.value == "ICD10"
        assert OntologyNamespace.GO.value == "GO"
        assert OntologyNamespace.HP.value == "HP"
        assert OntologyNamespace.MP.value == "MP"

    def test_enum_members(self):
        """Test that OntologyNamespace has expected members."""
        expected_members = {"EFO", "UBERON", "ICD10", "GO", "HP", "MP"}
        actual_members = {member.name for member in OntologyNamespace}
        assert actual_members == expected_members


class TestOntologyID:
    """Tests for OntologyID dataclass."""

    def test_from_string_valid(self):
        """Test parsing valid ontology ID strings."""
        oid = OntologyID.from_string("EFO:0000123")
        assert oid.namespace == "EFO"
        assert oid.id == "0000123"
        assert oid.full == "EFO:0000123"

    def test_from_string_with_uberon(self):
        """Test parsing UBERON ontology ID."""
        oid = OntologyID.from_string("UBERON:0003923")
        assert oid.namespace == "UBERON"
        assert oid.id == "0003923"
        assert oid.full == "UBERON:0003923"

    def test_from_string_with_multiple_colons(self):
        """Test parsing ontology ID with multiple colons."""
        oid = OntologyID.from_string("GO:0008150:some_suffix")
        assert oid.namespace == "GO"
        assert oid.id == "0008150:some_suffix"
        assert oid.full == "GO:0008150:some_suffix"

    def test_from_string_invalid_no_colon(self):
        """Test that parsing without colon raises ValueError."""
        with pytest.raises(ValueError, match="Invalid ontology ID format"):
            OntologyID.from_string("EFO0000123")

    def test_from_string_empty_string(self):
        """Test that parsing empty string raises ValueError."""
        with pytest.raises(ValueError, match="Invalid ontology ID format"):
            OntologyID.from_string("")

    def test_from_string_invalid_namespace(self):
        """Test that parsing with invalid namespace raises ValueError."""
        with pytest.raises(ValueError, match="Invalid ontology namespace"):
            OntologyID.from_string("INVALID:0000123")

    def test_from_string_valid_namespaces(self):
        """Test that all valid namespaces are accepted."""
        for namespace in [ns.value for ns in OntologyNamespace]:
            oid = OntologyID.from_string(f"{namespace}:0000123")
            assert oid.namespace == namespace
            assert oid.id == "0000123"
            assert oid.full == f"{namespace}:0000123"

    def test_to_dict(self):
        """Test conversion to dictionary."""
        oid = OntologyID(namespace="EFO", id="0000123", full="EFO:0000123")
        result = oid.to_dict()
        assert result == {
            "namespace": "EFO",
            "id": "0000123",
            "full": "EFO:0000123",
        }

    def test_frozen_dataclass(self):
        """Test that OntologyID is immutable."""
        oid = OntologyID(namespace="EFO", id="0000123", full="EFO:0000123")
        with pytest.raises(AttributeError):
            oid.namespace = "UBERON"

    def test_equality(self):
        """Test equality comparison."""
        oid1 = OntologyID(namespace="EFO", id="0000123", full="EFO:0000123")
        oid2 = OntologyID(namespace="EFO", id="0000123", full="EFO:0000123")
        oid3 = OntologyID(namespace="UBERON", id="0000123", full="UBERON:0000123")
        assert oid1 == oid2
        assert oid1 != oid3


class TestDataType:
    """Tests for DataType enum."""


class TestMetadataEnum:
    """Tests for MetadataEnum."""

    def test_trait_ontology_ids_exists(self):
        """Test that TRAIT_ONTOLOGY_IDS exists in MetadataEnum."""
        assert hasattr(MetadataEnum, "ONTOLOGY_IDS")

    def test_trait_ontology_ids_value(self):
        """Test TRAIT_ONTOLOGY_IDS has correct field name and dtype."""
        field = MetadataEnum.ONTOLOGY_IDS
        assert field.get_value() == "trait_ontology_ids"
        assert field.get_dtype() == DataType.STRING_PA.value

    def test_trait_ontology_ids_in_get_names(self):
        """Test that trait_ontology_ids appears in get_names()."""
        names = MetadataEnum.get_names()
        assert "trait_ontology_ids" in names

    def test_trait_ontology_ids_in_get_all_dtypes_dict(self):
        """Test that trait_ontology_ids appears in get_all_dtypes_dict()."""
        dtypes = MetadataEnum.get_all_dtypes_dict()
        assert "trait_ontology_ids" in dtypes
        assert dtypes["trait_ontology_ids"] == DataType.STRING_PA.value
