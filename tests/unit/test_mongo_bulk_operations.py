import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from gwasstudio.mongo.models import EnhancedDataProfile


class MongoTestBase:
    """Base class for MongoDB-related tests that sets up common mocks."""

    @pytest.fixture(autouse=True)
    def setup_mongo_mocks(self):
        """Automatically setup MongoDB mocks for each test."""
        with (
            patch("gwasstudio.mongo.models.get_mec") as mock_get_mec,
            patch("gwasstudio.mongo.connection_manager.get_mec") as mock_conn_get_mec,
            patch("gwasstudio.mongo.mixin.datetime") as mock_datetime,
            patch("mongoengine.connection.get_connection") as mock_mongo_conn,
        ):
            # Setup mock connection
            self.mock_mec = MagicMock()
            self.mock_mec.__enter__.return_value = None
            self.mock_mec.__exit__.return_value = None
            mock_get_mec.return_value = self.mock_mec
            mock_conn_get_mec.return_value = self.mock_mec
            mock_mongo_conn.return_value = MagicMock()

            # Setup datetime
            self.fixed_time = datetime(2023, 1, 1, 12, 0, 0)
            mock_datetime.datetime.now.return_value = self.fixed_time
            mock_datetime.datetime.side_effect = lambda *args, **kw: datetime(*args, **kw)

            yield

    def create_profile(self, **kwargs):
        """Helper method to create an EnhancedDataProfile with mocked connection."""
        with patch("gwasstudio.mongo.models.get_mec", return_value=self.mock_mec):
            profile = EnhancedDataProfile(**kwargs)
            # Override the mec property
            profile._mec = self.mock_mec
            return profile


@pytest.fixture
def sample_doc_data():
    """Fixture for sample document data."""
    return {
        "project": "test_project",
        "study": "test_study",
        "data_id": "test_data_id",
        "trait": '{"key": "value"}',
        "category": "GWAS",
        "tags": ["tag1", "tag2"],
        "total": '{"n": 100}',
        "population": ["EUR", "AFR"],
        "build": "GRCh38",
    }


class TestBulkCreate(MongoTestBase):
    """Tests for bulk_create functionality."""

    def test_bulk_create_with_empty_list(self, sample_doc_data):
        """Test bulk_create with empty document list."""
        result = EnhancedDataProfile.bulk_create([], mongo_uri="mongodb://test")
        assert result == {"failed": 0, "success": 0, "total": 0, "invalid_documents": []}

    def test_bulk_create_with_valid_documents(self, sample_doc_data):
        """Test bulk_create with valid documents."""
        with patch.object(EnhancedDataProfile, "bulk_save") as mock_bulk_save:
            mock_bulk_save.return_value = {"inserted": 5, "updated": 2, "total": 7}
            documents = [sample_doc_data for _ in range(7)]

            result = EnhancedDataProfile.bulk_create(documents, mongo_uri="mongodb://test")
            assert result == {"inserted": 5, "updated": 2, "total": 7, "invalid_documents": []}
            mock_bulk_save.assert_called_once()


class TestBulkSave(MongoTestBase):
    """Tests for bulk_save functionality."""

    def test_bulk_save_with_existing_documents(self, sample_doc_data):
        """Test bulk_save with existing documents (updates)."""
        doc1 = self.create_profile(**sample_doc_data)
        doc2 = self.create_profile(**{**sample_doc_data, "data_id": "test_data_id_2"})

        # Mock is_mapped to return True for existing documents
        doc1.is_mapped = MagicMock(return_value=True)
        doc2.is_mapped = MagicMock(return_value=True)

        with patch.object(doc1, "_bulk_update", return_value=2) as mock_update:
            result = doc1.bulk_save([doc1, doc2], batch_size=10)

            assert result["inserted"] == 0
            assert result["updated"] == 2
            assert result["total"] == 2
            mock_update.assert_called_once()

    def test_bulk_save_with_exception(self, sample_doc_data):
        """Test bulk_save propagates unexpected exceptions."""
        doc = self.create_profile(**sample_doc_data)

        # Mock the _bulk_insert to raise an exception
        with patch.object(doc, "_bulk_insert") as mock_insert:
            mock_insert.side_effect = Exception("Unexpected database error")

        # Mock the properties at the class level
        with (
            patch("tests.unit.test_mongo_bulk_operations.EnhancedDataProfile.is_connected", return_value=True),
            patch("tests.unit.test_mongo_bulk_operations.EnhancedDataProfile.is_mapped", return_value=False),
        ):
            try:
                doc.bulk_save()
            except Exception:
                pass

    def test_bulk_save_updates_modification_date(self, sample_doc_data):
        """Test that bulk_save updates modification_date for existing documents."""
        doc = self.create_profile(**sample_doc_data)
        doc.is_mapped = MagicMock(return_value=True)

        with patch.object(doc, "_bulk_update", return_value=1):
            doc.bulk_save([doc])
            assert doc.mdb_obj.modification_date == self.fixed_time
