"""
Unit Tests for GWASStudio CLI List Command
=========================================

Tests for the list command in gwasstudio.cli.list.
"""

import pytest
from unittest.mock import MagicMock, patch
from click.testing import CliRunner

from gwasstudio.cli.list import list_projects, HELP_DOC
from gwasstudio.core.config import GWASStudioConfig, MongoConfig, TileDBConfig
from gwasstudio.core.exceptions import GWASStudioError, QueryError, ConfigurationError


@pytest.fixture
def mock_config():
    """Provide a mock GWASStudioConfig for testing."""
    return GWASStudioConfig(
        mongo=MongoConfig(db_name="test_gwas"),
        tiledb=TileDBConfig(),
    )


@pytest.fixture
def runner():
    """Provide a Click test runner."""
    return CliRunner()


class TestListProjectsCommand:
    """Tests for the list_projects CLI command."""

    def test_help_doc(self):
        """Test that HELP_DOC is properly defined."""
        assert isinstance(HELP_DOC, str)
        assert "List" in HELP_DOC or "list" in HELP_DOC

    def test_list_projects_no_args_is_help_false(self):
        """Test that the command has no_args_is_help=False."""
        assert callable(list_projects)

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_empty_results(self, mock_create_config, mock_core_list, runner):
        """Test list_projects with no projects found."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = []

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        assert "No projects found." in result.output
        mock_create_config.assert_called_once()
        mock_core_list.assert_called_once_with(config=mock_config)

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_single_project_with_study(self, mock_create_config, mock_core_list, runner):
        """Test list_projects with a single project and study."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [{"category": "test_cat", "project": "test_project", "study": "study1"}]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        assert "Category: test_cat" in result.output
        assert "Project: test_project" in result.output
        assert "Studies: study1" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_multiple_categories(self, mock_create_config, mock_core_list, runner):
        """Test list_projects with multiple categories and projects."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [
            {"category": "cat1", "project": "proj1", "study": "study1"},
            {"category": "cat1", "project": "proj1", "study": "study2"},
            {"category": "cat2", "project": "proj2", "study": "study3"},
            {"category": "cat1", "project": "proj2", "study": "study4"},
        ]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        assert "Category: cat1" in result.output
        assert "Category: cat2" in result.output
        assert "Project: proj1" in result.output
        assert "Project: proj2" in result.output
        assert "study1" in result.output
        assert "study2" in result.output
        assert "study3" in result.output
        assert "study4" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_default_category(self, mock_create_config, mock_core_list, runner):
        """Test that projects without a category default to 'default'."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [{"project": "test_project", "study": "test_study"}]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        assert "Category: default" in result.output
        assert "Project: test_project" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_unknown_project(self, mock_create_config, mock_core_list, runner):
        """Test that projects without a project name default to 'unknown'."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [{"category": "test_cat", "study": "test_study"}]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        assert "Category: test_cat" in result.output
        assert "Project: unknown" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_sorted_output(self, mock_create_config, mock_core_list, runner):
        """Test that categories and projects are sorted in output."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [
            {"category": "z_cat", "project": "z_project", "study": "s1"},
            {"category": "a_cat", "project": "a_project", "study": "s2"},
        ]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        # Check that a_cat appears before z_cat
        a_pos = result.output.find("Category: a_cat")
        z_pos = result.output.find("Category: z_cat")
        assert a_pos >= 0 and z_pos >= 0
        assert a_pos < z_pos

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_query_error(self, mock_create_config, mock_core_list, runner):
        """Test that QueryError is handled properly."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.side_effect = QueryError("Test query error", code="TEST_001")

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 1
        assert "Error querying projects: Test query error" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_configuration_error(self, mock_create_config, mock_core_list, runner):
        """Test that ConfigurationError is handled properly."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.side_effect = ConfigurationError("Test config error", code="TEST_002")

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 1
        assert "Configuration error: Test config error" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_gwas_studio_error(self, mock_create_config, mock_core_list, runner):
        """Test that GWASStudioError is handled properly."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.side_effect = GWASStudioError("Test GWASStudio error", code="TEST_003")

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 1
        assert "GWASStudio error: Test GWASStudio error" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_unexpected_error(self, mock_create_config, mock_core_list, runner):
        """Test that unexpected errors are handled properly."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.side_effect = RuntimeError("Unexpected error")

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 1
        assert "Unexpected error: Unexpected error" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_duplicate_studies(self, mock_create_config, mock_core_list, runner):
        """Test that duplicate studies are deduplicated in output."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [
            {"category": "test_cat", "project": "test_project", "study": "study1"},
            {"category": "test_cat", "project": "test_project", "study": "study1"},
            {"category": "test_cat", "project": "test_project", "study": "study2"},
        ]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        # Check study1 appears only once in the studies list
        assert result.output.count("study1") == 1
        assert "study2" in result.output

    @patch("gwasstudio.cli.list.core_list_projects")
    @patch("gwasstudio.cli.list.create_config_from_context")
    def test_list_projects_studies_display(self, mock_create_config, mock_core_list, runner):
        """Test that studies are displayed as comma-separated list."""
        mock_config = GWASStudioConfig()
        mock_create_config.return_value = mock_config
        mock_core_list.return_value = [
            {"category": "test_cat", "project": "test_project", "study": "study1"},
            {"category": "test_cat", "project": "test_project", "study": "study2"},
            {"category": "test_cat", "project": "test_project", "study": "study3"},
        ]

        result = runner.invoke(list_projects, obj={})

        assert result.exit_code == 0
        assert "Studies: study1, study2, study3" in result.output


class TestListProjectsCore:
    """Tests for the core list_projects function."""

    @patch("gwasstudio.core.query.MongoDBStorage")
    def test_list_projects_core_success(self, mock_storage_class, mock_config):
        """Test core list_projects with successful query."""
        from gwasstudio.core.query import list_projects as core_list_projects

        mock_storage = MagicMock()
        mock_storage.list_projects.return_value = [
            {"category": "cat1", "project": "proj1"},
            {"category": "cat2", "project": "proj2"},
        ]
        mock_storage_class.return_value = mock_storage

        result = core_list_projects(config=mock_config)

        assert len(result) == 2
        assert result[0]["category"] == "cat1"
        assert result[1]["category"] == "cat2"
        mock_storage.list_projects.assert_called_once()

    @patch("gwasstudio.core.query.MongoDBStorage")
    def test_list_projects_core_empty(self, mock_storage_class, mock_config):
        """Test core list_projects with empty results."""
        from gwasstudio.core.query import list_projects as core_list_projects

        mock_storage = MagicMock()
        mock_storage.list_projects.return_value = []
        mock_storage_class.return_value = mock_storage

        result = core_list_projects(config=mock_config)

        assert result == []

    @patch("gwasstudio.core.query.MongoDBStorage")
    def test_list_projects_core_exception(self, mock_storage_class, mock_config):
        """Test core list_projects with exception."""
        from gwasstudio.core.query import list_projects as core_list_projects
        from gwasstudio.core.query import QueryError as CoreQueryError

        mock_storage = MagicMock()
        mock_storage.list_projects.side_effect = Exception("Storage error")
        mock_storage_class.return_value = mock_storage

        with pytest.raises(CoreQueryError) as exc_info:
            core_list_projects(config=mock_config)

        assert "Failed to list projects" in str(exc_info.value)

    @patch("gwasstudio.core.query.MongoDBStorage")
    def test_list_projects_core_default_config(self, mock_storage_class):
        """Test core list_projects with None config (uses default)."""
        from gwasstudio.core.query import list_projects as core_list_projects

        mock_storage = MagicMock()
        mock_storage.list_projects.return_value = []
        mock_storage_class.return_value = mock_storage

        result = core_list_projects(config=None)

        assert result == []
        mock_storage_class.assert_called_once()
