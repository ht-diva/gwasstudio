"""
GWASStudio CLI List Command (Core Version)
=================================================================
"""

from collections import defaultdict

import click
import cloup

from gwasstudio.core.query import list_projects as core_list_projects
from gwasstudio.core.exceptions import GWASStudioError, QueryError, ConfigurationError
from gwasstudio.cli.utils import create_config_from_context

HELP_DOC = """List every category → project → study hierarchy stored in the MongoDB."""


@cloup.command("list", no_args_is_help=False, help=HELP_DOC)
@click.pass_context
def list_projects(ctx: click.Context) -> None:
    """
    List every *category → project → study* hierarchy stored in MongoDB.
    """
    try:
        # Create configuration
        config = create_config_from_context(ctx)

        # Get projects from core (which queries MongoDB)
        projects = core_list_projects(config=config)

        if not projects:
            click.echo("No projects found.")
            return

        # Build hierarchy: category → project → {set of studies}
        cat_map: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(lambda: defaultdict(set))

        for doc in projects:
            category = doc.get("category", "default")
            project_name = doc.get("project", "unknown")
            study_name = doc.get("study")

            if study_name:
                cat_map[category][project_name].add(study_name)

        # Display hierarchy
        for category in sorted(cat_map.keys()):
            click.echo(f"Category: {category}")
            for project, studies in sorted(cat_map[category].items()):
                studies_str = ", ".join(sorted(studies))
                click.echo(f"  Project: {project}\n\tStudies: {studies_str}")

    except QueryError as e:
        click.echo(f"Error querying projects: {e.message}", err=True)
        raise SystemExit(1)
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e.message}", err=True)
        raise SystemExit(1)
    except GWASStudioError as e:
        click.echo(f"GWASStudio error: {e.message}", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {str(e)}", err=True)
        raise SystemExit(1)
