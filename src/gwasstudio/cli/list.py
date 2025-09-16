from collections import defaultdict
from typing import Iterable

import click
import cloup

from gwasstudio.config_manager import ConfigurationManager
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils.cfg import get_mongo_uri
from gwasstudio.utils.metadata import query_mongo_obj
from gwasstudio.utils.mongo_manager import manage_mongo

HELP_DOC = """List every category → project → study hierarchy stored in the MongoDB."""


def _collect_objects(cm: ConfigurationManager, profile: EnhancedDataProfile) -> list[dict]:
    """
    Query MongoDB for all objects belonging to the data‑categories defined in
    ``cm`` and return them as a flat list of dictionaries.
    """
    categories = cm.get_data_category_list
    return [obj for cat in categories for obj in query_mongo_obj({"category": cat}, profile)]


def _build_category_map(objs: Iterable[dict]) -> dict[str, dict[str, set[str]]]:
    """
    Transform a flat list of MongoDB objects into a nested mapping::

        {
            "category": {
                "project": {"study1", "study2", ...},
                ...
            },
            ...
        }

    ``defaultdict`` removes the need for repeated ``if key not in …`` checks.
    """
    cat_map: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    for obj in objs:
        cat_map[obj["category"]][obj["project"]].add(obj["study"])
    return cat_map


@cloup.command("list", no_args_is_help=False, help=HELP_DOC)
@click.pass_context
def list_projects(ctx: click.Context) -> None:
    """
    List every *category → project → study* hierarchy stored in the MongoDB
    configured for the current Click context.
    """
    cm = ConfigurationManager()

    with manage_mongo(ctx):
        mongo_uri = get_mongo_uri(ctx)
        profile = EnhancedDataProfile(uri=mongo_uri)

        raw_objects = _collect_objects(cm, profile)

    cat_map = _build_category_map(raw_objects)

    for category, projects in cat_map.items():
        click.echo(f"Category: {category}")
        for project, studies in projects.items():
            studies_str = ", ".join(sorted(studies))
            click.echo(f"  Project: {project}\n\tStudies: {studies_str}")
