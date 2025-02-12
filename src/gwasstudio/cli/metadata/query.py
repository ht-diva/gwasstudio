import json
from pathlib import Path
from typing import Any

import cloup
from ruamel.yaml import YAML

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
query metadata records from MongoDB
"""


def finditem(obj, key):
    """Search for a specific key in an object, including nested dictionaries.
    If the given key is found in the object, it returns the corresponding value.
    If the key is not present, the function recursively searches for the key
    in its child dictionaries."""
    if key in obj:
        return obj[key]
    for k, v in obj.items():
        if isinstance(v, dict):
            return finditem(v, key)


def load_search_topics(search_file: str) -> Any | None:
    """Loads search topics from a YAML file."""
    search_topics = None
    if search_file and Path(search_file).exists():
        yaml = YAML(typ="safe")
        with open(search_file, "r") as file:
            search_topics = yaml.load(file)
    return search_topics


@cloup.command("meta_query", no_args_is_help=True, help=help_doc)
@cloup.option("--search-file", required=True, default=None, help="Path to search template")
def meta_query(search_file):
    """
    Queries metadata based on given parameters.

    Args:
        search_file (str): Path to search template

    Returns:
        None
    """

    search_topics = load_search_topics(search_file)
    logger.debug(search_topics)

    obj = EnhancedDataProfile()
    objs = []

    for trait_desc_search_dict in search_topics["trait_desc"]:
        k, v = next(iter(trait_desc_search_dict.items()))
        # Create a new dictionary with all keys and values, including the current trait
        # Merge dictionaries using the | operator
        query_dict = search_topics | {"trait_desc": v}
        query_result = obj.query(**query_dict)

        for qr in query_result:
            if qr not in objs:
                leaf = k.split(".").pop()
                json_dict = json.loads(qr["trait_desc"])
                if v in finditem(json_dict, leaf):
                    objs.append(qr)

    output_fields = search_topics.get("output", ["data_id"])

    for meta_dict in objs:
        to_print = []
        for field in output_fields:
            if field.startswith("trait_desc"):
                keys = field.split(".")
                result = meta_dict
                for key in keys:
                    if isinstance(result, str):
                        result = json.loads(result)
                    result = result.get(key, None)
                to_print.append(result)
            else:
                result = meta_dict.get(field)
                to_print.append(result)
        print("\t".join(to_print))
