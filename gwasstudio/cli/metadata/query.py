import json
from pathlib import Path
from typing import List, Dict, Any

import click
import cloup
from comoda.yaml import load

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
query metadata records from MongoDB
"""


@cloup.command("meta_query", no_args_is_help=True, help=help_doc)
@cloup.option(
    "--key",
    default=None,
    help="query key",
)
@cloup.option(
    "--values",
    default=None,
    help="query values, separate multiple values with ;",
)
@cloup.option(
    "--output",
    type=click.Choice(["all", "data_id"]),
    default="data_id",
    help="The detail that you would like to retrieve from the metadata records",
)
@cloup.option("--search-file", default=None, help="Path to search template")
def meta_query(key, values, output, search_file):
    """
    Queries metadata based on given parameters.

    Args:
        key (str): Query key
        values (str): Query values separated by semicolons
        output (str): Output detail (all or data_id)
        search_file (str): Path to search template

    Returns:
        None
    """

    def flatten_list(nested_list):
        return [item for sublist in nested_list for item in sublist]

    def _load_search_topics(search_file: str) -> Any | None:
        """Loads search topics from a YAML file."""
        if search_file and Path(search_file).exists():
            return load(search_file)
        else:
            return None

    def _create_query_dicts(values: str, key: str) -> List[Dict]:
        """Creates query dictionaries from values and key."""
        query_dicts = []
        for value in values.split(";"):
            query_dict = {key: value}
            query_dicts.append(query_dict)
        return query_dicts

    search_topics = _load_search_topics(search_file)

    # If no search file is provided, create default query dictionary
    if not search_topics:
        query_dicts = _create_query_dicts(values, key)
    else:
        logger.debug(search_topics)
        query_dicts = []
        for trait in search_topics["trait_desc"]:
            # Get all keys and values from the original dictionary
            keys = list(search_topics.keys())
            values = list(search_topics.values())

            # Create a new dictionary with all the keys and values, including the current trait
            query_dict = {key: value for key, value in zip(keys, values)}
            query_dict["trait_desc"] = trait  # replace the existing trait_desc value

            # Append the new dictionary to the list
            query_dicts.append(query_dict)

    obj = EnhancedDataProfile()
    objs = []

    for query_dict in query_dicts:
        objs.append(obj.query(**query_dict))

    flat_list = flatten_list(objs)

    # Print results
    if search_topics:
        output_fields = search_topics.get("output", [])
    else:
        output_fields = [output]
    for meta_dict in flat_list:
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
