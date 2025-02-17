import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict

import cloup
from ruamel.yaml import YAML

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import lower_and_replace

help_doc = """
query metadata records from MongoDB
"""


def find_item(obj: dict, key: str) -> Any:
    """Recursively search for a specific key in an object."""
    if key in obj:
        return obj[key]
    else:
        return next((v for k, v in obj.items() if isinstance(v, dict) and find_item(v, key) is not None), None)


def load_search_topics(search_file: str) -> Any | None:
    """Loads search topics from a YAML file."""
    search_topics = None
    if search_file and Path(search_file).exists():
        yaml = YAML(typ="safe")
        with open(search_file, "r") as file:
            search_topics = yaml.load(file)
    return search_topics


def process_search_topics(search_topics: Dict[str, str]) -> Dict[str, str]:
    """Process search topics by lowercasing and replacing special characters."""

    for key, value in search_topics.items():
        if key in ("project", "study"):
            search_topics[key] = lower_and_replace(value)

    return search_topics


def query_mongo_obj(search_topics, mob):
    """
    Process search topics and query the object to find matching results.

    Args:
        search_topics (dict): Search topics dictionary.
        mob: Mongo Object to be queried.

    Returns:
        list: List of matched objects.
    """
    objs = []
    if "trait" in search_topics:
        for trait_search_dict in search_topics["trait"]:
            _objs = []
            for key, value in trait_search_dict.items():
                query_dict = {**search_topics, **{"trait": value}}
                logger.debug(query_dict)

                # Query the object and add matching results to the local _objs list
                query_results = mob.query(**query_dict)
                _objs.extend(
                    qr for qr in query_results if value in find_item(json.loads(qr["trait"]), key.split(".").pop())
                )

            _objs_set = set()
            _objs_set.add(o for o in _objs)
            # Check if we have a unique result and the numbers of fetched results are greater equal to the dictionary's items
            if len(_objs_set) == 1 and len(_objs) >= len(trait_search_dict):
                objs.extend(obj for obj in _objs if obj not in objs)

    else:
        logger.debug(search_topics)
        query_results = mob.query(**search_topics)

        # Add matching results to objs list
        objs.extend(obj for obj in query_results if obj not in objs)
    return objs


@cloup.command("meta_query", no_args_is_help=True, help=help_doc)
@cloup.option("--search-file", required=True, default=None, help="Path to search template file")
@cloup.option("--output-file", required=True, default=None, help="Path to output file")
@cloup.option("--stdout", default=False, is_flag=True, help="Do not write to stdout")
def meta_query(search_file, output_file, stdout):
    """
    Queries metadata records from MongoDB based on the search topics specified in the provided template file.

    The search topics are processed by lowercasing and replacing special characters before being used to query the database.
    The resulting metadata records are then written to the output file or printed to the console if the `--stdout` option is set.

    Args:
        search_file (str): Path to the search template YAML file
        output_file (str): Path to write the query results to
        stdout (bool): Whether to print the query results to the console instead of writing them to a file

    Returns:
        None
    """

    search_topics = process_search_topics(load_search_topics(search_file))
    logger.debug(search_topics)

    output_fields = ["project", "study", "category", "data_id"] + search_topics.pop("output", [])

    obj = EnhancedDataProfile()
    objs = query_mongo_obj(search_topics, obj)

    with open(output_file, "w") as f:
        msg = f"{len(objs)} results found. Writing to {output_file}"
        print(msg)
        logger.info(msg)

        writer = csv.writer(f, delimiter="\t", lineterminator="\n")
        if stdout:
            console_writer = csv.writer(sys.stdout)
        else:
            console_writer = None

        # write header
        writer.writerow(output_fields)
        if console_writer is not None:
            console_writer.writerow(output_fields)

        for meta_dict in objs:
            row = []
            for field in output_fields:
                if field.startswith(DataProfile.json_dictionary_keys()):
                    main_key = field.split(".")[0]
                    sub_key = field.split(".")[1]
                    json_dict = json.loads(meta_dict.get(main_key))
                    row.append(json_dict.get(sub_key, None))
                else:
                    row.append(meta_dict.get(field, None))
            # write each row
            writer.writerow(row)
            if console_writer is not None:
                console_writer.writerow(row)
