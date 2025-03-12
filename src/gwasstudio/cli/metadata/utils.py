import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict

import pandas as pd
from ruamel.yaml import YAML

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import lower_and_replace


def load_search_topics(search_file: str) -> Any | None:
    """Loads search topics from a YAML file."""
    search_topics = None
    if search_file and Path(search_file).exists():
        yaml = YAML(typ="safe")
        with open(search_file, "r") as file:
            search_topics = yaml.load(file)
    return search_topics


def process_search_topics(search_topics: Dict[str, str]) -> tuple[dict[str, str], list[str]]:
    """Process search topics by lowercasing and replacing special characters."""

    for key, value in search_topics.items():
        if key in ("project", "study"):
            search_topics[key] = lower_and_replace(value)

    output_fields = ["project", "study", "category", "data_id"] + search_topics.pop("output", [])

    return search_topics, output_fields


def query_mongo_obj(search_topics: Dict[str, Any], mob: EnhancedDataProfile, case_sensitive: bool = False) -> list:
    """
    Process search topics and query the object to find matching results.

    Args:
        search_topics (dict): Search topics dictionary.
        case_sensitive (bool): Enable case-sensitive search
        mob: Mongo Object to be queried.

    Returns:
        list: List of matched objects.
    """
    objs = []
    if "trait" in search_topics:
        for trait_search_dict in search_topics["trait"]:
            query_dict = {**search_topics, **{"trait": trait_search_dict}}
            logger.debug(query_dict)
            query_results = mob.query(case_sensitive, **query_dict)
            # Add matching results to objs list
            objs.extend(obj for obj in query_results if obj not in objs)

    else:
        logger.debug(search_topics)
        query_results = mob.query(**search_topics)

        # Add matching results to objs list
        objs.extend(obj for obj in query_results if obj not in objs)
    return objs


def dataframe_from_mongo_objs(fields: list, objs: list) -> pd.DataFrame:
    """
    Create a DataFrame from MongoDB objects.

    If `objs` is empty, returns an empty DataFrame with `fields` as columns.
    Otherwise, attempts to create a DataFrame by querying each object in `objs`
    for the specified `fields`.

    Parameters:
        fields (list): The field names to query from each object.
        objs (list): A list of MongoDB objects.

    Returns:
        pd.DataFrame: The resulting DataFrame.
    """
    if not objs:
        return pd.DataFrame(columns=fields)

    results = defaultdict(list)
    for field in fields:
        for obj in objs:
            if field.startswith(DataProfile.json_dict_fields()):
                main_key = field.split(".")[0]
                sub_key = field.split(".")[1]
                json_dict = json.loads(obj.get(main_key))
                results[field].append(json_dict.get(sub_key, None))
            else:
                results[field].append(obj.get(field, None))

    df = pd.DataFrame.from_dict(results)
    # specify the data type for each column
    data_types = {
        "project": "string[pyarrow]",
        "study": "string[pyarrow]",
        "category": "category",
        "data_id": "string[pyarrow]",
    }

    df = df.astype({col: dtype for col, dtype in data_types.items() if col in df}, errors="ignore")
    return df


def df_to_csv(df: pd.DataFrame, output_file: Path, index: bool = False, sep: str = "\t") -> None:
    """
    Save a Pandas DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame to be saved.
        output_file (str): The path to the output CSV file.
        index (bool): Write row names (index).
        sep (str): The separator to use between columns. Defaults to tab (\t).

    Returns:
        None

    Notes:
        This function will overwrite any existing file at the specified path.
    """
    df.to_csv(Path(output_file), index=index, sep=sep)
