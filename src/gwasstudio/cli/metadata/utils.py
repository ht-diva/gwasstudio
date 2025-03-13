import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Hashable

import pandas as pd
from ruamel.yaml import YAML

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import lower_and_replace, compute_sha256

metadata_dtypes = {"project": "category", "study": "category", "file_path": "string[pyarrow]", "category": "category"}


def load_search_topics(search_file: str) -> Any | None:
    """
    Loads search topics from a YAML-formatted file, processes them, and returns the updated topics along with output fields.

    Args:
        search_file (str): The path to the YAML file containing the search topics.
            If the file does not exist or is empty, returns `None` for both the topics and output fields.

    Returns:
        tuple[dict[str, str] | None, list[str] | None]: A tuple containing the updated search topics as a dictionary and a list of output fields.
            If the file is invalid or empty, returns `None` for both values.
    """

    def process_search_topics(topics: Dict[str, str]) -> tuple[dict[str, str], list[str]] | tuple[None, None]:
        """
        Processes search topics by lowercasing and replacing special characters in specific keys.

        Args:
            topics (dict[str, str] | None): A dictionary containing search topics with their corresponding values.
                The function modifies this dictionary in place if it's not `None`.

        Returns:
            tuple[dict[str, str] | None, list[str] | None]: A tuple containing the updated `search_topics` dictionary and a list of output fields.
                If `topics` is `None`, returns `None` for both values.
        """

        if topics is None:
            return None, None

        if not isinstance(topics, dict):
            raise ValueError("Input must be a dictionary")

        for key, value in topics.items():
            if key in ("project", "study"):
                topics[key] = lower_and_replace(value)

        output_fields = ["project", "study", "category", "data_id"] + topics.pop("output", [])

        return topics, output_fields

    search_topics = None
    if search_file and Path(search_file).exists():
        yaml = YAML(typ="safe")
        with open(search_file, "r") as file:
            search_topics = yaml.load(file)
    return process_search_topics(search_topics)


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
    data_types = metadata_dtypes

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


def load_metadata(file_path: Path, delimiter: str = "\t") -> pd.DataFrame:
    """Load metadata from a file in tabular format."""
    try:
        return pd.read_csv(
            file_path,
            sep=delimiter,
            dtype=metadata_dtypes,
        )
    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
        exit(1)
    except pd.errors.EmptyDataError:
        logger.error("No data found in the file. Please check the file content.")
        exit(1)
    except pd.errors.ParserError:
        logger.error("Error parsing the file. Please check the file format.")
        exit(1)


def process_row(row: pd.Series) -> Dict[Hashable, Any]:
    """Process a row of data to create a metadata dictionary."""
    project_key = lower_and_replace(row["project"])
    study_key = lower_and_replace(row["study"])

    metadata = defaultdict(lambda: {})
    metadata["project"] = project_key
    metadata["study"] = study_key
    metadata["data_id"] = compute_sha256(fpath=row["file_path"])

    for key, value in row.items():
        if "_" in key and key.startswith(DataProfile.json_dict_fields()):
            k, subk = key.split("_", 1)
            metadata[k][subk] = value
        else:
            if key not in metadata:
                metadata[key] = value

    return {
        _key: json.dumps(_value) if _key in DataProfile.json_dict_fields() else _value
        for _key, _value in metadata.items()
    }


def ingest_metadata(df: pd.DataFrame, mongo_uri: str = None) -> None:
    """Ingest data into the MongoDB collection."""
    documents = [process_row(row) for _, row in df.iterrows()]
    logger.info(f"{len(documents)} documents to ingest")
    print(f"{len(documents)} documents to ingest")
    for document in documents:
        obj = EnhancedDataProfile(uri=mongo_uri, **document)
        obj.save()
