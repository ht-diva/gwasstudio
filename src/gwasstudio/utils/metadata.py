import json
from pathlib import Path
from typing import Any, Dict, List, Hashable

import pandas as pd
from ruamel.yaml import YAML

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import lower_and_replace, Hashing
from gwasstudio.utils.enums import MetadataEnum


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

        output_fields = ["project", "study", "category", "data_id", MetadataEnum.get_source_id_field()] + topics.pop(
            "output", []
        )

        return topics, output_fields

    search_topics = None
    if search_file and Path(search_file).exists():
        yaml = YAML(typ="safe")
        try:
            with open(search_file, "r") as file:
                search_topics = yaml.load(file)
        except Exception as e:
            msg = f"Error loading YAML file: {e}"
            logger.error(msg)
            exit(msg)
    return process_search_topics(search_topics)


def query_mongo_obj(
    search_criteria: Dict[str, Any],
    data_profile: EnhancedDataProfile,
    case_sensitive: bool = False,
    exact_match: bool = False,
) -> List[Dict[str, Any]]:
    """
    Query the data profile object to find matching results based on search criteria.

    Args:
        search_criteria (Dict[str, Any]): Dictionary containing search criteria.
        data_profile (EnhancedDataProfile): Data profile object to be queried.
        case_sensitive (bool): Flag to enable case-sensitive search. Default is False.
        exact_match (bool): Flag to enable exact match search. Default is False.

    Returns:
        List[Dict[str, Any]]: List of matched data profile entries.
    """
    matched_entries = []
    keys_to_search = tuple(DataProfile.json_dict_fields())

    logger.debug(search_criteria)

    # Iterate over search criteria
    for key, value in search_criteria.items():
        # Create query dictionary and execute data profile query
        if key in keys_to_search:
            query_results = []
            for item in value:
                query_dict = {key: item}
                logger.debug(query_dict)
                query_results.extend(data_profile.query(case_sensitive, exact_match, **query_dict))
            matched_entries.append(query_results)
        else:
            query_dict = {key: value}
            logger.debug(query_dict)
            query_results = data_profile.query(case_sensitive, exact_match, **query_dict)
            matched_entries.append(query_results)

    # Create a dictionary of matched entries with their IDs as keys
    results_dict = {str(entry["_id"]): entry for entry_list in matched_entries for entry in entry_list}

    # Find common IDs among matched entries
    common_ids = set(str(entry["_id"]) for entry in matched_entries[0])
    for entry_list in matched_entries[1:]:
        common_ids.intersection_update(str(entry["_id"]) for entry in entry_list)

    # Filter final results by common IDs
    final_results = [entry for entry_id, entry in results_dict.items() if entry_id in common_ids]

    return final_results


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

    json_dict_fields = set(DataProfile.json_dict_fields())
    data_types = MetadataEnum.get_all_dtypes_dict()

    results = {}
    for field in fields:
        field = field.replace(".", "_")  # replace '.' with '_' (if any) to match data types
        main_key, *rest = field.split("_", 1)
        sub_key = rest[0] if rest else None

        values = []
        for obj in objs:
            if main_key in json_dict_fields and sub_key:
                json_dict = json.loads(obj.get(main_key, "{}"))
                values.append(json_dict.get(sub_key))
            else:
                values.append(obj.get(field))
        results[field] = pd.Series(values, dtype=data_types.get(field, "object"))

    return pd.DataFrame(data=results)


def load_metadata(file_path: Path, delimiter: str = "\t") -> pd.DataFrame:
    """Load metadata from a file in tabular format."""
    try:
        return pd.read_csv(file_path, sep=delimiter, dtype=MetadataEnum.get_all_dtypes_dict(), dtype_backend="pyarrow")
    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
        raise ValueError("File not found")
    except pd.errors.EmptyDataError:
        logger.error("No data found in the file. Please check the file content.")
        raise ValueError("No data found in the file")
    except pd.errors.ParserError:
        logger.error("Error parsing the file. Please check the file format.")
        raise ValueError("Error parsing the file")


def process_row(row: tuple[Any]) -> dict[Hashable, Any]:
    """
    Process a row (namedtuple from ``DataFrame.itertuples``) to create a
    metadata dictionary.

    """
    # ------ Normalise the namedtuple to a dict for easy iteration ------
    row_dict = row._asdict()  # OrderedDict of field_name → value

    # Helper for direct attribute access
    def get(key: str) -> Any:
        """Return the attribute ``key`` from the namedtuple."""
        return getattr(row, key)

    # Custom JSON encoder
    class CustomEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, type(pd.NA)):
                return None
            return super().default(obj)

    project_key = lower_and_replace(get("project"))
    study_key = lower_and_replace(get("study"))

    hg = Hashing()

    metadata = {"project": project_key, "study": study_key, "data_id": hg.compute_hash(fpath=get("file_path"))}

    for key, value in row_dict.items():
        if "_" in key and key.startswith(tuple(DataProfile.json_dict_fields())):
            k, subk = key.split("_", 1)
            metadata.setdefault(k, {})[subk] = value
        elif key in tuple(DataProfile.listfield_names()):
            if "," in value:
                parts = value.split(",")
                items = [part.strip() for part in parts]
            else:
                items = [value.strip()]
            metadata.setdefault(key, []).extend(items)
        else:
            metadata[key] = value

    return {
        _key: json.dumps(_value, cls=CustomEncoder) if _key in DataProfile.json_dict_fields() else _value
        for _key, _value in metadata.items()
    }


def ingest_metadata(df: pd.DataFrame, mongo_uri: str = None) -> None:
    """Ingest data into the MongoDB collection."""

    def _document_generator(df):
        for row in df.itertuples(index=False):
            yield process_row(row)

    logger.info("Starting metadata ingestion")
    rows = len(df.axes[0])
    processed_rows = 0
    logger.info(f"{rows} documents to ingest")

    # Helper that creates and saves a single document
    def _save_document(doc):
        obj = EnhancedDataProfile(uri=mongo_uri, **doc)
        obj.save()
        return 1  # count of one processed row

    for document in _document_generator(df):
        processed_rows += _save_document(document)

        # Print the row counter every 100 rows
        if processed_rows % 100 == 0:
            logger.info(f"{processed_rows} documents processed")
