import json
from collections import defaultdict
from typing import Dict, Any, Hashable

import cloup
import pandas as pd

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import compute_sha256, lower_and_replace

help_doc = """
Ingest metadata into a MongoDB collection.
"""


def load_data(file_path: str, delimiter: str = "\t") -> pd.DataFrame:
    """Load data from a file in tabular format."""
    try:
        return pd.read_csv(
            file_path,
            sep=delimiter,
            dtype={"project": "category", "study": "category", "file_path": "string[pyarrow]", "category": "category"},
        )
    except FileNotFoundError:
        logger.error("File not found. Please check the file path.")
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
        if "_" in key and key.startswith(("platform", "trait", "total")):
            k, v = key.split("_", 1)
            metadata[k][v] = value
        else:
            if key not in metadata:
                metadata[key] = value

    return {
        _key: json.dumps(_value) if _key in DataProfile.json_dictionary_keys() else _value
        for _key, _value in metadata.items()
    }


def ingest_data(df: pd.DataFrame) -> None:
    """Ingest data into the MongoDB collection."""
    documents = [process_row(row) for _, row in df.iterrows()]
    logger.info(f"{len(documents)} documents to ingest")
    print(f"{len(documents)} documents to ingest")
    for document in documents:
        obj = EnhancedDataProfile(**document)
        obj.save()


@cloup.command("meta_ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--file_path",
        default=None,
        help="Path to the file in tabular format to ingest",
    ),
    cloup.option(
        "--delimiter",
        default="\t",
        help="Character or regex pattern to treat as the delimiter.",
    ),
)
def meta_ingest(file_path: str, delimiter: str) -> None:
    """
    Ingest metadata from a tabular file into a MongoDB collection.

    The function loads the data from the specified file using the provided delimiter,
    processes each row of data into a metadata dictionary, and then saves these dictionaries
    as documents in the MongoDB collection.

    Args:
        file_path (str): Path to the file in tabular format to ingest
        delimiter (str): Character or regex pattern to treat as the delimiter

    Returns:
        None
    """
    df = load_data(file_path, delimiter)
    required_columns = ["project", "study", "file_path", "category"]
    if not all(col in df.columns for col in required_columns):
        raise ValueError(
            f"Missing column(s) in the input file: {', '.join([col for col in required_columns if col not in df.columns])}"
        )
    ingest_data(df)
