import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, Any, Hashable

import click
import cloup
import pandas as pd

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile, DataProfile
from gwasstudio.utils import compute_sha256, lower_and_replace

help_doc = """
Ingest metadata into a MongoDB collection.
"""


def load_data(file_path: Path, delimiter: str = "\t") -> pd.DataFrame:
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


def ingest_data(df: pd.DataFrame, mongo_uri: str = None) -> None:
    """Ingest data into the MongoDB collection."""
    documents = [process_row(row) for _, row in df.iterrows()]
    logger.info(f"{len(documents)} documents to ingest")
    print(f"{len(documents)} documents to ingest")
    for document in documents:
        obj = EnhancedDataProfile(uri=mongo_uri, **document)
        obj.save()


@cloup.command("meta-ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--file-path",
        default=None,
        help="Path to the tabular file containing the metadata to be ingested",
    ),
    cloup.option(
        "--delimiter",
        default="\t",
        help="Character or regex pattern to treat as the delimiter.",
    ),
)
@click.pass_context
def meta_ingest(ctx, file_path: str, delimiter: str) -> None:
    """
    Ingest metadata from a tabular file into a MongoDB collection.

    The function loads the data from the specified file using the provided delimiter,
    processes each row of data into a metadata dictionary, and then saves these dictionaries
    as documents in the MongoDB collection.

    Args:
        ctx (click.Context): Click context object
        file_path (str): Path to the file in tabular format to ingest
        delimiter (str): Character or regex pattern to treat as the delimiter

    Returns:
        None
    """
    df = load_data(Path(file_path), delimiter)
    required_columns = ["project", "study", "file_path", "category"]
    # We can simplify this by using a set intersection operation
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing column(s) in the input file: {', '.join(missing_cols)}")

    mongo_uri = ctx.obj["mongo_uri"]
    ingest_data(df, mongo_uri)
