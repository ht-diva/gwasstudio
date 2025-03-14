from pathlib import Path

import click
import cloup
from gwasstudio.cli.metadata.utils import get_mongo_uri
from gwasstudio.cli.metadata.utils import load_metadata, ingest_metadata

help_doc = """
Ingest metadata into a MongoDB collection.
"""


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
    df = load_metadata(Path(file_path), delimiter)
    required_columns = ["project", "study", "file_path", "category"]
    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing column(s) in the input file: {', '.join(missing_cols)}")

    mongo_uri = get_mongo_uri(ctx)

    ingest_metadata(df, mongo_uri)
