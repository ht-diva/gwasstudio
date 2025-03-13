from pathlib import Path

import click
import cloup

from gwasstudio import logger
from gwasstudio.cli.metadata.utils import (
    load_search_topics,
    query_mongo_obj,
    dataframe_from_mongo_objs,
    df_to_csv,
)
from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
query metadata records from MongoDB
"""


@cloup.command("meta-query", no_args_is_help=True, help=help_doc)
@cloup.option("--search-file", required=True, default=None, help="Path to search template file")
@cloup.option("--output-file", required=True, default=None, help="Path to output file")
@cloup.option("--case-sensitive", default=False, is_flag=True, help="Enable case sensitive search")
@click.pass_context
def meta_query(ctx, search_file, output_file, case_sensitive):
    """
    Queries metadata records from MongoDB based on the search topics specified in the provided template file.

    The search topics are processed by lowercasing and replacing special characters before being used to query the database.
    The resulting metadata records are then written to the output file or printed to the console if the `--stdout` option is set.

    Args:
        ctx (click.Context): Click context object
        search_file (str): Path to the search template YAML file
        output_file (str): Path to write the query results to
        case_sensitive (bool): Enable case-sensitive search

    Returns:
        None
    """

    search_topics, output_fields = load_search_topics(search_file)
    logger.debug(search_topics)

    mongo_uri = ctx.obj["mongo_uri"]
    obj = EnhancedDataProfile(uri=mongo_uri)
    objs = query_mongo_obj(search_topics, obj, case_sensitive=case_sensitive)
    logger.info(f"{len(objs)} results found. Writing to {output_file}")

    df_to_csv(dataframe_from_mongo_objs(output_fields, objs), Path(output_file))
