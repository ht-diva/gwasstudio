from pathlib import Path

import click
import cloup

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile
from gwasstudio.utils import check_file_exists, write_table
from gwasstudio.utils.cfg import get_mongo_uri
from gwasstudio.utils.metadata import load_search_topics, query_mongo_obj, dataframe_from_mongo_objs
from gwasstudio.utils.mongo_manager import manage_mongo

help_doc = """
Query metadata records from MongoDB
"""


@cloup.command("meta-query", no_args_is_help=True, help=help_doc)
@cloup.option("--search-file", required=True, help="The search file used for querying metadata")
@cloup.option("--output-prefix", default="out", help="Prefix to be used for naming the output files")
@cloup.option("--case-sensitive", default=False, is_flag=True, help="Enable case sensitive search")
@cloup.option("--exact-match", default=False, is_flag=True, help="Enable exact match search")
@click.pass_context
def query_metadata(ctx, search_file, output_prefix, case_sensitive, exact_match):
    """
    Queries metadata records from MongoDB based on the search topics specified in the provided template file.

    The search topics are processed by lowercasing and replacing special characters before being used to query the database.
    The resulting metadata records are then written to the output file or printed to the console if the `--stdout` option is set.

    Args:
        ctx (click.Context): Click context object
        search_file (str): Path to the search template YAML file
        output_prefix (str): Path to write the query results to
        case_sensitive (bool): Enable case-sensitive search
        exact_match (bool): Enable exact match search

    Returns:
        None
    """

    if not check_file_exists(search_file, logger):
        exit(1)

    search_topics, output_fields = load_search_topics(search_file)
    logger.debug(search_topics)

    with manage_mongo(ctx):
        mongo_uri = get_mongo_uri(ctx)
        obj = EnhancedDataProfile(uri=mongo_uri)
        objs = query_mongo_obj(search_topics, obj, case_sensitive=case_sensitive, exact_match=exact_match)

    # write metadata query result
    path = Path(output_prefix)
    output_path = path.with_suffix("").with_name(path.stem + "_meta")

    kwargs = {"index": False}
    log_msg = f"{len(objs)} results found. Writing to {output_path}.csv"
    write_table(
        dataframe_from_mongo_objs(output_fields, objs),
        str(output_path),
        logger,
        file_format="csv",
        log_msg=log_msg,
        **kwargs,
    )
