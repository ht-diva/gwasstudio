import json

import cloup

from gwasstudio import logger
from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
Ingest metadata into a MongoDB collection.
"""


@cloup.command("meta_ingest", no_args_is_help=True, help=help_doc)
@cloup.option_group(
    "Ingestion options",
    cloup.option(
        "--filepath",
        default=None,
        help="Path to the file of JSON dictionaries to ingest",
    ),
)
def meta_ingest(filepath):
    with open(filepath, "r") as fp:
        jd_list = json.load(fp)

    logger.info("{} documents to ingest".format(len(jd_list)))
    print("{} documents to ingest".format(len(jd_list)))

    for jd in jd_list:
        logger.debug(jd)
        obj = EnhancedDataProfile(**jd)
        obj.save()
