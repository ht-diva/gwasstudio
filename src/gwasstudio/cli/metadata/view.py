import pprint

import cloup

from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
Retrieve a dataset's metadata from MongoDB using its unique key
"""


@cloup.command("meta_view", no_args_is_help=True, help=help_doc)
@cloup.option(
    "--uk",
    default=None,
    help="dataset unique key",
)
def meta_view(uk):
    obj = EnhancedDataProfile()
    project = "DECODE_largescaleplasma2023"
    obj.unique_key = f"{project}:{uk}"

    pprint.pp(obj.view())
