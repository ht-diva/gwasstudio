import pprint

import cloup

from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
Metadata view
"""


@cloup.command("meta_view", no_args_is_help=True, help=help_doc)
@cloup.option(
    "--uk",
    default=None,
    help="Retrieve the metadata of a dataset from its own unique key",
)
def meta_view(uk):
    obj = EnhancedDataProfile()
    obj.unique_key = uk

    pprint.pp(obj.view())
