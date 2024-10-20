import pprint

import cloup

from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
Metadata view
"""


@cloup.command("meta_view", no_args_is_help=True, help=help_doc)
@cloup.option(
    "--unique-key",
    default=None,
    help="Retrieve the metadata of a dataset from its own unique key",
)
def meta_view(unique_key):
    obj = EnhancedDataProfile(unique_key=unique_key)
    pp = pprint.PrettyPrinter(indent=2, width=30, compact=True)
    pp.pprint(obj.view())
