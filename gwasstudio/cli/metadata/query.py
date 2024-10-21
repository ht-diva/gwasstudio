import cloup

from gwasstudio.mongo.models import EnhancedDataProfile

help_doc = """
Metadata query
"""


@cloup.command("meta_query", no_args_is_help=True, help=help_doc)
@cloup.option(
    "--key",
    default=None,
    help="query key",
)
@cloup.option(
    "--value",
    default=None,
    help="query value",
)
def meta_query(key, value):
    obj = EnhancedDataProfile()
    objs = obj.query(**{key: value})
    print(objs)
    pass
