"""GWASStudio CLI package.

Subcommand modules are exposed lazily (PEP 562) so that importing the
package — or a lightweight subcommand such as ``list``, ``meta-query`` or
``info`` — does not pull in the heavy Dask/TileDB/Pandas stack used by
``export`` and ``ingest``.
"""

from importlib import import_module

# attribute name → module that defines it
_LAZY_ATTRS = {
    "export": ".export",
    "info": ".info",
    "ingest": ".ingest",
    "list_projects": ".list",
    "query_metadata": ".metadata.query",
}

__all__ = ["export", "info", "ingest", "list_projects", "query_metadata"]


def __getattr__(name):
    module_name = _LAZY_ATTRS.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_name, __name__)
    attr = getattr(module, name)
    globals()[name] = attr  # cache so __getattr__ is only called once
    return attr


def __dir__():
    return sorted(set(globals()) | set(__all__))
