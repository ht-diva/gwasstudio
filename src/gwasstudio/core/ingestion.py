"""
GWASStudio Core Ingestion Module
=============================

This module provides functions for ingesting genomic data and metadata
stored in GWASStudio backends (TileDB, MongoDB).

"""

from typing import Any, Hashable, Optional

from gwasstudio.core import (
    GWASStudioConfig,
    Hashing,
    IngestionError,
)
from gwasstudio.core.storage import MongoDBStorage  # ,TileDBStorage
from gwasstudio.core.str_utils import lower_and_replace


def _document_generator(documents):
    for doc in documents:
        yield process_metadata_dict(doc)


def ingest_metadata(
    template: list[dict[Hashable, Any]],
    config: Optional[GWASStudioConfig] = None,
) -> None:
    if config is None:
        config = GWASStudioConfig()

    batch = []
    for i, document in enumerate(_document_generator(template), 1):
        batch.append(document)

    try:
        mongo_storage = MongoDBStorage(config)
        mongo_storage.bulk_store_metadata(batch)
    except Exception as e:
        raise IngestionError(f"Failed to query metadata: {str(e)}")


def process_metadata_dict(metadata: dict[Hashable, Any]) -> dict[Hashable, Any]:
    """
    Process a metadata dictionary.
    """
    # Perform transformations
    hg = Hashing()

    project_key = lower_and_replace(metadata.get("project"))
    study_key = lower_and_replace(metadata.get("study"))
    data_id = hg.compute_hash(fpath=metadata.get("file_path"))

    # Update the dictionary with new values
    metadata.update(
        {"project": project_key, "study": study_key, "data_id": data_id, "population": [metadata.get("population")]}
    )

    # Clean up unwanted keys
    metadata.pop("file_path")

    return metadata
