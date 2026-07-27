"""
GWASStudio Core Ingestion Module
=============================

This module provides functions for ingesting genomic data and metadata
stored in GWASStudio backends (TileDB, MongoDB).

"""

from collections.abc import Hashable
from typing import Any

from gwasstudio.core import (
    GWASStudioConfig,
    Hashing,
    IngestionError,
)
from gwasstudio.core.enums import AncestryEnum, BuildEnum, DataCategoryEnum
from gwasstudio.core.storage import MongoDBStorage  # ,TileDBStorage
from gwasstudio.core.str_utils import lower_and_replace


def _document_generator(documents):
    for doc in documents:
        yield process_metadata_dict(doc)


def ingest_metadata(
    template: list[dict[Hashable, Any]],
    config: GWASStudioConfig | None = None,
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
    # Validate and normalize population values against AncestryEnum
    population = metadata.get("population")
    if population is not None:
        # Handle both string and list inputs
        if isinstance(population, str):
            # Split comma-separated values and strip whitespace
            population_list = [p.strip() for p in population.split(",") if p.strip()]
        elif isinstance(population, list):
            population_list = population
        else:
            population_list = [str(population)]

        # Normalize each value and validate
        normalized_population = []
        for pop in population_list:
            if pop:  # Skip empty strings
                try:
                    normalized = AncestryEnum.normalize(pop)
                    normalized_population.append(normalized)
                except ValueError as e:
                    raise ValueError(f"Invalid population value '{pop}'. {str(e)}")
        population = normalized_population
    else:
        population = []

    # Validate data_category value
    category = metadata.get("category")
    if category is not None:
        try:
            DataCategoryEnum.validate(category)
        except ValueError as e:
            raise ValueError(f"Invalid category value '{category}'. {str(e)}")

    # Validate build value
    build = metadata.get("build")
    if build is not None:
        try:
            BuildEnum.validate(build)
        except ValueError as e:
            raise ValueError(f"Invalid build value '{build}'. {str(e)}")

    # Perform transformations
    hg = Hashing()

    project_key = lower_and_replace(metadata.get("project"))
    study_key = lower_and_replace(metadata.get("study"))
    data_id = hg.compute_hash(metadata.get("file_path"))

    # Update the dictionary with new values
    metadata.update({"project": project_key, "study": study_key, "data_id": data_id, "population": population})

    # Clean up unwanted keys
    metadata.pop("file_path")

    return metadata
