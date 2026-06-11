"""
GWASStudio Core Query Module
=============================

This module provides functions for querying genomic data and metadata
stored in GWASStudio backends (TileDB, MongoDB).

Updates:
- Support for nested YAML fields (e.g., trait: [{desc: "..."}])
- Support for output fields as a list
- Support for case_sensitive and exact_match parameters
- Validation for query fields against MetadataEnum
"""

from typing import Optional, Dict, Any, List, Tuple
from gwasstudio.core.config import GWASStudioConfig
from gwasstudio.core.storage import MongoDBStorage  # ,TileDBStorage
from gwasstudio.core.exceptions import QueryError, InvalidQueryError
from gwasstudio.core.enums import MetadataEnum


class InvalidQueryFieldError(InvalidQueryError):
    """Exception raised when a query field is not valid."""

    def __init__(self, message: str, invalid_fields: List[str] = None, valid_fields: List[str] = None):
        details = {"invalid_fields": invalid_fields, "valid_fields": valid_fields} if invalid_fields else {}
        super().__init__(message, details=details)


def _get_valid_metadata_fields() -> List[str]:
    """
    Get the list of valid metadata fields from MetadataEnum.

    Returns:
        List[str]: List of valid metadata field names.
    """
    return [field.get_value() for field in MetadataEnum]


def _generate_nested_mapping(schema_or_data: Dict[str, Any]) -> Dict[str, str]:
    """Convert from dot notation to underscore notation."""
    mapping = {}
    for field, value in schema_or_data.items():
        if isinstance(value, list):
            if value and all(isinstance(item, dict) for item in value):
                subfields = list(value[0].keys())
            else:
                subfields = value
            for subfield in subfields:
                mapping[f"{field}.{subfield}"] = f"{field}_{subfield}"
    return mapping


def _flatten_nested_template(template: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the nested template into a flat MongoDB query."""
    if not template:
        return {}

    field_mapping = _generate_nested_mapping(template)
    flattened = {}

    for field, value in template.items():
        if isinstance(value, list):
            if all(isinstance(item, dict) for item in value):
                nested_values = []
                for item in value:
                    nested_key = next(iter(item.keys()))
                    nested_values.append(item[nested_key])
                    full_field = f"{field}.{nested_key}"
                    target_field = field_mapping.get(full_field, full_field.replace(".", "_"))
                flattened[target_field] = {"$in": nested_values}
            else:
                flattened[field] = {"$in": value}
        else:
            target_field = field_mapping.get(field, field)
            flattened[target_field] = value

    return flattened


def _validate_query_template(template: Dict[str, Any]) -> None:
    """
    Validate that all fields in the query template are valid metadata fields.

    Args:
        template: Dictionary with query parameters.

    Raises:
        InvalidQueryFieldError: If any field in the template is not valid.
    """
    if not template:
        return

    # Get valid fields and mapping
    valid_fields = _get_valid_metadata_fields()
    field_mapping = _generate_nested_mapping(template)
    all_valid_fields = valid_fields + list(field_mapping.values())

    # Check all fields in the template
    invalid_fields = []
    for field in template.keys():
        if field not in all_valid_fields:
            invalid_fields.append(field)

    if invalid_fields:
        raise InvalidQueryFieldError(
            f"Invalid query field(s): {', '.join(invalid_fields)}",
            invalid_fields=invalid_fields,
            valid_fields=all_valid_fields,
        )


def _apply_query_options(
    template: Dict[str, Any],
    case_sensitive: bool = False,
    exact_match: bool = False,
) -> Dict[str, Any]:
    """
    Apply query options (case_sensitive, exact_match) to the template.

    Args:
        template: Dictionary with query parameters.
        case_sensitive: If True, perform case-sensitive search.
        exact_match: If True, perform exact match (no regex).

    Returns:
        Dict[str, Any]: Modified template with query options applied.
    """
    if not template:
        return template

    modified_template = {}

    for field, value in template.items():
        # Skip MongoDB operators (like $in, $regex, etc.)
        if field.startswith("$"):
            modified_template[field] = value
            continue

        if isinstance(value, dict):
            # Handle MongoDB operators (e.g., {"$in": [...]})
            modified_template[field] = value
        elif isinstance(value, str):
            # Apply case sensitivity and exact match
            if not case_sensitive and not exact_match:
                # Case-insensitive regex
                modified_template[field] = {"$regex": value, "$options": "i"}
            elif not case_sensitive and exact_match:
                # Case-insensitive exact match (use $regex with exact pattern)
                modified_template[field] = {"$regex": f"^{value}$", "$options": "i"}
            elif case_sensitive and exact_match:
                # Case-sensitive exact match
                modified_template[field] = value
            else:
                # Case-sensitive regex
                modified_template[field] = {"$regex": value}
        else:
            # For non-string values (numbers, booleans, lists, etc.)
            modified_template[field] = value

    return modified_template


def _parse_yaml_template(yaml_content: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[List[str]]]:
    """
    Parse a YAML template that may have nested structures and output fields.

    This function handles two formats:
    1. Simple format:
       ```yaml
       category: "GWAS"
       project: "opengwas"
       output:
         - build
         - population
       ```
       or
       ```yaml
       query_fields:
         project: opengwas
         category: GWAS
       output:
         - build
         - population

       ```

    2. Nested format:
       ```yaml
       project: opengwas
       study: ukb-d
       trait:
         - desc: skin and subcutaneous tissue
         - desc: Z01
       output:
         - build
         - population
       ```
       or
       ```yaml
       query_fields:
         project: opengwas
         study: ukb-d
         trait:
           - desc: skin and subcutaneous tissue
           - desc: Z01
       output:
         - build
         - population
       ```

    Args:
        yaml_content: Parsed YAML content as a dictionary.

    Returns:
        Tuple of (flattened_query_template, output_fields).
    """
    if not yaml_content:
        return {}, None

    for key, value in yaml_content.items():
        if key in ("project", "study"):
            yaml_content[key] = value.lower().replace(" ", "_")

    # Get output fields from either "output" or "output_fields" key
    output_fields = MetadataEnum.required_output_fields() + yaml_content.pop(
        "output", yaml_content.pop("output_fields", [])
    )

    query_template = yaml_content.get("query_fields", yaml_content)

    # Flatten nested structures in the query template
    flattened_template = _flatten_nested_template(query_template)
    return flattened_template, output_fields


def query_metadata(
    template: Optional[Dict[str, Any]] = None,
    config: Optional[GWASStudioConfig] = None,
    case_sensitive: bool = False,
    exact_match: bool = False,
    yaml_template: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> Tuple[List[Dict[str, Any]], Optional[List[str]]]:
    """
    Query metadata for projects stored in GWASStudio.

    Args:
        template: Dictionary to match against metadata (MongoDB query).
        # project_id: Optional project ID to filter by.
        config: GWASStudio configuration.
        case_sensitive: If True, perform case-sensitive search.
        exact_match: If True, perform exact match (no regex).
        yaml_template: Optional YAML content to parse (alternative to template).
        **kwargs: Additional arguments for the query.

    Returns:
        Tuple containing:
        - List of metadata dictionaries matching the query
        - List of output fields (if yaml_template was provided, None otherwise)


    Raises:
        QueryError: If the query fails.
        InvalidQueryFieldError: If template contains invalid fields.
    """
    if config is None:
        config = GWASStudioConfig()

    output_fields = None
    # If yaml_template is provided, parse it
    if yaml_template:
        template, output_fields = _parse_yaml_template(yaml_template)

    # Validate template fields
    if template:
        _validate_query_template(template)
    # Apply query options
    if template:
        template = _apply_query_options(template, case_sensitive, exact_match)
    # Build query
    query = {}
    if template:
        query.update(template)

    try:
        mongo_storage = MongoDBStorage(config)
        results = list(mongo_storage.query_metadata(query, **kwargs))
        return results, output_fields
    except Exception as e:
        raise QueryError(f"Failed to query metadata: {str(e)}")


# def query_data(
#         project_id: str,
#         region: Optional[Dict[str, Any]] = None,
#         snp_list: Optional[List[str]] = None,
#         pval_threshold: Optional[float] = None,
#         config: Optional[GWASStudioConfig] = None,
#         limit: Optional[int] = None,
#         **kwargs,
# ) -> pd.DataFrame:
#     """
#     Query genomic data for a project.
#
#     Args:
#         project_id: Unique identifier for the project.
#         region: Genomic region to query (e.g., {"chr": "1", "start": 100000, "end": 200000}).
#         snp_list: List of SNP IDs to query.
#         pval_threshold: Only return variants with p-value <= threshold.
#         config: GWASStudio configuration.
#         limit: Maximum number of records to return.
#         **kwargs: Additional arguments for the query.
#
#     Returns:
#         pandas DataFrame with the query results.
#
#     Raises:
#         QueryError: If the query fails.
#         ProjectNotFoundError: If the project is not found.
#         InvalidQueryError: If the query parameters are invalid.
#     """
#     _validate_project_id(project_id)
#
#     if config is None:
#         config = GWASStudioConfig()
#
#     # Parse region
#     region_info = _validate_region(region) if region else None
#
#     # Initialize storage backends
#     tiledb_storage = TileDBStorage(config)
#     mongo_storage = MongoDBStorage(config)
#
#     try:
#         # Check if project exists
#         if not tiledb_storage.project_exists(project_id):
#             raise ProjectNotFoundError(f"Project {project_id} not found")
#
#         # Query data from TileDB
#         df = tiledb_storage.query_data(
#             project_id,
#             region=region_info,
#             snp_list=snp_list,
#             pval_threshold=pval_threshold,
#             limit=limit,
#             **kwargs,
#         )
#
#         return df
#     except Exception as e:
#         raise QueryError(f"Failed to query data for project {project_id}: {str(e)}")


# def query_data_stream(
#         project_id: str,
#         region: Optional[Dict[str, Any]] = None,
#         snp_list: Optional[List[str]] = None,
#         pval_threshold: Optional[float] = None,
#         config: Optional[GWASStudioConfig] = None,
#         chunk_size: int = 10000,
#         **kwargs,
# ) -> Generator[pd.DataFrame, None, None]:
#     """
#     Query genomic data for a project in chunks (streaming).
#
#     Args:
#         project_id: Unique identifier for the project.
#         region: Genomic region to query.
#         snp_list: List of SNP IDs to query.
#         pval_threshold: Only return variants with p-value <= threshold.
#         config: GWASStudio configuration.
#         chunk_size: Number of records per chunk.
#         **kwargs: Additional arguments for the query.
#
#     Yields:
#         Chunks of pandas DataFrames with query results.
#
#     Raises:
#         QueryError: If the query fails.
#     """
#     _validate_project_id(project_id)
#
#     if config is None:
#         config = GWASStudioConfig()
#
#     region_info = _validate_region(region) if region else None
#
#     tiledb_storage = TileDBStorage(config)
#
#     try:
#         for chunk in tiledb_storage.query_data_stream(
#                 project_id,
#                 region=region_info,
#                 snp_list=snp_list,
#                 pval_threshold=pval_threshold,
#                 chunk_size=chunk_size,
#                 **kwargs,
#         ):
#             yield chunk
#     except Exception as e:
#         raise QueryError(f"Failed to stream data for project {project_id}: {str(e)}")


def list_projects(
    config: Optional[GWASStudioConfig] = None,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    List all projects stored in GWASStudio.

    Args:
        config: GWASStudio configuration.
        **kwargs: Additional arguments for the query.

    Returns:
        List of dictionaries with project metadata.
    """
    if config is None:
        config = GWASStudioConfig()

    mongo_storage = MongoDBStorage(config)
    try:
        return list(mongo_storage.list_projects(**kwargs))
    except Exception as e:
        raise QueryError(f"Failed to list projects: {str(e)}")


def _validate_project_id(project_id: str) -> None:
    """Validate that the project_id is not empty."""
    if not project_id:
        raise InvalidQueryError("project_id cannot be empty")


def _validate_region(region: str) -> Dict[str, Any]:
    """
    Validate and parse a genomic region string.

    Expected format: "chr:start-end" or "chr:start" or "chr"

    Args:
        region: Genomic region string.

    Returns:
        Dictionary with 'chr', 'start', and 'end' keys.
    """
    if not region:
        return {}

    parts = region.split(":")
    if len(parts) == 1:
        return {"chr": parts[0], "start": None, "end": None}
    elif len(parts) == 2:
        chr_part = parts[0]
        range_part = parts[1]
        if "-" in range_part:
            start, end = range_part.split("-")
            try:
                start = int(start) if start else None
                end = int(end) if end else None
            except ValueError:
                raise InvalidQueryError(f"Invalid region format: {region}")
            return {"chr": chr_part, "start": start, "end": end}
        else:
            try:
                start = int(range_part) if range_part else None
            except ValueError:
                raise InvalidQueryError(f"Invalid region format: {region}")
            return {"chr": chr_part, "start": start, "end": None}
    else:
        raise InvalidQueryError(f"Invalid region format: {region}")
