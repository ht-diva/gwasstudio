"""
GWASStudio CLI Metadata Query Command (Final Version)
=====================================================

This module provides the CLI command to query metadata, fully updated to use
ONLY the GWASStudio core module with support for:
- Nested YAML templates (e.g., trait: [{desc: "..."}])
- Output fields specification
- case_sensitive and exact_match options
- Validation against MetadataEnum

This command searches for metadata matching the criteria specified in a YAML file.

YAML Format Examples:
-------------------

1. Simple format:
```yaml
project: opengwas
study: ukb-d
category: GWAS
```

2. With nested fields and output:
```yaml
project: opengwas
study: ukb-d
category: GWAS

# Nested trait descriptions (will be converted to trait_desc: {$in: [...]})
trait:
  - desc: skin and subcutaneous tissue
  - desc: Z01
  - desc: pregnancy

# Output fields to include in results
output:
  - build
  - population
  - notes_consortium
  - notes_sex
  - total_samples
  - total_cases
  - total_controls
  - trait_desc
```

Notes:
------
- Nested fields like 'trait.desc' are mapped to MongoDB fields (e.g., 'trait_desc')
- The 'output' field specifies which fields to include in the output
- Invalid fields will raise an error with a list of valid fields
"""

from pathlib import Path
from typing import Any, Dict, Optional

import click
import cloup
import pandas as pd
import yaml

from gwasstudio import logger

# Import updated utilities
from gwasstudio.cli.utils import create_config_from_context
from gwasstudio.core import (
    ConfigurationError,
    GWASStudioError,
    InvalidInputError,
    InvalidQueryFieldError,
    QueryError,
)

# Import ONLY from core module
from gwasstudio.core.query import query_metadata as core_query_metadata

HELP_DOC = """
Query metadata records from MongoDB using GWASStudio core.
"""


def _load_yaml_file(search_file: str) -> Dict[str, Any]:
    """
    Load and parse a YAML file.

    Args:
        search_file: Path to the YAML file.

    Returns:
        Dict[str, Any]: Parsed YAML content.

    Raises:
        InvalidInputError: If the file doesn't exist or is invalid YAML.
    """
    try:
        logger.info(f"Processing {search_file}")
        with open(search_file, "r") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        raise InvalidInputError(f"Search file not found: {search_file}")
    except yaml.YAMLError as e:
        raise InvalidInputError(f"Invalid YAML in search file: {str(e)}")


def _write_output_table(
    df: pd.DataFrame,
    output_path: Path,
    file_format: str = "csv",
    log_msg: Optional[str] = None,
) -> None:
    """
    Write DataFrame to output file.

    Args:
        df: DataFrame to write.
        output_path: Path to the output file.
        file_format: Format for the output file ("csv", "parquet", "tsv").
        log_msg: Optional message to log.
    """
    if log_msg:
        logger.info(log_msg)

    if file_format == "csv":
        df.to_csv(str(output_path), index=False)
    elif file_format == "parquet":
        df.to_parquet(str(output_path))
    elif file_format == "tsv":
        df.to_csv(str(output_path), index=False, sep="\t")
    else:
        raise InvalidInputError(f"Unsupported file format: {file_format}")


@cloup.command("meta-query", no_args_is_help=True, help=HELP_DOC)
@cloup.option(
    "--search-file",
    required=True,
    help="Path to the YAML file containing search criteria.",
)
@cloup.option(
    "--output-prefix",
    default="out",
    help="Prefix for the output file name.",
)
@cloup.option(
    "--case-sensitive",
    default=False,
    is_flag=True,
    help="Enable case-sensitive search (exact string matching).",
)
@cloup.option(
    "--exact-match",
    default=False,
    is_flag=True,
    help="Enable exact match search (no regex for strings).",
)
@cloup.option(
    "--output-format",
    default="csv",
    type=click.Choice(["csv", "parquet", "tsv"]),
    help="Output file format.",
)
@click.pass_context
def query_metadata(
    ctx,
    search_file: str,
    output_prefix: str,
    case_sensitive: bool,
    exact_match: bool,
    output_format: str,
) -> None:
    """
    Query metadata records from MongoDB using GWASStudio core.

    This command supports:
    - Nested YAML fields (e.g., trait: [{desc: "..."}])
    - Output fields specification
    - Case-sensitive and exact match options
    - Validation against MetadataEnum

    Args:
        ctx: Click context object.
        search_file: Path to the YAML file containing search criteria.
        output_prefix: Prefix for the output file name.
        case_sensitive: Enable case-sensitive search.
        exact_match: Enable exact match search.
        output_format: Output file format (csv, parquet, tsv).

    Raises:
        InvalidInputError: If the search file is invalid or missing.
        InvalidQueryFieldError: If the YAML contains invalid fields.
        QueryError: If the query fails.
        ConfigurationError: If the configuration is invalid.
    """
    try:
        # Validate search file exists
        if not Path(search_file).exists():
            raise InvalidInputError(f"Search file not found: {search_file}")

        # Load YAML file
        yaml_content = _load_yaml_file(search_file)

        # Create configuration and adapter
        config = create_config_from_context(ctx)

        # Query metadata using core function with YAML template
        # The core function will handle parsing, validation, and query options
        results, output_fields = core_query_metadata(
            yaml_template=yaml_content,
            config=config,
            case_sensitive=case_sensitive,
            exact_match=exact_match,
        )

        if not results:
            logger.info("No results found.")
            return

        # Convert results to DataFrame
        df = pd.DataFrame(results)

        # Filter output fields if specified
        if output_fields:
            # Filter columns to only include output_fields (that exist in df)
            available_fields = [f for f in output_fields if f in df.columns]
            if available_fields:
                df = df[available_fields]
            else:
                logger.warning(
                    f"None of the specified output fields exist in results. Available fields: {list(df.columns)}"
                )

        # Write output
        output_path = Path(output_prefix)
        output_path = output_path.with_suffix("").with_name(output_path.stem + "_meta")

        if output_format == "csv":
            output_path = output_path.with_suffix(".csv")
        elif output_format == "parquet":
            output_path = output_path.with_suffix(".parquet")
        elif output_format == "tsv":
            output_path = output_path.with_suffix(".tsv")

        log_msg = f"{len(df)} results found. Writing to {output_path}"
        _write_output_table(df, output_path, file_format=output_format, log_msg=log_msg)

    except InvalidInputError as e:
        click.echo(f"Input error: {e.message}", err=True)
        raise SystemExit(1)
    except InvalidQueryFieldError as e:
        click.echo(f"Invalid query field(s): {', '.join(e.details.get('invalid_fields', []))}", err=True)
        click.echo(f"Valid fields: {', '.join(e.details.get('valid_fields', []))}", err=True)
        raise SystemExit(1)
    except QueryError as e:
        click.echo(f"Query error: {e.message}", err=True)
        raise SystemExit(1)
    except ConfigurationError as e:
        click.echo(f"Configuration error: {e.message}", err=True)
        raise SystemExit(1)
    except GWASStudioError as e:
        click.echo(f"GWASStudio error: {e.message}", err=True)
        raise SystemExit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {str(e)}", err=True)
        raise SystemExit(1)
