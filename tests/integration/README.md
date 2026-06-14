# GWASStudio Integration Tests

## Overview

The `tests/integration/` directory contains integration tests for the GWASStudio CLI tool.
These tests spin up a local MongoDB instance, ingest data, run queries and exports, and
verify the results — covering the full end-to-end workflow.

## Structure

| File | Purpose |
|---|-
| `conftest.py` | Pytest fixture that manages the local MongoDB lifecycle |
| `mongo_test_utils.py` | Standalone utilities: `MongoDBManager` class, context manager, CLI tool |
| `test_ingest_metadata.sh` | Tests metadata ingestion and YAML-based queries |
| `test_full_export.sh` | Tests full pipeline: ingest, query, export, regions/SNPs filtering, locusbreaker, meta-analysis |
| `test_ingest_with_recalc.sh` | Tests ingestion with `--pvalue` flag (recalculates -log10p) |

## Purpose

1. **End-to-End Testing**: Validates that ingestion, querying, exporting, and analysis workflows
   work together correctly with a real MongoDB instance.
2. **Regression Testing**: Each run exercises all CLI commands to catch regressions early.
3. **Usage Reference**: The scripts serve as living documentation of real-world GWASStudio usage.

## Prerequisites

- **MongoDB** installed and on `PATH` (for `mongod` and `mongostat` commands)
- **GWASStudio** installed (`gwasstudio` command available)
- **Conda environment** activated

## Running the Tests

### Via Makefile

```bash
make test-integration      # Run all integration tests (Docker-based)
make test-integration-setup  # Start Docker services
make test-integration-exec   # Run integration tests
make test-integration-stop   # Tear down Docker services
```

### Via pytest (local MongoDB)

```bash
# Run all integration tests with the conftest.py fixture
pytest tests/integration/ -v

# Run a specific test
pytest tests/integration/conftest.py -v
```

### Via shell scripts (legacy, for manual inspection)

```bash
cd tests/integration
./test_ingest_metadata.sh    # Ingest + query tests
./test_full_export.sh        # Full pipeline tests
./test_ingest_with_recalc.sh # Ingest with -log10p recalculation
```

## Test Scripts Breakdown

### `test_ingest_metadata.sh`
- Ingests metadata into MongoDB
- Lists projects
- Runs multiple YAML-based queries (basic, case-sensitive, exact-match, trait description)
- Verifies expected result counts

### `test_full_export.sh`
- Ingests metadata
- Queries and exports data
- Tests all export formats (CSV, Parquet, plots)
- Tests region/SNP filtering (hapmap3, inline regions)
- Tests lead-SNP search
- Tests locusbreaker and meta-analysis

### `test_ingest_with_recalc.sh`
- Ingests metadata with `--pvalue` flag (recalculates -log10p per variant)
- Queries and exports data
- Tests region/SNP filtering and locusbreaker

## Example Data

Search YAML files (`search_example_*.yml`) and other test data live in `data/` at the
project root. The test scripts reference these files via relative paths.

## Troubleshooting

- **MongoDB not found**: Ensure `mongod` and `mongostat` are on your `PATH`.
- **Port conflict**: The tests use port 27018. Kill any existing process on that port first.
- **Missing files**: Verify `data/metadata_table.tsv` and `data/search_example_*.yml` exist.
- **GWASStudio not installed**: `conda activate gwasstudio` or `poetry install`.
