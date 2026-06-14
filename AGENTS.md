# AGENTS.md — Guidelines for AI Agents

This file defines conventions that AI coding agents must follow when modifying this codebase.

## Type Hints — PEP 585 & PEP 604

**Python 3.12+ is required.** Always use modern type hint syntax:

| Do ✅ | Don't ❌ |
|---|-|
| `list[str]` | `typing.List[str]` |
| `dict[str, Any]` | `typing.Dict[str, Any]` |
| `tuple[int, ...]` | `typing.Tuple[int, ...]` |
| `set[str]` | `typing.Set[str]` |
| `str | None` | `typing.Optional[str]` |
| `str | list[str]` | `typing.Union[str, list[str]]` |

### Rules

1. **Never import** `List`, `Dict`, `Tuple`, `Set`, `Union`, `Optional` from `typing`.
2. **Never use** `Optional[X]` — prefer `X | None`.
3. **Never use** `Union[X, Y]` — prefer `X | Y`.
4. **Keep** `Any` from `typing` — it has no built-in equivalent.
5. **Remove** the `from typing import ...` line when it no longer contains any needed imports.
6. If a `from typing import` line becomes empty after your changes, **delete the entire import line**.

### Examples

```python
# Good ✅
def process(traits: str | list[str], config: dict[str, Any] | None) -> list[Path]:
    ...

# Bad ❌
from typing import Optional, Union, Any
from typing import List

def process(
    traits: Union[str, List[str]],
    config: Optional[dict[str, Any]],
) -> Optional[List[Path]]:
    ...
```

## Code Style

- **Style**: PEP 8, 4-space indentation, ~120 char line width
- **Naming**: `snake_case` for functions/variables, `PascalCase` for classes, `UPPER_SNAKE` for constants
- **Docstrings**: Google style on public APIs (modules, classes, public functions)
- **Imports**:
  - Standard library first
  - Third-party packages second
  - Local project imports last
  - Group each section with a blank line between them
  - Sort alphabetically within each group

## Project Structure

- `src/gwasstudio/` — main package (importable)
  - `core/` — business logic (config, ingestion, query, storage, hashing, str_utils)
  - `cli/` — CLI commands and utilities
  - `methods/` — analysis methods
  - `utils/` — standalone helpers
- `tests/` — test suite (mirrors src structure)
  - `unit/` — unit tests
  - `integration/` — integration tests (require MongoDB)
  - `data/` — test fixtures and sample data
- `docs/` — documentation

## Running Tests

### Prerequisites

1. **Conda environment** activated with `gwasstudio` installed (dev dependencies).
2. **MongoDB** — the `mongod` binary must be on `$PATH`. For integration tests, `mongostat` is also required (usually bundled with MongoDB).
3. **Git submodules** initialized — run `git submodule update --init --recursive` before running integration tests that depend on external data.

### Unit Tests

Run all unit tests:

```bash
pytest tests/unit/
```

Run with coverage:

```bash
make unit_test
```

Run a specific test module:

```bash
pytest tests/unit/core/test_hashing.py -v
pytest tests/unit/core/test_ingestion.py -v
pytest tests/unit/core/test_query.py -v
```

### Integration Tests

Integration tests require a live MongoDB instance. They start/stop `mongod` automatically via `tests/integration/conftest.py`.

**Via Makefile targets:**

```bash
make test_ingest_metadata      # Metadata ingestion + query tests
make test_full_export           # Full pipeline export tests
make test_ingest_with_recalc    # Ingest with -log10p recalculation tests
```

**Via pytest directly:**

```bash
pytest tests/integration/ -v --tb=short
```

### Full Test Suite

```bash
make test                      # Runs unit tests + metadata integration
```

### Test Data & Environment Notes

- Test sample data lives in `tests/data/` and `data/dataset/`.
- The `metadata_table.tsv` in `data/` uses `data/dataset/`-prefixed paths so file resolution works from the project root.
- All integration scripts must be run from the project root directory.
- The `mongod` fixture in `tests/integration/conftest.py` manages the MongoDB lifecycle automatically for pytest-based integration tests.
