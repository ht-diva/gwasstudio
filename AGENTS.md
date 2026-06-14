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
- `docs/` — documentation
