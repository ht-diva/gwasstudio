# YAML Crash Course for GWASStudio

This guide covers the YAML features you need to understand GWASStudio's configuration, queries, and metadata files. Everything else is unnecessary for this project.

## What Is YAML?

YAML (**Y**AML **A**in't **M**arkup **L**anguage) is a human-readable data serialization format. It is essentially a cleaner, whitespace-sensitive alternative to JSON. GWASStudio uses YAML to describe study configurations, metadata queries, and export templates.

### Minimal Example

```yaml
# A single study definition
trait: "type 2 diabetes"
cohort: "UK Biobank"
p_value: 0.05
```

This represents a single key-value mapping, which is the foundation of everything in GWASStudio.

---

## Core Concepts You Need to Know

### 1. Key-Value Pairs (Scalars)

The simplest building block:

```yaml
key: value
```

The key is on the left, the value on the right, separated by `: ` (colon + space). Values can be:

- **Strings**: `trait: "type 2 diabetes"` (quotes optional for simple values)
- **Numbers**: `p_value: 0.05` or `n: 500000`
- **Booleans**: `active: true` or `active: yes`
- **Null**: `description: null` or `description: ~`

### 2. Nested Objects (Dictionaries)

Use **indentation** (always spaces, never tabs) to nest:

```yaml
study:
  trait: "type 2 diabetes"
  cohort: "UK Biobank"
  details:
    sample_size: 500000
    p_value_threshold: 0.05
```

This represents a dictionary where `study` maps to another dictionary. In GWASStudio, this is how `export.yaml` templates describe complex export queries.

### 3. Lists (Arrays)

Prefix items with `- ` (dash + space) at the same indentation level:

```yaml
trait:
  - "type 2 diabetes"
  - "type 1 diabetes"
  - "gestational diabetes"
```

This is a list (array) of strings. In GWASStudio queries:

- **Plain lists** of strings → MongoDB `$in` operator (match any value)
- **Lists of nested dicts** → regex OR pattern on flattened fields

You can also write lists on a single line using JSON-like syntax:

```yaml
trait: ["type 2 diabetes", "type 1 diabetes"]
```

### 4. Combining Both — Lists of Objects

This is the most important pattern for GWASStudio:

```yaml
query:
  filters:
    - cohort: "UK Biobank"
      population: "EUR"
    - cohort: "All of Us"
      population: "AFR"
```

This is a **list of dictionaries**. Each dash starts a new object. In GWASStudio's query engine, this triggers a regex OR pattern — it will match studies where *either* the first dict's conditions *or* the second dict's conditions are satisfied.

---

## Indentation Rules

Indentation defines structure. The rules are strict:

| Rule | Example |
|---|---|
| Use **spaces only** (2 or 4 per level). Never tabs. | ✅ `trait: "T2D"` |
| Children are indented deeper than their parent. | ✅ 2–4 spaces |
| Items in a list share the same indentation. | ✅ `- value1` |
| Keys inside a dict share the same indentation. | ✅ `key: value` |

**Common mistake:** Mixing indentation levels breaks the document:

```yaml
# ❌ Wrong — inconsistent indentation
study:
trait: "T2D"       # Not indented under study

# ✅ Correct
study:
  trait: "T2D"    # Indented under study
```

---

## Syntax You Will Encounter in GWASStudio

### Comments

Start a line with `#`:

```yaml
trait: "type 2 diabetes"  # This is a comment
# This entire line is ignored
```

### Multi-line Strings

Use `|` for literal blocks (preserves newlines) or `>` for folded blocks (wraps to single line):

```yaml
# Literal block — newlines are preserved
description: |
  This is a long
  description with
  multiple lines

# Folded block — newlines become spaces
summary: >
  This is a long description
  that will be folded into
  a single line
```

### Quotes vs No Quotes

- **Unquoted**: Simple strings, numbers, booleans (`yes`, `no`, `true`, `false`)
- **Double quotes**: Strings with special characters, leading/trailing spaces
- **Single quotes**: Strings that contain double quotes or backslashes

```yaml
# No quotes needed
trait: diabetes
n: 500000

# Quotes required
trait: "type 2 diabetes"    # space in value
trait: 'contains "quotes"'  # quote in value
trait: "  spaced  "         # preserves leading/trailing spaces
```

### Special Characters to Escape

Some characters have meaning in YAML. When used as data, they may need quotes:

```yaml
# ❌ May cause parse errors
trait: type: 2 diabetes      # colon confuses parser

# ✅ Safe
trait: "type: 2 diabetes"    # quoted
trait: 'type: 2 diabetes'    # or single-quoted
```

---

## How GWASStudio Reads YAML

When GWASStudio loads a YAML file (e.g., `export.yaml` or a metadata query), it converts the YAML structure into Python dictionaries and lists. The query engine then interprets those structures:

| YAML Structure | MongoDB Interpretation |
|---|---|
| `key: "value"` | Exact match on `key = "value"` |
| `key: ["a", "b"]` | `$in` operator: `key in ["a", "b"]` |
| `key: {nested: val}` | Regex OR on flattened field path |
| `key: [{a: 1}, {a: 2}]` | Regex OR on nested dict fields |

---

## Quick Reference Cheat Sheet

```yaml
# ─── Scalars ───
string: hello
number: 42
float: 3.14
bool: true
null: null

# ─── Mapping (dict) ───
person:
  name: Alice
  age: 30

# ─── Sequence (list) ───
colors:
  - red
  - green
  - blue

# ─── List of objects ───
studies:
  - name: study1
    trait: diabetes
  - name: study2
    trait: obesity

# ─── Inline JSON-style ───
traits: [diabetes, obesity]

# ─── Comment ───
# This is ignored by the parser
key: value  # inline comment also works
```

---

## Further Reading

For anything not covered here (anchors, tags, merge keys, etc.), the [official YAML spec](https://yaml.org/spec/1.2/spec.html) is the definitive reference. But for GWASStudio, the concepts above cover 100% of what you will encounter.
