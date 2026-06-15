# GWASStudio YAML Query Format

This document describes the YAML format for querying GWASStudio metadata.

## What is YAML

YAML is a human-readable data serialization format that uses indentation and simple syntax to represent structured data like mappings, lists, and scalars.

See the [YAML Crash Course for GWASStudio](yaml-intro.md) for a focused introduction to YAML covering only the concepts you need to work with GWASStudio configuration, query, and metadata files.




## Basic Format

### Simple Query

```yaml
project: opengwas
category: GWAS
```

This queries for all documents where:
- `project` field matches "opengwas" (case-insensitive by default)
- `category` field matches "GWAS" (case-insensitive by default)

### With Explicit Query Fields

```yaml
query_fields:
  project: opengwas
  category: GWAS
```

The `query_fields` key is optional. If present, the query parameters are nested under it.

## Nested Structure Format

### Single Nested Field

```yaml
query_fields:
  project: opengwas
  category: GWAS
  notes:
    - consortium: GIANT

output_fields:
  - build
  - population
  - notes_consortium
```

The `notes.consortium` field is automatically mapped to `notes_consortium` (underscore notation).

**Resulting MongoDB Query:**
```json
{
  "project": {"$regex": "opengwas", "$options": "i"},
  "category": {"$regex": "GWAS", "$options": "i"},
  "notes_consortium": {"$regex": "GIANT", "$options": "i"}
}
```

### Multiple Nested Values

When the same nested key appears in multiple list items, the values are combined into a **regex OR** pattern:

```yaml
query_fields:
  project: opengwas
  category: GWAS
  notes:
    - consortium: GIANT
    - consortium: MRC-IEU
```

**Resulting MongoDB Query:**
```json
{
  "project": {"$regex": "opengwas", "$options": "i"},
  "category": {"$regex": "GWAS", "$options": "i"},
  "notes_consortium": {"$regex": "GIANT|MRC-IEU", "$options": "i"}
}
```

> **Note:** This is equivalent to an `$in` match on `notes_consortium`, but GWASStudio produces a regex pattern internally.

### Plain List Values

When a field is a plain list (not a list of dicts), GWASStudio uses `$in` in the MongoDB query:

```yaml
query_fields:
  population:
    - EUR
    - ASN
```

**Resulting MongoDB Query:**
```json
{
  "population": {"$in": ["EUR", "ASN"]}
}
```

## Trait-Specific Queries

To filter by trait attributes, use the `trait` key with a list of dicts:

```yaml
project: opengwas
study: ukb-d

trait:
  - desc: heart failure
  - desc: BMI

output:
  - build
  - population
  - trait_desc
```

**Resulting MongoDB Query:**
```json
{
  "project": {"$regex": "opengwas", "$options": "i"},
  "study": {"$regex": "ukb-d", "$options": "i"},
  "trait_desc": {"$regex": "heart failure|BMI", "$options": "i"}
}
```

## Output Fields

Specify which fields to include in the output:

```yaml
output_fields:
  - build
  - population
  - notes_consortium
  - notes_sex
  - total_samples
  - total_cases
  - total_controls
  - trait_desc
```

The `output` key can also be used. This is for backward compatibility.

```yaml
output:
  - build
  - population
  - notes_consortium
```

## Query Options

### Case Sensitivity

- **Default**: Case-insensitive matching
- **Case-sensitive**: Use `--case-sensitive` flag

### Exact Match

- **Default**: Partial matching with regex
- **Exact match**: Use `--exact-match` flag

**Example with exact match:**

```bash
gwasstudio meta-query --exact-match --search-file search.yml
```

**Resulting MongoDB Query:**
```json
{
  "project": {"$regex": "^opengwas$", "$options": "i"},
  "category": {"$regex": "^GWAS$", "$options": "i"},
  "notes_consortium": {"$regex": "^(GIANT|MRC-IEU)$", "$options": "i"}
}
```

With both `--exact-match` and `--case-sensitive`:
```json
{
  "project": "opengwas",
  "category": "GWAS",
  "notes_consortium": {"$regex": "^(GIANT|MRC-IEU)$"}
}
```

## Complete Examples

### Example 1: Basic Query

**File: `search_example_01.yml`**
```yaml
query_fields:
  project: opengwas
  category: GWAS
  build: GRCh37

output_fields:
  - study
  - population
  - total_samples
```

### Example 2: Nested Query

**File: `search_example_02.yml`**
```yaml
query_fields:
  project: opengwas
  category: GWAS
  notes:
    - consortium: GIANT

output_fields:
  - build
  - population
  - notes_consortium
  - total_samples
  - total_cases
  - total_controls
  - trait_desc
```

### Example 3: Trait-Specific Query

**File: `search_example_04.yml`**
```yaml
project: opengwas
category: GWAS
trait:
  - desc: Body Mass Index
  - desc: BMI

output:
  - build
  - population
  - trait_desc
```

### Example 4: Multi-Value Population Filter

**File: `search_example_05.yml`**
```yaml
project: opengwas
category: GWAS

population:
  - EUR
  - ASN

output:
  - study
  - trait_desc
```

## Field Mapping

Nested fields are automatically mapped from dotted key notation to underscore notation:

| YAML Format | MongoDB Field |
|------|---|
| `notes.consortium` | `notes_consortium` |
| `notes.sex` | `notes_sex` |
| `trait.desc` | `trait_desc` |
| `trait.code` | `trait_code` |

## Valid Metadata Fields

Refer to `MetadataEnum` in `gwasstudio.core.enums` for the complete list of valid fields:

- `project`
- `study`
- `file_path`
- `category`
- `data_id`
- `build`
- `notes_consortium`
- `notes_sex`
- `notes_source_id`
- `total_samples`
- `total_cases`
- `total_controls`
- `trait_code`
- `trait_desc`
- `trait_gene_ids`
- `trait_protein_ids`
- `trait_seqid`
- `trait_tissue`
- `trait_unit`

## Usage with CLI

```bash
# Basic query
gwasstudio meta-query \
  --search-file search_example_02.yml \
  --output-prefix results/

# With exact match
gwasstudio meta-query --exact-match \
  --search-file search_example_02.yml \
  --output-prefix results/

# With case-sensitive matching
gwasstudio meta-query --case-sensitive \
  --search-file search_example_02.yml \
  --output-prefix results/

# With both exact and case-sensitive
gwasstudio meta-query --exact-match --case-sensitive \
  --search-file search_example_02.yml \
  --output-prefix results/

# With verbose logging
gwasstudio --stdout --verbosity loud meta-query \
  --search-file search_example_02.yml \
  --output-prefix results/
```

## Notes

1. **Project and Study Transformation**: The `project` and `study` fields are automatically transformed to lowercase with spaces replaced by underscores before the query is sent to MongoDB.

2. **Empty Values**: Empty or null values are handled gracefully.

3. **Validation**: All query fields are validated against the metadata schema. Invalid fields will raise an `InvalidQueryFieldError` with a list of valid fields.

4. **Logging**: Use `--verbosity loud` to see detailed information about the parsed template and final MongoDB query.

5. **Matching Behavior**:
   - String values → regex match (`$regex`) by default
   - List values → `$in` match if the list contains plain values (e.g., populations)
   - List of dicts (e.g., `trait: [{desc: ...}]`) → regex OR (`|`) on the nested field
