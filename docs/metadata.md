# Metadata records

Each project-study combination is associated with a **metadata record** that describes the key characteristics of the available summary-statistics file(s).
These records are used throughout the platform (e.g., for filtering, querying, and API responses) and therefore follow a **stable schema**. When new attributes become relevant they should be added to this table and, if possible, given a concise, human-readable label.

---

## Core metadata fields

| Field (canonical name) | Friendly label              | Description                                            | Possible values / format                                                 |
|------------------------|-----------------------------|--------------------------------------------------------|--------------------------------------------------------------------------|
| `category`             | **Summary-statistic type**  | Type of summary statistics                             | `GWAS`, `pQTL`, `eQTL`, …                                                |
| `project`              | **Project**                 | Identifier of the project the data belongs to          | `opengwas`, `pqtl`, `genesandhealth`, …                                  |
| `study`                | **Study**                   | Identifier of the specific study                       | `ukb-a`, `ukb-b`, `ukb-d`, …                                             |
| `data_id`              | **Record ID**               | Unique identifier for the metadata entry               | e.g. `89f31189b3`                                                        |
| `file_path`            | **File paths**              | The paths where the summary-statistics files are stored | e.g. `./ukb-d_sampled/ukb-d-XVIII_MISCFINDINGS.gwaslab.tsv.sampled.gz`      |
| `build`                | **Genome build**            | Reference genome build                                 | `GRCh37`, `GRCh38`                                                       |
| `population`           | **Broad ancestry category** | Ancestry of the cohort                                 | see the [ancestry categories](population.md) page                        |
| `total_samples`        | **Number of samples**       | Sample size of the cohort                              | integer                                                                  |                                            | integer
| `total_cases`          | **Number of cases**         | Count of case subjects (if applicable)                 | integer                                                                  |
| `total_controls`       | **Number of controls**      | Count of control subjects (if applicable)              | integer                                                                  |
| `trait_desc`           | **Trait description**       | Human-readable description of the phenotype or protein | e.g. “Pregnancy, childbirth and the puerperium”, “Alpha-1B-glycoprotein” |
| `notes_sex`            | **Sex of participants**     | Sex composition of the cohort                          | `Males`, `Females`, `Combined`                                           |


---

## Additional optional fields (may appear in specific projects)

| Field                                | Friendly label             | When to use                                                    |
|--------------------------------------|----------------------------|----------------------------------------------------------------|
| `notes_maker`                        | **Data producer**          | Institution or consortium that generated the data              |
| `notes_maker_platform_technology`    | **Assay technology**       | Technology used to generate the data (e.g., array, sequencing) |
| `notes_maker_platform_description`   | **Assay description**      | Provide extra detail beyond the technology name                |
| `notes_maker_platform_version`       | **Assay version**          | Useful when multiple releases exist                            |
| `notes_maker_platform_normalization` | **Normalization strategy** | Clarifies how raw data were transformed                        |
| `notes_software_description`         | **Analysis software**      | Record the tool that generated the statistics                  |
| `notes_source_id`                    | **Source ID**              | Original identifier of the upstream summary‑statistics file    |
| `trait_gene_ids`                     | **Gene identifiers**       | When multiple genes are linked to the trait                    |
| `trait_icd10`                        | **Clinical code**          | When the trait is a disease phenotype                          |
| `trait_protein_ids`                  | **Protein identifiers**    | For proteomics‑related summary statistics                      |
| `trait_seqid`                        | **Sequence identifier**    | SomaLogic specific sequence identifier for the trait           |                                                     |
| `trait_tissue`                       | **Measured tissue**        | eQTL/pQTL studies that are tissue‑specific                     |
| `trait_unit`                         | **Measurement unit**       | For quantitative traits (e.g., blood pressure)                 |
