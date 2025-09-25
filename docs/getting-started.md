# **Getting Started**

This short overview of GWASStudio will help you get started.

---

## **Usage on the HT-HPC**

To use GWASStudio on the HT computing cluster, simply type:

```
source /exchange/healthds/singularity_functions
```

Verify with:

```
gwasstudio --version
```

### **Vault token**

To securely access (meta)data according to your user permissions, you will be provided with a vault token.

To authenticate automatically, please save your token in the following file:

```
${HOME}/.vault-token
``` 

**NOTE**: 

- The vault token is **personal and confidential**. **Do not** share it with other users
- If `${HOME}/.vault-token` is missing, you will be prompted to manually paste the token during commmand execution


## **Main commands**

GWASStudio has three main commands for users:

- [list](#1-list): list available/accessible data
- [meta-query](#2-meta-query): query metadata of interest
- [export](#3-export): export data of interest

---

### **1. `list`** 

The `list` command is used to display all summary statistics available on MongoDB, based on your access permissions (see [vault token](#vault-token)) as category → project → study.

##### List example

```
gwasstudio list
```

##### List output example

```
Category: GWAS
  Project: opengwas
        Studies: ukb-a, ukb-b, ukb-d
```

---

### **2. `meta-query`** 

The `meta-query` command retrieves [metadata](#metadata) of interest using a [query file](#query-file). It can be used to verify the availability and characteristics of the data to [export](#3-export). 

##### Meta-query example

```
gwasstudio meta-query --search-file query_ex01.txt --output-prefix output_query_ex01 
```

The output is a [metadata](#metadata) table named [output_query_ex01.csv](#meta-query-output-example) with records filtered by the query file [query_ex01.txt](#query-file-example).

For a detailed explanation of all command options, see also [meta-query command](commands.md#meta-query).

---

#### Metadata

A metadata table is assigned to each project-study and describes key information about the available/accessible summary statistics.</br >
Metadata records may contain the following fields:

| Metadata field | Description | Possible values |
| --- | --- | --- |
| `category` | The type of summary statistics | GWAS, pQTL |
| `project` | Project to which the data belongs | opengwas, pqtl |
| `study` | The study under which the summary statistics fall | ukb-a, ukb-b,  ukb-d, believe, meta_chris_interval |
| `data_id` | Unique ID for a summary statistics record | e.g. 89f31189b3 |
| `build` | Genome build | GRCh37, GRCh38 |
| `population` | The ancestry of the population used for the study | see ancestry [here](../src/gwasstudio/config/config.yaml) |
| `total.samples` | Total sample size | nr. |
| `total.cases` | Total amount of cases | nr. |
| `total.controls` | Total amount of controls | nr. |
| `trait.desc` | A description of the trait (e.g. phenotypes or protein) associated to the summary statistics record | e.g. "Pregnancy, childbirth and the puerperium", "Alpha-1B-glycoprotein" ... |
| `notes.sex` | The sex of the individuals that participated to the study | Males and Females, Combined |
| `notes.source_id` | The source ID of the original summary statistics record | e.g. ukb-a-1, ukb-d-256, ... |

---

#### **Query file**

The query file used to retrieve (meta)data follows a structured format with two sections:

- Filtering criteria: [metadata fields](#metadata) used to query the database, specified as `metadata field: filtering value` pairs
- Output specification (`output:`): a list of valid [metadata fields](#metadata) to include in the output

##### Query file example

```
project: opengwas
study: ukb-d
category: GWAS

trait:
  - desc: Z42
  - desc: pregnancy

output:
  - build
  - population
  - notes.sex
  - notes.source_id
  - total.samples
  - total.cases
  - total.controls
  - trait.desc
```

This query file searches within the `ukb-d` study for all trait descriptions containing `Z42` or `pregnancy`, and returns a table with the columns specified in section `output:`.

**NOTES**: 

- Filtering values can include partial matches  (e.g. trait descriptions containing `Z42` or `pregnancy`)
- Filtering values are processed by lowercasing and replacing special characters before being used to query the database
- It is possible to query across different projects and studies by not specifiyng `project` and `study` in the query file

---

##### Meta-query output example

| project | study | category | data_id | build | population | notes_sex | notes_source_id | total_samples | total_cases | total_controls | trait_desc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| opengwas | ukb-d | GWAS | 47e96deafe | GRCh38 | European | Males and Females | ukb-d-XV_PREGNANCY_BIRTH | 361194 | 11959 | 349235 | "Pregnancy,  childbirth and the puerperium" |
| opengwas | ukb-d | GWAS | 531f0d4bcc | GRCh38 | European | Males and Females | ukb-d-Z42 | 361194 | 1963 | 359231 | Diagnoses - main ICD10: Z42 Follow-up care involving plastic surgery |
| opengwas | ukb-d | GWAS | cc18ce8683 | GRCh38 | European | Males and Females | ukb-d-O26 | 361194 | 1289 | 359905 | Diagnoses - main ICD10: O26 Maternal care for other conditions predominantly related to pregnancy |

---

### **3. `export`** 

The `export` command is used to extract records of summary statistics (and associated metadata) from TileDB as speficied in the query file. 

---

#### Enter compute node

The `export` command is a computanionally intensive operation. Therefore, it must be executed from a **compute node**.

To enter a compute node, run the following command:

```
salloc --partition=cpu-interactive --nodes=1 --ntasks-per-node=2 --mem-per-cpu=2048M --time=12:00:00
```

---

#### Filtering options

Exports can also be performed with different filtering options.

---

##### Region filtering

Command example to export data by filtering regions provided in [`regions_query.tsv`](../data/regions_query.tsv):

```
gwasstudio export --search-file query_ex01.txt --get-regions regions_query.tsv
```

---

##### SNP filtering

Command example to export data by filtering SNPIDs listed in [`hapmap3_snps.csv`](../data/hapmap3/hapmap3_snps.csv):

```
gwasstudio export --search-file query_ex01.txt --snp-list-file hapmap3_snps.csv
```

---

##### Locusbreaker

Command example to export data with locusbreaker:

```
gwasstudio export --search-file query_ex01.txt --locusbreaker
```

---

For a detailed explanation of all command options, see also [export command](commands.md#export).

---