# Commands usage

## Commands

### `export`

 Export summary statistics from TileDB datasets with various filtering options.

**Usage:**

```shell
gwasstudio export [OPTIONS]
```

**Options:**

- `--uri TEXT`: URI of the TileDB dataset.
- `--output-prefix TEXT`: Prefix for naming output files (default: `out`).
- `--output-format [parquet|csv.gz|csv]`: Output file format (default: `csv.gz`).
- `--search-file TEXT`: Input file for querying metadata (required).
- `--attr TEXT`: String delimited by comma with the attributes to export (default: `BETA,SE,EAF,MLOG10P`).


**Locusbreaker Options:**

- `--locusbreaker`: Option to run locusbreaker (flag).
- `--pvalue-sig FLOAT`: Maximum log p-value threshold within the window (default: `5.0`).
- `--pvalue-limit FLOAT`: Log p-value threshold for loci borders (default: `3.3`).
- `--hole-size INTEGER`: Minimum pair-base distance between SNPs in different loci (default: `250000`).

**SNP ID List Filtering Options:**

- `--snp-list-file TEXT`: A txt file which must include CHR and POS columns.

**Plotting Options:**

- `--plot-out`: Boolean to plot results. If enabled, the output will be plotted as a Manhattan plot (flag).
- `--color-thr TEXT`: Color for the points passing the threshold line in the plot (default: `red`).
- `--s-value INTEGER`: Value for the suggestive p-value line in the plot (default: `5`).

**Regions Filtering Options:**

- `--get-regions TEXT`: Bed file with regions to filter.
- `--maf FLOAT`: MAF filter to apply to each region (default: `0.01`).
- `--phenovar`: Boolean to compute phenovariance (Work in progress, not fully implemented yet) (flag).
- `--nest`: Estimate effective population size (Work in progress, not fully implemented yet) (flag).

---

### `info`

Show GWASStudio details

**Usage:**

```shell
gwasstudio info
```

---

### `ingest`

Ingest data in TileDB datasets.

**Usage:**

```bash
gwasstudio ingest [OPTIONS]
```

**Options:**

- `--file-path TEXT`: Path to the tabular file containing details for the ingestion (required).
- `--delimiter TEXT`: Character or regex pattern to treat as the delimiter (default: `\t`).
- `--uri TEXT`: Destination path where to store the tiledb dataset. The prefix can be `s3://` or `file://` (required).
- `--ingestion-type [metadata|data|both]`: Choose between metadata ingestion, data ingestion, or both (default: `both`).
- `--pvalue`: Indicate whether to ingest the p-value from the summary statistics instead of calculating it (default: `True`).

---

### `list`

List every category → project → study hierarchy stored in the metadata DB

**Usage:**

```shell
gwasstudio list
```

---

### `meta-query`

Query metadata records from MongoDB

**Usage:**

```bash
gwasstudio meta-query [OPTIONS]
```

**Options:**

- `--search-file`: The search file used for querying metadata  [required]
- `--output-prefix`: Prefix to be used for naming the output files
- `--case-sensitive`: Enable case sensitive search

---
