# Commands usage

## Commands

### `export`

Export summary statistics from TileDB datasets with various filtering options.

**Usage:**

```shell
gwasstudio export [OPTIONS]
```

**TileDB options:**

- `--uri TEXT`: URI of the TileDB dataset
- `--output-prefix TEXT`: Prefix for naming output files
- `--output-format [parquet|csv.gz|csv]`: Output file format
- `--search-file TEXT`: Input file for querying metadata (required)
- `--attr TEXT`: string delimited by comma with the attributes to export (required)

**Meta-analysis options:**

- `--meta-analysis`: Option to run meta-analysis

**Locusbreaker options:**

- `--locusbreaker`: Option to run locusbreaker
- `--pvalue-sig FLOAT`: Maximum log p-value threshold within the window
- `--pvalue-limit FLOAT`: Log p-value threshold for loci borders
- `--hole-size INTEGER`: Minimum pair-base distance between SNPs in different loci
- `--maf FLOAT`: MAF filter to apply before locusbreaker
- `--phenovar`: Boolean to compute phenovariance (Work in progress, not fully implemented yet)
- `--locus-flanks INTEGER`: Flanking regions (in bp) to extend each locus in both directions

**Regions or SNP ID filtering options:**

- `--get-regions-snps TEXT`: BED (CHR\tSTART\tEND) or SNP list (CHR,POS) file paths, or string equivalents: (CHR,START,END;CHR,START,END) for regions; (CHR,POS;CHR,POS) for SNPs
- `--pvalue-filt FLOAT`: Minimum -log10(p-value) threshold to keep significant filtered SNPs
- `--skip-out`: Do not write regions output (default: False)
- `--skip-meta`: Do not add metadata columns (default: False)
- `--nest`: Estimate effective population size (Work in progress, not fully implemented yet)
- `--region-buffer INTEGER`: Extend each BED region by this many bp in both directions (default: 0, no extension). SNP rows are unaffected.

**Trait-specific lead-SNP search options:**

- `--get-regions-leadsnps TEXT`: A DataFrame containing SOURCE_ID (trait), CHR, POS, EA and NEA for lead-SNP search
- `--cis-flanks INTEGER`: Flanking region (in bp) around POS for the search of CIS lead-SNP
- `--trans-flanks INTEGER`: Flanking region (in bp) around POS for the search of TRANS lead-SNP
- `--exact-alleles`: Whether exact lead match includes also EA and NEA, or only CHR and POS (default: False)

**Multiallelic filtering options:**

- `--filter-multiallelic`: Whether to filter multiallelic loci by keeping the biallelic SNV with the highest MAF (default: False)

**P-value filtering options:**

- `--pvalue-thr FLOAT`: Minimum -log10(p-value) threshold to filter significant SNPs

**Option to plot results:**

- `--plot-out`: Boolean to plot results. If enabled, the output will be plotted as a Manhattan plot.
- `--color-thr TEXT`: Color for the points passing the threshold line in the plot
- `--s-value INTEGER`: Value for the suggestive p-value line in the plot

**Option to query metadata before export:**

- `--case-sensitive`: Perform case-sensitive matching on query values (default: False)
- `--exact-match`: Perform exact match on query values (default: False)

**Result checking:**

- `--fail-on-empty`: Exit with status `3` when the query returns no rows (default: False). Useful in pipelines to detect wrong trait IDs, builds, or out-of-store regions.

**Execution:**

- `--workers INTEGER` (global option, e.g. `gwasstudio --workers 0 export …`): Number of Dask workers. `0` runs the export in-process without starting a Dask cluster (fast path — saves a few seconds of cluster startup on small queries). Default (2): in-process automatically for a single dataset on a `local` deployment, otherwise a local Dask cluster with that many workers. Set `GWASSTUDIO_FORCE_CLUSTER=1` in the environment to disable the fast path (benchmarking / debugging).

---

### `info`

Show GWASStudio details.

**Usage:**

```shell
gwasstudio info
```

---

### `ingest`

Ingest data in a TileDB-unified dataset.

**Usage:**

```bash
gwasstudio ingest [OPTIONS]
```

**Options:**

- `--file-path TEXT`: Path to the tabular file containing details for the ingestion (required)
- `--delimiter TEXT`: Character or regex pattern to treat as the delimiter
- `--uri TEXT`: Destination path where to store the tiledb dataset. The prefix must be s3:// or file://
- `--ingestion-type [metadata|data|both]`: Choose between metadata ingestion, data ingestion, or both
- `--pvalue`: Indicate whether to ingest the p-value from the summary statistics instead of calculating it

---

### `list`

List every category → project → study hierarchy stored in the metadata DB.

**Usage:**

```shell
gwasstudio list
```

---

### `meta-query`

Query metadata records from MongoDB using GWASStudio core.

**Usage:**

```bash
gwasstudio meta-query [OPTIONS]
```

**Options:**

- `--search-file PATH`: Path to the YAML file containing search criteria (required)
- `--output-prefix TEXT`: Prefix for the output file name
- `--output-format [csv|parquet|tsv]`: Output file format
- `--case-sensitive`: Enable case-sensitive search (exact string matching)
- `--exact-match`: Enable exact match search (no regex for strings)

---
