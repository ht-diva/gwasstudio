# Commands usage

## Commands

### `cluster`

Manage Dask cluster lifecycle independently from gwasstudio operations. This allows you to start a cluster once and reuse it across multiple commands (ingest, export, query), avoiding repeated cluster creation and teardown overhead.

**Usage:**

```shell
gwasstudio cluster [OPTIONS] COMMAND [ARGS]...
```

**Cluster options:**

- `-n, --name TEXT`: Cluster profile name (default: 'default')

**Commands:**

#### `start`

Start a persistent Dask cluster with the specified configuration.

**Usage:**

```shell
gwasstudio cluster start [OPTIONS]
```

**Options:**

- `--deployment [local|gateway|slurm]`: Deployment type (required)
- `--workers INTEGER`: Number of workers to start (default: 2)
- `--cores-per-worker INTEGER`: CPU cores per worker (default: 2)
- `--memory-per-worker TEXT`: Memory per worker (e.g., 36GiB) (default: 4GiB)
- `--gw-address TEXT`: Dask Gateway address (required for gateway deployment)
- `--gw-image TEXT`: Dask Gateway worker image
- `--interface TEXT`: Network interface for workers (e.g., ib0)
- `--local-directory TEXT`: Fast local directory for Dask workers
- `--walltime TEXT`: Walltime for each worker (SLURM only, default: 12:00:00)
- `--python TEXT`: Python executable for Dask workers
- `--job-script-prologue TEXT`: Commands to add to SLURM script before launching workers

**Important Note:**

> **Local deployment does not work with persistent cluster management.** Local clusters run in the same process as the CLI command, so they are terminated when the command exits. Use `cluster start` only with **SLURM** or **Gateway** deployments. For local deployment, use the automatic cluster management (default behavior without `--use-existing-cluster`).

**Examples:**

Start a SLURM cluster with 8 workers:

```shell
gwasstudio cluster --name slurm-cluster start --deployment slurm --workers 8 --walltime 24:00:00
```

Start a Gateway cluster:

```shell
gwasstudio cluster --name gw-cluster start --deployment gateway --gw-address dask-gateway.example.com:8000 --gw-image my-dask-image:latest
```

#### `stop`

Stop a running Dask cluster and clean up its state.

**Usage:**

```shell
gwasstudio cluster stop [OPTIONS]
```

**Options:**

- `--force, -f`: Force stop and cleanup state files even if error occurs

**Examples:**

```shell
gwasstudio cluster --name mycluster stop
gwasstudio cluster --name mycluster stop --force
```

#### `status`

Show detailed status information for a cluster.

**Usage:**

```shell
gwasstudio cluster status [OPTIONS]
```

**Examples:**

```shell
gwasstudio cluster --name mycluster status
```

**Output includes:**

- Cluster name
- Status (RUNNING/STOPPED)
- Deployment type
- Dashboard URL
- Number of workers
- Creation timestamp
- Worker addresses

#### `list`

List all saved cluster profiles and their status.

**Usage:**

```shell
gwasstudio cluster list
```

**Output:** Table showing name, deployment, status, worker count, and creation time for all cluster profiles.

#### `cleanup`

Remove stale cluster state files for clusters that are no longer running.

**Usage:**

```shell
gwasstudio cluster cleanup [OPTIONS]
```

**Options:**

- `--all, -a`: Remove all cluster state files (not just stale ones)

**Examples:**

```shell
# Clean up stale state for a specific cluster
gwasstudio cluster --name mycluster cleanup

# Clean up all cluster state files
gwasstudio cluster cleanup --all
```

---

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

**Cluster options:**

- `--use-existing-cluster`: Use an existing cluster instead of creating a new one. Requires a running cluster (started via 'gwasstudio cluster start').
- `--cluster-name TEXT`: Name of the cluster to use when --use-existing-cluster is enabled (default: 'default')

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

**Cluster options:**

- `--use-existing-cluster`: Use an existing cluster instead of creating a new one. Requires a running cluster (started via 'gwasstudio cluster start').
- `--cluster-name TEXT`: Name of the cluster to use when --use-existing-cluster is enabled (default: 'default')

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
