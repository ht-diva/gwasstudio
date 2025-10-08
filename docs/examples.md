# **Examples**

The following examples assume you have already set up the GWASStudio CLI.

## **Retrieving the full list of trait descriptions from a study**

In this example, we retrieve every trait descriptions from a specific project/study.
The chosen study is "believe", which belongs to the "pqtl" project.

First, let's create a query file named `search_believe.yml` containing the following:

```yaml
project: pqtl
study: believe

output:
  - trait.desc
```

In the YAML file, we have specified the project and study that we want to query, as well as the field that we want in
the output.

Then, we can run the query using the following command:

```
gwasstudio meta-query --search-file search_believe.yml
```

You’ll see a log similar to:

```terminaloutput
2025-10-08 15:59:56.294 | INFO     | Gwasstudio started
2025-10-08 15:59:56.295 | INFO     | Processing search_believe.yml
2025-10-08 15:59:57.013 | INFO     | 7244 results found. Writing to out_meta.csv
```

The command reads the YAML file, queries the metadata database, and writes the results to `out_meta.csv` in the current
directory.
Use `--output-prefix <prefix>` to change the output file’s prefix.

## **Filtering a study by SNPs on some traits - pQTL study**

This example shows how to filter a study by a list of SNPs and retrieve the corresponding summary statistics.

The chosen study is "believe", which belongs to the "pqtl" project.

First, let's create a query file named `search_believe.yml` containing the following:

```yaml
project: pqtl
study: believe

trait:
  - seqid: 10000-28
  - seqid: 7059-14
  - seqid: 9995-6

output:
  - build
  - notes.sex
  - notes.source_id
  - trait.desc
  - total.samples
```

In the YAML file, we have specified the project and study that we want to query, as well as some specific seqid traits.

*The `trait` block limits the export to the specified `seqid` traits.*

Secondly, we need a list of SNPs that we use for filtering the data.
We can create a file named `snp_list.txt` containing the following:

```text
CHR,POS
4,29721067
5,157004314
6,167099480
7,31837152
7,140838327
8,131244697
10,25816112
11,11521474
12,70397835
21,43962980
```

Then, we can run the `export` using the following command:

```
gwasstudio export --search-file search_believe.yml --snp-list-file snp_list.txt
```

Sample log output:

```terminaloutput
2025-10-08 17:05:50.707 | INFO     | Gwasstudio started
2025-10-08 17:05:50.737 | INFO     | Processing search_believe.yml
2025-10-08 17:05:51.488 | INFO     | 3 results found. Writing to out_meta.csv
2025-10-08 17:05:53.877 | INFO     | Dask local cluster: starting 2 workers, with 4GiB of memory and 2 cpus per worker
2025-10-08 17:05:53.877 | INFO     | Dask cluster dashboard: http://127.0.0.1:8787/status
2025-10-08 17:05:53.889 | INFO     | Processing the group pqtl_believe
2025-10-08 17:05:53.890 | INFO     | Running batch 1/1 (4 items)
2025-10-08 17:06:55.436 | INFO     | Batch 1 completed.
2025-10-08 17:06:55.437 | INFO     | Shutting down Dask client and cluster.
```

The resulting summary‑statistics tables are written to the current directory.
Again, you can change the output file prefix with `--output-prefix` <prefix>.

## **Filtering a study by SNPs on some traits - GWAS study**

This example shows how to filter a study by a list of SNPs and retrieve the corresponding summary statistics.

The chosen study is "ukb-d", which belongs to the "opengwas" project.

First, let's create a query file named `search_ukb.yml` containing the following:

```yaml
project: opengwas
study: ukb-d

trait:
  - desc: heart failure

output:
  - build
  - notes.sex
  - notes.source_id
  - total.samples
  - total.cases
  - total.controls
  - trait.desc
```

By default, GWASStudio performs a case-insensitive substring search of the trait description.
This query matches four traits that contain 'heart failure' anywhere in the description.


Secondly, we need a list of SNPs that we use for filtering the data.
We can create a file named `snp_list.txt` containing the following:

```text
CHR,POS
4,29721067
5,157004314
6,167099480
7,31837152
7,140838327
8,131244697
10,25816112
11,11521474
12,70397835
21,43962980
```

Then, we can run the `export` using the following command:

```
gwasstudio export --search-file search_ukb.yml --snp-list-file snp_list.txt
```

Log snippet:

```terminaloutput
2025-10-08 18:35:36.884 | INFO     | Gwasstudio started
2025-10-08 18:35:36.925 | INFO     | Processing search_ukb.yml
2025-10-08 18:35:37.378 | INFO     | 4 results found. Writing to out_meta.csv
2025-10-08 18:35:40.466 | INFO     | Dask local cluster: starting 2 workers, with 4GiB of memory and 2 cpus per worker
2025-10-08 18:35:40.467 | INFO     | Dask cluster dashboard: http://127.0.0.1:8787/status
2025-10-08 18:35:40.532 | INFO     | Processing the group opengwas_ukb-d
2025-10-08 18:35:40.534 | INFO     | Running batch 1/1 (4 items)
2025-10-08 18:36:04.388 | INFO     | Batch 1 completed.
2025-10-08 18:36:04.389 | INFO     | Shutting down Dask client and cluster.
```

Results are written to the current directory; use `--output‑prefix` to customise the filename.
