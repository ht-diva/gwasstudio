# TileDB for summary statistics data

## General commands
This program works using python on the HT HPC cluster using the tiledb conda environment. This program consists of different subcommands that will be described in details below. This program makes also optionally use of Dask to accelerate the computation.


```
gwasstudio --help

Usage: gwasstudio [OPTIONS] COMMAND [ARGS]...

  GWASStudio

Dask options:
  --distribute                  Distribute the load to a Dask cluster
  --minimum_workers INTEGER     Minimum amount of running workers
  --maximum_workers INTEGER     Maximum amount of running workers
  --memory_workers TEXT         Memory amount per worker
  --cpu_workers INTEGER         CPU numbers per worker

TileDB configuration:
  --aws-access-key-id TEXT      aws access key id
  --aws-secret-access-key TEXT  aws access key
  --aws-endpoint-override TEXT  endpoint where to connect
  --aws-use-virtual-addressing TEXT
                                virtual address option
  --aws-scheme TEXT             type of scheme used at the endpoint
  --aws-region TEXT             region where the s3 bucket is located
  --aws-verify-ssl TEXT         if ssl verification is needed

Other options:
  --version                     Show the version and exit.
  -q, --quiet                   Set log verbosity
  --help                        Show this message and exit.

Commands:
  export       Exports data from a TileDB dataset.
  info         Show GWASStudio details
  ingest       Ingest data data in a TileDB-unified dataset.
  meta_ingest  Ingest metadata into a MongoDB collection.
  meta_query   query metadata records from MongoDB
  meta_view    Retrieve a dataset's metadata from MongoDB using its unique key
```
</br>

## Ingestion
To ingest new data into a TileDB use the ```ingestion``` option of the program. To check all the possible option that can be given below. Please if you decide to use this option contact first the developers Gianmauro Cuccuru and Bruno Ariano for building the metadata and checksum file prior this step

``` gwasstudio ingest --help```

<details>
  <summary>Help message</summary>

  ```
  Usage: gwasstudio ingest [OPTIONS]

  Ingest data data in a TileDB-unified dataset.

Ingestion options:
  --checksum TEXT  Path to the checksum file
  --uri TEXT       S3 path where to store the tiledb dataset.

Other options:
  --help           Show this message and exit.
  ```
</details>
</br>

### Query metadata
To query the metadata of a TileDB you need to use the command ```--meta_query```. Below we provide a coipl

``` gwasstudio meta_query --help```

<details>
  <summary>Help message</summary>

```
Usage: gwasstudio meta_query [OPTIONS]

  query metadata records from MongoDB

Options:
  --key TEXT              query key
  --values TEXT           query values, separate multiple values with ;
  --output [all|data_id]  The detail that you would like to retrieve from the
                          metadata records
  --search-file TEXT      Path to search template
  --help                  Show this message and exit.
```
</details>
</br>

### Query TileDB and export the data

There are different options to query the TileDB. Below we describe all of them with their usage
```
gwasstudio export --help
```
<details>
  <summary>Help message</summary>
```
gwasstudio export --help

Usage: gwasstudio export [OPTIONS]

  Exports data from a TileDB dataset.

TileDB mandatory options:
  --uri TEXT            TileDB dataset URI.
  --output_path TEXT    The path where you want the output.
  --trait_id_file TEXT  A file containing a list of trait id to query.
  --chr_list            A list of chromosomes to be used for the analysis.

Options for Locusbreaker:
  --locusbreaker BOOL   Activate locusbreaker
  --pvalue-sig FLOAT    Minimum P-value threshold within the window.
  --pvalue-limit FLOAT  P-value threshold for loci borders
  --hole-size INTEGER   Minimum pair-base distance between SNPs in different
                        loci (default: 250000)

Options for filtering using a genomic regions or a list of SNPs ids:
  --snp-list TEXT       A txt file with a column containing the SNP ids

Other options:
  --help                Show this message and exit.
```
</details>
</br>

### Step 2 investigate the schema of the TileDB

To get details on how the TileDB you created is made run the following command:

```
python main.py export --uri dummy_tiledb_CD14 --schema
```
<details>
  <summary>Schema of the file</summary>

```
ArraySchema(
  domain=Domain(*[
    Dim(name='cell_type', domain=('', ''), tile=None, dtype='|S0', var=True, filters=FilterList([LZ4Filter(level=5), ])),
    Dim(name='gene', domain=('', ''), tile=None, dtype='|S0', var=True, filters=FilterList([LZ4Filter(level=5), ])),
    Dim(name='position', domain=(1, 3000000000), tile=3000000000, dtype='uint32', filters=FilterList([LZ4Filter(level=5), ])),
  ]),
  attrs=[
    Attr(name='SNP', dtype='ascii', var=True, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
    Attr(name='af', dtype='float32', var=False, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
    Attr(name='beta', dtype='float32', var=False, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
    Attr(name='se', dtype='float32', var=False, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
    Attr(name='allele0', dtype='ascii', var=True, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
    Attr(name='allele1', dtype='ascii', var=True, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
    Attr(name='p-value', dtype='float64', var=False, nullable=False, enum_label=None, filters=FilterList([LZ4Filter(level=5), ])),
  ],
  cell_order='row-major',
  tile_order='row-major',
  capacity=10000,
  sparse=True,
  allows_duplicates=True,
)
```
</details>
This will print the schema of the TileDB created.
</br>

### Step 3 query and export data by a list of SNPs

Here we are showing how to query the TileDB created above for a series of SNPs within cell types

```
python main.py export --uri dummy_tiledb_CD14 --snp test_data/dummy_SNPs.txt --cell_types test_data/dummy_cell_list.txt --output_path test_out_snp_CD14
```
</br>

### Step 4 query and export data by a list of genes and cell types for all the positions

```
python main.py export --uri dummy_tiledb_CD14 --genes test_data/dummy_gene_list.txt --cell_types test_data/dummy_cell_list.txt --output_path test_out_genes_CD14
```
</br>

### Step 5 run locus-breaker

```
python main.py export --uri dummy_tiledb_CD14 --genes test_data/dummy_gene_list.txt --cell_types test_data/dummy_cell_list.txt --output_path test_out_genes_CD14
```
This will create 2 files in txt format. One containing the the limit of each clumped region and another file with all the positions included in each independent region defined by significant SNPs.
