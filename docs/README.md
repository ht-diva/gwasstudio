# Usage documentation

- [Usage documentation](#usage-documentation)
- [1. Ingestion](#1-ingestion)
  - [1.1 Input](#11-input)
    - [1.1.1 Summary statistics preparation](#111-summary-statistics-preparation)
    - [1.1.2 Metadata preparation](#112-metadata-preparation)
  - [1.2 Command](#12-command)
- [2. Export](#2-export)
  - [2.1 Input](#21-input)
    - [2.1.1 Query file](#211-query-file)
  - [2.2 Command](#22-command)
  - [2.3 Output](#23-output)
    - [2.3.1 SNP, region, all selection](#231-snp-region-all-selection)
    - [2.3.2 Locusbreaker](#232-locusbreaker)
    - [2.3.1 Metadata](#231-metadata)
- [3. Meta-query](#3-meta-query)
  - [3.1 Input](#31-input)
  - [3.2 Command](#32-command)
  - [3.3 Output](#33-output)
- [4. Deployment](#4-deployment)
  - [4.1 Dask](#41-dask)
    - [4.1.1 Local](#411-local)
    - [4.1.2 Slurm](#412-slurm)
  - [4.2 MongoDB](#42-mongodb)
  - [4.3 Vault](#43-vault)
  - [4.4 MinIO](#44-minio)

# 1. Ingestion

## 1.1 Input

GWASStudio requires 2 inputs, a series of harmonised summary statistics in tsv.gz format and a table with the metadata and the path where the summary statistics are stored.

### 1.1.1 Summary statistics preparation

Summary statistics needs to be formatted in tsv.gz with the foillowing mandatory and optional columns:

Mandatory columns

| Column | Description |
| --- | --- |
| `CHR` | Chromomsome in integer format |
| `POS` | Position of the variant |
| `EA` | Effect allele |
| `NEA` | Non effect alle |
| `EAF` | Effect allele frequency |
| `BETA` | Effect size |
| `SE` | Standard error |

Optional columns

| Column | Description |
| --- | --- |
| `Z` | Z-score |
| `MLOG10P` | p-value in -log10 format |
| `rsID` | SNP ID following dbSNP convention |
| `SNPID` | SNPID described as CHR:POS:A1:A2 with A1 and A2 being the alleles in alphabetical order|
| `N` | Sample size |
| `STATUS` | Sample size |


An example of gwas with the required column is shown below

<details>
  <summary> data/ukb-d-XIII_MUSCULOSKELET.gwaslab.tsv.sampled </summary>

  ```

  BETA    POS     EAF     STATUS  NEA     N       SE      SNPID   rsID    Z       MLOG10P EA      CHR
-0.006605340    55353555        0.99485 3850019 T       361194  0.006997750     3:55353555:C:T  rs137963191     -0.9439 0.46192 C       3
0.001314370     91448043        0.12102 3850099 G       361194  0.001471490     14:91448043:A:G rs10140425      0.8932  0.42976 A       14
0.002021300     236497326       0.04092 3850099 T       361194  0.002440150     1:236497326:A:T rs117083672     0.8284  0.38990 A       1
0.000595666     93685520        0.81739 3850019 C       361194  0.001240620     9:93685520:A:C  rs12351976      0.4801  0.19988 A       9
0.002929680     43207314        0.96716 3870319 C       361194  0.002694540     22:43207314:CCAAA:C     22_43603320_CCAAA_C     1.0873  0.55765 CCAAA   22
-0.000095110    123260687       0.75512 3850019 T       361194  0.001116510     10:123260687:C:T        rs17587364      -0.0852 0.03053 C       10
-0.000808090    4550458 0.77164 3850019 C       361194  0.001143620     2:4550458:A:C   rs1351242       -0.7066 0.31893 A       2
-0.000689003    113936998       0.46018 3850019 T       361194  0.000961501     9:113936998:C:T rs1888161       -0.7166 0.32456 C       9
0.003135670     202384532       0.96145 3850019 T       361194  0.002567760     2:202384532:C:T rs552928178     1.2212  0.65360 C       2

```
</details>

<br>

To harmonize and format data in the appropriate way please refer to this other repo (https://github.com/ht-diva/harmonization_pipeline)

### 1.1.2 Metadata preparation

The ingestion of summary statistics data need always to be accompanyed by its relative metadata. Below are the mandatory and optional fields of the metadata table together with an example of metadata table

Mandatory columns

| Column | Description |
| --- | --- |
| `category` | The type of summary staitsics to be stored (GWAS, QTL) |
| `file_path` | The path where the files are stored |
| `project` | Project to which the data belongs (e.g. UKB, GTEx, UKB-PPP, etc...) |
| `study` | The study under which the summary statistics fall. Example (a, b, d, which are groups of UK Biobank studies withing Open GWAS) |

Optional columns

| Column | Description |
| --- | --- |
| `build` | Builds of the summary statistics in cas is not GRCh38 (GRCh37) |
| `notes_source_id` | The source ID from the original summary statistics (e.g. ukb-d-256, etc...) |
| `notes_sex` | The sex of the individuals that participated to that study (e.g., Males and Females)|
| `population` | The ancestry of the population used for the study |
| `total_samples` | Total sample size |
| `total_cases` | Total amount of cases for binary traits|
| `total_controls` | Total amount of control for binary traits |
| `trait_desc` | A description of the summary statistics study |

<br>

Example of table for metadata input.

<br>


<details>
  <summary> data/metadata_ukb_d_sampled.tsv </summary>

```

project study   category        file_path       notes_sex       notes_source_id build   population      total_samples   total_cases     total_controls  trait_desc
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-XVIII_MISCFINDINGS.gwaslab.tsv.sampled.gz Males and Females       ukb-d-XVIII_MISCFINDINGS        GRCh37  European        361194  97602   263592  Symptoms, signs and abnormal clinical and laboratory findings, not elsewhere classified
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-XII_SKIN_SUBCUTAN.gwaslab.tsv.sampled.gz  Males and Females       ukb-d-XII_SKIN_SUBCUTAN GRCh37  European        361194  27074   334120  Diseases of the skin and subcutaneous tissue
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-XV_PREGNANCY_BIRTH.gwaslab.tsv.sampled.gz Males and Females       ukb-d-XV_PREGNANCY_BIRTH        GRCh37  European        361194  11959   349235  Pregnancy, childbirth and the puerperium
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-X_RESPIRATORY.gwaslab.tsv.sampled.gz      Males and Females       ukb-d-X_RESPIRATORY     GRCh37  European        361194  25381   335813  Diseases of the respiratory system
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-Z01.gwaslab.tsv.sampled.gz        Males and Females       ukb-d-Z01       GRCh37  European        361194  1370    359824  Diagnoses - main ICD10: Z01 Other special examinations and investigations of persons without complaint or reported diagnosis
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-XVII_MALFORMAT_ABNORMAL.gwaslab.tsv.sampled.gz    Males and Females       ukb-d-XVII_MALFORMAT_ABNORMAL   GRCh37  European        361194  2121    359073  Congenital malformations, deformations and chromosomal abnormalities
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-XIII_MUSCULOSKELET.gwaslab.tsv.sampled.gz Males and Females       ukb-d-XIII_MUSCULOSKELET        GRCh37  European        361194  77099   284095  Diseases of the musculoskeletal system and connective tissue
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-Z42.gwaslab.tsv.sampled.gz        Males and Females       ukb-d-Z42       GRCh37  European        361194  1963    359231  Diagnoses - main ICD10: Z42 Follow-up care involving plastic surgery
UKB     d       GWAS    ./ukb-d_sampled/ukb-d-XI_DIGESTIVE.gwaslab.tsv.sampled.gz       Males and Females       ukb-d-XI_DIGESTIVE      GRCh37  European        361194  115893  245301  Diseases of the digestive system

```
</details>

## 1.2 Command

The ingestion is done using the command ```gwasstudio ingest```. To check all the possible option that can be given below.

<details>
  <summary>gwasstudio ingest --help</summary>

  ```                                                                                                               [±main ●●]
Usage: gwasstudio ingest [OPTIONS]

  Ingest data in a TileDB-unified dataset.

Ingestion options:
  --file-path TEXT  Path to the tabular file containing details for the
                    ingestion  [required]
  --delimiter TEXT  Character or regex pattern to treat as the delimiter.
  --uri TEXT        Destination path where to store the tiledb dataset. The
                    prefix must be s3:// or file://
  --ingestion-type [metadata|data|both]
                    Choose between metadata ingestion, data ingestion, or both.
  --pvalue          Indicate whether to ingest the p-value from the summary
                    statistics instead of calculating it (Default: True).

Other options:
  --help            Show this message and exit.
                                                        
```
</details>

<br>

Example of ingestion using test data:

```bash

cd data

gwasstudio ingest --file-path metadata_ukb_d_sampled.tsv --uri destination 

```

<br>

Once you run the command above you should see a folder called destination with the tiledb storing the summary statistics data.

# 2. Export

## 2.1 Input

GWASStudio requires as input a query file containing information about the studies to retrieve.

### 2.1.1 Query file

The query can be done on any of the information used during the ingestion of the data. Below there is an example of query file

<details>
  <summary> data/search_ukb_d.txt </summary>

  ```

project: UKB
study: d
category: GWAS

trait:
  - desc: skin and subcutaneous tissue
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
</details>

<br>

## 2.2 Command

The export is done using the command ```gwasstudio export```. The following type of options are available

| Options | Description |
| --- | --- |
| `--uri` | Given a BED file tab separated with CHR,START,END it will retrieve all these regions from all the traits [required] |
| `--maf` | Minor allele frequency to filter the summary statistics |
| `--attr` | A string defining the columns to get as results from the query (e.g. MLOG10P, EAF, EA, NEA) |
| `--search-file` | A file containing the attributes filter to search a study [required] |
| `--snp-list-file` | A list of SNPs stored in a file with column CHR POS tab separated. |
| `--get-regions` | Bed file with regions to filter with column CHR POS tab separated. |
| `--export-all` | Export the entire summary statistics data |
| `--locusbreaker` | It run the locusbreaker program to divide a summary statistics in independent genomic regions according to their p-value  |
| `--pvalue-sig` | Maximum log10 p-value threshold within the window |
| `--pvalue-limit` | Log10 p-value threshold for loci borders  |
| `--hole-size` | Minimum pair-base distance between SNPs in different loci (default: 250000) |

<br>

Example of command

<br>

``` bash

gwasstudio export --search-file search_ukb_d.txt --attr NEA,EA,EAF --maf 0.01 --uri destination --snp-list-file hapmap3/hapmap3_snps.csv

```

## 2.3 Output

After your export command has finished to run you will have different results depending on which option you selected:

### 2.3.1 SNP, region, all selection

When you choose the ```--snp-list-file``` or the ```--get-regions``` ```--export-all``` options you will get as results a file in parquet format for each trait you interrogated with the SNPs or the reigions or the entire summary statistics information.

### 2.3.2 Locusbreaker

When you decide to  run ```--locusbreaker``` you will get 2 files, a segment file and an interval file for each trait interrogated. The segment file contains for each independent genomic region selected the SNPs with the highest MLOG10P value. The interval file instead contains also the information about all the SNPs present in the region. 

### 2.3.1 Metadata

After you run each of the options above you will always get a metadata file with information about the traits. An example of metadata is shown below:


project,study,category,data_id,build,population,notes.sex,notes.source_id,total.samples,total.cases,total.controls,trait.desc
UKB,d,GWAS,95386150a6,GRCh37,European,Males and Females,ukb-d-XII_SKIN_SUBCUTAN,361194,27074,334120,Diseases of the skin and subcutaneous tissue
UKB,d,GWAS,f588175d46,GRCh37,European,Males and Females,ukb-d-Z42,361194,1963,359231,Diagnoses - main ICD10: Z42 Follow-up care involving plastic surgery
UKB,d,GWAS,3440ed14ff,GRCh37,European,Males and Females,ukb-d-XV_PREGNANCY_BIRTH,361194,11959,349235,"Pregnancy, childbirth and the puerperium"


# 3. Meta-query

The command ```meta-query``` give to the user the option of getting only information from the metadata without running anything on the data itself

## 3.1 Input

Similarly to here [See query file](#211-query-file) this command requires a search file.

## 3.2 Command

The meta-query command require the following options

| Options | Description |
| --- | --- |
| `--search-file` [See query file](#211-query-file) [required] |
| `--output-prefix` | Prefix of the file to store the metadata  |
| `--case-sensitive` | Enable case sensitive search |

<br>

Example of command

<br>

``` bash

cd data

gwasstudio meta-query --search-file search_ukb_d.txt --uri destination

```

## 3.3 Output

The output is a table of metadata similar to what shown in 

<details>
  <summary> data/search_ukb_d.txt </summary>

```
  project,study,category,data_id,build,population,notes.sex,notes.source_id,total.samples,total.cases,total.controls,trait.desc
UKB,d,GWAS,95386150a6,GRCh37,European,Males and Females,ukb-d-XII_SKIN_SUBCUTAN,361194,27074,334120,Diseases of the skin and subcutaneous tissue
UKB,d,GWAS,f588175d46,GRCh37,European,Males and Females,ukb-d-Z42,361194,1963,359231,Diagnoses - main ICD10: Z42 Follow-up care involving plastic surgery
UKB,d,GWAS,3440ed14ff,GRCh37,European,Males and Females,ukb-d-XV_PREGNANCY_BIRTH,361194,11959,349235,"Pregnancy, childbirth and the puerperium"

```
  
</details>


<br>

# 4. Deployment

## 4.1 Dask

In order to parallelize the computation GWASStudio uses Dask. Dask can be deployed in 3 different ways using the option ``` --dask-deployment ```:

| Deployment | Description |
| --- | --- |
| `local` | A Dask creater is created on your local machine |
| `slurm` | In case you are using a HPC with Slurm you can Dask taking care of distributing the jobs across workers with Slurm |
| `gateway` | This is a more advanced option in case you have already a Dask cluster running some (eg a Kubernetes) and you have to give the address where the cluster is available |

### 4.1.1 Local

For local deployment (for example your laptio or a single node on a HPC) your CPUs can be divided in independent workers, each with a certain amount of virtual CPU available. the following options can be used:

| Local options | Description |
| --- | --- |
| `--local-workers` | Number of workers for local cluster |
| `--local-threads` | Threads per worker for local cluster |
| `--local-memory` | Memory per worker for local cluster |

### 4.1.2 Slurm

For slurm deployment instead Dask will interact directly with the Slurm job manager and distribute the workloads. Only for non-local deployment you can dustribute the workloads on a flexible amount of workers depending on how much resources are needed.  

| Slurm options | Description |
| --- | --- |
| `--minimum-workers` | Minimum amount of running workers |
| `--maximum-workers` | Maximum amount of running workers |
| `--memory-workers` | Memory amount per worker in GB |
| `--cpu-workers` | CPU numbers per worker |
| `--address` | Dask gateway address |

## 4.2 MongoDB

GWASStudio store the metadata information in a MongoDB. When you want to deploy mongodb on a different address that is not localhost you can use the option ``` --mongo-uri ```

## 4.3 Vault

## 4.4 MinIO