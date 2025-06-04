# Usage documentation

- [Usage documentation](#usage-documentation)
  - [1. Ingestion](#1-ingestion)
    - [1.1 Input](#11-input)
      - [1.1.1 Summary statistics preparation](#111-summary-statistics-preparation)
      - [1.1.2 Metadata preparation](#112-metadata-preparation)
    - [1.2 Command](#12-command)
  - [2. Export](#2-export)
  
## 1. Ingestion

### 1.1 Input

GWASSTUDIO requires 2 inputs, a series of harmonised summary statistics in tsv.gz format and a table with the metadata and the path where the summary statistics are stored.

#### 1.1.1 Summary statistics preparation

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

To harmonize and format data in the appropriate way please refer to this other repo (https://github.com/ht-diva/sumstats_load_pipeline_in_tiledb/tree/main)


#### 1.1.2 Metadata preparation

The ingestion of summary statistics data need always to be accompanyed by its relative metadata. Below are the mandatory and optional fields of the metadata table together with an example of metadata table

Mandatory columns

| Column | Description |
| --- | --- |
| `project` | Project to which the data belongs (example UKB, GTEx, UKB-PPP, etc...) |
| `study` | The study under which the summary statistics fall. Example (a, b, d, which are groups of UK Biobank studies withing Open GWAS) |
| `file_path` | The path where the files are stored |
| `category` | The type of summary staitsics to be stored (GWAS, QTL) |

Optional columns

| Column | Description |
| --- | --- |
| `notes_sex` | The sex of the individuals that participated to that study (example, Males and Females)|
| `population` | The ancestry of the population used for the study |
| `total_samples` | Total sample size |
| `total_cases` | Total amount of cases for binary traits|
| `total_controls` | Total amount of control for binary traits |
| `trait_desc` | A description of the summary statistics study |
| `build` | Builds of the summary statistics (example, hg38, hg19, GRCh38, GRCh37, etc....) |
| `notes_source_id` | The source ID from the original summary statistics (e.g. ukb-d-256, etc...) |

```
Example of table for metadata input.
```

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

### 1.2 Command

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

```

cd data

gwasstudio ingest --file-path metadata_ukb_d_sampled.tsv --uri destination 

```

<br>

Once you run the command above you should see a folder called destination with the tiledb storing the summary statistics data.

## 2. Export