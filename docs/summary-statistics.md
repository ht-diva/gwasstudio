# Summary Statistics

GWASStudio requires summary-statistics files(s) to be harmonized and formatted in tsv.gz.

---

## Core Summary Statistics columns

| Column | Description |
| --- | --- |
| `CHR` | Chromomsome in integer format |
| `POS` | Position of the variant |
| `EA` | Effect allele |
| `NEA` | Non-effect alle |
| `EAF` | Effect allele frequency |
| `BETA` | Effect size |
| `SE` | Standard error |


---

## Additional optional columns

| Column | Description |
| --- | --- |
| `Z` | Z-score |
| `MLOG10P` | p-value in -log10 format |
| `rsID` | SNP ID following dbSNP convention |
| `SNPID` | SNPID described as CHR:POS:A1:A2 with A1 and A2 being the alleles in alphabetical order |
| `N` | Sample size |
| `STATUS` | Sample size |


---

## Note

To harmonize and format data in the appropriate way, please refer to the [harmonization_pipeline](https://github.com/ht-diva/harmonization_pipeline).