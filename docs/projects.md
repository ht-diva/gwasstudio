# **Projects**

This is an overview of the GWASStudio projects available on-premises at Human Tecnopole, along with the relevant core metadata fields (see metadata.md#core-metadata-fields).

*Note*: All summary statistics were harmonized to the GWASLab format via [harmonization_pipeline](https://github.com/ht-diva/harmonization_pipeline.git).


| **Project name** | `category` | `project` | `study`|
| --- | --- | --- | --- |
| [deCODE](#decode) | `pQTL` | `DECODE` | `large scale plasma 2023` |
| [FinnGen](#finngen) | `GWAS` | `finngen` | `R12` |
| [Genes & Health](#genes--health) | `GWAS` | `genesandhealth` | `v010_binary_traits_3digitICD10`, `v010_quantitative_traits_median_values` |
| [UKB](#uk-biobank-ukb) | `GWAS` |  `opengwas` | `ukb-a`, `ukb-b`, `ukb-d` |
| [UKB-PPP](#uk-biobank-pharma-proteomics-project-ukb-ppp) | `pQTL` | `ukb-ppp_consortium` | `ukb-ppp_African`, `ukb-ppp_American`, `ukb-ppp_Combined`, `ukb-ppp_East_Asian`, `ukb-ppp_European`, `ukb-ppp_Middle_Eastern`, `ukb-ppp_South_Asian` |


---

## **deCODE**

The **deCODE** project is a large-scale population genetics initiative that integrates medical and genetic data from over 160,000 Icelandic participants. This includes genotype information, whole-genome sequencing, quantitative traits, binary disease phenotypes, and extensive genealogical records. Through global collaborations with medical institutions, deCODE has also incorporated data from ~500,000 individuals worldwide. A primary goal is to identify genetic risk factors for common diseases (e.g., heart attack, asthma, stroke, and cancer) and to advance personalized medicine.

**GWASStudio** includes 5,284 summary statistics from the [large-scale plasma proteomics GWAS](https://doi.org/10.1038/s41588-021-00978-w) conducted by deCODE.

For more information on the deCODE Project, see the [deCODE genetics Inc. article](https://doi.org/10.1517/phgs.4.2.209.22627).

For details on the plasma proteomics GWAS, see the [original article](https://doi.org/10.1038/s41588-021-00978-w) and the 2023 follow-up study that integrates results with data from [UKB-PPP](#uk-biobank-pharma-proteomics-project-ukb-ppp): [UKB-deCODE comparative analysis](https://doi.org/10.1038/s41586-023-06563-x).

### **Key Metadata**

* **Number of Protein Analytes**
    * 4,907 aptamers (measuring 4,719 proteins)
* **Protein Platform**
    * SomaScan multiplex aptamer assay (version 4)
* **Number of Variants**
    * ~27.2 million imputed variants
* **Imputation Panel**
    * Whole-genome sequencing (WGS on ~60,000 Icelanders) used as population-specific reference panel for genotype imputation into the larger Icelandic cohort (~160,000 individuals)
* **Genome build**
    * GRCh38
* **Sample Size and Ancestry**
    * ~36,000 Icelandic individuals (i.e. primarily Icelandic and European ancestry)
* **GWAS Method**
    * linear mixed model (BOLT-LMM)

*Note*: For detailed GWAS methodology, see the [original article](https://doi.org/10.1038/s41588-021-00978-w).

---

## **FinnGen**

**FinnGen** is a large-scale research initiative that integrates genomic data with national health registry data from 500,000 Finnish biobank participants. The project aims to identify genetic risk factors for disease, understand disease progression and treatment response, and advance personalized medicine.

**GWASStudio** includes 2,469 summary statistics from the [FinnGen GWAS R12 analysis](https://finngen.gitbook.io/documentation/methods/phewas).

For more information on the FinnGen Project and methods, see the [FinnGen documentation](https://finngen.gitbook.io/documentation) and [FinnGen Handbook](https://docs.finngen.fi/).

To browse FinnGen traits (including trait definition, prevalence, and longitudinal comorbidities), see the [Risteys portal](https://risteys.finngen.fi/).

### **Key Metadata**

* **Number of Traits**
    * 2,469
* **Trait Types**
    * 2,466 binary traits
    * 3 quantitative traits (`HEIGHT_IRN`, `WEIGHT_IRN`, `BMI_IRN`)
* **Number of Variants**
    * ~21 million imputed variants
* **Imputation Panel**
    * [Sisu v4.2 reference panel](https://docs.finngen.fi/finngen-data-specifics/red-library-data-individual-level-data/genotype-data/imputation-panel/sisu-v4.2-reference-panel) (Finnish-specific reference panel combining high-coverage WGS and WES)
* **Genome build**
    * GRCh38
* **Sample Size and Ancestry**
    * 500,348 individuals of primarily Finnish/European ancestry
* **GWAS Method**
    * regenie v2.2.4 (v3.3 used for traits not converging under v2.2.4)

*Note*: For detailed GWAS methodology, see the [FinnGen documentation](https://finngen.gitbook.io/documentation/methods/phewas).

---

## **Genes & Health**

**Genes & Health** is a community‑based genetic research initiative, focusing on British Bangladeshi and British Pakistani communities, with over 100,000 participants. The project aims to reduce health disparities in these groups (e.g. in diabetes, cardiovascular diseases, and mental health disorders) by researching genetic risk factors, disease progression, treatment response, and supporting more inclusive precision medicine.

**GWASStudio** includes 762 binary‑trait summary statistics and 107 quantitative‑trait summary statistics from Genes & Health (2025 Realease).

For more information on the Genes & Health project, data access, and methods, see the Genes & Health ["Data & Researchers"](https://genesandhealth.org/researchers/data/) page and the [project website](https://genesandhealth.org/).

### **Key Metadata**

* **Number of Traits**
    * `v010_binary_traits_3digitICD10`: 762 binary traits
    * `v010_quantitative_traits_median_values`: 107 quantitative traits (median values)
* **Trait Types**
    * `v010_binary_traits_3digitICD10`: ICD10-coded binary traits (e.g. A01–Q99)
    * `v010_quantitative_traits_median_values`: quantitative (e.g. BMI, LDL-cholesterol)
* **Number of Variants**
    * ~37.5 million imputed variants
* **Imputation Panel**
    * TOPMed-r3
* **Genome build**
    * GRCh38
* **Sample Size and Ancestry**
    * ~100,000 participants of British Bangladeshi and British Pakistani (South Asian) ancestry
* **GWAS Method**
    * regenie

*Note*: For quantitative traits, Genes & Health provides 321 quantitative‑trait summary statistics derived from minimum, median, and maximum observed values per individual. For consistency and to reduce variability caused by extreme measurements or outliers, median values were selected to represent each quantitative trait in the GWAS analyses included in GWASStudio.

*Note*: For more information on the phenotypes and notation, see the [dedicated page](https://www.genesandhealth.org/researchers/healthdataphenotypes/) on the project website.

---

## **UK Biobank (UKB)**

**UK Biobank (UKB)** is a large-scale biomedical database and research resource containing detailed health, lifestyle, environmental, and genetic data from approximately 500,000 UK participants aged 40–69 at recruitment (2006–2010). Participants have been followed longitudinally through linked electronic health records, imaging, biomarker assays, and repeat assessments.

A wide range of GWAS analyses have been conducted on UKB phenotypes by multiple research groups, often focusing on different trait types, QC pipelines, or analysis methods. **GWASStudio** includes results from three major sources, for a total of 3,945 UKB-based GWAS summary statistics:

* `ukb-a`: Neale Lab Phase 1 Release (2017) — early GWAS on core phenotype (596)
* `ukb-b`: MRC-IEU OpenGWAS project (2021+) — harmonized and quality-controlled GWAS across thousands of traits (2,514)
* `ukb-d`: Neale Lab Phase 2/3 Release (2018) — expanded GWAS with improved imputation, additional phenotypes, and updated QC pipelines (835)

For more information on the UK Biobank project, see [https://www.ukbiobank.ac.uk/](https://www.ukbiobank.ac.uk/) and [UKB Data Showcase](https://biobank.ndph.ox.ac.uk/showcase/), which provides searchable metadata on all available phenotypes and samples.

### **Key Metadata**

* **Number of Traits**
    * `ukb-a`: 596
    * `ukb-b`: 2,514
    * `ukb-d`: 835
* **Trait Types**
    * `ukb-a` and `ukb-d`: binary (disease case-control), continuous (physical and lab measures)
    * `ukb-b`: binary, continuous, categorical ordered (e.g. job satisfaction)
* **Number of Variants**
    * All studies begin with ~17 million imputed variants
    * `ukb-a`: filtered to ~10.5 million
    * `ukb-b`: filtered to ~10 million
    * `ukb-d`: filtered to ~13.5 million
* **Imputation Panel**
    * UK Biobank Imputation Release v3 (R3)
* **Genome build**
    * GRCh38 (lifted via [GWASLab liftover method](https://cloufield.github.io/gwaslab/LiftOver/))
* **Sample Size and Ancestry**
    * `ukb-a`: ~337,000 unrelated White British individuals
    * `ukb-b`: ~460,000 full UKB cohort (mostly European), including related individuals
    * `ukb-d`: ~361,000 White British individuals, including related individuals
* **GWAS Method**
    * `ukb-a` and `ukb-d`: linear regression (Hail, `linreg3`)
    * `ukb-b`: linear mixed model (BOLT-LMM or SAIGE, depending on the trait)

*Note*: For detailed GWAS methodology, see:

* `ukb-a` and `ukb-d`: [GitHub repo](https://github.com/Nealelab/UK_Biobank_GWAS) and [Blog](https://www.nealelab.is/blog/2017/9/11/details-and-considerations-of-the-uk-biobank-gwas)
* `ukb-b`: [GitHub repo](https://github.com/MRCIEU/UKBiobankGWAS) and [Documentation](https://data.bris.ac.uk/data/dataset/1ovaau5sxunp2cv8rcy88688v)

---

## **UK Biobank Pharma Proteomics Project (UKB-PPP)**

**UK Biobank Pharma Proteomics Project (UKB-PPP)** is a biopharmaceutical consortium that performed plasma proteomic profiling of 54,219 UK Biobank participants. Protein quantitative trait loci (pQTL) analyses were conducted across European individuals and five additional non-European ancestry groups.

**GWASStudio** includes a total of 20,576 UKB-PPP GWAS summary statistics.

For more information on the UKB-PPP project, see the original [UKB-PPP article](https://doi.org/10.1038/s41586-023-06592-6).

### **Key Metadata**

* **Number of Protein Analytes**:
    * 2,941 protein analytes (2,943 unique proteins)
* **Protein Panel**:
    * [Olink Explore 3072 Panel](https://olinkpanel.creative-proteomics.com/table-list-of-olink-explore-3072-8-panel-combination.html)
* **Number of Variants**:
    * ~23.8 million imputed variants
* **Genome build**:
    * GRCh38
* **Sample Size and Ancestry**:
    * `ukb-ppp_European`: 34,557 European individuals
    * `ukb-ppp_African`: 931 African individuals
    * `ukb-ppp_South_Asian`: 920 Central South Asian individuals
    * `ukb-ppp_Middle_Eastern`: 308 Middle Eastern individuals
    * `ukb-ppp_East_Asian`: 262 East Asian individuals
    * `ukb-ppp_American`: 97 Admixed American (mostly Hispanic/Latino groups)
    * `ukb-ppp_Combined`: 52,363 individuals in total — the combined UKB-PPP cohort includes all above groups plus 17,806 additional European individuals from the replication cohort. The combined cohort results from QC filtering starting from 54,219 initial participants selected by the UKB-PPP consortium
* **GWAS Method**
    * regenie v2.2.1

*Note*: Ancestry groups in UKB-PPP are defined following the [pan-UKBB](https://pan.ukbb.broadinstitute.org/) framework, based on clustering individuals in principal component (PC) space using reference populations from the [1000 Genomes Project](https://www.internationalgenome.org/) and the Human Genome Diversity Panel. For more information, see the [pan-UKBB study design](https://pan-dev.ukbb.broadinstitute.org/docs/study-design).

*Note*: For detailed GWAS methodology, see the original [UKB-PPP article](https://doi.org/10.1038/s41586-023-06592-6).
