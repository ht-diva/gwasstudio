# **Projects**

Overview of GWASStudio projects.

| | `project` | `study`|
| --- | --- | --- |
| [UKB](#uk-biobank-ukb) | `opengwas` | `ukb-a`, `ukb-b`, `ukb-d` |
| [UKB-PPP](#uk-biobank-pharma-proteomics-project-ukb-ppp) | `ukb-ppp_consortium` | `ukb-ppp_African`, `ukb-ppp_American`, `ukb-ppp_Combined`, `ukb-ppp_East_Asian`, `ukb-ppp_European`, `ukb-ppp_Middle_Eastern`, `ukb-ppp_South_Asian` |

---

## **UK Biobank (UKB)**

The **UK Biobank (UKB)** is a large-scale biomedical database and research resource containing genetic, lifestyle, and health information from ca. 500,000 UK participants. For more information, see [https://www.ukbiobank.ac.uk/](https://www.ukbiobank.ac.uk/) and [UKB Showcase](https://biobank.ndph.ox.ac.uk/showcase/).

GWAS studies were performed on UKB phenotypes by different research groups:

* `ukb-a`: Neale Lab Phase 1 release (2017)
* `ukb-b`: MRC-IEU OpenGWAS project (2021+)
* `ukb-d`: Neale Lab Phase 2/3 release with expanded traits and improved QC (2018)

Raw (non-harmonized) GWAS Summary Statistics are available at [https://gwas.mrcieu.ac.uk/](https://gwas.mrcieu.ac.uk/).

### **Key Metadata**

* **Number of Traits**: 
    * `ukb-a`: 596
    * `ukb-b`: 2,514
    * `ukb-b`: 835
* **Trait Types**:
    * `ukb-a` and `ukb-b`: Binary (disease case-control), Continuous (physical and lab measures)
    * `ukb-b`: Binary, Continuous, Categorical ordered (e.g. job satisfaction)
* **Number of Variants**: 
    * All studies begin with ~17 million imputed variants
    * `ukb-a`: filtered to ~10.55M SNPs per trait
    * `ukb-b`: filtered to 9.85M SNPs, ranging from ~1M to ~9M SNPs per trait
    * `ukb-b`: filtered to 13.5M SNPs, ranging from ~9M to ~13.5M SNPs per trait
* **Imputation Panel** is UK Biobank Imputation Release v3 (R3):
    * Haplotype Reference Consortium (HRC v1.1) plus...
    * Merged UK10K + 1000 Genomes Phase 3 panel ([link](https://ega-archive.org/studies/EGAS00001000713) to panel)
* **Genome build**: 
    * GRCh37 in raw
    * lifted to GRCh38 via [GWASLab liftover method](https://cloufield.github.io/gwaslab/LiftOver/)
* **Sample Size and Ancestry**: 
    * `ukb-a`: ~337,000 unrelated White British individuals
    * `ukb-b`: ~460,000 full UKB cohort (mostly European), including related individuals
    * `ukb-b`: ~361,000 White British individuals, including related individuals

*Note*: Actual sample size varies per trait depending on data availability, phenotype type, and missingness.

### **GWAS Pipelines Overview**

Raw GWAS Summary Statistics (https://gwas.mrcieu.ac.uk/) were computed with different pipelines depending on the sub-study:

| Feature | `ukb-a` | `ukb-b` | `ukb-b` |
| --- | --- | --- | --- |
| **Consortium** | Neale Lab | MRC-IEU (via OpenGWAS) | Neale Lab |
| **Model** | Linear Regression (OLS) | Linear Mixed Model (LMM) | Linear Regression (OLS) |
| **Tool (function)** | Hail (`linreg3`) | BOLT-LMM v2.3 | Hail (`linreg3`) |
| **SNP Model** | Additive (0,1,2) | Additive, with polygenic random effect | Additive (0,1,2) |
| **Covariates** | Sex, 10 genetic PCs | Sex, Genotyping array | Sex, 10 genetic PCs |
| **Relatedness Control** |	Excluded related individuals | Modeled via random effects in LMM | Included related individuals, no LMM adjustment |
| **Population Structure Control** | 10 PCs | LMM + kinship matrix | 10 PCs |
| **References** | [GitHub repo](https://github.com/Nealelab/UK_Biobank_GWAS) & [Blog](https://www.nealelab.is/blog/2017/9/11/details-and-considerations-of-the-uk-biobank-gwas) | [GitHub repo](https://github.com/MRCIEU/UKBiobankGWAS) & [Documentation](https://data.bris.ac.uk/data/dataset/1ovaau5sxunp2cv8rcy88688v) | Same as `ukb-a` |

Raw GWAS Summary Statistics were harmonized to the GWASLab format and lifted to GRCh38 via the [harmonization_pipeline](https://github.com/ht-diva/harmonization_pipeline.git).


## **UK Biobank Pharma Proteomics Project (UKB-PPP)**

The **UK Biobank Pharma Proteomics Project (UKB-PPP)** is a biopharmaceutical consortium which perforned the plasma proteomic profiling of 54,219 UK Biobank participants. Proteomic profiling was conducted using the antibody-based Olink Explore 3072 proximity extension assay (PEA), with a total of 2923 proteins measured across eight panels: cardiometabolic, cardiometabolic II, inflammation, inflammation
II, neurology, neurology II, oncology and oncology II (for more information, see the [Olink Explore 3072 Panel](https://olinkpanel.creative-proteomics.com/table-list-of-olink-explore-3072-8-panel-combination.html)). Additionally, protein quantitative trait loci (pQTL) were performed on European individuals and five non-European ancestry groups.

For more information, see the original [UKB-PPP article](https://doi.org/10.1038/s41586-023-06592-6) and the [UKB documentation](#uk-biobank-ukb).

### **Key Metadata**

* **Number of Protein Analytes**: 
    * 2,941 protein analytes (2,943 unique proteins)
* **Protein Panel**:
    * [Olink Explore 3072 Panel](https://olinkpanel.creative-proteomics.com/table-list-of-olink-explore-3072-8-panel-combination.html)
* **Number of Variants**: 
    * Up to 23.8 million (for the combined UKB-PPP cohort)
* **Genome build**: 
    * GRCh38
* **Sample Size and Ancestry**: 
    * `ukb-ppp_European`: European, 34,557
    * `ukb-ppp_African`: African, 931
    * `ukb-ppp_South_Asian`: Central South Asian, 920
    * `ukb-ppp_Middle_Eastern`: Middle Eastern, 308
    * `ukb-ppp_East_Asian`: East Asian, 262
    * `ukb-ppp_American`: Admixed American (mostly Hispanic/Latino groups), 97
    * `ukb-ppp_Combined`: Combined UKB-PPP cohort (all the above), 52,363 (after QC-filtering starting from 54,219 participants selected by the UKB-PPP consortium)

*Note*: In UKB-PPP, ancestry is defined following the groups of [pan-UKBB](https://pan.ukbb.broadinstitute.org/). Ancestry groups are defined according to the [1000 Genomes Project](https://www.internationalgenome.org/) and Human Genome Diversity Panel as follows: after running a PCA on UKB samples, individuals were clustered based on the proximity in PC space (which captures population structure) to the reference groups; for more details, see the [study design of pan-UKBB](https://pan-dev.ukbb.broadinstitute.org/docs/study-design).

### **GWAS Pipelines Overview**

| **Feature** | **Description** |
| --- | --- |
| **Tool** | **REGENIE v2.2.1**, 2-step approach |
| **Model**  | **Step 1**: Whole-genome ridge regression for individual trait prediction (LOCO) <br />**Step 2**: Standard linear regression on residuals using imputed variants |
| **SNP Model** | Additive (genotype dosage from imputation: 0-2) |
| **Covariates** | Age, Sex, Batch, UKB center, UKB genotyping array, Time between blood sampling and measurement, First 20 genetic principal components (PCs) |
| **Population Structure Control** | Controlled via REGENIE Step 1 LOCO polygenic predictions |
| **Discovery Cohort** | 34,557 individuals of European ancestry |
| **Replication Cohort** | 17,806 individuals from all ancestries |
| **Ancestry-Specific Analysis** | For African, Admixed American, Central South Asian, East Asian, Middle Eastern sub-groups |
| **Genotyped Variant Filters (Step 1)** | - MAF > 1% <br> - MAC > 100 <br> - Genotyping rate > 99% <br> - HWE P > 1×10⁻¹⁵ <br> - <10% missingness <br> - LD pruning: window=1000, step=100, r² < 0.8 |
| **Imputed Variant Filters (Step 2)** | - INFO > 0.7 <br> - MAC > 50 (or >10 for ancestry-specific analysis) |
| **Total Variants Tested** | Up to 23.8 million imputed variants in the Combined cohort |
| **Proteins Analyzed** | 2,922 proteins from [Olink Explore 3072 Panel](https://olinkpanel.creative-proteomics.com/table-list-of-olink-explore-3072-8-panel-combination.html) |
| **Software/Method References** | [REGENIE article](https://doi.org/10.1038/s41588-021-00870-7) & [UKB-PPP article](https://doi.org/10.1038/s41586-023-06592-6) |
