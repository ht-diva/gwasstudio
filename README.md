

# Gwasstudio: A Tool for Genomic Data Management

## Overview

Gwasstudio is a comprehensive command-line interface (CLI) tool that serves as the front-end for the [Sumstat Computational Data Hub](https://github.com/ht-diva/cdh_in_a_box) (SCDH) infrastructure.
The SCDH infrastructure is designed to facilitate Cross-Dataset Exploration of Genomics Summary Statistics, providing researchers with efficient means to manage, query, and analyze large-scale genomic datasets, particularly genome-wide association studies (GWAS) and quantitative trait loci (QTL) data.

## Core Purpose

Gwasstudio provides a unified interface across the SCDH infrastructure, handling the ingestion, storage, querying and export of genomic data using high performance technologies.

## Key Components

Gwasstudio consists of several core components:

### 1. Data Ingestion
- **Raw Data Ingestion**: Imports summary statistics data into TileDB, supporting both single files and batches.
- **Metadata Management**: Captures essential information about studies, samples, and data parameters in MongoDB.
- **Support for Multiple Storage Options**: Works with both local filesystems and cloud storage (S3).

### 2. Data Querying
- **Flexible Search**: Enables searching metadata using template files.
- **Case-Sensitivity Options**: Provides configurable search parameters.
- **Output Formatting**: Delivers query results in tabular formats for downstream analysis.

### 3. Data Export
- **Selective Export**: Extracts subsets of data based on genomic regions, SNPs, or other criteria.
- **Format Conversion**: Outputs data in modern formats compatible with common bioinformatics tools.
- **Batch Processing**: Handles large-scale exports efficiently.

## Technical Architecture

Gwasstudio leverages several advanced technologies:

1. **TileDB**: A high-performance array storage engine that enables efficient storage and retrieval of genomic data.
2. **MongoDB**: Used for storing and querying metadata associated with genomic datasets.
3. **Dask** (optional): Provides distributed computing capabilities for processing large datasets.
4. **Python Ecosystem**: Built on Python with libraries like Click/Cloup for CLI interfaces, Pandas for data manipulation, and various genomics-specific tools.

## Use Cases

Researchers can use Gwasstudio for various genomics workflows:

- **Meta-Analysis**: Combine results across multiple studies to increase statistical power.
- **Variant Exploration**: Quickly locate specific genetic variants across multiple datasets.
- **Population Comparisons**: Compare genetic associations across different populations or cohorts.
- **Data Sharing**: Standardize and export data for collaboration with other researchers.
- **Quality Control**: Assess data quality and consistency across datasets.

## Values

Gwasstudio provides significant value to genomic researchers by:

1. **Increasing Efficiency**: Reducing the time needed to process and analyze large genomic datasets.
2. **Enhancing Discovery**: Enabling novel insights through cross-dataset comparisons.
3. **Improving Reproducibility**: Standardizing data formats and analysis workflows.
4. **Facilitating Collaboration**: Making it easier to share and integrate datasets from multiple sources.

By streamlining the management of genomic data, Gwasstudio enables researchers to focus more on scientific questions and less on data handling challenges, ultimately accelerating discoveries in the field of genomics.


## Getting started

To get started with Gwasstudio, follow these installation steps:
```bash
# Clone the repository
git clone https://github.com/your-organization/gwasstudio.git
cd gwasstudio

# Create a virtual environment (recommended)
conda env create --file base_environment.yml
conda activate gwasstudio

# Install the package
make install

# Verify installation
gwasstudio --version
```
