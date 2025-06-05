

# GWASSTUDIO: A Tool for Genomic Data Management

![alt text](image.png)


## Overview

GWASSTUDIO is a comprehensive command-line interface (CLI) tool that serves as summary statistics store manager.
The GWASSTUDIO infrastructure is designed to facilitate Cross-Dataset Exploration of Genomics Summary Statistics, providing researchers with efficient means to manage, query, and analyze large-scale genomic GWAS QTL data.

## Core Purpose

Gwasstudio provides a unified interface across the SCDH infrastructure, handling the ingestion, storage, querying and export of genomic data using high performance technologies.

## Key Components

Gwasstudio consists of several core components:

### 1. Data Ingestion
- **Data Ingestion**: Imports summary statistics data and its metadata associated, supporting both files singularly and in batches.
- **Support for Multiple Storage Options**: Works with both local filesystems and cloud storage (S3).

### 2. Data Querying
- **Flexible Search**: Enables searching metadata using template files.

### 3. Data Export
- **Selective Export**: Extracts subsets of data and its metadata associated based on genomic regions, SNPs, or the entire set of data.

## Technical Architecture

Gwasstudio leverages several advanced technologies:

1. **TileDB**: A high-performance array storage engine that enables efficient storage and retrieval of genomic data.
2. **MongoDB**: Used for storing and querying metadata associated with genomic datasets.
3. **Dask** (optional): Provides distributed computing capabilities for processing large datasets.
4. **Python Ecosystem**: Built on Python with libraries like Click/Cloup for CLI interfaces, Pandas for data manipulation, and various genomics-specific tools.

## Please check the docs for more details on how to Install and use it

## Installation

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
