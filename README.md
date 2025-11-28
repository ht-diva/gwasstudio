

# GWASStudio: A Tool for Genomic Data Management

![alt text](image.png)


## Overview

GWASStudio is a powerful CLI tool designed for efficient storage, retrieval, and querying of genomic summary statistics. It offers a high-performance infrastructure for handling and analyzing large-scale GWAS and QTL datasets, enabling seamless cross-dataset exploration.

## Core Purpose

GWASStudio provides a unified interface across the [CDH](https://github.com/ht-diva/cdh_in_a_box) infrastructure, handling the ingestion, storage, querying and export of genomic data using high-performance technologies.

## Key Functionalities

GWASStudio consists of several key functionalities:

### 1. Data Ingestion
- **Data Ingestion**: Imports summary statistics data and its metadata associated.
- **Support for Multiple Storage Options**: Works with both local filesystems and cloud storage (S3).

### 2. Data Querying
- **Flexible Search**: Enables searching metadata using template files.

### 3. Data Export
- **Selective Export**: Extracts subsets of data and its metadata associated based on genomic regions, SNPs, or the entire set of data.

## Technical Architecture

GWASStudio leverages several advanced technologies:

1. **TileDB Embedded**: A high-performance array storage engine that enables efficient storage and retrieval of genomic data.
2. **MongoDB**: A flexible, scalable NoSQL database used for storing and querying metadata associated with genomic datasets.
3. **Dask**: Provides distributed computing capabilities for processing large datasets.
4. **Python Ecosystem**: Built on Python with libraries like Click/Cloup for CLI interfaces, Pandas for data manipulation, and various genomics-specific tools.

## Installation

For detailed installation instructions, please refer to the documentation at [https://ht-diva.github.io/gwasstudio/](https://ht-diva.github.io/gwasstudio/)

## Usage

For detailed instructions on how to use this tool, please refer to the [documentation](https://ht-diva.github.io/gwasstudio/) and check the [cli_test](scripts/) script for a practical guide by examples.

## Citation

Example files are derived from:

The variant call format provides efficient and robust storage of GWAS summary statistics. Matthew Lyon, Shea J Andrews, Ben Elsworth, Tom R Gaunt, Gibran Hemani, Edoardo Marcora. bioRxiv 2020.05.29.115824; doi: https://doi.org/10.1101/2020.05.29.115824
