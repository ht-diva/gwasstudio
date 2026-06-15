# Welcome to GWASStudio documentation

GWASStudio is a powerful CLI tool designed for efficient storage, retrieval, and querying of genomic summary statistics. It offers a high-performance infrastructure for handling and analyzing large-scale GWAS and QTL datasets, enabling seamless cross-dataset exploration.

## Commands

* [`export`](commands.md#export) - Export summary statistics from the DB with various filtering options.
* [`info`](commands.md#info) - Show GWASStudio details
* [`ingest`](commands.md#ingest) - Ingest datasets into the DB.
* [`list`](commands.md#list) - List every category → project → study hierarchy stored in the metadata DB.
* [`meta-query`](commands.md#meta-query) - Query the DB for specific records.


## [Getting Started](getting-started.md)

This short overview of GWASStudio will help you get started.

## [Query Format](yaml_query_format.md)

Detailed reference for GWASStudio's YAML query format. It covers metadata fields, filtering options, export templates, and how queries map to MongoDB operations.
The reference also contains a [focused introduction to the YAML format](yaml-intro.md).

## [Examples](examples.md)

Showcase GWASStudio's common workflows by using examples.

## [Projects](projects.md)

Overview of GWASStudio projects.

## [Installation](installation.md)

If you are interested in developing GWASStudio, here's how to set up the CLI on your system.
