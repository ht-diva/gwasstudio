# Changelog

All notable changes to this project will be documented in this file.

[Git history](https://github.com/ht-diva/gwasstudio/commits/main/)
## [3.2.1] - 2026-08-13

### 💼 Other

- *(cli)* More descriptive messages for better diagnostics

## [3.2.0] - 2026-08-12

### 🚀 Features

- *(core)* Add `UBERON_IDS` to the metadata
- *(core)* Introduce structured trait ontology IDs support

### 🐛 Bug Fixes

- *(cli)* Follows loguru's recommended practices for error logging

### 💼 Other

- Bump version

## [3.1.0] - 2026-07-27

### 🚀 Features

- *(core)* Introduce OLINK_ID, ICD10 and SOFTWARE_DESCRIPTION attributes to the MetadataEnum class

### 🐛 Bug Fixes

- Modify src/gwasstudio/core/ingestion.py to handle comma-separated population values

### 💼 Other

- Bump version

### 🧪 Testing

- Add 4 new unit tests to tests/unit/core/test_ingestion.py to ensure that the comma-separated population values like AFA,AFR  is properly tested

## [3.0.0] - 2026-07-24

### 🚀 Features

- Add population validation with normalization
- Add DataCategoryEnum and validation
- Add enum-based validation for genome build field
- Add total_variants field to metadata
- Notes_maker fields to metadata
- *(cli)* Add compose_tiledb_uri and refactor export/ingest commands
- *(core)* Implement Vault-based authorization service
- *(cli)* Implement authorization checks in export command
- Support warehouse_uri in metadata for flexible storage
- *(core)* Introduce `WAREHOUSE_URI` to metadata for robust export

### 🐛 Bug Fixes

- Switch to Arrow 23.0.1 due to CVE-2026-25087
- When data_id is present, it takes precedence and is used exclusively for the query
- Query metadata exact-match and nested structure handling. Some refactoring.
- Establish a public surface for the core package
- `query_fields` nested `project`/`study` is lowercased now
- Remove any leading, and trailing whitespaces
- Resolve the KeyError: 'link_id' in the Lead-SNP query
- Update gitignore
- Update the README

### 💼 Other

- Revert "fix: remove any leading, and trailing whitespaces"

This reverts commit 8b6ba9fc7c5a1b356ae27b41d5b90caeb2e36323.
- Bump version

### 🚜 Refactor

- [**breaking**] Beginning of transition to a "Library-First" model.
- Reorganize utility functions and cleanup storage/metadata modules
- Update CLI export to query metadata through the new core. Remove old stuff
- Move integration tests
- Migrate Dask, TileDB config and Vault to core dataclasses
- Clean up legacy code
- Upgrade Hashing class with full/balanced/path_only methods

### 📚 Documentation

- Align the documentation with the current version of the codebase

### 🎨 Styling

- Change the option names
- Enforce PEP585 and other conventions

### 🧪 Testing

- Improve mongodb start-up and error messages printing
- Add core unit tests for ingestion, query and hashing
- Remove python 3.11 from the test github action
- Remove findpython
- The error logs suggest plugin interference is contributing to the transaction failure
- Try miniforge
- Move mongo dependencies to test

### ⚙️ Miscellaneous Tasks

- Remove unused lock files
- Update python matrix
- Update setup-miniconda action
- Fix pypi publisher

## [2.19.1] - 2026-06-03

### 💼 Other

- Bump version

### ⚡ Performance

- Implement multiple chunk hashing

## [2.19.0] - 2026-06-03

### 🚀 Features

- Add option to skip regions output
- Implement bulk metadata ingestion

### 💼 Other

- Bump version

### 📚 Documentation

- Add skip_out documentation

### 🎨 Styling

- Minor tweak

### ⚙️ Miscellaneous Tasks

- Update .gitignore

## [2.18.0] - 2026-05-26

### 🚀 Features

- Add CL string input to get-region-snps

### 🐛 Bug Fixes

- Set ancestry_groups dictionary

### 💼 Other

- Bump version
- Remove Icelandic from population documentation

### 📚 Documentation

- Update get-regions-snps documentation/help

### 🧪 Testing

- Add unit tests for regions-snps inline strings
- Add functional tests for regions-snps inline strings

## [2.17.0] - 2026-04-17

### 🚀 Features

- Add CHR,POS exact match for lead-snp search

## [2.16.1] - 2026-02-18

### 🐛 Bug Fixes

- Add the argument to spin up single-node jobs

### 💼 Other

- Bump version

## [2.16.0] - 2026-02-12

### 🚀 Features

- Add p-value filter to snp/region extraction
- Update & add summary to pvalue-region filtering

### 💼 Other

- Reformat
- Enhance file ingestion to support both parquet and tsv.gz formats

- Add support for reading parquet files alongside existing tsv.gz support
- Implement file format validation and required column checking
- Move file reading logic into a dedicated helper function
- Add proper type conversion after reading data
- Improve error handling for missing columns
- Bump version

## [2.15.0] - 2026-01-12

### 🚀 Features

- Lead-SNP window cis-trans-specific

### 💼 Other

- Bump version

## [2.14.0] - 2026-01-08

### 🚀 Features

- Multi-query for trait-specific lead-SNPs

### 🐛 Bug Fixes

- Export exact regions
- Test to validate indels

### 💼 Other

- Bump version

## [2.13.0] - 2025-12-05

### 🚀 Features

- Add get-regions-leadsnps method and doc
- Exclude multiallelic lead SNPs
- Add alpha-ordered alleles for exact match

### 🐛 Bug Fixes

- Do not use equality operator to check for truth values and some linting

### 💼 Other

- Bump version

### 🧪 Testing

- Add functional test for get-regions-leadsnps

## [2.12.3] - 2025-12-05

### 🐛 Bug Fixes

- Typo

### 💼 Other

- Addin meta_analysis
- Fix metaanalysis
- Bump version

### 🚜 Refactor

- Create a generic extraction helper and enable delayed functions uniformly

### 🎨 Styling

- Linting

### 🧪 Testing

- Add a meta analysis entry

### ⚙️ Miscellaneous Tasks

- Fix release workflow
- Cleaning
- More cleaning
- Restore submodule dataset

## [2.12.2] - 2025-11-28

### 💼 Other

- Bump version

### ⚡ Performance

- Enable capacity_mode for ingest_to_s3
- Improve metadata ingestion

## [2.12.1] - 2025-11-27

### 💼 Other

- Bump version

### ⚙️ Miscellaneous Tasks

- Fix release workflow

## [2.12.0] - 2025-11-27

### 🚀 Features

- Enhance BED file processing
- Enable copy_on_write mode

### 💼 Other

- Bump version

### 🚜 Refactor

- Bed/snp list processing

### 🧪 Testing

- Add unit test for bed/snp list reader

### ⚙️ Miscellaneous Tasks

- Add release workflow
- Update release workflow
- Fix invalid workflow error

## [2.11.0] - 2025-11-20

### 🚀 Features

- Generate SNPID column

### 🐛 Bug Fixes

- Add exact match for query
- Correct column name

### 💼 Other

- Make pytest happy
- Revert "perf: export tasks in one batch"

This reverts commit 4e6546b860962a845fd8d3f3459d43ed79b47572.
- Bump version

### 📚 Documentation

- Add ingestion and summary-stats
- Fix menu links
- Typo
- Update README
- Fix yaml syntax

### ⚡ Performance

- Export tasks in one batch
- New config for the batch size

### 🎨 Styling

- Linting

### 🧪 Testing

- Save execution time on log files
- Set a different batch size

### ⚙️ Miscellaneous Tasks

- Dealing with SettingWithCopyWarning

## [2.10.0] - 2025-11-03

### 🚀 Features

- Add locus-flanks option
- Combine regions and snps filtering
- Add metadata broadcasting and skip option

### 🐛 Bug Fixes

- Increase TileDB timeout, close #102
- Handle empty dataframe
- Restore file format

### 💼 Other

- Bump version

### 🚜 Refactor

- Dataframe output for extraction methods

### 🎨 Styling

- Linting

## [2.9.0] - 2025-10-22

### 🚀 Features

- Add p-value filtering

### 🐛 Bug Fixes

- Return empty df with non-existent region/snps, close #98
- Always return two df in _locus_breaker
- Wait indefinitely for slurm workers
- Move maf and phenovar to locusbreaker options
- Add group_name to the output filename to avoid overwriting

### 💼 Other

- Bump version

## [2.8.4] - 2025-10-17

### 🐛 Bug Fixes

- Add wait time for Dask workers, close #90
- SNPs filtering output with header and overwrite, close #89
- Set the dedup_coords configuration variable to its default value

### 💼 Other

- Bump version

### ⚙️ Miscellaneous Tasks

- Ensure that the version is filtered correctly
- Add a Makefile target for version bumping and changelog generation, close #99
- Set the current version
- Add git-cliff ignore file
- Fix bump-version target

## [2.8.3] - 2025-10-13

### 🐛 Bug Fixes

- Adjiust site_url
- Add a field to the output_fields list used in the load_search_topics function, close #88
- Add the missing field to the metadata_utils unit test, close #94

### 💼 Other

- Change auth to simple
- Add image as option
- Make docker image
- Format mem
- Remove time & close gateway
- Update cluster connection & wait
- Add error handling for client connection
- Debug connection loss
- Explicitly pass client
- Writable log_dir
- Writable all dirs
- Writable all dirs debug
- Writable all dirs update
- Prepare PR
- Bump version

### 🚜 Refactor

- Do some linting

### 📚 Documentation

- Update README
- Add main page and details about installation
- Add getting started info
- Typo
- Unused
- Fix url
- Minor
- Minor updates for export
- Add the following tables to the docs:
- Add projects page & ukb/-ppp info
- Add decode, finngen, g&h info
- Improve the introduction
- Add the examples section
- Add a changelog to the documentation

### 🎨 Styling

- Update changelog template
- Linting

### ⚙️ Miscellaneous Tasks

- Update pre-commit tools
- Fix typos package name
- Add output
- Commit the changelog to the repo
- Update generate changelog action
- Comment out the changelog commit, as it isn't working at the moment

## [2.8.2] - 2025-09-25

### 🚀 Features

- Set a default for the uri

### 💼 Other

- Bump version

## [2.8.1] - 2025-09-24

### 🚀 Features

- Adjust the paths in the dockerfile so that it behaves like the conda setup, add several Dask options for SLURMCluster.

### 💼 Other

- Bump version

## [2.8.0] - 2025-09-22

### 🚀 Features

- Simplify Dask configuration

### 💼 Other

- Bump version

## [2.7.0] - 2025-09-18

### 🐛 Bug Fixes

- Avoid sending live TileDB objects across the network; add utility for joining filesystem paths and S3 URIs
- Better logger message about the batches

### 💼 Other

- Bump version

### 🧪 Testing

- Add unit tests for the list CLI module helpers

## [2.6.0] - 2025-09-16

### 🚀 Features

- Add the list projects command and expose cli commands through __init__.py

### 🐛 Bug Fixes

- Poetry-core 2.2.0 from conda-forge is broken

### 💼 Other

- Bump version

## [2.5.0] - 2025-08-11

### 🚀 Features

- TileDB datasets grouping

### 🐛 Bug Fixes

- F-string syntax error

### 💼 Other

- Bump version

### ⚡ Performance

- Add new metadata enums and set pyarrow as backend

### ⚙️ Miscellaneous Tasks

- Add pandas-stubs dependency
- Remove unused stuff

## [2.4.0] - 2025-08-11

### 🚀 Features

- Update Mongo query composition
- Add output prefix dictionary generator

### 🐛 Bug Fixes

- Catch yaml loading error

### 💼 Other

- Bump version
- Copying from lzv

### 🚜 Refactor

- Add missing column check and simplify dataframe processing

### ⚙️ Miscellaneous Tasks

- Add example files module

## [2.3.1] - 2025-07-17

### ⚙️ Miscellaneous Tasks

- Bump version

## [2.3.0] - 2025-07-15

### 🚀 Features

- Update data model, queries and ingestion.
- *(extraction_methods.py)* Refactor tiledb_array_query to handle errors by adjusting attributes.
- *(tdb_schema)* Implement TileDB schema creator

### 🐛 Bug Fixes

- Replace separator in the output fields to match column data types

### 💼 Other

- Update dataframe column access to use `.loc`
- Make it compatible with py3.11

### 🚜 Refactor

- Update metadata query function for better performance
- Rename notes.source_id to notes_source_id in export
- Refactor TileDBSchemaCreator to use BaseEnum
-  refactor: Update test scripts for improved testing coverage

      - Adding tests for data ingestion without pvalue
      - Ensuring correct query results when using the 'search_example_01.yml' file and 'search_example_04.yml' file
      - Verifying export functionality with different file formats and settings

## [2.2.1] - 2025-07-04

### 💼 Other

- Bump version

### 🚜 Refactor

- Update Dask cluster deployment options for SLURM and gateway setups

## [2.2.0] - 2025-07-03

### 🚀 Features

- *(cli/export)* Enhance export command with several improvements
-  feat(cli/ingest.py): Implement TileDB SM configuration support in ingest

   This commit adds support for using the TileDB SM configuration when ingesting datasets from the command line interface (CLI). The change includes retrieving the SM configuration from the configuration file and passing it to the `process_and_ingest` function. Additionally, minor adjustments were made to import statements to reflect this new functionality.

### 🐛 Bug Fixes

-  fix(cli/export.py): Update attribute export options in export.py

   Updated the default attribute export options for the '--attr' argument in `src/gwasstudio/cli/export.py`. The default attribute string now includes "MLOG10P" as well as the original "BETA,SE,EAF".

   GWASStudio now ingests and exports the MLOG10P attribute using default options.

### 🧪 Testing

-  test: Add End-to-End Test Script for CLI

     This commit introduces an end-to-end test script (cli_test.sh) to the existing suite of tests. The script runs through the complete pipeline, including ingesting data, querying data, filtering by traits and regions, exporting data in different formats, and performing Locusbreaker analysis. This is aimed at ensuring the correctness and consistency of the command-line interface (CLI) functionality.

### ⚙️ Miscellaneous Tasks

- Remove a print statement
- Bump version

## [2.1.0] - 2025-06-26

### 🚀 Features

-  feat(hashing): Refactor compute_hash function to improve flexibility

   If a file path is provided, the function computes the hash based on both filename and file content.
   Previous behavior: Function only accepted a file path as input and returned the SHA-256 hash based on that file's content.

   This refactoring fixes #64

### 🐛 Bug Fixes

-  fix: Fix get_snp_list call for None case
   The function now handles the case when snp_list_file is None by not calling get_snp_list, preventing a potential error.

### 🚜 Refactor

- *(mongo)* Update logger message for non-unique ID (warning instead of error)
- Adapt Dask SLURM cluster for auto-scaling and improved performance

### ⚙️ Miscellaneous Tasks

- Bump version

## [2.0.0] - 2025-06-23

### 🚀 Features

- Remove PyArrow dependency in _process_regions and refactor related functions
- Embedded MongoDBManager with customizable port and timeout
- Add Gwasstudio CLI Test Script and related files
-  feat(CLI): Add output format option to export command

   This commit introduces an option for specifying the output file format when using the export command in the CLI. The available formats are parquet, csv.gz, and csv.

   Note that the 'compression' parameter has been deprecated and replaced with a boolean flag to indicate whether the file should be compressed or not. Also, the default value for the file format is now 'csv.gz'.

   Previously, the output format was hardcoded as csv, which made it inconvenient if users wanted to export their data in a different format. With this change, users have the flexibility to choose the format that best suits their needs.

   Footer: No breaking changes or issues are closed with this commit.

### 🐛 Bug Fixes

- Fix missing argument
- Update mongo manager to use default port 27018
- Reenable Dask functionality in export.py
- Switch to delayed() method for reading snp list file
-  fix: Fix MongoDB URI handling for embedded and localhost configurations
   The script was not properly checking if the MongoDB URI provided in the configuration was localhost or not when using an embedded MongoDB server. This commit corrects the issue by adding a condition to check if the URI contains "localhost:27018".

### 💼 Other

- Modifications to improve space
- Adapt the export to new schema
- Add option to print the snps and pvalue
- Add scipy library
- Update `ingest.py` to include `pvalue` as a flag in the CLI options and add documentation for the new flag
- Linting and fix tiledb_iterator_query_df used before assignment
- Rename the boolean argument to clearly state the action
- Change order of traits column and locusbreaker
- Add Dask to export
- Add function for SNPID and change to write_table
- Review copilot: Moving sno list function outside dask. Modifying description of function
- Changing output_file in output_prefix
- Re-arranging functions in files
- Ruff re-formatting
- Resolving warning and empty regions error
- Modifying the README
- Adding table of contents
- Adding output
- Small edits
- Resolving review comments
- Move trait_id list calculation to handle both notes and data_id cases
- Update README.md
- Bump version

### 🚜 Refactor

- Refactor export functions for output prefix handling
- *(extraction_methods)* Introduce TileDB dimensions constants for consistent query usage

### 📚 Documentation

- Update README

### 🧪 Testing

- Add test for writing CSV with GZip compression

### ⚙️ Miscellaneous Tasks

- *(build_decode_dictionary.py)* Remove obsolete script
- Update gitignore
- Simplify cli_test.sh script by introducing a run_command function
- Update README and example files
- Check if the gwasstudio command is available

## [1.0.0] - 2025-05-28

### 🚀 Features

- Add Bokeh dependency and refactor Dask cluster management

### 🐛 Bug Fixes

- Fixing locusbreaker
- Fix test

### 💼 Other

- Makefile updates:

* Added create-env target to create the conda environment if it doesn't exist.
* Simplified the if statements for activating the conda environment.
* Removed the mongo_docker_run, mongo_docker_stop, and m1_env targets.
* Updated the TARGETS variable to include create-env.
* Added ENV_NAME and CONDA_ACTIVATE variables for better readability and maintainability.
- Remove the project_list property and its associated methods from the ConfigurationManager class.
- Linting
- Changing region function
- Changing functions of regions
- Adding sampled files for testing
- Provide an embedded mongodb setup, fix #46
- Add an option to shorten hashes
- Use a shorter version of the hash
- Reformat the hashing function and set the default algorithm and hash length in the configuration file.
- Enable dask ingestion
- Set by default local Dask deployment
- Merge the two ingest actions into one
- Add an option to choose between metadata ingestion, data ingestion, or both
- Assume file system ingestion if scheme it is not S3
- Update Makefile
- Add search and metadata support files
- Update Getting started section in the README
- Update authors and bump version
- Update docker environment

## [0.9.5] - 2025-04-14

### 💼 Other

- Update metadata processing and ingestion logic

- Improve `ingest_metadata` to use a generator for processing rows, reducing memory usage.
- Add logging for the number of documents processed during ingestion.

## [0.9.4] - 2025-04-10

### 🐛 Bug Fixes

- Fix NoneType error; refactoring
- Fix choice
- Fix the save function to update records properly
- Fix search files

### 💼 Other

- Improve wording
- Refactor export functionality and add file existence check

- Refactored the `export` command in `src/gwasstudio/cli/export.py` to use a prefix for output files instead of a single output file.
- Added a `check_file_exists` function in `src/gwasstudio/utils/__init__.py` to check if a file exists and log the appropriate message.
- Updated the import statements in `src/gwasstudio/cli/export.py` to include the new `check_file_exists` function and `get_mongo_uri` function.
- Updated the `export` function to use the new `check_file_exists` function, the `output_prefix` instead of `output_file` and `get_mongo_uri` function to retrieve mongodb details.
- Bumped code version.
- Refactor data export and metadata query functionality

- Refactored the `export` command in `src/gwasstudio/cli/export.py` to use the `write_table` function for writing DataFrames to disk.
- Updated the `meta_query` command in `src/gwasstudio/cli/metadata/query.py` to use the `write_table` function for writing query results.
- Removed the deprecated `df_to_csv` function from `src/gwasstudio/utils/metadata.py`.
- Added unit tests for the `write_table` function in `tests/unit/test_utils.py`.
- Updated the `write_table` function in `src/gwasstudio/utils/__init__.py` to include a logger parameter and handle custom log messages.
- Bumped code version

## [0.9.2] - 2025-04-04

### 💼 Other

- Bump version
- Write metadata query result during export
- Update pre-commit config
- Update README
- Add Vault mount point option and update Vault integration

## [0.9.1] - 2025-03-19

### 💼 Other

- Refactor Dask deployment options, update cluster creation and client setup in dask_client.py, ingest.py and main.py
- Remove comments
- Remove other comments
- Add metadata to export
- Adding searchfile example
- Data.id does not exist in the DB, data_id will be added to the result by default
- Remove duplicated options
- Make the options consistent
- Pyarrow is needed
- Rename `format` to `file_format` to avoid shadowing the built-in Python function and other fixes
- Refactor config handling and reorganize code structure

- Rename cfg to tiledb in context object for clarity
- Create new utils/cfg.py module for centralized configuration management
- Move metadata utilities from cli/metadata/utils.py to utils/metadata.py
- Enhance Dask deployment checks with specific deployment types
- Update import statements across codebase to reflect new structure

### 🚜 Refactor

- Refactor export.py

## [0.9.0] - 2025-03-14

### 💼 Other

- Reformat metadata util functions
- Implement functions to retrieve configuration data from HashCorp vault
- Bump version

### 🚜 Refactor

- Refactor the meta-query

## [0.8.2] - 2025-03-12

### 🐛 Bug Fixes

- Fix missing dependency; add deptry package to check for issues with dependencies

## [0.8.1] - 2025-03-12

### 💼 Other

- Add better logging options
- Add Dask client option
- Some linting
- Resolve comments
- Follow the same convention for the CLI options
- Bump version

## [0.8.0] - 2025-03-09

### 🐛 Bug Fixes

- Fix arch, make docker image lighter

### 💼 Other

- Build multi platform docker image
- Update conda lock files
- Slim down the docker image
- Make hadolint happy
- Update python to version 3.12
- Update test action
- Update base environment
- Update the base environment
- Fit the test action

## [0.7.0] - 2025-03-03

### 🐛 Bug Fixes

- Fix test

### 💼 Other

- Add notes as field to store generic content; improve JSON support for storing JSON-formatted data within MongoDB
- Modifying LB
- Modifying locusbreaker and adding S and NEFF
- Answer review gmauro
- Linting, reformatting and fixing an import
- Make a lighter image
- Add arm64 build
- Create the s3 bucket if if it does not exists
- Add make
- Try ubuntu-arm runner
- Bump version

## [0.6.8] - 2025-02-25

### 🐛 Bug Fixes

- Fix variable name

## [0.6.7] - 2025-02-25

### 🐛 Bug Fixes

- Fix SSL check

## [0.6.6] - 2025-02-25

### 💼 Other

- Add s3 credentials
- Bump version

## [0.6.5] - 2025-02-24

### 💼 Other

- Typo
- Add type hint
- Bump version

## [0.6.4] - 2025-02-24

### 🐛 Bug Fixes

- Fix parse_uri

### 💼 Other

- Bump version

## [0.6.3] - 2025-02-24

### 💼 Other

- Create tiledb schema
- Bump version

## [0.6.2] - 2025-02-23

### 💼 Other

- Enable ingestion to fs

## [0.6.1] - 2025-02-23

### 💼 Other

- Add mongodb uri as a cli option
- Meta_query use mongodb uri option
- Bump version

## [0.6.0] - 2025-02-19

### 💼 Other

- Update meta_ingest command to ingest metadata from a JSON array
- Add whole GWAS output option
- Linting
- Reformat meta_query cli file
- Add study to the unique_key
- Update metadata ingest and query to handle input from table file
- Improve meta-query logic fixing multi-key queries for trait, add case-sensitive option for the search
- Bump version

### 🚜 Refactor

- Refactor meta_query to handle trait dictionaries with more elements

## [0.5.1] - 2025-01-15

### 🐛 Bug Fixes

- Fix print
- Fix mlog10p function
- Fix ge, le filtering functions and add min
- Fix few parameters
- Fix few parameters and add option for using an external list of SNPs
- Fix function name
- Fix few parameters to make the pipeline work
- Fix for ImportError: cannot import name '_manylinux' from 'packaging'
- Fix connection manager
- Fix env
- Fix export version2
- Fix import
- Fix multiple errors, some code was only commented
- Fix imports

### 💼 Other

- Add filter on pvalue
- Add mlog10p_more_than filter and fix mlog10p values
- Relax scipy version
- Add the mlog10p maximum value for each sample
- Add samples option
- Proper output format
- Add ingestion and s3 configuration
- Adding s3 configuration also for export and query functions
- Add locus breaker
- Remove unwanted files
- Use Ruff as linter and formatter
- Memory_budget_mb has been deprecated
- Remove unwanteed accessory file
- Add hapmap3 list of SNPs
- Put main.py back in its place
- Remove cache files
- Linting
- Minor linting fixes
- Simplify s3 configuration
- Stops @bruno-ariano from continuing to move the main around
- Checkout the repository
- Remove python 3.12
- Use miniconda env
- Typo
- Simplify environment.yml
- Update python build system
- Downgrade
- Disable base
- Not ignore bash profile files
- Add pybedtools dependency
- Downgrade numpy as required by tiledbvcf
- Add dask dependency
- Update actions/checkout version
- Add mongodb models and some test units
- Update github action
- Update actions version
- Still fix for ImportError: cannot import name '_manylinux' from 'packaging'
- Add is_connected unit test
- Add hash utils
- Add compute_sha256 test
- Remove poetry.lock
- Update conda-locks
- Update dependencies
- Update README
- Use unique key to retrieve objects
- Add metadata ingest stub
- Add click command
- Save EnhancedDataProfile object
- Move config inside the app, fix connection manager and add a unit test, optimize imports
- Improve conda environment
- Bump version
- Add view command
- Add command into main
- Add meta-view command
- Add meta-query command
- Get total_samples from the dataset
- Put the correct attribute
- Add query test
- Update metadata cli
- Branch for ingestin of tiledb_unified
- Adjusting for sha
- Complete ingestion edits
- Modify export
- Adding export
- Adding polars to snp selection
- Include polars dependency
- Add pandas dependency
- Slight changes in the ingestions
- Slightly adjust parameters
- Update dependencies; remove pybedtools, add boto
- Remove EA, NEA attributes
- Update ingest with different path for s3 and files
- Remove functions as they are under utils
- Restore tests
- Add pyarrow dependency
- Remove EA, NEA attributes from schema also
- Catch NotUniqueError
- Update project name
- Update meta-ingest
- Provide an option to use as input the checksum list
- Do not exit for the moment
- Relax the string length constraint on the trait_desc argument
- Use Singleton pattern for the configuration manager
- Add an example of search template
- Use src layouts and improve Docker support
- Update conda environment
- Implement meta_query traversing dictionary keys

### 🧪 Testing

- Test ruff
- Test ruff 2

## [0.2.0] - 2023-10-15

### 🐛 Bug Fixes

- Fix the exporter

### 💼 Other

- First commit
- Add github workflow
- Update README
- Remove pip install poetry
- Change image
- Update shell
- Remove mongomock
- Change to debian image
- Set locale
- Add info and query commands
