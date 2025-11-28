#!/usr/bin/env bash

# Determine the current directory and navigate to the data directory accordingly
if [[ $(basename "$PWD") == "scripts" ]]; then
  cd "../data"
else
  cd "./data"
fi

# Check if gwasstudio command is available
if ! command -v gwasstudio &> /dev/null; then
  echo "gwasstudio command not found. Activate the conda env. Exiting."
  exit 1
fi

# Define the data and test directory variables
GWASSTUDIO_DATA_DIR=$(gwasstudio info | grep "data dir:" | awk -F': ' '{print $2}')
TEST_DIR="../scripts/tests/01"
MDB_URI="mongodb://localhost:27018/test_01"
TILEDB_DIR="${TEST_DIR}/tileDB"

# Clone example files sumbmodule if it is not already cloned
git submodule update --init --recursive

# Clean up existing mongodata and test directories
echo "Cleaning up existing test directories..."
rm -rf "${TEST_DIR}"

# Create the test directory structure
mkdir -p "${TILEDB_DIR}"

# Function to run a command with a description
run_command() {
  local description=$1
  local cmd=$2
  echo " "
  echo "${description}"
  echo "Running command: ${cmd}"
  echo "Date: $(date)" >> "${TEST_DIR}/execution_times.log"
  echo "Command: ${cmd}" >> "${TEST_DIR}/execution_times.log"
  echo "Description: ${description}" >> "${TEST_DIR}/execution_times.log"
  echo "Software Version: $(gwasstudio --version)" >> "${TEST_DIR}/execution_times.log"
  echo "Execution Time:" >> "${TEST_DIR}/execution_times.log"
  { time eval ${cmd}; } 2>> "${TEST_DIR}/execution_times.log"
  echo "---" >> "${TEST_DIR}/execution_times.log"
}

# Ingest data
run_command "Ingesting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path metadata_table.tsv --uri ${TILEDB_DIR}"

# Query data
run_command "Querying data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_query"

# Query data by trait description
run_command "Querying data by trait description..." "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_04.yml --output-prefix ${TEST_DIR}/example_query_by_trait_desc"

# Query data by data_ids - it is a precision query, only 2 results expected
run_command "Querying data by data_ids..." "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_06.yml --output-prefix ${TEST_DIR}/example_query_by_trait_desc"

# Export data
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_06.yml --output-prefix ${TEST_DIR}/example_export --uri ${TILEDB_DIR} --plot-out"

# Export data
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_06.yml --output-prefix ${TEST_DIR}/example_export_attrs --uri ${TILEDB_DIR} --attr BETA,SE,EAF,MLOG10P,EA,NEA"

# Export data with skip-meta
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_06.yml --output-prefix ${TEST_DIR}/example_export_skip_meta --uri ${TILEDB_DIR} --plot-out --skip-meta"

# Export data with a different file format and batch size
run_command "Exporting data..." "gwasstudio --stdout --batch-size 4 --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_export --output-format parquet --uri ${TILEDB_DIR}"

# Regions filtering
run_command "Regions filtering..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_regions_filtering --output-format csv --uri ${TILEDB_DIR} --get-regions-snps regions_query.tsv"

# Hapmap3 SNPs filtering
run_command "SNPs filtering..." "gwasstudio --stdout --workers 4 --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_snps_filtering --uri ${TILEDB_DIR} --get-regions-snps hapmap3/hapmap3_snps.csv"

# Locusbreaker
run_command "Locusbreaker..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_locusbreaker --uri ${TILEDB_DIR} --locusbreaker"

echo "Results are available in ${TEST_DIR}"
