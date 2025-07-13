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
TEST_DIR="../../scripts/tests/01"
MDB_URI="mongodb://localhost:27018/test_01"
TILEDB_DIR="${TEST_DIR}/tiledB"

# Clean up existing mongodata and test directories
echo "Cleaning up existing test directories..."
rm -rf "${TEST_DIR}"

# Create the test directory structure
mkdir -p "${TEST_DIR}"

# Function to run a command with a description
run_command() {
  local description=$1
  local cmd=$2
  echo " "
  echo "${description}"
  echo "Running command: ${cmd}"
  eval ${cmd}
}

# Ingest data
run_command "Ingesting data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path metadata_table.tsv --uri ${TILEDB_DIR}"

# Query data
run_command "Querying data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_query"

# Query data by trait description
run_command "Querying data by trait description..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_04.yml --output-prefix ${TEST_DIR}/example_query_by_trait_desc"

# Export data
run_command "Exporting data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_export --uri ${TILEDB_DIR}"

# Export data with a different file format
run_command "Exporting data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_export --output-format parquet --uri ${TILEDB_DIR}"

# Regions filtering
run_command "Regions filtering..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_regions_filtering --uri ${TILEDB_DIR} --get-regions regions_query.tsv"

# Hapmap3 SNPs filtering
run_command "SNPs filtering..." "time gwasstudio --stdout --local-workers 4 --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_snps_filtering --uri ${TILEDB_DIR} --snp-list-file hapmap3/hapmap3_snps.csv"

# Locusbreaker
run_command "Locusbreaker..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_locusbreaker --uri ${TILEDB_DIR} --locusbreaker"

echo "Results are available in ${TEST_DIR}"
