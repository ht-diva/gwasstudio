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
TEST_DIR="../scripts/tests/00"
MDB_URI="mongodb://localhost:27018/test_00"
TILEDB_DIR="${TEST_DIR}/tiledB"

# Clone example files sumbmodule if it is not already cloned
git submodule update --init --recursive

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
run_command "Ingesting data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --ingestion-type metadata --file-path metadata_table.tsv --uri ${TILEDB_DIR}"

# Query data
run_command "Querying data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_01.yml --output-prefix ${TEST_DIR}/example_query_01"

# Query data
run_command "Querying data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_02.yml --output-prefix ${TEST_DIR}/example_query_02"

# Query data
run_command "Querying data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_03.yml --output-prefix ${TEST_DIR}/example_query_03"

# Query data by trait description
run_command "Querying data..." "time gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_example_04.yml --output-prefix ${TEST_DIR}/example_query_04"

echo "Results are available in ${TEST_DIR}"
