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
TEST_DIR="../scripts/test_results/cli"
MDB_URI="mongodb://localhost:27018/cli_test"

# Clean up existing mongodata and test directories
echo "Cleaning up existing test directories..."
rm -rf "${TEST_DIR}"

# Create the test directory structure
mkdir -p "${TEST_DIR}"

# Function to run a command with a description
run_command() {
  local description=$1
  local cmd=$2
  echo "${description}"
  echo "Running command: ${cmd}"
  eval ${cmd}
}

# Ingest data
run_command "Ingesting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path metadata_ukb_d_sampled.tsv --uri ${TEST_DIR}/destination"

# Query data
run_command "Querying data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_query"

# Query data by trait description
run_command "Querying data by trait description..." "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file search_ukb_d_filter_by_trait_desc.txt --output-prefix ${TEST_DIR}/ukb_d_query_by_trait_desc"

# Export data
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_export --uri ${TEST_DIR}/destination"

# Export data with a different file format
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_export --output-format parquet --uri ${TEST_DIR}/destination"

# Regions filtering
run_command "Regions filtering..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_regions_filtering --uri ${TEST_DIR}/destination --get-regions regions_query.tsv"

# Hapmap3 SNPs filtering
run_command "SNPs filtering..." "gwasstudio --stdout --local-workers 4 --mongo-uri ${MDB_URI} export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_snps_filtering --uri ${TEST_DIR}/destination --snp-list-file hapmap3/hapmap3_snps.csv"

# Locusbreaker
run_command "Locusbreaker..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_locusbreaker --uri ${TEST_DIR}/destination --locusbreaker"

echo "Results are available in ${TEST_DIR}"
