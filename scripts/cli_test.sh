#!/usr/bin/env bash

# Determine the current directory and navigate to the data directory accordingly
if [[ $(basename "$PWD") == "scripts" ]]; then
  cd "../data"
else
  cd "./data"
fi

# Define the data and test directory variables
GWASSTUDIO_DATA_DIR=`gwasstudio info | grep "data dir:" | awk -F': ' '{print $2}'`
TEST_DIR="../scripts/test_results/cli"

# Clean up existing mongodata and test directories
echo "Cleaning up existing data and test directories..."
rm -rf "${GWASSTUDIO_DATA_DIR}/mongo_db"
rm -rf "${TEST_DIR}"

# Create the test directory structure
mkdir -p "${TEST_DIR}"

# Ingest data
echo "Ingesting data..."
INGEST_CMD="gwasstudio --stdout ingest --file-path metadata_ukb_d_sampled.tsv --uri ${TEST_DIR}/destination"
echo "Running command: ${INGEST_CMD}"
$INGEST_CMD

# Query data
echo "Querying data..."
QUERY_CMD="gwasstudio --stdout meta-query --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_query"
echo "Running command: ${QUERY_CMD}"
$QUERY_CMD

# Query data
echo "Querying data by trait description..."
QUERY_CMD_BY_TRAIT_DESC="gwasstudio --stdout meta-query --search-file search_ukb_d_filter_by_trait_desc.txt --output-prefix ${TEST_DIR}/ukb_d_query_by_trait_desc"
echo "Running command: ${QUERY_CMD_BY_TRAIT_DESC}"
$QUERY_CMD_BY_TRAIT_DESC

# Export data
echo "Exporting data..."
EXPORT_CMD="gwasstudio --stdout export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_export --uri ${TEST_DIR}/destination"
echo "Running command: ${EXPORT_CMD}"
$EXPORT_CMD

# Regions filtering
echo "Regions filtering..."
RF_CMD="gwasstudio --stdout export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_regions_filtering --uri ${TEST_DIR}/destination --get-regions regions_query.tsv"
echo "Running command: ${RF_CMD}"
$RF_CMD

# Hapmap3 SNPs filtering
echo "SNPs filtering..."
SF_CMD="gwasstudio --stdout export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_snps_filtering --uri ${TEST_DIR}/destination --snp-list-file hapmap3/hapmap3_snps.csv"
echo "Running command: ${SF_CMD}"
$SF_CMD


# Locusbreaker
echo "Locusbreaker..."
LB_CMD="gwasstudio --stdout export --search-file search_ukb_d.txt --output-prefix ${TEST_DIR}/ukb_d_locusbreaker --uri ${TEST_DIR}/destination --locusbreaker"
$LB_CMD

echo "Results are available in ${TEST_DIR}"
