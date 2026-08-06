#!/usr/bin/env bash

# Integration test for trait_ontology_ids queries
# Tests the structured ontology ID field with various query patterns

# Navigate to project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

# Check if gwasstudio command is available
if ! command -v gwasstudio &> /dev/null; then
  echo "gwasstudio command not found. Activate the conda env. Exiting."
  exit 1
fi

# Define the test directory variables (relative to project root)
TEST_DIR="tests/integration/tests/02/trait_ontology_ids"
TILEDB_DIR="${TEST_DIR}/tileDB"
MDB_URI="mongodb://localhost:27018/test_trait_ontology_ids"

MONGO_UTILS="${PROJECT_ROOT}/tests/integration/mongo_test_utils.py"

# Clean up existing mongodata and test directories
echo "Cleaning up existing test directories..."
rm -rf "${TEST_DIR}"

# Create the test directory structure
mkdir -p "${TILEDB_DIR}"

# Function to run a command with a description
run_command() {
  local description=$1
  local cmd=$2
  local expected=$3
  echo " "
  echo "${description}"
  echo "Running command: ${cmd}"
  echo "Date: $(date)" >> "${TEST_DIR}/execution_times.log"
  echo "Command: ${cmd}" >> "${TEST_DIR}/execution_times.log"
  echo "Description: ${description}" >> "${TEST_DIR}/execution_times.log"
  echo "Expected: ${expected}" >> "${TEST_DIR}/execution_times.log"
  echo "Software Version: $(gwasstudio --version)" >> "${TEST_DIR}/execution_times.log"
  echo "Execution Time:" >> "${TEST_DIR}/execution_times.log"
  { time eval ${cmd}; } 2>> "${TEST_DIR}/execution_times.log"
  echo "---" >> "${TEST_DIR}/execution_times.log"
}

# Start MongoDB
sleep 2
python "${MONGO_UTILS}" start --port 27018 --dbpath "${TEST_DIR}/mongo_db" --logpath "${TEST_DIR}/mongod.log" --pid-file "${TEST_DIR}/mongod.pid"

# Ingest data with trait_ontology_ids
run_command "Ingesting data with trait_ontology_ids..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --ingestion-type metadata --file-path data/metadata_table.tsv --uri ${TILEDB_DIR} 2>&1" \
  "Success"

# Test 1: Query by single ontology ID string
run_command "Querying by single ontology ID (EFO:0000123)..." \
  "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --exact-match --search-file data/search_trait_ontology_ids.yml --output-prefix ${TEST_DIR}/query_01 2>&1" \
  "Multiple results expected (all BMI traits)"

# Test 2: Query by list of ontology IDs
run_command "Querying by list of ontology IDs..." \
  "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --exact-match --search-file data/search_trait_ontology_ids_list.yml --output-prefix ${TEST_DIR}/query_02 2>&1" \
  "3 results expected (BMI, body fat percentage, whole body fat mass)"

# Test 3: Query by namespace
run_command "Querying by namespace (EFO)..." \
  "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --exact-match --search-file data/search_trait_ontology_ids_namespace.yml --output-prefix ${TEST_DIR}/query_03 2>&1" \
  "Multiple results expected (all EFO traits)"

# Test 4: Query by ID part
run_command "Querying by ID part (0000123)..." \
  "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --exact-match --search-file data/search_trait_ontology_ids_id.yml --output-prefix ${TEST_DIR}/query_04 2>&1" \
  "Multiple results expected (all traits with ID 0000123)"

# Graceful cleanup: stop mongod even if it's already gone
if [ -f "${TEST_DIR}/mongod.pid" ]; then
  python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" || true
fi

python "${MONGO_UTILS}" status --pid-file "${TEST_DIR}/mongod.pid" || true

echo "Results are available in ${TEST_DIR}"
echo "Integration test for trait_ontology_ids completed!"
