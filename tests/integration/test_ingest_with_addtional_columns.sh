#!/usr/bin/env bash

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
TEST_DIR="tests/integration/tests/additional_columns"
MDB_URI="mongodb://localhost:27018/test_02"
TILEDB_DIR="${TEST_DIR}/tileDB"

# Clone example files submodule if it is not already cloned
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

MONGO_UTILS="${PROJECT_ROOT}/tests/integration/mongo_test_utils.py"

sleep 2
python "${MONGO_UTILS}" start --port 27018 --dbpath "${TEST_DIR}/mongo_db" --logpath "${TEST_DIR}/mongod.log" --pid-file "${TEST_DIR}/mongod.pid"

# Ingest data adding MLOG10P and N from susmstats files
run_command "Ingesting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path data/metadata_table_eqtl.tsv --uri ${TILEDB_DIR} --add-cols MLOG10P,N"

# List projects
run_command "Listing metadata..." "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} list"

# Query data
run_command "Querying data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file data/search_example_09.yml --output-prefix ${TEST_DIR}/example_query"

# Export data
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file data/search_example_09.yml --output-prefix ${TEST_DIR}/example_export --uri ${TILEDB_DIR}"

# Export data with different attributes
run_command "Exporting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file data/search_example_09.yml --output-prefix ${TEST_DIR}/example_export_with_add_attrs --uri ${TILEDB_DIR} --attr BETA,SE,EAF,MLOG10P,EA,NEA,N"

python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" || true

python "${MONGO_UTILS}" status --pid-file "${TEST_DIR}/mongod.pid" || true

echo "Results are available in ${TEST_DIR}"
