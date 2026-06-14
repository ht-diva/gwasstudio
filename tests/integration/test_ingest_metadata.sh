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
TEST_DIR="tests/integration/tests/00"
TILEDB_DIR="${TEST_DIR}/tileDB"
MDB_URI="mongodb://localhost:27018/test_00"

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

# Ingest data
run_command "Ingesting data..." "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --ingestion-type metadata --file-path data/metadata_table.tsv --uri ${TILEDB_DIR} 2>&1"

# Ingest data again
run_command "Ingesting data again..." "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --ingestion-type metadata --file-path data/metadata_table.tsv --uri ${TILEDB_DIR} 2>&1"

# List projects
run_command "Listing metadata..." "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} list 2>&1"

# Query data
run_command "Querying data... 23 results expected" "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --case-sensitive --exact-match --search-file data/search_example_01.yml --output-prefix ${TEST_DIR}/example_query_01 2>&1"

# Query data
run_command "Querying data... 6 results expected" "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --exact-match --search-file data/search_example_02.yml --output-prefix ${TEST_DIR}/example_query_02 2>&1"

# Query data
run_command "Querying data... 5 results expected" "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file data/search_example_03.yml --output-prefix ${TEST_DIR}/example_query_03 2>&1"

# Query data by trait description
run_command "Querying data... 7 results expected" "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --search-file data/search_example_04.yml --output-prefix ${TEST_DIR}/example_query_04 2>&1"

# Query data
run_command "Querying data... 3 results expected" "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file data/search_example_05.yml --output-prefix ${TEST_DIR}/example_query_05 2>&1"

# Query data
run_command "Querying data... 2 results expected" "gwasstudio --stdout --verbosity loud --mongo-uri ${MDB_URI} meta-query --search-file data/search_example_06.yml --output-prefix ${TEST_DIR}/example_query_06 2>&1"

# Query data
run_command "Querying data... 0 results expected" "gwasstudio --stdout --mongo-uri ${MDB_URI} meta-query --search-file data/search_example_07.yml --output-prefix ${TEST_DIR}/example_query_07 2>&1"

# Graceful cleanup: stop mongod even if it's already gone
if [ -f "${TEST_DIR}/mongod.pid" ]; then
  python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" || true
fi

python "${MONGO_UTILS}" status --pid-file "${TEST_DIR}/mongod.pid" || true

echo "Results are available in ${TEST_DIR}"
