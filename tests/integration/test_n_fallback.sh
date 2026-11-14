#!/usr/bin/env bash

# Integration test for N fallback functionality
# Case 1: N attribute missing from TileDB but total_samples exists in metadata ->
#         the export command adds an N column with the value from total_samples.
# Case 2: N attribute present in TileDB AND total_samples exists in metadata ->
#         the export command keeps the N values stored in TileDB (no fallback).
# Case 3: N attribute missing from TileDB and total_samples missing from metadata ->
#         the export succeeds without an N column (no crash, no injected values).

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
TEST_DIR="tests/integration/tests/test_n_fallback"
MDB_URI="mongodb://localhost:27018/test_n_fallback"
TILEDB_DIR="${TEST_DIR}/tileDB"
TILEDB_DIR_WITH_N="${TEST_DIR}/tileDB_with_N"
TILEDB_DIR_NO_TS="${TEST_DIR}/tileDB_no_ts"
MONGO_UTILS="${PROJECT_ROOT}/tests/integration/mongo_test_utils.py"

# Clean up existing test directories
echo "Cleaning up existing test directories..."
rm -rf "${TEST_DIR}"

# Create the test directory structure
mkdir -p "${TILEDB_DIR}" "${TILEDB_DIR_WITH_N}" "${TILEDB_DIR_NO_TS}"

# Function to run a command with a description
run_command() {
  local description=$1
  local cmd=$2
  echo " "
  echo "${description}"
  echo "Running: ${cmd}"
  { time eval ${cmd}; } 2>&1 | tee -a "${TEST_DIR}/test_output.log"
  if [ ${PIPESTATUS[0]} -ne 0 ]; then
    echo "ERROR: Command failed with exit code ${PIPESTATUS[0]}"
    exit 1
  fi
}

# Function to verify that the exported CSV contains an N column with the expected value
verify_n_column() {
  local output_file=$1
  local expected_n=$2
  local description=$3

  echo ""
  echo "Verifying: ${description}"

  if [ ! -f "${output_file}" ]; then
    echo "✗ Output file not found: ${output_file}"
    exit 1
  fi

  # Find the 1-based index of the N column in the header (column positions vary,
  # e.g. N=9 when MLOG10P is not stored in TileDB and N=10 otherwise)
  local n_col_index
  n_col_index=$(gunzip -c "${output_file}" | head -1 | tr ',' '\n' | grep -nx "N" | head -1 | cut -d: -f1)
  if [ -n "${n_col_index}" ]; then
    echo "✓ N column found in output header (column ${n_col_index})"
  else
    echo "✗ N column NOT found in output header"
    echo "Header: $(gunzip -c "${output_file}" | head -1)"
    exit 1
  fi

  local n_values
  n_values=$(gunzip -c "${output_file}" | tail -n +2 | awk -F',' -v col=$n_col_index '{print $col}' | head -1 | tr -d ' ')
  if [ "$n_values" = "$expected_n" ]; then
    echo "✓ N values are correct (${expected_n})"
  else
    echo "✗ N values are incorrect. Expected ${expected_n}, got: $n_values"
    exit 1
  fi
}

# Function to verify that the exported CSV does NOT contain an N column
verify_n_absent() {
  local output_file=$1
  local description=$2

  echo ""
  echo "Verifying: ${description}"

  if [ ! -f "${output_file}" ]; then
    echo "✗ Output file not found: ${output_file}"
    exit 1
  fi

  local n_col_index
  n_col_index=$(gunzip -c "${output_file}" | head -1 | tr ',' '\n' | grep -nx "N" | head -1 | cut -d: -f1)
  if [ -n "${n_col_index}" ]; then
    echo "✗ N column unexpectedly present in output header (column ${n_col_index})"
    echo "Header: $(gunzip -c "${output_file}" | head -1)"
    exit 1
  fi
  echo "✓ N column is absent from output header (as expected)"
}

# If a previous mongod still holds the port, wait for it to release it (up to 30s)
for i in $(seq 1 30); do
  if ! nc -z localhost 27018 2>/dev/null; then break; fi
  sleep 1
done

# Start MongoDB
python "${MONGO_UTILS}" start --port 27018 --dbpath "${TEST_DIR}/mongo_db" --logpath "${TEST_DIR}/mongod.log" --pid-file "${TEST_DIR}/mongod.pid"

# Wait for MongoDB to be ready (up to 60s); fail fast if the process died (e.g. port in use)
MONGO_READY=0
for i in $(seq 1 60); do
  if python -c "from pymongo import MongoClient; MongoClient('mongodb://localhost:27018', serverSelectionTimeoutMS=1000).admin.command('ping')" >/dev/null 2>&1; then
    MONGO_READY=1
    break
  fi
  if ! kill -0 "$(cat "${TEST_DIR}/mongod.pid" 2>/dev/null)" 2>/dev/null; then
    echo "ERROR: mongod failed to start. Check ${TEST_DIR}/mongod.log"
    tail -5 "${TEST_DIR}/mongod.log" || true
    exit 1
  fi
  sleep 1
done
if [ "${MONGO_READY}" -ne 1 ]; then
  echo "ERROR: MongoDB did not become ready within 60s."
  exit 1
fi

# ---------------------------------------------------------------------------
# Case 1: N NOT in TileDB -> fallback to total_samples
# ---------------------------------------------------------------------------

# Create test data without N column
cat > "${TEST_DIR}/data.tsv" << 'EOF'
CHR	POS	EA	NEA	EAF	SE	BETA
1	100	A	T	0.4	0.5	2.0
1	200	G	A	0.3	0.5	1.0
2	300	C	G	0.2	0.4	0.8
2	400	T	A	0.1	0.2	0.4
EOF

# Compress the data file
gzip -c "${TEST_DIR}/data.tsv" > "${TEST_DIR}/data.tsv.gz"

# Create a minimal metadata file for testing N fallback
cat > "${TEST_DIR}/metadata.tsv" << EOF
project	study	category	file_path	access_level	warehouse_uri	notes_consortium	notes_sex	notes_source_id	build	population	total_variants	total_samples	total_cases	total_controls	trait_desc	trait_ontology_ids
n_fallback_test	study_a	GWAS	${TEST_DIR}/data.tsv.gz	public	${TILEDB_DIR}		Combined	n_fallback_test-1	GRCh37	EUR	10	10000	5000	5000	Test trait for N fallback	EFO:0000001
EOF

# Ingest data (N attribute NOT in TileDB)
run_command "Ingesting data without N attribute..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path ${TEST_DIR}/metadata.tsv --uri ${TILEDB_DIR}"

# Create search file
cat > "${TEST_DIR}/search.yml" << 'EOF'
query_fields:
  notes_source_id: n_fallback_test-1

output_fields:
  - total_samples
EOF

# Export with N in attributes (should use total_samples=10000 from metadata)
run_command "Exporting with N attribute (should fallback to total_samples)..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file ${TEST_DIR}/search.yml --output-prefix ${TEST_DIR}/export_n_fallback --uri ${TILEDB_DIR} --attr BETA,SE,EAF,MLOG10P,EA,NEA,N"

# Verify the output file has N column with value 10000
OUTPUT_FILE="${TEST_DIR}/export_n_fallback_n_fallback_test_study_a_n_fallback_test-1.csv.gz"
verify_n_column "${OUTPUT_FILE}" "10000" "Case 1: N missing in TileDB, fallback to total_samples=10000"

# ---------------------------------------------------------------------------
# Case 2: N in TileDB AND total_samples in metadata -> N from TileDB is kept
# ---------------------------------------------------------------------------

# Create test data WITH N column (value 12345)
cat > "${TEST_DIR}/data_with_n.tsv" << 'EOF'
CHR	POS	EA	NEA	EAF	SE	BETA	N
1	100	A	T	0.4	0.5	2.0	12345
1	200	G	A	0.3	0.5	1.0	12345
2	300	C	G	0.2	0.4	0.8	12345
2	400	T	A	0.1	0.2	0.4	12345
EOF

# Compress the data file
gzip -c "${TEST_DIR}/data_with_n.tsv" > "${TEST_DIR}/data_with_n.tsv.gz"

# Create a minimal metadata file: total_samples=10000 (different from N=12345 in TileDB)
cat > "${TEST_DIR}/metadata_with_n.tsv" << EOF
project	study	category	file_path	access_level	warehouse_uri	notes_consortium	notes_sex	notes_source_id	build	population	total_variants	total_samples	total_cases	total_controls	trait_desc	trait_ontology_ids
n_fallback_with_n	study_b	GWAS	${TEST_DIR}/data_with_n.tsv.gz	public	${TILEDB_DIR_WITH_N}		Combined	n_fallback_with_n-1	GRCh37	EUR	10	10000	5000	5000	Test trait with N in TileDB	EFO:0000001
EOF

# Ingest data (N attribute IS in TileDB)
run_command "Ingesting data with N attribute..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path ${TEST_DIR}/metadata_with_n.tsv --uri ${TILEDB_DIR_WITH_N} --add-cols N"

# Create search file
cat > "${TEST_DIR}/search_with_n.yml" << 'EOF'
query_fields:
  notes_source_id: n_fallback_with_n-1

output_fields:
  - total_samples
EOF

# Export with N in attributes (should keep N=12345 from TileDB, not fall back to total_samples)
run_command "Exporting with N attribute (should keep N from TileDB)..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file ${TEST_DIR}/search_with_n.yml --output-prefix ${TEST_DIR}/export_n_with_tiledb --uri ${TILEDB_DIR_WITH_N} --attr BETA,SE,EAF,MLOG10P,EA,NEA,N"

# Verify the output file has N column with value 12345 (from TileDB, not total_samples)
OUTPUT_FILE="${TEST_DIR}/export_n_with_tiledb_n_fallback_with_n_study_b_n_fallback_with_n-1.csv.gz"
verify_n_column "${OUTPUT_FILE}" "12345" "Case 2: N present in TileDB (12345) takes precedence over total_samples (10000)"

# ---------------------------------------------------------------------------
# Case 3: N missing in TileDB AND total_samples missing in metadata ->
#         the export succeeds and no N column is added
# ---------------------------------------------------------------------------

# Reuse the case-1 data (no N column)
cp "${TEST_DIR}/data.tsv.gz" "${TEST_DIR}/data_no_ts.tsv.gz"

# Create a minimal metadata file with an EMPTY total_samples field
cat > "${TEST_DIR}/metadata_no_ts.tsv" << EOF
project	study	category	file_path	access_level	warehouse_uri	notes_consortium	notes_sex	notes_source_id	build	population	total_variants	total_samples	total_cases	total_controls	trait_desc	trait_ontology_ids
n_fallback_no_ts	study_c	GWAS	${TEST_DIR}/data_no_ts.tsv.gz	public	${TILEDB_DIR_NO_TS}		Combined	n_fallback_no_ts-1	GRCh37	EUR	10		5000	5000	Test trait without N and total_samples	EFO:0000001
EOF

# Ingest data (N attribute NOT in TileDB)
run_command "Ingesting data without N attribute and without total_samples in metadata..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} ingest --file-path ${TEST_DIR}/metadata_no_ts.tsv --uri ${TILEDB_DIR_NO_TS}"

# Create search file (no total_samples output field)
cat > "${TEST_DIR}/search_no_ts.yml" << 'EOF'
query_fields:
  notes_source_id: n_fallback_no_ts-1
EOF

# Export with N in attributes (should succeed without adding any N column)
run_command "Exporting with N attribute (should NOT add N, no fallback source)..." \
  "gwasstudio --stdout --mongo-uri ${MDB_URI} export --search-file ${TEST_DIR}/search_no_ts.yml --output-prefix ${TEST_DIR}/export_no_n_no_ts --uri ${TILEDB_DIR_NO_TS} --attr BETA,SE,EAF,MLOG10P,EA,NEA,N"

# Verify the output file has NO N column
OUTPUT_FILE="${TEST_DIR}/export_no_n_no_ts_n_fallback_no_ts_study_c_n_fallback_no_ts-1.csv.gz"
verify_n_absent "${OUTPUT_FILE}" "Case 3: no N in TileDB and no total_samples in metadata -> no N column"

# Clean up
python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid"
echo ""
echo "=== All N fallback integration tests PASSED ==="
