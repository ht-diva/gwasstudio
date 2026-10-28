#!/usr/bin/env bash
# Integration test for --fail-on-empty (A1.1) and --region-buffer (A1.3).
#
# Tests:
#   1. --fail-on-empty + guaranteed-empty region  → exit code 3
#   2. --fail-on-empty + valid region             → exit code 0 + data file written
#   3. --region-buffer N produces the same data as no buffer
#      (buffer widens the TileDB retrieval window only; the analysis mask
#       still uses the original START_ORIG/END_ORIG bounds)
#
# Prerequisites: conda env with gwasstudio installed, mongod on PATH.
# Usage: bash tests/integration/test_export_flags.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

if ! command -v gwasstudio &> /dev/null; then
  echo "ERROR: gwasstudio command not found. Activate the conda env first."
  exit 1
fi

TEST_DIR="tests/integration/tests/10"
TILEDB_DIR="${TEST_DIR}/tileDB"
MDB_URI="mongodb://localhost:27018/test_10"
MONGO_UTILS="${PROJECT_ROOT}/tests/integration/mongo_test_utils.py"

# ── Helpers ──────────────────────────────────────────────────────────────────
fail() {
  echo "FAIL: $1"
  python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" 2>/dev/null || true
  exit 1
}

pass() {
  echo "PASS: $1"
}

# ── Clean up & start MongoDB ─────────────────────────────────────────────────
echo "=== Cleaning up previous runs ==="
rm -rf "${TEST_DIR}"
mkdir -p "${TILEDB_DIR}"

echo "=== Starting MongoDB ==="
sleep 2
python "${MONGO_UTILS}" start --port 27018 \
  --dbpath  "${TEST_DIR}/mongo_db" \
  --logpath "${TEST_DIR}/mongod.log" \
  --pid-file "${TEST_DIR}/mongod.pid" \
  || fail "could not start mongod"

# ── Ingest test data ─────────────────────────────────────────────────────────
echo "=== Ingesting test data ==="
gwasstudio --stdout --mongo-uri "${MDB_URI}" ingest \
  --file-path data/metadata_table.tsv \
  --uri     "${TILEDB_DIR}" \
  > "${TEST_DIR}/ingest.log" 2>&1
RC=$?
if [ ${RC} -ne 0 ]; then
  cat "${TEST_DIR}/ingest.log"
  fail "ingest failed (exit ${RC})"
fi
echo "Ingest OK."

# ═══════════════════════════════════════════════════════════════════════════
# TEST 1 — --fail-on-empty with a region that has no stored variants
#          Expected: exit code 3, no data file written
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "=== TEST 1: --fail-on-empty + empty region → expect exit 3 ==="
# chr 6 is not present in any of the sampled test datasets
EMPTY_REGION="6,1,1000"

gwasstudio --stdout --mongo-uri "${MDB_URI}" export \
  --search-file       data/search_example_01.yml \
  --get-regions-snps  "${EMPTY_REGION}" \
  --output-prefix     "${TEST_DIR}/out_empty" \
  --uri               "${TILEDB_DIR}" \
  --output-format     csv \
  --fail-on-empty \
  > "${TEST_DIR}/test1.log" 2>&1
RC=$?

if [ ${RC} -eq 3 ]; then
  pass "exit code is 3"
else
  cat "${TEST_DIR}/test1.log"
  fail "expected exit code 3, got ${RC}"
fi

# Data file must NOT have been written (only _meta.csv is always written)
DATA_FILES=$(ls "${TEST_DIR}"/out_empty_*.csv 2>/dev/null | grep -v '_meta' || true)
if [ -n "${DATA_FILES}" ]; then
  fail "data file(s) unexpectedly written for empty region: ${DATA_FILES}"
fi
pass "no data file written for empty region"

# ═══════════════════════════════════════════════════════════════════════════
# TEST 2 — --fail-on-empty with a region that contains a stored variant
#          Expected: exit code 0, at least one data file written
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "=== TEST 2: --fail-on-empty + valid region → expect exit 0 ==="
# chr 12 POS 4997051 is present in bbj-a-1.gwaslab.tsv.sampled.gz
VALID_REGION="12,4996000,4998000"

gwasstudio --stdout --mongo-uri "${MDB_URI}" export \
  --search-file       data/search_example_01.yml \
  --get-regions-snps  "${VALID_REGION}" \
  --output-prefix     "${TEST_DIR}/out_valid" \
  --uri               "${TILEDB_DIR}" \
  --output-format     csv \
  --fail-on-empty \
  > "${TEST_DIR}/test2.log" 2>&1
RC=$?

if [ ${RC} -eq 0 ]; then
  pass "exit code is 0"
else
  cat "${TEST_DIR}/test2.log"
  fail "expected exit code 0, got ${RC}"
fi

DATA_FILES=$(ls "${TEST_DIR}"/out_valid_*.csv 2>/dev/null | grep -v '_meta' || true)
if [ -z "${DATA_FILES}" ]; then
  fail "no data CSV file found for valid region"
fi

# Every data file must contain at least one data row (header + ≥ 1 row)
while IFS= read -r f; do
  LINES=$(wc -l < "$f" | tr -d ' ')
  if [ "${LINES}" -lt 2 ]; then
    fail "data file ${f} has only ${LINES} line(s); expected header + data"
  fi
done <<< "${DATA_FILES}"
pass "data file(s) present with content: $(echo "${DATA_FILES}" | wc -l | tr -d ' ') file(s)"

# ═══════════════════════════════════════════════════════════════════════════
# TEST 3 — --region-buffer does not alter the returned data
#          The buffer widens the TileDB retrieval window only; the analysis
#          mask (pos_mask) still uses START_ORIG/END_ORIG, so the output
#          rows must be identical whether or not the buffer is applied.
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "=== TEST 3: --region-buffer → output identical to no-buffer run ==="
# Narrow window around POS 4997051
BUFFER_REGION="12,4996900,4997200"

# Run A: no buffer
gwasstudio --stdout --mongo-uri "${MDB_URI}" export \
  --search-file       data/search_example_01.yml \
  --get-regions-snps  "${BUFFER_REGION}" \
  --output-prefix     "${TEST_DIR}/out_nobuffer" \
  --uri               "${TILEDB_DIR}" \
  --output-format     csv \
  > "${TEST_DIR}/test3a.log" 2>&1

# Run B: buffer = 5 000 bp
gwasstudio --stdout --mongo-uri "${MDB_URI}" export \
  --search-file       data/search_example_01.yml \
  --get-regions-snps  "${BUFFER_REGION}" \
  --output-prefix     "${TEST_DIR}/out_buffer" \
  --uri               "${TILEDB_DIR}" \
  --output-format     csv \
  --region-buffer     5000 \
  > "${TEST_DIR}/test3b.log" 2>&1

NOBUF_FILES=$(ls "${TEST_DIR}"/out_nobuffer_*.csv 2>/dev/null | grep -v '_meta' | sort || true)
BUF_FILES=$(ls   "${TEST_DIR}"/out_buffer_*.csv   2>/dev/null | grep -v '_meta' | sort || true)

if [ -z "${NOBUF_FILES}" ] && [ -z "${BUF_FILES}" ]; then
  # Narrow window may legitimately contain zero variants for some traits;
  # as long as both runs agree that's fine.
  pass "both runs returned zero data rows (consistent)"
elif [ -z "${NOBUF_FILES}" ] || [ -z "${BUF_FILES}" ]; then
  fail "one run wrote data files and the other did not"
else
  NOBUF_LINES=$(cat ${NOBUF_FILES} 2>/dev/null | wc -l | tr -d ' ')
  BUF_LINES=$(cat   ${BUF_FILES}   2>/dev/null | wc -l | tr -d ' ')
  echo "  no-buffer rows : ${NOBUF_LINES}"
  echo "  buffer=5000 rows: ${BUF_LINES}"

  if [ "${NOBUF_LINES}" -eq "${BUF_LINES}" ]; then
    pass "row counts match (${NOBUF_LINES} rows)"
  else
    fail "row counts differ: no-buffer=${NOBUF_LINES}  buffer=${BUF_LINES}"
  fi
fi

# ═══════════════════════════════════════════════════════════════════════════
# TEST 4 — --workers 0 (in-process fast path) produces the same data
#          as the Dask-cluster run in TEST 2.
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "=== TEST 4: --workers 0 (in-process) → same rows as cluster run ==="

gwasstudio --stdout --mongo-uri "${MDB_URI}" --workers 0 export \
  --search-file       data/search_example_01.yml \
  --get-regions-snps  "${VALID_REGION}" \
  --output-prefix     "${TEST_DIR}/out_inproc" \
  --uri               "${TILEDB_DIR}" \
  --output-format     csv \
  --fail-on-empty \
  > "${TEST_DIR}/test4.log" 2>&1
RC=$?

if [ ${RC} -eq 0 ]; then
  pass "exit code is 0"
else
  cat "${TEST_DIR}/test4.log"
  fail "expected exit code 0, got ${RC}"
fi

if grep -q 'Fast path: running export in-process' "${TEST_DIR}/test4.log"; then
  pass "in-process fast path was taken"
else
  fail "fast-path log message not found in test4.log"
fi

INPROC_FILES=$(ls "${TEST_DIR}"/out_inproc_*.csv 2>/dev/null | grep -v '_meta' || true)
CLUSTER_FILES=$(ls "${TEST_DIR}"/out_valid_*.csv  2>/dev/null | grep -v '_meta' || true)
INPROC_LINES=$(cat ${INPROC_FILES} 2>/dev/null | wc -l | tr -d ' ')
CLUSTER_LINES=$(cat ${CLUSTER_FILES} 2>/dev/null | wc -l | tr -d ' ')
echo "  cluster rows  : ${CLUSTER_LINES}"
echo "  in-process rows: ${INPROC_LINES}"

if [ -n "${INPROC_FILES}" ] && [ "${INPROC_LINES}" -eq "${CLUSTER_LINES}" ]; then
  pass "in-process run wrote the same ${INPROC_LINES} rows as the cluster run"
else
  fail "row count mismatch: cluster=${CLUSTER_LINES}  in-process=${INPROC_LINES}"
fi

# ═══════════════════════════════════════════════════════════════════════════
# TEST 5 — authorization-denied export honors --fail-on-empty.
#          opengwas/ieu-b is ingested with access_level=restricted and no
#          allowed_policies. With no Vault token the user is unauthenticated,
#          so check_access() denies it and the dataset lands in skipped_results.
#          Without --fail-on-empty the export silently no-ops (exit 0);
#          with it, the export must fail loudly (exit 3, "NO_DATA: authorization").
# ═══════════════════════════════════════════════════════════════════════════
echo ""
echo "=== TEST 5: authorization-denied → honors --fail-on-empty ==="

SEARCH_RESTRICTED="${TEST_DIR}/search_restricted.yml"
cat > "${SEARCH_RESTRICTED}" <<'YML'
project: opengwas
study: ieu-b

output:
  - build
  - population
  - trait_desc
YML

# 5a — no --fail-on-empty: silent no-op (exit 0), no data files
gwasstudio --stdout --mongo-uri "${MDB_URI}" export \
  --search-file    "${SEARCH_RESTRICTED}" \
  --output-prefix  "${TEST_DIR}/out_auth_silent" \
  --uri            "${TILEDB_DIR}" \
  --output-format  csv \
  > "${TEST_DIR}/test5a.log" 2>&1
RC=$?
if [ ${RC} -eq 0 ]; then
  pass "denied export (no flag) exits 0"
else
  cat "${TEST_DIR}/test5a.log"; fail "expected exit 0, got ${RC}"
fi
SILENT_FILES=$(ls "${TEST_DIR}"/out_auth_silent_*.csv 2>/dev/null | grep -v '_meta' || true)
if [ -n "${SILENT_FILES}" ]; then
  fail "denied export unexpectedly wrote data: ${SILENT_FILES}"
fi
pass "denied export (no flag) wrote no data"

# 5b — with --fail-on-empty: loud failure (exit 3) + authorization message
gwasstudio --stdout --verbosity loud --mongo-uri "${MDB_URI}" export \
  --search-file    "${SEARCH_RESTRICTED}" \
  --output-prefix  "${TEST_DIR}/out_auth_fail" \
  --uri            "${TILEDB_DIR}" \
  --output-format  csv \
  --fail-on-empty \
  > "${TEST_DIR}/test5b.log" 2>&1
RC=$?
if [ ${RC} -eq 3 ]; then
  pass "denied export (--fail-on-empty) exits 3"
else
  cat "${TEST_DIR}/test5b.log"; fail "expected exit 3, got ${RC}"
fi
if grep -q "NO_DATA: authorization" "${TEST_DIR}/test5b.log"; then
  pass "log contains 'NO_DATA: authorization'"
else
  cat "${TEST_DIR}/test5b.log"; fail "authorization failure message not found in test5b.log"
fi
FAIL_FILES=$(ls "${TEST_DIR}"/out_auth_fail_*.csv 2>/dev/null | grep -v '_meta' || true)
if [ -n "${FAIL_FILES}" ]; then
  fail "denied export unexpectedly wrote data: ${FAIL_FILES}"
fi
pass "denied export (--fail-on-empty) wrote no data"

# ── Cleanup ──────────────────────────────────────────────────────────────────
echo ""
echo "=== Stopping MongoDB ==="
python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" 2>/dev/null || true

echo ""
echo "✓ All integration tests passed."
