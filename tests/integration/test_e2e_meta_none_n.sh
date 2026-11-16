#!/usr/bin/env bash
# End-to-end reproduction of the user's report:
#   --get-regions-snps with N in --attr should yield an N column in BOTH
#   --meta none and --meta full. (N is synthesized from metadata total_samples.)
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

TEST_DIR="tests/integration/tests/e2e_meta_none_n"
MDB_URI="mongodb://localhost:27018/e2e_meta_none_n"
TILEDB_DIR="${TEST_DIR}/tileDB"
MONGO_UTILS="${PROJECT_ROOT}/tests/integration/mongo_test_utils.py"

rm -rf "${TEST_DIR}"
mkdir -p "${TILEDB_DIR}"

# wait for any previous mongod on 27018 to release the port
for i in $(seq 1 30); do
  if ! nc -z localhost 27018 2>/dev/null; then break; fi
  sleep 1
done
python "${MONGO_UTILS}" start --port 27018 --dbpath "${TEST_DIR}/mongo_db" \
  --logpath "${TEST_DIR}/mongod.log" --pid-file "${TEST_DIR}/mongod.pid" >/dev/null
for i in $(seq 1 60); do
  if python -c "from pymongo import MongoClient; MongoClient('${MDB_URI}', serverSelectionTimeoutMS=1000).admin.command('ping')" >/dev/null 2>&1; then break; fi
  sleep 1
done

cleanup() { python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# data WITHOUT an N column, but metadata has total_samples=10000
# (mirrors the known-good test_n_fallback.sh fixture)
cat > "${TEST_DIR}/data.tsv" << 'EOF'
CHR	POS	EA	NEA	EAF	SE	BETA
1	100	A	T	0.4	0.5	2.0
1	200	G	A	0.3	0.5	1.0
2	300	C	G	0.2	0.4	0.8
2	400	T	A	0.1	0.2	0.4
EOF
gzip -c "${TEST_DIR}/data.tsv" > "${TEST_DIR}/data.tsv.gz"

cat > "${TEST_DIR}/metadata.tsv" << EOF
project	study	category	file_path	access_level	warehouse_uri	notes_consortium	notes_sex	notes_source_id	build	population	total_variants	total_samples	total_cases	total_controls	trait_desc	trait_ontology_ids
e2e_n	study_a	GWAS	${TEST_DIR}/data.tsv.gz	public	${TILEDB_DIR}		Combined	e2e_n-1	GRCh37	EUR	4	10000	5000	5000	E2E trait	EFO:0000001
EOF

cat > "${TEST_DIR}/search.yml" << 'EOF'
query_fields:
  notes_source_id: e2e_n-1
output_fields:
  - total_samples
EOF

# region file covering chr1 and chr2 variants (BED format: NO header row, tab-separated)
cat > "${TEST_DIR}/region.tsv" << 'EOF'
1	1	300
2	250	450
EOF

# NOTE: no --workers 0 on ingest (batch_size would be 0 -> range() crash in ingest.py);
# the user's --workers 0 flag applies to the *export* command, which handles it fine.
gwasstudio --stdout --mongo-uri "${MDB_URI}" ingest \
  --file-path "${TEST_DIR}/metadata.tsv" --uri "${TILEDB_DIR}" > "${TEST_DIR}/ingest.log" 2>&1

echo "============================================================"
echo "Export with --meta none  (user's reported failing case)"
gwasstudio --stdout --workers 0 --verbosity loud --mongo-uri "${MDB_URI}" export \
  --search-file "${TEST_DIR}/search.yml" \
  --get-regions-snps "${TEST_DIR}/region.tsv" \
  --attr "BETA,SE,EAF,MLOG10P,EA,NEA,N" \
  --region-buffer 100000 \
  --meta none \
  --output-format csv \
  --output-prefix "${TEST_DIR}/out_none" > "${TEST_DIR}/export_none.log" 2>&1

echo "------------------------------------------------------------"
echo "Export with --meta full  (control, used to work)"
gwasstudio --stdout --workers 0 --verbosity loud --mongo-uri "${MDB_URI}" export \
  --search-file "${TEST_DIR}/search.yml" \
  --get-regions-snps "${TEST_DIR}/region.tsv" \
  --attr "BETA,SE,EAF,MLOG10P,EA,NEA,N" \
  --region-buffer 100000 \
  --meta full \
  --output-format csv \
  --output-prefix "${TEST_DIR}/out_full" >/dev/null 2>&1

report() {
  local label="$1"; local file="$2"
  echo ""
  echo "### ${label}"
  if [ ! -f "${file}" ]; then echo "  (no output file: ${file})"; return; fi
  echo "  Header: $(head -1 "${file}")"
  local idx
  idx=$(head -1 "${file}" | tr ',' '\n' | grep -nx "N" | head -1 | cut -d: -f1)
  if [ -n "${idx}" ]; then
    local v; v=$(tail -n +2 "${file}" | awk -F',' -v c="${idx}" '{print $c}' | head -1)
    echo "  => N column PRESENT (col ${idx}), value = ${v}"
  else
    echo "  => N column MISSING"
  fi
}

report "--meta none" "${TEST_DIR}/out_none_e2e_n_study_a_e2e_n-1.csv"
report "--meta full" "${TEST_DIR}/out_full_e2e_n_study_a_e2e_n-1.csv"
