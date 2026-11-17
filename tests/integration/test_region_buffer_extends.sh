#!/usr/bin/env bash
# Integration test: --region-buffer extends the *returned* window.
#
#   T1. region with no variants inside,  buffer=0      -> 0 rows
#   T2. same region, buffer=100kb        -> flanking variants are returned
#   T3. region covering the variants,    buffer=0      -> variants returned (control)
#   T4. empty region + buffer + --pvalue-filt -> flag computed on the REQUESTED
#       region (no variants inside) -> False -> 0 rows, even though a
#       significant variant sits in the buffered window
#   T5. region with a significant variant + buffer + --pvalue-filt
#       -> flag True -> requested region plus its buffer context returned;
#       flag file reports the ORIGINAL region bounds
#
# Fixture (synthetic, no N column):
#   chr1:100  BETA=3.0 SE=0.5  z=6   MLOG10P~8.7  (passes --pvalue-filt 5)
#   chr1:200  BETA=1.0 SE=0.5  z=2   MLOG10P~1.3  (does not pass)
#   chr2:300  BETA=2.0 SE=0.4  z=5   MLOG10P~6.0
#   chr2:400  BETA=0.8 SE=0.4  z=2   MLOG10P~1.3
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

TEST_DIR="tests/integration/tests/region_buffer_extends"
MDB_URI="mongodb://localhost:27020/region_buffer_extends"
TILEDB_DIR="${TEST_DIR}/tileDB"
MONGO_UTILS="${PROJECT_ROOT}/tests/integration/mongo_test_utils.py"

rm -rf "${TEST_DIR}"
mkdir -p "${TILEDB_DIR}"

# wait for any previous mongod on 27020 to release the port
for i in $(seq 1 30); do
  if ! nc -z localhost 27020 2>/dev/null; then break; fi
  sleep 1
done
python "${MONGO_UTILS}" start --port 27020 --dbpath "${TEST_DIR}/mongo_db" \
  --logpath "${TEST_DIR}/mongod.log" --pid-file "${TEST_DIR}/mongod.pid" >/dev/null
for i in $(seq 1 60); do
  if python -c "from pymongo import MongoClient; MongoClient('${MDB_URI}', serverSelectionTimeoutMS=1000).admin.command('ping')" >/dev/null 2>&1; then break; fi
  sleep 1
done

cleanup() { python "${MONGO_UTILS}" stop --pid-file "${TEST_DIR}/mongod.pid" >/dev/null 2>&1 || true; }
trap cleanup EXIT

cat > "${TEST_DIR}/data.tsv" << 'EOF'
CHR	POS	EA	NEA	EAF	SE	BETA
1	100	A	T	0.4	0.5	3.0
1	200	G	A	0.3	0.5	1.0
2	300	C	G	0.2	0.4	2.0
2	400	T	A	0.1	0.2	0.8
EOF
gzip -c "${TEST_DIR}/data.tsv" > "${TEST_DIR}/data.tsv.gz"

cat > "${TEST_DIR}/metadata.tsv" << EOF
project	study	category	file_path	access_level	warehouse_uri	notes_consortium	notes_sex	notes_source_id	build	population	total_variants	total_samples	total_cases	total_controls	trait_desc	trait_ontology_ids
buf	study_a	GWAS	${TEST_DIR}/data.tsv.gz	public	${TILEDB_DIR}		Combined	buf-1	GRCh37	EUR	4	10000	5000	5000	Buffer trait	EFO:0000001
EOF

cat > "${TEST_DIR}/search.yml" << 'EOF'
query_fields:
  notes_source_id: buf-1
output_fields:
  - total_samples
EOF

PASS=0
FAIL=0
ok()  { echo "PASS: $1"; PASS=$((PASS+1)); }
bad() { echo "FAIL: $1"; FAIL=$((FAIL+1)); }

# run_export <prefix> <region> <buffer> [extra args...]
run_export() {
  local prefix="$1" region="$2" buffer="$3"; shift 3
  gwasstudio --stdout --workers 0 --mongo-uri "${MDB_URI}" export \
    --search-file       "${TEST_DIR}/search.yml" \
    --get-regions-snps  "${region}" \
    --region-buffer     "${buffer}" \
    --attr              "BETA,SE,EAF,MLOG10P,EA,NEA" \
    --meta              none \
    --output-format     csv \
    --output-prefix     "${TEST_DIR}/${prefix}" \
    "$@" > "${TEST_DIR}/${prefix}.log" 2>&1
}

# data_rows <prefix> -> data rows (header excluded) of the main data file, or "" if none
data_rows() {
  local f
  f=$(ls "${TEST_DIR}"/$1_*.csv 2>/dev/null | grep -v '_meta' | grep -v '_pvalue_filt' | head -1)
  [ -z "$f" ] && return 0
  tail -n +2 "$f"
}

# positions <prefix> -> space separated POS values (3rd column)
positions() { data_rows "$1" | awk -F',' 'NF {print $3}' | tr '\n' ' '; }

# has_pos <prefix> <pos> -> 0 (true) if the given POS appears in the data rows
has_pos() { positions "$1" | tr ' ' '\n' | grep -qx "$2" 2>/dev/null; }

flag_row() { # flag_row <prefix> -> the single flag line (START<TAB>END<TAB>FLAG)
  local f
  f=$(ls "${TEST_DIR}"/$1_*_pvalue_filt.csv 2>/dev/null | head -1)
  [ -z "$f" ] && return 0
  tail -n +2 "$f"
}

# ── Ingest ───────────────────────────────────────────────────────────────────
gwasstudio --stdout --mongo-uri "${MDB_URI}" ingest \
  --file-path "${TEST_DIR}/metadata.tsv" --uri "${TILEDB_DIR}" \
  > "${TEST_DIR}/ingest.log" 2>&1
if [ $? -ne 0 ]; then echo "ingest failed:"; cat "${TEST_DIR}/ingest.log"; exit 1; fi

# ── T1: empty region, no buffer → 0 rows ─────────────────────────────────────
run_export "t1" "1,250,350" 0
n=$(data_rows "t1" | grep -c . || true)
[ "${n:-0}" -eq 0 ] && ok "T1: region 250-350 (no variants), buffer=0 → 0 rows" \
                    || bad "T1: expected 0 rows, got ${n}"

# ── T2: same region, buffer=100kb → flanking variants returned ───────────────
run_export "t2" "1,250,350" 100000
pos=$(positions "t2")
n=$(data_rows "t2" | grep -c . || true)
[ "${n:-0}" -eq 2 ] && ok "T2: region 250-350 + buffer 100kb → 2 context rows" \
                    || bad "T2: expected 2 rows, got ${n:-0}"
echo "  POS = ${pos:-<none>}"
if has_pos "t2" 100 && has_pos "t2" 200; then
  ok "T2: buffer pulled in flanking variants 100 and 200"
else
  bad "T2: expected POS 100 and 200, got '${pos}'"
fi

# ── T3: region covering variants, no buffer → unchanged behavior ────────────
run_export "t3" "1,50,250" 0
pos=$(positions "t3")
n=$(data_rows "t3" | grep -c . || true)
[ "${n:-0}" -eq 2 ] && ok "T3: region 50-250, buffer=0 → 2 rows (control)" \
                    || bad "T3: expected 2 rows, got ${n:-0}"
if has_pos "t3" 100 && has_pos "t3" 200; then
  ok "T3: POS 100 and 200 returned"
else
  bad "T3: expected POS 100 and 200, got '${pos}'"
fi

# ── T4: flag anchored to the REQUESTED region ────────────────────────────────
# chr1:100 (MLOG10P~8.7) is inside the buffered window but OUTSIDE the
# requested region 250-350 → flag must be False → no rows in output.
run_export "t4" "1,250,350" 100000 --pvalue-filt 5
n=$(data_rows "t4" | grep -c . || true)
[ "${n:-0}" -eq 0 ] && ok "T4: significant variant in buffer only → excluded (flag on requested region)" \
                    || bad "T4: expected 0 rows, got ${n}"
frow=$(flag_row "t4")
echo "  flag row: ${frow:-<none>}"
if [ "${frow}" = "1,250,350,False" ]; then
  ok "T4: flag reports original bounds 250-350, False"
else
  bad "T4: expected flag row '1,250,350,False', got '${frow}'"
fi

# ── T5: flag True → requested region + buffer context returned ──────────────
# chr1:100 is inside requested region 50-250 and significant → flag True →
# whole buffered window (1..100350) returned: POS 100 AND 200.
run_export "t5" "1,50,250" 100000 --pvalue-filt 5
pos=$(positions "t5")
n=$(data_rows "t5" | grep -c . || true)
[ "${n:-0}" -eq 2 ] && ok "T5: flag True → region + buffer context (2 rows)" \
                    || bad "T5: expected 2 rows, got ${n:-0}"
if has_pos "t5" 100 && has_pos "t5" 200; then
  ok "T5: POS 100 (in region) and 200 (buffer context) returned"
else
  bad "T5: expected POS 100 and 200, got '${pos}'"
fi
frow=$(flag_row "t5")
echo "  flag row: ${frow:-<none>}"
if [ "${frow}" = "1,50,250,True" ]; then
  ok "T5: flag reports original bounds 50-250, True"
else
  bad "T5: expected flag row '1,50,250,True', got '${frow}'"
fi

echo ""
echo "=============================================="
echo "Passed: ${PASS}   Failed: ${FAIL}"
if [ "${FAIL}" -gt 0 ]; then
  echo "=== region-buffer extension tests FAILED ==="
  exit 1
fi
echo "=== region-buffer extension tests PASSED ==="
