#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------
# Fetch + unpack DVSA monthly CSV ZIPs (results & items) for a year
#
# Usage:
#   bash scripts/fetch_and_unpack_dvsa.sh 2024 \
#     "https://example.com/MOT_TEST_RESULT_2024.zip" \
#     "https://example.com/MOT_TEST_ITEM_2024.zip"
#
# Or, if you already uploaded the zips into scripts/:
#   bash scripts/fetch_and_unpack_dvsa.sh 2024 "scripts/MOT_TEST_RESULT_2024.zip" "scripts/MOT_TEST_ITEM_2024.zip"
#
# Files are extracted into: apps/web/etl/data/
# Script:
#  - makes OUTDIR if needed
#  - downloads zips (with resume/retry) unless local file paths are given
#  - unzips into a temp dir, then moves/renames to OUTDIR
#  - verifies there are 12 test_result_YYYYMM.csv and 12 test_item_YYYYMM.csv
#  - cleans up temp
# ---------------------------------------------

YEAR="${1:-}"
RES_ZIP_SRC="${2:-}"
ITEM_ZIP_SRC="${3:-}"
OUTDIR="apps/web/etl/data"

if [[ -z "${YEAR}" || -z "${RES_ZIP_SRC}" || -z "${ITEM_ZIP_SRC}" ]]; then
  echo "Usage:"
  echo "  $0 <YEAR> <RESULTS_ZIP_URL|path.zip> <ITEMS_ZIP_URL|path.zip>"
  exit 1
fi

mkdir -p "${OUTDIR}"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

download_if_url () {
  local src="$1"
  local dest="$2"
  if [[ "${src}" =~ ^https?:// ]]; then
    echo "[INFO] Downloading ${src}"
    curl -L --fail --retry 5 -C - -o "${dest}" "${src}"
  else
    echo "[INFO] Using local zip: ${src}"
    cp -f "${src}" "${dest}"
  fi
}

RES_ZIP="${TMP}/results_${YEAR}.zip"
ITEM_ZIP="${TMP}/items_${YEAR}.zip"

download_if_url "${RES_ZIP_SRC}"  "${RES_ZIP}"
download_if_url "${ITEM_ZIP_SRC}" "${ITEM_ZIP}"

echo "[INFO] Unzipping results ZIP …"
unzip -q -d "${TMP}/res" "${RES_ZIP}" || { echo "[ERROR] Failed to unzip results"; exit 2; }
echo "[INFO] Unzipping items ZIP …"
unzip -q -d "${TMP}/itm" "${ITEM_ZIP}" || { echo "[ERROR] Failed to unzip items"; exit 2; }

# Move any CSVs we recognise into OUTDIR, normalising names:
# We search recursively to handle any nested folders in the zips.
shopt -s nullglob
echo "[INFO] Collecting monthly CSVs → ${OUTDIR}"

found=0
while IFS= read -r -d '' f; do
  base="$(basename "$f")"
  # Try to detect YYYYMM in the filename
  if [[ "$base" =~ ([0-9]{6}) ]]; then
    yyyymm="${BASH_REMATCH[1]}"
    # Heuristic: result vs item
    if [[ "$base" =~ [Rr]esult|RESULT|test_result|TEST_RESULT ]]; then
      dest="${OUTDIR}/test_result_${yyyymm}.csv"
    elif [[ "$base" =~ [Ii]tem|ITEM|test_item|TEST_ITEM ]]; then
      dest="${OUTDIR}/test_item_${yyyymm}.csv"
    else
      # fallback based on source tree
      case "$f" in
        *"/res/"*) dest="${OUTDIR}/test_result_${yyyymm}.csv" ;;
        *"/itm/"*) dest="${OUTDIR}/test_item_${yyyymm}.csv" ;;
        *)          dest="${OUTDIR}/${base}" ;;
      esac
    fi
    mv -f "$f" "$dest"
    echo "  -> $dest"
    ((found++)) || true
  fi
done < <(find "${TMP}/res" "${TMP}/itm" -type f -name '*.csv' -print0)

# Verify counts
res_count=$(ls -1 ${OUTDIR}/test_result_${YEAR}[0-9][0-9].csv 2>/dev/null | wc -l | tr -d ' ')
itm_count=$(ls -1 ${OUTDIR}/test_item_${YEAR}[0-9][0-9].csv 2>/dev/null | wc -l | tr -d ' ')

echo "[INFO] Found result files: ${res_count}"
echo "[INFO] Found item files:   ${itm_count}"

if [[ "${res_count}" -ne 12 || "${itm_count}" -ne 12 ]]; then
  echo "[WARN] Expected 12+12 monthly CSVs for ${YEAR}. Check the zip contents / names."
  echo "       OUTDIR listing:"
  ls -lh "${OUTDIR}" | sed -n "1,120p"
  exit 3
fi

echo "[OK] Unpacked ${res_count}+${itm_count} monthly CSVs for ${YEAR} into ${OUTDIR}"
