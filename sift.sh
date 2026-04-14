#!/usr/bin/env bash

set -euo pipefail

REPORT_DIR="/tmp/sift_reports"
INPUT_LOG="system_raw.log"
REPORT_FILE="${REPORT_DIR}/final_report.csv"

# Create the report directory up front so downstream steps always have a known
# destination. We immediately lock it down with 700 permissions because reliability
# reports often contain operationally sensitive details that should remain
# owner-only.
mkdir -p "${REPORT_DIR}"
chmod 700 "${REPORT_DIR}"

if [[ ! -f "${INPUT_LOG}" ]]; then
  echo "Input log file not found: ${INPUT_LOG}" >&2
  exit 1
fi

# This pipeline intentionally uses pipes instead of temporary files.
# WHY: each stage can stream data directly into the next process in memory,
# which avoids extra disk I/O, reduces latency, and lowers the chance of
# leaving partially processed intermediate data behind on disk.
grep -E 'ERROR|CRITICAL' "${INPUT_LOG}" \
  | sed -r 's/[0-9]{1,3}(\.[0-9]{1,3}){3}/[IP_REDACTED]/g' \
  | awk -F' ' '{print $1, $2, $5, $0}' \
  | python3 process.py

if [[ ! -f "${REPORT_FILE}" ]]; then
  echo "Expected report was not created: ${REPORT_FILE}" >&2
  exit 1
fi

# Display just the "Error Type" CSV column. Using cut here keeps the post-step
# lightweight because we only need one field from the already structured report.
echo "$(cut -d',' -f2 "${REPORT_FILE}")"
