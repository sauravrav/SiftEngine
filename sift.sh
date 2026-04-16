#!/usr/bin/env bash

set -euo pipefail

REPORT_DIR="/tmp/sift_reports"
INPUT_LOG="system_raw.log"
REPORT_FILE="${REPORT_DIR}/final_report.csv"
STATE_FILE="${REPORT_DIR}/last_read.offset"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_EMAIL="${ALERT_EMAIL_TO:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "${REPORT_DIR}"
chmod 700 "${REPORT_DIR}"

if [[ ! -f "${INPUT_LOG}" ]]; then
  echo "Input log file not found: ${INPUT_LOG}" >&2
  exit 1
fi

send_webhook_alert() {
  local alert_message="$1"
  local payload

  if [[ -z "${WEBHOOK_URL}" ]]; then
    return 1
  fi

  if ! command -v curl >/dev/null 2>&1; then
    echo "curl is required for webhook alerts but was not found." >&2
    return 1
  fi

  payload="$(python3 -c 'import json, sys; print(json.dumps({"content": sys.argv[1]}))' "${alert_message}")"

  curl --silent --show-error --fail \
    -H "Content-Type: application/json" \
    -d "${payload}" \
    "${WEBHOOK_URL}" >/dev/null
}

send_email_alert() {
  local alert_message="$1"

  if [[ -z "${ALERT_EMAIL}" ]]; then
    return 1
  fi

  if command -v mail >/dev/null 2>&1; then
    printf '%s\n' "${alert_message}" | mail -s "SiftEngine critical alert" "${ALERT_EMAIL}"
    return 0
  fi

  if command -v sendmail >/dev/null 2>&1; then
    {
      printf 'Subject: SiftEngine critical alert\n'
      printf 'To: %s\n' "${ALERT_EMAIL}"
      printf 'Content-Type: text/plain; charset=UTF-8\n'
      printf '\n'
      printf '%s\n' "${alert_message}"
    } | sendmail "${ALERT_EMAIL}"
    return 0
  fi

  echo "No supported local email command was found for ALERT_EMAIL_TO." >&2
  return 1
}

send_critical_alerts() {
  local critical_lines="$1"
  local alert_message

  if [[ -z "${critical_lines}" ]]; then
    return 0
  fi

  alert_message=$(
    cat <<EOF
SiftEngine detected CRITICAL errors in ${INPUT_LOG}.
Host: $(hostname)
Working directory: ${SCRIPT_DIR}
Recent CRITICAL summary:
${critical_lines}
EOF
  )

  if send_webhook_alert "${alert_message}"; then
    echo "Critical alert sent to webhook." >&2
    return 0
  fi

  if send_email_alert "${alert_message}"; then
    echo "Critical alert sent by email." >&2
    return 0
  fi

  echo "Critical errors were found, but no alert transport succeeded." >&2
  echo "${alert_message}" >&2
}

last_offset=0
if [[ -f "${STATE_FILE}" ]]; then
  read -r last_offset < "${STATE_FILE}" || last_offset=0
fi

current_size="$(wc -c < "${INPUT_LOG}")"
if (( current_size < last_offset )); then
  # Log rotation or truncation happened, so we restart from byte zero rather
  # than skipping fresh data.
  last_offset=0
fi

if (( current_size == last_offset )); then
  echo "No new log data to process."
  exit 0
fi

# This pipeline intentionally uses pipes instead of temporary files.
# WHY: each stage can stream fresh log data directly into the next process in
# memory, which reduces disk I/O, lowers latency for alerts, and avoids leaving
# partially processed operational data behind on disk as intermediate files.
tail -c +"$((last_offset + 1))" "${INPUT_LOG}" \
  | { grep -E 'ERROR|CRITICAL' || true; } \
  | sed -r 's/[0-9]{1,3}(\.[0-9]{1,3}){3}/[IP_REDACTED]/g' \
  | awk -F' ' '{print $1, $2, $5, $0}' \
  | python3 process.py

printf '%s\n' "${current_size}" > "${STATE_FILE}"

if [[ ! -f "${REPORT_FILE}" ]]; then
  echo "Expected report was not created: ${REPORT_FILE}" >&2
  exit 1
fi

echo "$(cut -d',' -f2 "${REPORT_FILE}")"

critical_summary="$(awk -F',' 'NR > 1 && $2 == "CRITICAL" {gsub(/\r/, "", $0); print "- " $5 " (count=" $6 ")"}' "${REPORT_FILE}")"
send_critical_alerts "${critical_summary}"
