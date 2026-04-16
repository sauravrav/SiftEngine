#!/usr/bin/env bash

set -euo pipefail

REPORT_DIR="${SIFT_REPORT_DIR:-/tmp/sift_reports}"
INPUT_LOG="${SIFT_INPUT_LOG:-system_raw.log}"
REPORT_BASENAME="${SIFT_REPORT_BASENAME:-final_report}"
STATE_FILE="${SIFT_STATE_FILE:-${REPORT_DIR}/pipeline_state.env}"
ALERT_STATE_FILE="${SIFT_ALERT_STATE_FILE:-${REPORT_DIR}/alert_state.env}"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_EMAIL="${ALERT_EMAIL_TO:-}"
ALERT_COOLDOWN_SECONDS="${ALERT_COOLDOWN_SECONDS:-900}"
ALERT_ON_NO_TRANSPORT="${ALERT_ON_NO_TRANSPORT:-true}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_REPORT_FILE="${REPORT_DIR}/${REPORT_BASENAME}_${RUN_ID}.csv"
LATEST_REPORT_FILE="${REPORT_DIR}/${REPORT_BASENAME}.csv"

mkdir -p "${REPORT_DIR}"
chmod 700 "${REPORT_DIR}"

if [[ ! -f "${INPUT_LOG}" ]]; then
  echo "Input log file not found: ${INPUT_LOG}" >&2
  exit 1
fi

get_file_stats() {
  if stat -f '%i %z' "${INPUT_LOG}" >/dev/null 2>&1; then
    stat -f '%i %z' "${INPUT_LOG}"
    return 0
  fi

  stat -c '%i %s' "${INPUT_LOG}"
}

hash_text() {
  local content="$1"

  if command -v shasum >/dev/null 2>&1; then
    printf '%s' "${content}" | shasum -a 256 | awk '{print $1}'
    return 0
  fi

  if command -v sha256sum >/dev/null 2>&1; then
    printf '%s' "${content}" | sha256sum | awk '{print $1}'
    return 0
  fi

  python3 -c 'import hashlib, sys; print(hashlib.sha256(sys.stdin.read().encode()).hexdigest())' <<<"${content}"
}

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

should_send_alert() {
  local fingerprint="$1"
  local now_epoch="$2"
  local previous_fingerprint=""
  local previous_epoch=0

  if [[ -f "${ALERT_STATE_FILE}" ]]; then
    # shellcheck disable=SC1090
    source "${ALERT_STATE_FILE}"
    previous_fingerprint="${last_alert_fingerprint:-}"
    previous_epoch="${last_alert_epoch:-0}"
  fi

  if [[ "${fingerprint}" == "${previous_fingerprint}" ]] && (( now_epoch - previous_epoch < ALERT_COOLDOWN_SECONDS )); then
    return 1
  fi

  return 0
}

record_alert_state() {
  local fingerprint="$1"
  local now_epoch="$2"

  cat > "${ALERT_STATE_FILE}" <<EOF
last_alert_fingerprint='${fingerprint}'
last_alert_epoch='${now_epoch}'
EOF
}

send_critical_alerts() {
  local critical_lines="$1"
  local alert_message
  local fingerprint
  local now_epoch

  if [[ -z "${critical_lines}" ]]; then
    return 0
  fi

  fingerprint="$(hash_text "${critical_lines}")"
  now_epoch="$(date +%s)"

  if ! should_send_alert "${fingerprint}" "${now_epoch}"; then
    echo "Skipping duplicate CRITICAL alert within cooldown window." >&2
    return 0
  fi

  alert_message=$(
    cat <<EOF
SiftEngine detected CRITICAL errors in ${INPUT_LOG}.
Host: $(hostname)
Working directory: ${SCRIPT_DIR}
Report file: ${RUN_REPORT_FILE}
Recent CRITICAL summary:
${critical_lines}
EOF
  )

  if send_webhook_alert "${alert_message}"; then
    record_alert_state "${fingerprint}" "${now_epoch}"
    echo "Critical alert sent to webhook." >&2
    return 0
  fi

  if send_email_alert "${alert_message}"; then
    record_alert_state "${fingerprint}" "${now_epoch}"
    echo "Critical alert sent by email." >&2
    return 0
  fi

  if [[ "${ALERT_ON_NO_TRANSPORT}" == "true" ]]; then
    record_alert_state "${fingerprint}" "${now_epoch}"
    echo "Critical errors were found, but no alert transport succeeded." >&2
    echo "${alert_message}" >&2
  fi
}

current_inode=0
current_size=0
read -r current_inode current_size < <(get_file_stats)

last_inode=0
last_offset=0
if [[ -f "${STATE_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${STATE_FILE}"
  last_inode="${last_inode:-0}"
  last_offset="${last_offset:-0}"
fi

start_offset=0
if [[ "${current_inode}" == "${last_inode}" ]] && (( current_size >= last_offset )); then
  start_offset="${last_offset}"
fi

if (( current_size == start_offset )); then
  echo "No new log data to process."
  exit 0
fi

# This pipeline intentionally uses pipes instead of temporary files.
# WHY: each stage streams only the new log bytes directly into the next command
# in memory, which avoids extra disk I/O, reduces alert latency, and prevents
# temporary intermediate files from exposing sensitive operational data.
tail -c +"$((start_offset + 1))" "${INPUT_LOG}" \
  | { grep -E 'ERROR|CRITICAL' || true; } \
  | sed -r 's/[0-9]{1,3}(\.[0-9]{1,3}){3}/[IP_REDACTED]/g' \
  | awk -F' ' '{print $1, $2, $5, $0}' \
  | SIFT_RUN_ID="${RUN_ID}" SIFT_REPORT_DIR="${REPORT_DIR}" SIFT_REPORT_BASENAME="${REPORT_BASENAME}" python3 process.py

cat > "${STATE_FILE}" <<EOF
last_inode='${current_inode}'
last_offset='${current_size}'
EOF

if [[ ! -f "${RUN_REPORT_FILE}" ]]; then
  echo "Expected report was not created: ${RUN_REPORT_FILE}" >&2
  exit 1
fi

cp "${RUN_REPORT_FILE}" "${LATEST_REPORT_FILE}"

echo "$(cut -d',' -f2 "${RUN_REPORT_FILE}")"

critical_summary="$(awk -F',' 'NR > 1 && $2 == "CRITICAL" {gsub(/\r/, "", $0); print "- " $5 " (count=" $6 ")"}' "${RUN_REPORT_FILE}")"
send_critical_alerts "${critical_summary}"
