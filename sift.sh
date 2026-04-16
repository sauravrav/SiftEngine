#!/usr/bin/env bash

set -euo pipefail

REPORT_DIR="${SIFT_REPORT_DIR:-/tmp/sift_reports}"
INPUT_LOG="${SIFT_INPUT_LOG:-system_raw.log}"
LOG_FORMAT="${SIFT_LOG_FORMAT:-plain}"
REPORT_BASENAME="${SIFT_REPORT_BASENAME:-final_report}"
STATE_FILE="${SIFT_STATE_FILE:-${REPORT_DIR}/pipeline_state.env}"
ALERT_STATE_FILE="${SIFT_ALERT_STATE_FILE:-${REPORT_DIR}/alert_state.env}"
METRICS_FILE="${SIFT_METRICS_FILE:-${REPORT_DIR}/metrics.prom}"
STATUS_FILE="${SIFT_STATUS_FILE:-${REPORT_DIR}/latest_status.json}"
LOCK_FILE="${SIFT_LOCK_FILE:-${REPORT_DIR}/sift.lock}"
LOCK_DIR="${SIFT_LOCK_DIR:-${REPORT_DIR}/sift.lockdir}"
WEBHOOK_URL="${ALERT_WEBHOOK_URL:-}"
ALERT_EMAIL="${ALERT_EMAIL_TO:-}"
ALERT_COOLDOWN_SECONDS="${ALERT_COOLDOWN_SECONDS:-900}"
ALERT_MAX_RETRIES="${ALERT_MAX_RETRIES:-3}"
ALERT_RETRY_DELAY_SECONDS="${ALERT_RETRY_DELAY_SECONDS:-2}"
ALERT_ON_NO_TRANSPORT="${ALERT_ON_NO_TRANSPORT:-true}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_ID="$(date -u '+%Y%m%dT%H%M%SZ')"
RUN_REPORT_FILE="${REPORT_DIR}/${REPORT_BASENAME}_${RUN_ID}.csv"
LATEST_REPORT_FILE="${REPORT_DIR}/${REPORT_BASENAME}.csv"
RUN_EXIT_CODE=0
RUN_SUMMARY_FILE="${REPORT_DIR}/run_summary_${RUN_ID}.json"
LOCK_MODE="none"
LOCK_FD=0

mkdir -p "${REPORT_DIR}"
chmod 700 "${REPORT_DIR}"

if [[ ! -f "${INPUT_LOG}" ]]; then
  echo "Input log file not found: ${INPUT_LOG}" >&2
  exit 1
fi

write_status_file() {
  local run_status="$1"
  local processed_rows="${2:-0}"
  local critical_rows="${3:-0}"
  local alerts_sent="${4:-0}"
  local duplicate_alerts="${5:-0}"
  local transport_failures="${6:-0}"
  local alert_transport="${7:-none}"
  local report_file_path="${8:-${RUN_REPORT_FILE}}"

  python3 - <<PY
import json
from pathlib import Path

status = {
    "run_id": "${RUN_ID}",
    "status": "${run_status}",
    "input_log": "${INPUT_LOG}",
    "log_format": "${LOG_FORMAT}",
    "report_file": "${report_file_path}",
    "processed_rows": int("${processed_rows}"),
    "critical_rows": int("${critical_rows}"),
    "alerts_sent": int("${alerts_sent}"),
    "duplicate_alerts_suppressed": int("${duplicate_alerts}"),
    "alert_transport_failures": int("${transport_failures}"),
    "alert_transport": "${alert_transport}",
}
Path("${STATUS_FILE}").write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
PY
}

cleanup() {
  local exit_code="$?"

  if [[ "${LOCK_MODE}" == "flock" ]]; then
    flock -u "${LOCK_FD}" || true
    eval "exec ${LOCK_FD}>&-"
  elif [[ "${LOCK_MODE}" == "mkdir" ]]; then
    rmdir "${LOCK_DIR}" >/dev/null 2>&1 || true
  fi

  if (( exit_code != 0 )) && [[ ! -f "${STATUS_FILE}" ]]; then
    write_status_file "failed" 0 0 0 0 0 "none"
  fi

  exit "${exit_code}"
}

trap cleanup EXIT

acquire_lock() {
  if command -v flock >/dev/null 2>&1; then
    LOCK_FD=9
    eval "exec ${LOCK_FD}>\"${LOCK_FILE}\""
    if ! flock -n "${LOCK_FD}"; then
      echo "Another SiftEngine run is already in progress." >&2
      write_status_file "skipped_locked" 0 0 0 0 0 "none"
      exit 0
    fi
    LOCK_MODE="flock"
    return 0
  fi

  if mkdir "${LOCK_DIR}" 2>/dev/null; then
    LOCK_MODE="mkdir"
    return 0
  fi

  echo "Another SiftEngine run is already in progress." >&2
  write_status_file "skipped_locked" 0 0 0 0 0 "none"
  exit 0
}

acquire_lock

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

retry_command() {
  local attempt=1

  while true; do
    if "$@"; then
      return 0
    fi

    if (( attempt >= ALERT_MAX_RETRIES )); then
      return 1
    fi

    sleep "${ALERT_RETRY_DELAY_SECONDS}"
    attempt=$((attempt + 1))
  done
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

  retry_command curl --silent --show-error --fail \
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
    retry_command bash -lc "printf '%s\n' \"\$1\" | mail -s 'SiftEngine critical alert' \"\$2\"" -- "${alert_message}" "${ALERT_EMAIL}"
    return $?
  fi

  if command -v sendmail >/dev/null 2>&1; then
    retry_command bash -lc "{
      printf 'Subject: SiftEngine critical alert\n'
      printf 'To: %s\n' \"\$2\"
      printf 'Content-Type: text/plain; charset=UTF-8\n'
      printf '\n'
      printf '%s\n' \"\$1\"
    } | sendmail \"\$2\"" -- "${alert_message}" "${ALERT_EMAIL}"
    return $?
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

  ALERTS_SENT=0
  DUPLICATE_ALERTS=0
  ALERT_TRANSPORT_FAILURES=0
  ALERT_TRANSPORT="none"

  if [[ -z "${critical_lines}" ]]; then
    return 0
  fi

  fingerprint="$(hash_text "${critical_lines}")"
  now_epoch="$(date +%s)"

  if ! should_send_alert "${fingerprint}" "${now_epoch}"; then
    DUPLICATE_ALERTS=1
    echo "Skipping duplicate CRITICAL alert within cooldown window." >&2
    return 0
  fi

  alert_message=$(
    cat <<EOF
SiftEngine detected CRITICAL errors in ${INPUT_LOG}.
Host: $(hostname)
Working directory: ${SCRIPT_DIR}
Log format: ${LOG_FORMAT}
Report file: ${RUN_REPORT_FILE}
Recent CRITICAL summary:
${critical_lines}
EOF
  )

  if send_webhook_alert "${alert_message}"; then
    record_alert_state "${fingerprint}" "${now_epoch}"
    ALERTS_SENT=1
    ALERT_TRANSPORT="webhook"
    echo "Critical alert sent to webhook." >&2
    return 0
  fi

  if send_email_alert "${alert_message}"; then
    record_alert_state "${fingerprint}" "${now_epoch}"
    ALERTS_SENT=1
    ALERT_TRANSPORT="email"
    echo "Critical alert sent by email." >&2
    return 0
  fi

  ALERT_TRANSPORT_FAILURES=1
  if [[ "${ALERT_ON_NO_TRANSPORT}" == "true" ]]; then
    record_alert_state "${fingerprint}" "${now_epoch}"
    echo "Critical errors were found, but no alert transport succeeded." >&2
    echo "${alert_message}" >&2
  fi
}

write_metrics_file() {
  local processed_rows="$1"
  local critical_rows="$2"
  local alerts_sent="$3"
  local duplicate_alerts="$4"
  local transport_failures="$5"

  cat > "${METRICS_FILE}" <<EOF
# HELP siftengine_processed_rows Number of summarized log rows written in the latest report.
# TYPE siftengine_processed_rows gauge
siftengine_processed_rows ${processed_rows}
# HELP siftengine_critical_rows Number of critical rows written in the latest report.
# TYPE siftengine_critical_rows gauge
siftengine_critical_rows ${critical_rows}
# HELP siftengine_alerts_sent Number of alerts sent in the latest run.
# TYPE siftengine_alerts_sent gauge
siftengine_alerts_sent ${alerts_sent}
# HELP siftengine_duplicate_alerts_suppressed Number of alerts suppressed by cooldown in the latest run.
# TYPE siftengine_duplicate_alerts_suppressed gauge
siftengine_duplicate_alerts_suppressed ${duplicate_alerts}
# HELP siftengine_alert_transport_failures Number of alert transport failures in the latest run.
# TYPE siftengine_alert_transport_failures gauge
siftengine_alert_transport_failures ${transport_failures}
EOF
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
  write_status_file "idle" 0 0 0 0 0 "none" "${LATEST_REPORT_FILE}"
  echo "No new log data to process."
  exit 0
fi

# This pipeline intentionally uses pipes instead of temporary files.
# WHY: each stage streams only the new log bytes directly into the next command
# in memory, which avoids extra disk I/O, reduces alert latency, and prevents
# temporary intermediate files from exposing sensitive operational data.
if [[ "${LOG_FORMAT}" == "plain" ]]; then
  tail -c +"$((start_offset + 1))" "${INPUT_LOG}" \
    | { grep -E 'ERROR|CRITICAL' || true; } \
    | sed -r 's/[0-9]{1,3}(\.[0-9]{1,3}){3}/[IP_REDACTED]/g' \
    | awk -F' ' '{print $1, $2, $5, $0}' \
    | SIFT_RUN_ID="${RUN_ID}" \
        SIFT_REPORT_DIR="${REPORT_DIR}" \
        SIFT_REPORT_BASENAME="${REPORT_BASENAME}" \
        SIFT_LOG_FORMAT="${LOG_FORMAT}" \
        python3 process.py
else
  tail -c +"$((start_offset + 1))" "${INPUT_LOG}" \
    | SIFT_RUN_ID="${RUN_ID}" \
        SIFT_REPORT_DIR="${REPORT_DIR}" \
        SIFT_REPORT_BASENAME="${REPORT_BASENAME}" \
        SIFT_LOG_FORMAT="${LOG_FORMAT}" \
        python3 process.py
fi

cat > "${STATE_FILE}" <<EOF
last_inode='${current_inode}'
last_offset='${current_size}'
EOF

if [[ ! -f "${RUN_REPORT_FILE}" ]]; then
  echo "Expected report was not created: ${RUN_REPORT_FILE}" >&2
  write_status_file "failed" 0 0 0 0 0 "none"
  exit 1
fi

cp "${RUN_REPORT_FILE}" "${LATEST_REPORT_FILE}"

echo "$(cut -d',' -f2 "${RUN_REPORT_FILE}")"

PROCESSED_ROWS="$(awk -F',' 'NR > 1 {count++} END {print count + 0}' "${RUN_REPORT_FILE}")"
CRITICAL_ROWS="$(awk -F',' 'NR > 1 && $2 == "CRITICAL" {count++} END {print count + 0}' "${RUN_REPORT_FILE}")"
critical_summary="$(awk -F',' 'NR > 1 && $2 == "CRITICAL" {gsub(/\r/, "", $0); print "- " $5 " (count=" $6 ")"}' "${RUN_REPORT_FILE}")"

send_critical_alerts "${critical_summary}"
write_metrics_file "${PROCESSED_ROWS}" "${CRITICAL_ROWS}" "${ALERTS_SENT:-0}" "${DUPLICATE_ALERTS:-0}" "${ALERT_TRANSPORT_FAILURES:-0}"
write_status_file "ok" "${PROCESSED_ROWS}" "${CRITICAL_ROWS}" "${ALERTS_SENT:-0}" "${DUPLICATE_ALERTS:-0}" "${ALERT_TRANSPORT_FAILURES:-0}" "${ALERT_TRANSPORT:-none}"
