#!/usr/bin/env python3

import csv
import json
import os
import re
import sys
from collections import OrderedDict
from pathlib import Path


REPORT_DIR = Path(os.environ.get("SIFT_REPORT_DIR", "/tmp/sift_reports"))
REPORT_BASENAME = os.environ.get("SIFT_REPORT_BASENAME", "final_report")
RUN_ID = os.environ.get("SIFT_RUN_ID", "manual")
LOG_FORMAT = os.environ.get("SIFT_LOG_FORMAT", "plain")
REPORT_FILE = REPORT_DIR / f"{REPORT_BASENAME}_{RUN_ID}.csv"
ERROR_TYPE_PATTERN = re.compile(r"\b(ERROR|CRITICAL)\b")
IPV4_PATTERN = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")


def redact_ipv4(value: str) -> str:
    return IPV4_PATTERN.sub("[IP_REDACTED]", value)


def parse_plain_stream_line(raw_line: str) -> tuple[str, str, str, str, str] | None:
    """Parse a streamed plain-text record arriving from the Bash pipeline."""
    stripped = raw_line.strip()
    if not stripped:
        return None

    parts = stripped.split(maxsplit=3)
    if len(parts) < 4:
        return None

    date_value, time_value, process_id, full_line = parts
    error_type_match = ERROR_TYPE_PATTERN.search(full_line)
    if not error_type_match:
        return None

    error_type = error_type_match.group(1)
    error_message = full_line.split(error_type, 1)[1].strip(" :-")
    if not error_message:
        error_message = full_line

    return date_value, time_value, process_id, error_type, error_message


def parse_json_stream_line(raw_line: str) -> tuple[str, str, str, str, str] | None:
    """Parse one JSON log record and normalize it into the same CSV schema."""
    stripped = raw_line.strip()
    if not stripped:
        return None

    payload = json.loads(stripped)
    severity = str(payload.get("severity") or payload.get("level") or "").upper()
    if severity not in {"ERROR", "CRITICAL"}:
        return None

    timestamp = str(payload.get("timestamp") or payload.get("time") or "")
    if "T" in timestamp:
        date_value, remainder = timestamp.split("T", 1)
        time_value = remainder.split("Z", 1)[0].split("+", 1)[0]
    else:
        timestamp_parts = timestamp.split(maxsplit=1)
        date_value = timestamp_parts[0] if timestamp_parts else "UNKNOWN_DATE"
        time_value = timestamp_parts[1] if len(timestamp_parts) > 1 else "UNKNOWN_TIME"

    process_id = str(payload.get("process_id") or payload.get("pid") or payload.get("service") or "UNKNOWN_PID")
    message = str(payload.get("message") or payload.get("error") or payload)
    message = redact_ipv4(message)

    for candidate_key in ("client_ip", "source_ip", "remote_ip", "ip"):
        if candidate_key in payload and payload[candidate_key] is not None:
            message = f"{message} {candidate_key}={redact_ipv4(str(payload[candidate_key]))}"

    return date_value, time_value, process_id, severity, message


def parse_stream_line(raw_line: str) -> tuple[str, str, str, str, str] | None:
    if LOG_FORMAT == "json":
        return parse_json_stream_line(raw_line)
    return parse_plain_stream_line(raw_line)


def build_summary(
    parsed_rows: list[tuple[str, str, str, str, str]],
) -> OrderedDict[str, dict[str, str | int]]:
    """Collapse repeated log entries into one CSV row per unique error message."""
    summary: OrderedDict[str, dict[str, str | int]] = OrderedDict()

    for date_value, time_value, process_id, error_type, error_message in parsed_rows:
        if error_message not in summary:
            summary[error_message] = {
                "date": date_value,
                "error_type": error_type,
                "time": time_value,
                "process_id": process_id,
                "count": 0,
            }

        summary[error_message]["count"] += 1

    return summary


def write_report(summary: OrderedDict[str, dict[str, str | int]]) -> Path:
    """Persist the summary into a timestamped CSV file for later inspection."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    with REPORT_FILE.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Date", "Error Type", "Time", "Process ID", "Error Message", "Count"])

        for error_message, metadata in summary.items():
            writer.writerow(
                [
                    metadata["date"],
                    metadata["error_type"],
                    metadata["time"],
                    metadata["process_id"],
                    error_message,
                    metadata["count"],
                ]
            )

    return REPORT_FILE


def main() -> int:
    # We consume stdin directly because it lets the shell pipeline stream log
    # events into Python without creating temporary files.
    # WHY: pipes keep the data moving in memory, so we avoid extra disk I/O,
    # reduce processing latency, and minimize the number of intermediate files
    # that could expose sensitive operational data.
    parsed_rows: list[tuple[str, str, str, str, str]] = []

    for line in sys.stdin:
        parsed = parse_stream_line(line)
        if parsed is not None:
            parsed_rows.append(parsed)

    summary = build_summary(parsed_rows)
    write_report(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
