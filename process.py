#!/usr/bin/env python3

import csv
import os
import re
import sys
from collections import OrderedDict
from pathlib import Path


REPORT_DIR = Path(os.environ.get("SIFT_REPORT_DIR", "/tmp/sift_reports"))
REPORT_BASENAME = os.environ.get("SIFT_REPORT_BASENAME", "final_report")
RUN_ID = os.environ.get("SIFT_RUN_ID", "manual")
REPORT_FILE = REPORT_DIR / f"{REPORT_BASENAME}_{RUN_ID}.csv"
ERROR_TYPE_PATTERN = re.compile(r"\b(ERROR|CRITICAL)\b")


def parse_stream_line(raw_line: str) -> tuple[str, str, str, str, str] | None:
    """Parse a single streamed record arriving from the Bash pipeline.

    The shell pipeline uses awk to prepend:
    1. date
    2. time
    3. process identifier
    4. the full original log line

    Keeping parsing in Python makes it much easier to maintain summarization
    rules than pushing every transformation into shell one-liners.
    """
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
