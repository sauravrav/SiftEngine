#!/usr/bin/env python3

import csv
import re
import sys
from collections import Counter
from pathlib import Path


REPORT_DIR = Path("/tmp/sift_reports")
REPORT_FILE = REPORT_DIR / "final_report.csv"
ERROR_TYPE_PATTERN = re.compile(r"\b(ERROR|CRITICAL)\b")


def parse_stream_line(raw_line: str) -> tuple[str, str, str, str, str] | None:
    """Parse one stdin record coming from the shell pipeline.

    The awk stage sends:
      1. the original date field
      2. the original time field
      3. the original process identifier field
      4. the full log line

    We keep those first three values as explicit columns and then inspect the
    original line to determine the error type and a normalized error message.
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


def main() -> int:
    # We consume stdin directly because it lets the Bash pipeline stream each
    # filtered log line into Python without creating temporary files.
    # WHY: streaming through pipes reduces disk writes, lowers I/O overhead,
    # and keeps intermediate operational data from lingering on disk longer
    # than necessary.
    parsed_rows: list[tuple[str, str, str, str, str]] = []

    for line in sys.stdin:
        parsed = parse_stream_line(line)
        if parsed is not None:
            parsed_rows.append(parsed)

    message_counts = Counter(row[4] for row in parsed_rows)

    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    with REPORT_FILE.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Date", "Error Type", "Time", "Process ID", "Error Message", "Count"])

        for date_value, time_value, process_id, error_type, error_message in parsed_rows:
            writer.writerow(
                [
                    date_value,
                    error_type,
                    time_value,
                    process_id,
                    error_message,
                    message_counts[error_message],
                ]
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
