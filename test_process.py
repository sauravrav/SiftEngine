#!/usr/bin/env python3

import csv
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import process


class ProcessTests(unittest.TestCase):
    def test_parse_stream_line_extracts_fields(self) -> None:
        raw_line = (
            "2026-04-14 09:16:44 7821 "
            "2026-04-14 09:16:44 web02 payments 7821 CRITICAL "
            "Payment queue stalled remote=[IP_REDACTED]"
        )

        parsed = process.parse_stream_line(raw_line)

        self.assertEqual(
            parsed,
            (
                "2026-04-14",
                "09:16:44",
                "7821",
                "CRITICAL",
                "Payment queue stalled remote=[IP_REDACTED]",
            ),
        )

    def test_build_summary_counts_duplicate_messages_once(self) -> None:
        rows = [
            ("2026-04-14", "09:16:02", "4312", "ERROR", "Database timeout"),
            ("2026-04-14", "09:16:12", "4312", "ERROR", "Database timeout"),
            ("2026-04-14", "09:16:22", "7821", "CRITICAL", "Queue stalled"),
        ]

        summary = process.build_summary(rows)

        self.assertEqual(len(summary), 2)
        self.assertEqual(summary["Database timeout"]["count"], 2)
        self.assertEqual(summary["Queue stalled"]["count"], 1)


class PipelineTests(unittest.TestCase):
    def test_script_creates_timestamped_and_latest_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_dir = Path("/Users/sauravbhatta/Desktop/SiftEngine")
            temp_path = Path(temp_dir)
            log_path = temp_path / "test.log"
            report_dir = temp_path / "reports"

            log_path.write_text(
                "\n".join(
                    [
                        "2026-04-14 09:16:02 web01 authsvc 4312 ERROR Authentication backend timeout src=10.24.8.17",
                        "2026-04-14 09:16:44 web02 payments 7821 CRITICAL Payment queue stalled remote=172.16.44.19",
                        "2026-04-14 09:17:18 web01 authsvc 4312 ERROR Authentication backend timeout src=10.24.8.17",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            environment = os.environ.copy()
            environment.update(
                {
                    "SIFT_INPUT_LOG": str(log_path),
                    "SIFT_REPORT_DIR": str(report_dir),
                    "ALERT_ON_NO_TRANSPORT": "false",
                }
            )

            completed = subprocess.run(
                [str(repo_dir / "sift.sh")],
                cwd=repo_dir,
                env=environment,
                capture_output=True,
                text=True,
                check=True,
            )

            latest_report = report_dir / "final_report.csv"
            timestamped_reports = sorted(report_dir.glob("final_report_*.csv"))

            self.assertIn("CRITICAL", completed.stdout)
            self.assertTrue(latest_report.exists())
            self.assertEqual(len(timestamped_reports), 1)

            with latest_report.open(newline="", encoding="utf-8") as csv_file:
                rows = list(csv.DictReader(csv_file))

            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["Count"], "2")


if __name__ == "__main__":
    unittest.main()
