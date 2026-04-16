# SiftEngine

SiftEngine is an SRE-focused log processing pipeline built with Bash and Python. It scans log files for urgent failures, redacts IPv4 addresses, summarizes repeated incidents, writes timestamped CSV reports, and can send alerts for new critical failures.

## Why this project is useful for SRE interviews

- It demonstrates Unix pipeline usage with `grep`, `sed`, `awk`, and `stdin`-driven Python processing.
- It shows operational thinking: alert deduplication, retry logic, log-rotation-safe state tracking, locking, and report hygiene.
- It produces artifacts that SRE teams actually use: CSV summaries, status output, metrics, and schedulable service files.

## Features

- Plain-text log mode using a shell pipeline
- JSON log mode for structured logs
- IPv4 redaction before reports and alerts
- Summary counts for repeated error messages
- Timestamped reports plus a stable `final_report.csv`
- Webhook or email alerting for `CRITICAL` issues
- Alert cooldown to avoid repeated paging for the same incident
- Metrics output in Prometheus text format
- Latest run status in JSON
- Locking to prevent overlapping runs
- `systemd` service and timer files for every-5-minute scheduling
- Tests for parsing and end-to-end execution

## Files

- [sift.sh](/Users/sauravbhatta/Desktop/SiftEngine/sift.sh): orchestration, alerting, state tracking, metrics, status, locking
- [process.py](/Users/sauravbhatta/Desktop/SiftEngine/process.py): log normalization and CSV report generation
- [test_process.py](/Users/sauravbhatta/Desktop/SiftEngine/test_process.py): unit and integration tests
- [siftengine.service](/Users/sauravbhatta/Desktop/SiftEngine/siftengine.service): `systemd` service unit
- [siftengine.timer](/Users/sauravbhatta/Desktop/SiftEngine/siftengine.timer): `systemd` timer
- [system_raw.log](/Users/sauravbhatta/Desktop/SiftEngine/system_raw.log): demo input log
- [Dockerfile](/Users/sauravbhatta/Desktop/SiftEngine/Dockerfile): containerized run target
- [.env.example](/Users/sauravbhatta/Desktop/SiftEngine/.env.example): sample configuration

## Architecture

### Plain-text mode

`tail -> grep -> sed -> awk -> python3 process.py`

- `grep` keeps only `ERROR` and `CRITICAL`
- `sed` redacts IPv4 addresses
- `awk` extracts date, time, and process ID while preserving the full log line
- Python summarizes repeated messages and writes CSV output

The pipeline intentionally uses pipes instead of temporary files to reduce disk I/O, lower alert latency, and avoid leaving intermediate sensitive log data on disk.

### JSON mode

When `SIFT_LOG_FORMAT=json`, the shell script streams raw JSON lines directly into Python. Python then parses structured fields like `timestamp`, `severity`, `pid`, and `message`.

## Outputs

SiftEngine writes its outputs into `SIFT_REPORT_DIR`:

- `final_report_<RUN_ID>.csv`: timestamped report for each run
- `final_report.csv`: latest report copy
- `pipeline_state.env`: inode and byte-offset state
- `alert_state.env`: alert fingerprint and cooldown state
- `metrics.prom`: Prometheus-style metrics for the latest run
- `latest_status.json`: run status and summary counts

## Run locally

```bash
chmod +x sift.sh process.py test_process.py
./sift.sh
```

## Run tests

```bash
python3 -m unittest -v
```

## Example environment variables

```bash
export SIFT_INPUT_LOG=system_raw.log
export SIFT_REPORT_DIR=/tmp/sift_reports
export SIFT_LOG_FORMAT=plain
export ALERT_WEBHOOK_URL=https://example.invalid/webhook
export ALERT_COOLDOWN_SECONDS=900
```

## Docker

Build:

```bash
docker build -t siftengine .
```

Run:

```bash
docker run --rm \
  -e SIFT_INPUT_LOG=/app/system_raw.log \
  -e SIFT_REPORT_DIR=/tmp/sift_reports \
  siftengine
```

## systemd setup

Copy the unit files on a Linux host:

```bash
sudo cp siftengine.service /etc/systemd/system/
sudo cp siftengine.timer /etc/systemd/system/
sudo mkdir -p /etc/siftengine
sudo cp .env.example /etc/siftengine/siftengine.env
sudo systemctl daemon-reload
sudo systemctl enable --now siftengine.timer
```

The service reads configuration from `/etc/siftengine/siftengine.env`.

## Good portfolio talking points

- Why inode-aware state matters during log rotation
- Why alert deduplication reduces pager fatigue
- Why locking matters for scheduled jobs
- Why metrics and status outputs help operate the pipeline itself
- Why supporting both plain text and JSON logs makes the tool more reusable
