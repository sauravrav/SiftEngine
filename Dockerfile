FROM python:3.13-slim

WORKDIR /app

COPY sift.sh process.py system_raw.log ./

RUN chmod +x /app/sift.sh /app/process.py

ENV SIFT_REPORT_DIR=/tmp/sift_reports
ENV SIFT_INPUT_LOG=/app/system_raw.log
ENV SIFT_LOG_FORMAT=plain

CMD ["/app/sift.sh"]
