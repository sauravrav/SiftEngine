#!/usr/bin/env python3

"""
parser.py

This file handles the first milestone of SiftEngine:
reading simple service logs and turning each line into a dictionary.

Example log line:
2026-07-06 10:00 API CALL Auth

That line becomes:
{
    "timestamp": "2026-07-06 10:00",
    "service": "API",
    "action": "CALL",
    "value": "Auth"
}
"""


def parse_file(file_path):
    """
    Read a log file and return a list of parsed log events.

    Each event is stored as a dictionary because dictionaries are easy
    to inspect, print, and pass into later parts of the project.
    """
    events = []

    # We open the file in read mode because the parser should only read logs,
    # not change them.
    file = open(file_path, "r")

    for line in file:
        # strip() removes the newline at the end and also catches blank lines.
        line = line.strip()

        # Blank lines do not contain useful log information, so we skip them.
        if line == "":
            continue

        parts = line.split()

        # Our current log format expects:
        # date time service action value
        date = parts[0]
        time = parts[1]
        service = parts[2]
        action = parts[3]
        value = parts[4]

        # The date and time belong together, so we combine them into one field.
        timestamp = date + " " + time

        event = {
            "timestamp": timestamp,
            "service": service,
            "action": action,
            "value": value,
        }

        events.append(event)

    file.close()

    return events


if __name__ == "__main__":
    parsed_events = parse_file("logs/sample_logs.txt")

    print("Parsed log events:")
    print(parsed_events)