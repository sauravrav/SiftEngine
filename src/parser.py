#!/usr/bin/env python3

"""
parser.py

This file handles two parser milestones for SiftEngine:
1. Reading simple service logs and turning each line into a dictionary.
2. Reconstructing a nested execution trace using a stack.

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

    # The "with" statement automatically closes the file when we are done.
    # This is safer than calling file.close() ourselves.
    with open(file_path, "r") as file:
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

    return events


def build_execution_trace(events):
    """
    Build a nested execution trace from parsed log events.

    The stack keeps track of which service is currently running.
    The last item in the stack is always the current active service.
    """
    trace = []
    stack = []

    for event in events:
        action = event["action"]
        service = event["service"]
        value = event["value"]

        if action == "START":
            # Sometimes a CALL line appears right before the START line
            # for the same service. In that case, CALL already created
            # the nested node, so START should only fill in the request id.
            if len(stack) > 0 and stack[-1]["service"] == service:
                stack[-1]["request_id"] = value
                continue

            node = {
                "service": service,
                "request_id": value,
                "children": [],
            }

            # If the stack is empty, this service starts the whole trace.
            # Otherwise, it is nested inside the service currently on top.
            if len(stack) == 0:
                trace.append(node)
            else:
                parent = stack[-1]
                parent["children"].append(node)

            stack.append(node)

        elif action == "CALL":
            target_service = value

            # A CALL tells us that the current service is about to enter
            # another service, so we add the target as a nested child.
            if len(stack) > 0:
                node = {
                    "service": target_service,
                    "request_id": None,
                    "children": [],
                }

                parent = stack[-1]
                parent["children"].append(node)
                stack.append(node)

        elif action == "END":
            # END means the current service is finished, so we remove it
            # from the stack and return to the previous service.
            if len(stack) > 0:
                stack.pop()

    return trace
def print_trace(trace, indentation):
    """
    Print the nested trace in a readable tree format.
    """
    for node in trace:
        spaces = "  " * indentation
        print(spaces + node["service"])
        print_trace(node["children"], indentation + 1)

if __name__ == "__main__":
    parsed_events = parse_file("logs/sample_logs.txt")
    execution_trace = build_execution_trace(parsed_events)

    print("Parsed log events:")
    print(parsed_events)

    print()
    print("Execution trace:")
    print_trace(execution_trace, 0)
