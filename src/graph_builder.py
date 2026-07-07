#!/usr/bin/env python3

"""
graph_builder.py

This file handles the second milestone of SiftEngine:
turning parsed CALL events into a directed dependency graph.

The parser gives us events like:
{
    "timestamp": "2026-07-06 10:00",
    "service": "API",
    "action": "CALL",
    "value": "Auth"
}

For a CALL event, "service" is the source and "value" is the target.
So API CALL Auth becomes:

API -> Auth
"""

from collections import defaultdict
from parser import parse_file


def build_graph(events):
    """
    Build a directed graph from parsed log events.

    The graph is stored as an adjacency list:
    each service points to a list of services it called.
    """
    graph = defaultdict(list)

    for event in events:
        # START and END events describe request boundaries.
        # They do not describe service-to-service calls, so we ignore them here.
        if event["action"] != "CALL":
            continue

        source = event["service"]
        target = event["value"]

        # This creates a directed edge from the caller to the service it called.
        graph[source].append(target)

    return graph


if __name__ == "__main__":
    parsed_events = parse_file("logs/sample_logs.txt")
    graph = build_graph(parsed_events)

    print("Service dependency graph:")

    for service in graph:
        dependencies = graph[service]
        print(service + " -> " + str(dependencies))