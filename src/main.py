#!/usr/bin/env python3

"""
Main entry point for SiftEngine.

Program flow:
1. Read and parse sequential service logs.
2. Reconstruct the nested execution trace using a stack.
3. Build a directed service dependency graph.
4. Detect circular dependencies using DFS.
"""

import argparse

from parser import parse_file, build_execution_trace, print_trace
from graph_builder import build_graph
from cycle_detection import has_cycle


def print_graph(graph):
    """
    Print the service dependency graph in a readable format.
    """
    if len(graph) == 0:
        print("No service dependencies found.")
        return

    for service, dependencies in graph.items():
        if len(dependencies) == 0:
            print(service)
        else:
            print(service + " -> " + ", ".join(dependencies))


def main():
    parser = argparse.ArgumentParser(
        description="Analyze service logs using stacks, graphs, and DFS."
    )

    parser.add_argument(
        "log_file",
        nargs="?",
        default="logs/sample_logs.txt",
        help="Path to the service log file",
    )

    args = parser.parse_args()

    try:
        events = parse_file(args.log_file)
    except FileNotFoundError:
        print("Error: log file not found: " + args.log_file)
        return
    except (IndexError, ValueError):
        print("Error: the log file contains an invalid log entry.")
        return

    print("Parsed Events:")
    print("--------------")
    print("Total events:", len(events))

    print()
    print("Execution Trace:")
    print("----------------")

    execution_trace = build_execution_trace(events)

    if len(execution_trace) == 0:
        print("No execution trace found.")
    else:
        print_trace(execution_trace, 0)

    print()
    print("Dependency Graph:")
    print("-----------------")

    graph = build_graph(events)
    print_graph(graph)

    print()
    print("Cycle Detection:")
    print("----------------")

    cycle_found = has_cycle(graph)
    print("Cycle detected:", cycle_found)


if __name__ == "__main__":
    main()