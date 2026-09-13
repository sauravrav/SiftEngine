# SiftEngine

SiftEngine is a learning-focused Python prototype that transforms sequential
service logs into nested execution traces and directed dependency graphs.

The project was created to explore practical applications of stacks, graphs,
depth-first search, and log parsing. It uses a stack to reconstruct the nested
order of service calls and DFS to detect circular dependencies between services.

## Project Purpose

In a service-based application, a single request may pass through multiple
services.

For example:

```text
API -> Auth -> User -> Database
Raw logs normally represent these interactions as sequential events. SiftEngine
parses those events and produces two representations:

A nested execution trace showing how one request moved through the services.
A directed dependency graph showing which services called other services.

The dependency graph can then be analyzed for circular service dependencies.
Sample Log Format

SiftEngine currently uses a simple five-field log format:

date time service action value

Example:

2026-07-06 10:00 API START req1
2026-07-06 10:00 API CALL Auth
2026-07-06 10:00 Auth START req1
2026-07-06 10:00 Auth CALL User
2026-07-06 10:00 User START req1
2026-07-06 10:00 User CALL Database
2026-07-06 10:00 Database END req1
2026-07-06 10:00 User END req1
2026-07-06 10:00 Auth END req1
2026-07-06 10:00 API END req1

The supported actions are:

START: A service begins processing a request.
CALL: A service calls another service.
END: A service finishes processing the request.
Program Flow
Sequential log file
        |
        v
Parse lines into structured events
        |
        +-----------------------------+
        |                             |
        v                             v
Build execution trace          Build dependency graph
using a stack                  using CALL events
                                      |
                                      v
                              Detect cycles using DFS
Log Parsing

Each log line is converted into a Python dictionary.

For example:

2026-07-06 10:00 API CALL Auth
Becomes:

{
    "timestamp": "2026-07-06 10:00",
    "service": "API",
    "action": "CALL",
    "value": "Auth"
}

This structured representation is passed to the trace builder and graph builder.
Stack-Based Execution Trace

The execution-trace builder uses a stack to track the currently active service
and its parent call context.

When a service starts or is called, it is added to the stack. When the service
ends, it is removed from the stack. The service currently at the top of the
stack represents the active service.

For the sample logs, the generated trace is:

API
  Auth
    User
      Database

This converts flat sequential logs into a nested representation of the request's
execution flow.
Directed Dependency Graph

For every CALL event, SiftEngine creates a directed edge from the calling
service to the target service.

For example:

API CALL Auth

Creates the edge:

API -> Auth

The complete sample graph is stored as an adjacency list:

{
    "API": ["Auth"],
    "Auth": ["User"],
    "User": ["Database"]
}

An adjacency list was selected because it provides a simple and
space-efficient representation for a sparse service graph.

DFS-Based Cycle Detection

SiftEngine uses depth-first search to detect circular dependencies in the
directed graph.

The algorithm maintains two sets:

visiting: Services in the current DFS path.
visited: Services that have already been completely explored.

If DFS encounters a service that is already in visiting, the current path
contains a cycle.

Example:

API -> Auth -> User -> API

This cycle could indicate a potentially risky circular dependency. It does not
necessarily prove that an infinite runtime loop occurred, but it identifies a
dependency path that may require further investigation.

Project Structure
SiftEngine/
├── logs/
│   └── sample_logs.txt
├── src/
│   ├── main.py
│   ├── parser.py
│   ├── graph_builder.py
│   └── cycle_detection.py
└── README.md
Core Files
src/main.py: Runs the complete analysis flow.
src/parser.py: Parses logs and reconstructs nested execution traces.
src/graph_builder.py: Builds the directed service graph.
src/cycle_detection.py: Detects cycles using depth-first search.
Running the Project
Requirements
Python 3.10 or later
No third-party Python packages are required
Run

From the project root:

python3 src/main.py logs/sample_logs.txt

The sample log file is used automatically when no path is provided:

python3 src/main.py