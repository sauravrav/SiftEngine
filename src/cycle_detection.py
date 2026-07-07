#!/usr/bin/env python3

"""
cycle_detection.py

This file handles the third milestone of SiftEngine:
checking whether a directed service graph contains a cycle.

A cycle means we can start at one service and eventually come back
to that same service by following CALL edges.

Example cycle:
API -> Auth -> User -> API
"""


def has_cycle(graph):
    """
    Return True if the directed graph contains a cycle.
    Return False if it does not.

    graph is an adjacency list, like:
    {
        "API": ["Auth"],
        "Auth": ["User"],
        "User": ["Database"]
    }
    """
    visited = set()
    visiting = set()

    def dfs(node):
        """
        Use DFS to check whether this node leads back to a node
        that is already in the current recursion path.
        """
        # "visiting" means this node is currently in the DFS path.
        # Seeing it again before we finish means there is a cycle.
        if node in visiting:
            return True

        # "visited" means we already checked this node completely.
        # We do not need to repeat work for it.
        if node in visited:
            return False

        visiting.add(node)

        # graph.get(node, []) is used because some services may appear
        # only as targets and may not have their own key in the graph.
        for neighbor in graph.get(node, []):
            if dfs(neighbor):
                return True

        # We are done exploring this path, so the node is no longer
        # part of the active recursion stack.
        visiting.remove(node)
        visited.add(node)

        return False

    for node in graph:
        # Start DFS from every node because the graph may have
        # separate disconnected parts.
        if node not in visited:
            if dfs(node):
                return True

    return False


if __name__ == "__main__":
    graph_without_cycle = {
        "API": ["Auth"],
        "Auth": ["User"],
        "User": ["Database"],
    }

    graph_with_cycle = {
        "API": ["Auth"],
        "Auth": ["User"],
        "User": ["API"],
    }

    print("Graph 1 has cycle:", has_cycle(graph_without_cycle))
    print("Graph 2 has cycle:", has_cycle(graph_with_cycle))