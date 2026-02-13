#!/usr/bin/env python
# Copyright 2025-2026 Gregorio Elias Roecker Momm and nxCypher contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Command-line interface for the nxcypher Bolt server.

Usage:
    nxcypher-bolt [--host HOST] [--port PORT] [--graph FILE]
"""

import argparse
import asyncio
import sys

import networkx as nx

from .server import BoltServer


def create_sample_graph() -> nx.DiGraph:
    """Create a sample graph for demonstration."""
    G = nx.DiGraph()

    # Add some sample nodes
    G.add_node("alice", name="Alice", age=30, city="New York", __labels__={"Person"})
    G.add_node("bob", name="Bob", age=25, city="Boston", __labels__={"Person"})
    G.add_node("carol", name="Carol", age=35, city="Chicago", __labels__={"Person"})

    G.add_node("acme", name="Acme Corp", industry="Tech", __labels__={"Company"})

    # Add relationships
    G.add_edge("alice", "bob", since=2020, __labels__={"KNOWS"})
    G.add_edge("bob", "carol", since=2021, __labels__={"KNOWS"})
    G.add_edge("alice", "acme", role="Engineer", __labels__={"WORKS_AT"})

    return G


def load_graph(path: str) -> nx.Graph:
    """Load a graph from a file."""
    ext = path.lower().split('.')[-1]

    loaders = {
        'gml': nx.read_gml,
        'graphml': nx.read_graphml,
        'gexf': nx.read_gexf,
        'edgelist': nx.read_edgelist,
        'adjlist': nx.read_adjlist,
        'json': nx.node_link_graph,  # requires json loading first
        'gpickle': nx.read_gpickle,
    }

    if ext == 'json':
        import json
        with open(path) as f:
            data = json.load(f)
        return nx.node_link_graph(data)

    loader = loaders.get(ext)
    if loader is None:
        print(f"Unknown graph format: {ext}", file=sys.stderr)
        print(f"Supported formats: {', '.join(loaders.keys())}", file=sys.stderr)
        sys.exit(1)

    return loader(path)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Start a Neo4j-compatible Bolt server for nxcypher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  nxcypher-bolt                           Start with sample graph
  nxcypher-bolt --port 7688               Use different port
  nxcypher-bolt --graph my_graph.gml      Load graph from file

Then connect with any Neo4j driver:
  bolt://localhost:7687
        """
    )

    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host address to bind to (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=7687,
        help="Port to listen on (default: 7687)"
    )
    parser.add_argument(
        "--graph",
        help="Path to graph file (GML, GraphML, GEXF, JSON, etc.)"
    )

    args = parser.parse_args()

    # Load or create graph
    if args.graph:
        print(f"Loading graph from {args.graph}...")
        G = load_graph(args.graph)
    else:
        print("Creating sample graph...")
        G = create_sample_graph()

    print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")

    # Start server
    server = BoltServer(G, host=args.host, port=args.port)

    print(f"\nStarting Bolt server on bolt://{args.host}:{args.port}")
    print("Press Ctrl+C to stop\n")

    try:
        asyncio.run(server.start())
    except KeyboardInterrupt:
        print("\nShutting down...")


if __name__ == "__main__":
    main()
