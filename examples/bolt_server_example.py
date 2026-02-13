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
Example Bolt server for nxcypher.

This script demonstrates how to start a Bolt protocol server that allows
Neo4j drivers to connect and execute Cypher queries against a NetworkX graph.

Usage:
    python examples/bolt_server_example.py

Then connect with a Neo4j driver:
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost:7687")
    with driver.session() as session:
        result = session.run("MATCH (n:Person) RETURN n.name, n.age")
        for record in result:
            print(record)
"""

import asyncio
import networkx as nx

from nxcypher_bolt import BoltServer


def create_sample_graph() -> nx.DiGraph:
    """Create a sample social network graph."""
    G = nx.DiGraph()

    # Add people
    G.add_node("alice", name="Alice", age=30, city="New York", __labels__={"Person"})
    G.add_node("bob", name="Bob", age=25, city="Boston", __labels__={"Person"})
    G.add_node("carol", name="Carol", age=35, city="Chicago", __labels__={"Person"})
    G.add_node("dave", name="Dave", age=28, city="Denver", __labels__={"Person"})

    # Add companies
    G.add_node("acme", name="Acme Corp", industry="Tech", __labels__={"Company"})
    G.add_node("globex", name="Globex Inc", industry="Finance", __labels__={"Company"})

    # Add relationships
    G.add_edge("alice", "bob", since=2020, __labels__={"KNOWS"})
    G.add_edge("bob", "carol", since=2021, __labels__={"KNOWS"})
    G.add_edge("carol", "alice", since=2019, __labels__={"KNOWS"})
    G.add_edge("dave", "alice", since=2022, __labels__={"KNOWS"})

    G.add_edge("alice", "acme", role="Engineer", since=2018, __labels__={"WORKS_AT"})
    G.add_edge("bob", "acme", role="Manager", since=2017, __labels__={"WORKS_AT"})
    G.add_edge("carol", "globex", role="Analyst", since=2020, __labels__={"WORKS_AT"})
    G.add_edge("dave", "globex", role="Director", since=2015, __labels__={"WORKS_AT"})

    return G


async def main():
    # Create the graph
    G = create_sample_graph()

    print("Sample graph created with:")
    print(f"  - {G.number_of_nodes()} nodes")
    print(f"  - {G.number_of_edges()} edges")
    print()

    # Start the Bolt server
    server = BoltServer(G, host="0.0.0.0", port=7687)

    print("Starting Bolt server...")
    print("Connect with: bolt://localhost:7687")
    print()
    print("Example queries to try:")
    print('  MATCH (n:Person) RETURN n.name, n.age')
    print('  MATCH (p:Person)-[:WORKS_AT]->(c:Company) RETURN p.name, c.name')
    print('  MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name')
    print()
    print("Press Ctrl+C to stop the server")

    await server.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer stopped")
