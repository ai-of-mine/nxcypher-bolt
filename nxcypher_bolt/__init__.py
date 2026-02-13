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
Bolt protocol server for nxcypher.

This module implements a Neo4j-compatible Bolt protocol server that allows
standard Neo4j drivers (Python, JavaScript, Java, etc.) to connect to nxcypher
and execute Cypher queries against NetworkX graphs.

Usage:
    import asyncio
    import networkx as nx
    from nxcypher.bolt import BoltServer

    # Create a graph
    G = nx.DiGraph()
    G.add_node("alice", name="Alice", __labels__={"Person"})
    G.add_node("bob", name="Bob", __labels__={"Person"})
    G.add_edge("alice", "bob", __labels__={"KNOWS"})

    # Start the Bolt server
    server = BoltServer(G, host="0.0.0.0", port=7687)
    asyncio.run(server.start())

Then connect with any Neo4j driver:
    from neo4j import GraphDatabase
    driver = GraphDatabase.driver("bolt://localhost:7687")
    with driver.session() as session:
        result = session.run("MATCH (n:Person) RETURN n.name")
        for record in result:
            print(record["n.name"])
"""

from .server import BoltServer, run_server
from .connection import BoltConnection
from .session import BoltSession, QueryResult
from .converter import ResultConverter
from .packstream import PackStreamEncoder, PackStreamDecoder, Structure, encode, decode
from .chunking import ChunkReader, ChunkWriter
from .state import ConnectionState, StateMachine
from .messages import (
    BoltNode,
    BoltRelationship,
    BoltPath,
    BoltUnboundRelationship,
    # Message tags
    MSG_HELLO,
    MSG_GOODBYE,
    MSG_RESET,
    MSG_RUN,
    MSG_BEGIN,
    MSG_COMMIT,
    MSG_ROLLBACK,
    MSG_DISCARD,
    MSG_PULL,
    MSG_SUCCESS,
    MSG_FAILURE,
    MSG_RECORD,
    MSG_IGNORED,
)

__all__ = [
    # Server
    "BoltServer",
    "run_server",
    # Connection handling
    "BoltConnection",
    "BoltSession",
    "QueryResult",
    "ResultConverter",
    # State management
    "ConnectionState",
    "StateMachine",
    # Serialization
    "PackStreamEncoder",
    "PackStreamDecoder",
    "Structure",
    "encode",
    "decode",
    "ChunkReader",
    "ChunkWriter",
    # Data types
    "BoltNode",
    "BoltRelationship",
    "BoltPath",
    "BoltUnboundRelationship",
    # Message tags
    "MSG_HELLO",
    "MSG_GOODBYE",
    "MSG_RESET",
    "MSG_RUN",
    "MSG_BEGIN",
    "MSG_COMMIT",
    "MSG_ROLLBACK",
    "MSG_DISCARD",
    "MSG_PULL",
    "MSG_SUCCESS",
    "MSG_FAILURE",
    "MSG_RECORD",
    "MSG_IGNORED",
]
