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
Integration test for Bolt protocol server.

This test starts a server, connects to it, and executes queries.
Run directly with: python -m nxcypher.bolt.tests.test_integration
"""

import asyncio
import networkx as nx

from nxcypher.bolt import BoltServer
from nxcypher.bolt.packstream import encode, decode, Structure
from nxcypher.bolt.chunking import ChunkWriter, ChunkReader
from nxcypher.bolt.messages import MSG_HELLO, MSG_RUN, MSG_PULL, MSG_GOODBYE


BOLT_MAGIC = b'\x60\x60\xb0\x17'


async def test_bolt_server():
    """Test the Bolt server with a simulated client."""
    # Create test graph
    G = nx.DiGraph()
    G.add_node("alice", name="Alice", age=30, __labels__={"Person"})
    G.add_node("bob", name="Bob", age=25, __labels__={"Person"})
    G.add_node("carol", name="Carol", age=35, __labels__={"Person"})
    G.add_edge("alice", "bob", since=2020, __labels__={"KNOWS"})
    G.add_edge("bob", "carol", since=2021, __labels__={"KNOWS"})

    # Start server on random port
    server = BoltServer(G, host="127.0.0.1", port=0)
    server._server = await asyncio.start_server(
        server._handle_connection,
        server._host,
        0
    )
    server._running = True
    port = server._server.sockets[0].getsockname()[1]

    print(f"Server started on port {port}")

    try:
        # Connect to server
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        chunk_writer = ChunkWriter()
        chunk_reader = ChunkReader()

        # === Handshake ===
        print("Sending handshake...")
        writer.write(BOLT_MAGIC)
        # Propose Bolt 4.4
        writer.write(b'\x00\x00\x04\x04' + b'\x00\x00\x00\x00' * 3)
        await writer.drain()

        response = await asyncio.wait_for(reader.read(4), timeout=5.0)
        version_major = response[2]
        version_minor = response[1]
        print(f"Negotiated Bolt {version_major}.{version_minor}")
        assert version_major in (4, 5), f"Unexpected version: {version_major}"

        # === HELLO ===
        print("Sending HELLO...")
        hello = Structure(MSG_HELLO, [{"user_agent": "test-client/1.0"}])
        writer.write(chunk_writer.write(encode(hello)))
        await writer.drain()

        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        messages = chunk_reader.feed(data)
        response = decode(messages[0])
        assert response.tag == 0x70, f"Expected SUCCESS, got 0x{response.tag:02x}"
        print(f"HELLO response: {response.fields[0]}")

        # === RUN MATCH query ===
        print("\nRunning: MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
        run = Structure(MSG_RUN, [
            "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name",
            {},
            {}
        ])
        writer.write(chunk_writer.write(encode(run)))
        await writer.drain()

        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        messages = chunk_reader.feed(data)
        response = decode(messages[0])
        assert response.tag == 0x70, f"Expected SUCCESS, got 0x{response.tag:02x}"
        fields = response.fields[0].get('fields', [])
        print(f"Fields: {fields}")

        # === PULL results ===
        print("Pulling results...")
        pull = Structure(MSG_PULL, [{"n": -1}])
        writer.write(chunk_writer.write(encode(pull)))
        await writer.drain()

        records = []
        while True:
            data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            messages = chunk_reader.feed(data)
            for msg_data in messages:
                msg = decode(msg_data)
                if msg.tag == 0x71:  # RECORD
                    records.append(msg.fields[0])
                elif msg.tag == 0x70:  # SUCCESS
                    print(f"Pull complete: {msg.fields[0]}")
                    break
            else:
                continue
            break

        print(f"Got {len(records)} records:")
        for record in records:
            print(f"  {record}")

        assert len(records) == 3, f"Expected 3 records, got {len(records)}"

        # Verify expected data
        names = [r[0] for r in records]
        assert set(names) == {"Alice", "Bob", "Carol"}, f"Unexpected names: {names}"

        # === Another query with relationship ===
        print("\nRunning: MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name")
        run = Structure(MSG_RUN, [
            "MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name",
            {},
            {}
        ])
        writer.write(chunk_writer.write(encode(run)))
        await writer.drain()

        data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
        messages = chunk_reader.feed(data)
        response = decode(messages[0])
        assert response.tag == 0x70

        pull = Structure(MSG_PULL, [{"n": -1}])
        writer.write(chunk_writer.write(encode(pull)))
        await writer.drain()

        records = []
        while True:
            data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
            messages = chunk_reader.feed(data)
            for msg_data in messages:
                msg = decode(msg_data)
                if msg.tag == 0x71:
                    records.append(msg.fields[0])
                elif msg.tag == 0x70:
                    break
            else:
                continue
            break

        print(f"Got {len(records)} relationship records:")
        for record in records:
            print(f"  {record[0]} -> {record[1]}")

        assert len(records) == 2, f"Expected 2 KNOWS relationships, got {len(records)}"

        # === GOODBYE ===
        print("\nSending GOODBYE...")
        goodbye = Structure(MSG_GOODBYE, [])
        writer.write(chunk_writer.write(encode(goodbye)))
        await writer.drain()

        writer.close()
        await writer.wait_closed()

        print("\n✓ All tests passed!")

    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(test_bolt_server())
