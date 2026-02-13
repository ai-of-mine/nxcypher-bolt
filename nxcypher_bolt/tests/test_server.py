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

"""Integration tests for the Bolt server."""

import asyncio
import pytest
import networkx as nx

from nxcypher.bolt import BoltServer
from nxcypher.bolt.packstream import encode, decode, Structure
from nxcypher.bolt.chunking import ChunkWriter, ChunkReader
from nxcypher.bolt.messages import MSG_HELLO, MSG_RUN, MSG_PULL, MSG_GOODBYE


BOLT_MAGIC = b'\x60\x60\xb0\x17'


class TestBoltServer:
    """Test the Bolt server."""

    @pytest.fixture
    def graph(self):
        """Create a test graph."""
        G = nx.DiGraph()
        G.add_node("alice", name="Alice", age=30, __labels__={"Person"})
        G.add_node("bob", name="Bob", age=25, __labels__={"Person"})
        G.add_node("carol", name="Carol", age=35, __labels__={"Person"})
        G.add_edge("alice", "bob", since=2020, __labels__={"KNOWS"})
        G.add_edge("bob", "carol", since=2021, __labels__={"KNOWS"})
        return G

    @pytest.fixture
    async def server(self, graph):
        """Start a test server."""
        server = BoltServer(graph, host="127.0.0.1", port=0)  # Port 0 = random free port

        # Start server in background
        async def run_server():
            server._server = await asyncio.start_server(
                server._handle_connection,
                server._host,
                0  # Let OS assign port
            )
            server._running = True

        await run_server()

        # Get the assigned port
        sock = server._server.sockets[0]
        server._port = sock.getsockname()[1]

        yield server

        # Cleanup
        await server.stop()

    @pytest.mark.asyncio
    async def test_server_accepts_connection(self, server):
        """Test that server accepts connections."""
        reader, writer = await asyncio.open_connection(
            server.host, server.port
        )

        try:
            # Send handshake
            writer.write(BOLT_MAGIC)
            # Bolt 4.4 version proposal
            writer.write(b'\x00\x00\x04\x04' + b'\x00\x00\x00\x00' * 3)
            await writer.drain()

            # Read response
            response = await asyncio.wait_for(reader.read(4), timeout=5.0)
            assert len(response) == 4
            # Should get a valid version back
            major = response[2]
            assert major in (4, 5)
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_hello_message(self, server):
        """Test HELLO message handling."""
        reader, writer = await asyncio.open_connection(
            server.host, server.port
        )

        try:
            # Handshake
            writer.write(BOLT_MAGIC)
            writer.write(b'\x00\x00\x04\x04' + b'\x00\x00\x00\x00' * 3)
            await writer.drain()
            await reader.read(4)

            # Send HELLO
            chunk_writer = ChunkWriter()
            hello = Structure(MSG_HELLO, [{"user_agent": "test/1.0"}])
            writer.write(chunk_writer.write(encode(hello)))
            await writer.drain()

            # Read SUCCESS response
            chunk_reader = ChunkReader()
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            messages = chunk_reader.feed(data)

            assert len(messages) >= 1
            response = decode(messages[0])
            assert isinstance(response, Structure)
            assert response.tag == 0x70  # SUCCESS
        finally:
            writer.close()
            await writer.wait_closed()

    @pytest.mark.asyncio
    async def test_run_and_pull_query(self, server):
        """Test running a query and pulling results."""
        reader, writer = await asyncio.open_connection(
            server.host, server.port
        )
        chunk_writer = ChunkWriter()
        chunk_reader = ChunkReader()

        try:
            # Handshake
            writer.write(BOLT_MAGIC)
            writer.write(b'\x00\x00\x04\x04' + b'\x00\x00\x00\x00' * 3)
            await writer.drain()
            await reader.read(4)

            # HELLO
            hello = Structure(MSG_HELLO, [{"user_agent": "test/1.0"}])
            writer.write(chunk_writer.write(encode(hello)))
            await writer.drain()
            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            chunk_reader.feed(data)

            # RUN query
            run = Structure(MSG_RUN, ["MATCH (n:Person) RETURN n.name", {}, {}])
            writer.write(chunk_writer.write(encode(run)))
            await writer.drain()

            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            messages = chunk_reader.feed(data)
            assert len(messages) >= 1
            response = decode(messages[0])
            assert response.tag == 0x70  # SUCCESS
            assert 'fields' in response.fields[0]

            # PULL results
            pull = Structure(MSG_PULL, [{"n": -1}])
            writer.write(chunk_writer.write(encode(pull)))
            await writer.drain()

            # Read until we get SUCCESS (end of results)
            records = []
            while True:
                data = await asyncio.wait_for(reader.read(4096), timeout=5.0)
                messages = chunk_reader.feed(data)
                for msg_data in messages:
                    msg = decode(msg_data)
                    if msg.tag == 0x71:  # RECORD
                        records.append(msg.fields[0])
                    elif msg.tag == 0x70:  # SUCCESS
                        break
                else:
                    continue
                break

            # Should have 3 records (Alice, Bob, Carol)
            assert len(records) == 3
            names = [r[0] for r in records]
            assert set(names) == {"Alice", "Bob", "Carol"}

        finally:
            writer.close()
            await writer.wait_closed()


class TestResultConverter:
    """Test result conversion."""

    def test_convert_simple_result(self):
        from nxcypher.bolt.converter import ResultConverter

        G = nx.Graph()
        converter = ResultConverter(G)

        result = {
            'name': ['Alice', 'Bob'],
            'age': [30, 25]
        }

        query_result = converter.convert_result(result)

        assert query_result.fields == ['name', 'age']
        assert len(query_result.records) == 2
        assert query_result.records[0] == ['Alice', 30]
        assert query_result.records[1] == ['Bob', 25]

    def test_convert_empty_result(self):
        from nxcypher.bolt.converter import ResultConverter

        G = nx.Graph()
        converter = ResultConverter(G)

        result = {}
        query_result = converter.convert_result(result)

        assert query_result.fields == []
        assert query_result.records == []


class TestQueryResult:
    """Test QueryResult functionality."""

    def test_pull_all(self):
        from nxcypher.bolt.session import QueryResult

        qr = QueryResult(
            fields=['a', 'b'],
            records=[[1, 2], [3, 4], [5, 6]]
        )

        records = qr.pull(-1)
        assert records == [[1, 2], [3, 4], [5, 6]]
        assert not qr.has_more()

    def test_pull_partial(self):
        from nxcypher.bolt.session import QueryResult

        qr = QueryResult(
            fields=['a'],
            records=[[1], [2], [3], [4], [5]]
        )

        records = qr.pull(2)
        assert records == [[1], [2]]
        assert qr.has_more()

        records = qr.pull(2)
        assert records == [[3], [4]]
        assert qr.has_more()

        records = qr.pull(2)
        assert records == [[5]]
        assert not qr.has_more()

    def test_discard(self):
        from nxcypher.bolt.session import QueryResult

        qr = QueryResult(
            fields=['a'],
            records=[[1], [2], [3]]
        )

        discarded = qr.discard(2)
        assert discarded == 2
        assert qr.has_more()

        records = qr.pull(-1)
        assert records == [[3]]
