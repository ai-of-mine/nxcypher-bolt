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
Bolt protocol TCP server.

Provides a Neo4j-compatible Bolt protocol server that allows standard
Neo4j drivers to connect and execute Cypher queries against NetworkX graphs.
"""

import asyncio
import logging
import signal
from typing import Optional, Set

import networkx as nx

from .connection import BoltConnection


logger = logging.getLogger(__name__)


class BoltServer:
    """
    Async TCP server implementing the Bolt protocol.

    Allows Neo4j drivers to connect and execute Cypher queries
    against a NetworkX graph using nxcypher.

    Usage:
        import asyncio
        import networkx as nx
        from nxcypher.bolt import BoltServer

        G = nx.DiGraph()
        G.add_node("alice", name="Alice", __labels__={"Person"})

        server = BoltServer(G)
        asyncio.run(server.start())
    """

    def __init__(
        self,
        graph: nx.Graph,
        host: str = "127.0.0.1",
        port: int = 7687,
        server_name: str = "nxcypher/1.0.0"
    ):
        """
        Initialize the Bolt server.

        Args:
            graph: NetworkX graph to query
            host: Host address to bind to
            port: Port number to listen on (default: 7687, standard Bolt port)
            server_name: Server identification string
        """
        self._graph = graph
        self._host = host
        self._port = port
        self._server_name = server_name

        self._server: Optional[asyncio.AbstractServer] = None
        self._connections: Set[BoltConnection] = set()
        self._running = False

    @property
    def host(self) -> str:
        """Get the host address."""
        return self._host

    @property
    def port(self) -> int:
        """Get the port number."""
        return self._port

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running

    @property
    def connection_count(self) -> int:
        """Get number of active connections."""
        return len(self._connections)

    async def start(self, block: bool = True) -> None:
        """
        Start the Bolt server.

        Args:
            block: If True, blocks until server is stopped.
                   If False, returns immediately after starting.
        """
        self._server = await asyncio.start_server(
            self._handle_connection,
            self._host,
            self._port
        )

        self._running = True

        addrs = ', '.join(str(sock.getsockname()) for sock in self._server.sockets)
        logger.info(f"Bolt server listening on {addrs}")
        print(f"Bolt server listening on bolt://{self._host}:{self._port}")

        if block:
            # Set up signal handlers for graceful shutdown
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))

            async with self._server:
                await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop the Bolt server."""
        if not self._running:
            return

        self._running = False
        logger.info("Stopping Bolt server...")

        # Close all connections
        for conn in list(self._connections):
            try:
                # Connection cleanup will be handled in _handle_connection
                pass
            except Exception:
                pass

        # Stop accepting new connections
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

        logger.info("Bolt server stopped")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
    ) -> None:
        """Handle a new client connection."""
        peer = writer.get_extra_info('peername')
        logger.info(f"New connection from {peer}")

        connection = BoltConnection(
            reader=reader,
            writer=writer,
            graph=self._graph,
            server_name=self._server_name
        )

        self._connections.add(connection)

        try:
            await connection.handle()
        except Exception as e:
            logger.error(f"Connection error: {e}")
        finally:
            self._connections.discard(connection)
            logger.info(f"Connection closed from {peer}")


def run_server(
    graph: nx.Graph,
    host: str = "127.0.0.1",
    port: int = 7687,
    **kwargs
) -> None:
    """
    Convenience function to start a Bolt server.

    This is a blocking call that runs until interrupted.

    Args:
        graph: NetworkX graph to query
        host: Host address to bind to
        port: Port number to listen on
        **kwargs: Additional arguments passed to BoltServer
    """
    server = BoltServer(graph, host=host, port=port, **kwargs)
    asyncio.run(server.start())
