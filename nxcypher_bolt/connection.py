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
Bolt connection handler.

Manages a single Bolt protocol connection including handshake,
message dispatch, and query execution via nxcypher.
"""

import asyncio
import logging
import struct
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from nxcypher import NXCypher

from .packstream import PackStreamEncoder, PackStreamDecoder, Structure
from .chunking import ChunkReader, ChunkWriter
from .messages import (
    MSG_HELLO, MSG_GOODBYE, MSG_RESET, MSG_RUN, MSG_BEGIN, MSG_COMMIT,
    MSG_ROLLBACK, MSG_DISCARD, MSG_PULL, MSG_ROUTE, MSG_LOGON, MSG_LOGOFF,
    MSG_TELEMETRY,
    create_success, create_failure, create_record, create_ignored
)
from .state import ConnectionState, StateMachine
from .session import BoltSession, QueryResult
from .converter import ResultConverter


logger = logging.getLogger(__name__)

# Bolt magic bytes
BOLT_MAGIC = b'\x60\x60\xb0\x17'

# Supported Bolt versions (we'll support 4.4 and 5.0+)
# Version format: [major, minor, range, 0]
SUPPORTED_VERSIONS = [
    (5, 4),  # Bolt 5.4
    (5, 0),  # Bolt 5.0
    (4, 4),  # Bolt 4.4
    (4, 3),  # Bolt 4.3
]


class BoltConnection:
    """
    Handles a single Bolt protocol connection.

    Manages the connection lifecycle from handshake through query execution
    and result streaming.
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        graph: nx.Graph,
        server_name: str = "nxcypher/1.0.0"
    ):
        self._reader = reader
        self._writer = writer
        self._graph = graph
        self._server_name = server_name

        self._state = StateMachine()
        self._session = BoltSession(graph=graph)
        self._converter = ResultConverter(graph)
        self._nxcypher = NXCypher(graph)

        self._chunk_reader = ChunkReader()
        self._chunk_writer = ChunkWriter()
        self._encoder = PackStreamEncoder()

        self._negotiated_version: Optional[Tuple[int, int]] = None
        self._client_info: Dict[str, Any] = {}

        # Connection metadata
        peer = writer.get_extra_info('peername')
        self._peer_addr = f"{peer[0]}:{peer[1]}" if peer else "unknown"

    async def handle(self) -> None:
        """Main connection handler loop."""
        try:
            # Perform handshake
            if not await self._handshake():
                return

            # Process messages until connection closes
            while not self._state.is_defunct():
                try:
                    messages = await self._read_messages()
                    if not messages:
                        break

                    for message in messages:
                        await self._dispatch_message(message)

                except asyncio.CancelledError:
                    break
                except ConnectionResetError:
                    break
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    await self._send_failure(
                        "Neo.ClientError.Request.Invalid",
                        str(e)
                    )
                    self._state.transition_to(ConnectionState.FAILED)

        except Exception as e:
            logger.error(f"Connection error from {self._peer_addr}: {e}")
        finally:
            await self._close()

    async def _handshake(self) -> bool:
        """
        Perform Bolt handshake.

        Returns:
            True if handshake succeeded, False otherwise.
        """
        try:
            # Read magic bytes
            magic = await self._reader.readexactly(4)
            if magic != BOLT_MAGIC:
                logger.warning(f"Invalid magic bytes from {self._peer_addr}: {magic.hex()}")
                return False

            # Read 4 version proposals (4 bytes each)
            # Version format is big-endian uint32: 0x00_range_minor_major
            # - Byte 0 (high): reserved (0x00)
            # - Byte 1: range (number of minor versions supported below)
            # - Byte 2: minor version
            # - Byte 3 (low): major version
            version_data = await self._reader.readexactly(16)
            versions = []
            for i in range(4):
                v = struct.unpack('>I', version_data[i*4:(i+1)*4])[0]
                major = v & 0xFF
                minor = (v >> 8) & 0xFF
                version_range = (v >> 16) & 0xFF
                if major > 0:
                    versions.append((major, minor, version_range))

            # Find best matching version
            selected = None
            for major, minor, version_range in versions:
                # Check if we support any version in the range
                for sup_major, sup_minor in SUPPORTED_VERSIONS:
                    if sup_major == major:
                        if version_range > 0:
                            # Range specified: minor - range to minor
                            if (minor - version_range) <= sup_minor <= minor:
                                selected = (sup_major, sup_minor)
                                break
                        else:
                            # Exact match needed
                            if sup_minor == minor:
                                selected = (sup_major, sup_minor)
                                break
                if selected:
                    break

            if selected is None:
                # No compatible version found
                logger.warning(f"No compatible version with {self._peer_addr}: {versions}")
                self._writer.write(b'\x00\x00\x00\x00')
                await self._writer.drain()
                return False

            self._negotiated_version = selected
            logger.info(f"Negotiated Bolt {selected[0]}.{selected[1]} with {self._peer_addr}")

            # Send selected version as big-endian uint32: 0x00_00_minor_major
            response = struct.pack('>I', (selected[1] << 8) | selected[0])
            self._writer.write(response)
            await self._writer.drain()

            self._state.transition_to(ConnectionState.AUTHENTICATION)
            return True

        except asyncio.IncompleteReadError:
            return False
        except Exception as e:
            logger.error(f"Handshake error with {self._peer_addr}: {e}")
            return False

    async def _read_messages(self) -> List[Structure]:
        """Read and decode messages from the connection."""
        # Read available data
        try:
            data = await asyncio.wait_for(
                self._reader.read(65536),
                timeout=300.0  # 5 minute timeout
            )
        except asyncio.TimeoutError:
            self._state.mark_defunct()
            return []

        if not data:
            self._state.mark_defunct()
            return []

        # Feed to chunk reader
        complete_messages = self._chunk_reader.feed(data)

        # Decode PackStream messages
        messages = []
        for msg_bytes in complete_messages:
            decoder = PackStreamDecoder(msg_bytes)
            try:
                msg = decoder.decode()
                if isinstance(msg, Structure):
                    messages.append(msg)
                else:
                    logger.warning(f"Expected Structure, got {type(msg)}")
            except Exception as e:
                logger.error(f"Failed to decode message: {e}")

        return messages

    async def _dispatch_message(self, message: Structure) -> None:
        """Dispatch a message to the appropriate handler."""
        tag = message.tag
        fields = message.fields

        handlers = {
            MSG_HELLO: self._handle_hello,
            MSG_LOGON: self._handle_hello,  # LOGON is similar to HELLO
            MSG_GOODBYE: self._handle_goodbye,
            MSG_RESET: self._handle_reset,
            MSG_RUN: self._handle_run,
            MSG_PULL: self._handle_pull,
            MSG_DISCARD: self._handle_discard,
            MSG_BEGIN: self._handle_begin,
            MSG_COMMIT: self._handle_commit,
            MSG_ROLLBACK: self._handle_rollback,
            MSG_ROUTE: self._handle_route,
            MSG_LOGOFF: self._handle_logoff,
            MSG_TELEMETRY: self._handle_telemetry,
        }

        handler = handlers.get(tag)
        if handler:
            await handler(fields)
        else:
            logger.warning(f"Unknown message tag: 0x{tag:02X}")
            await self._send_failure(
                "Neo.ClientError.Request.Invalid",
                f"Unknown message type: 0x{tag:02X}"
            )

    async def _handle_hello(self, fields: List[Any]) -> None:
        """Handle HELLO message - initialize connection."""
        if self._state.state != ConnectionState.AUTHENTICATION:
            await self._send_ignored()
            return

        extra = fields[0] if fields else {}
        self._client_info = extra

        # Extract client info
        user_agent = extra.get('user_agent', 'unknown')
        logger.info(f"HELLO from {self._peer_addr}: {user_agent}")

        # Send SUCCESS with server info
        metadata = {
            'server': self._server_name,
            'connection_id': f"bolt-{id(self)}",
        }

        # Add hints for Bolt 5.x
        if self._negotiated_version and self._negotiated_version[0] >= 5:
            metadata['hints'] = {}

        await self._send_success(metadata)
        self._state.transition_to(ConnectionState.READY)

    async def _handle_goodbye(self, fields: List[Any]) -> None:
        """Handle GOODBYE message - close connection."""
        logger.info(f"GOODBYE from {self._peer_addr}")
        self._state.mark_defunct()

    async def _handle_reset(self, fields: List[Any]) -> None:
        """Handle RESET message - reset connection state."""
        logger.debug(f"RESET from {self._peer_addr}")

        # Clear any pending results
        self._session.clear_result()

        # Rollback any open transaction
        if self._session.in_transaction:
            self._session.rollback_transaction()

        # Reset state machine
        self._state.reset()

        await self._send_success({})

    async def _handle_run(self, fields: List[Any]) -> None:
        """Handle RUN message - execute a Cypher query."""
        if self._state.state not in (ConnectionState.READY, ConnectionState.TX_READY):
            if self._state.state == ConnectionState.FAILED:
                await self._send_ignored()
            else:
                await self._send_failure(
                    "Neo.ClientError.Request.Invalid",
                    "Cannot run query in current state"
                )
            return

        query = fields[0] if fields else ""
        params = fields[1] if len(fields) > 1 else {}
        extra = fields[2] if len(fields) > 2 else {}

        logger.debug(f"RUN from {self._peer_addr}: {query[:100]}...")

        try:
            # Get the working graph (transaction graph if in TX)
            working_graph = self._session.get_working_graph()

            # Update nxcypher to use the working graph
            self._nxcypher = NXCypher(working_graph)

            # Execute query
            result = self._nxcypher.run(query, params=params)

            # Convert to streaming format
            query_result = self._converter.convert_result(result)
            qid = self._session.set_result(query_result)

            # Transition to streaming state
            if self._session.in_transaction:
                self._state.transition_to(ConnectionState.TX_STREAMING)
            else:
                self._state.transition_to(ConnectionState.STREAMING)

            # Send SUCCESS with field names
            metadata = {
                'fields': query_result.fields,
                't_first': 0,  # Time to first record (ms)
            }
            if self._negotiated_version and self._negotiated_version[0] >= 4:
                metadata['qid'] = qid

            await self._send_success(metadata)

        except Exception as e:
            logger.error(f"Query error: {e}")
            await self._send_failure(
                "Neo.ClientError.Statement.SyntaxError",
                str(e)
            )
            self._state.transition_to(ConnectionState.FAILED)

    async def _handle_pull(self, fields: List[Any]) -> None:
        """Handle PULL message - fetch query results."""
        if self._state.state not in (ConnectionState.STREAMING, ConnectionState.TX_STREAMING):
            if self._state.state == ConnectionState.FAILED:
                await self._send_ignored()
            else:
                await self._send_failure(
                    "Neo.ClientError.Request.Invalid",
                    "No results to pull"
                )
            return

        extra = fields[0] if fields else {}
        n = extra.get('n', -1)
        qid = extra.get('qid', -1)

        if self._session.current_result is None:
            await self._send_failure(
                "Neo.ClientError.Request.Invalid",
                "No active result"
            )
            return

        # Pull records
        records = self._session.current_result.pull(n)

        # Send records
        for record in records:
            await self._send_record(record)

        # Check if more records available
        has_more = self._session.current_result.has_more()

        metadata = {
            'has_more': has_more,
            't_last': 0,  # Time to last record (ms)
        }

        if not has_more:
            # Include stats
            metadata['type'] = 'r'  # Read query
            metadata['stats'] = {}

            # Transition back to ready state
            if self._session.in_transaction:
                self._state.transition_to(ConnectionState.TX_READY)
            else:
                self._state.transition_to(ConnectionState.READY)

            self._session.clear_result()

        await self._send_success(metadata)

    async def _handle_discard(self, fields: List[Any]) -> None:
        """Handle DISCARD message - discard query results."""
        if self._state.state not in (ConnectionState.STREAMING, ConnectionState.TX_STREAMING):
            if self._state.state == ConnectionState.FAILED:
                await self._send_ignored()
            else:
                await self._send_failure(
                    "Neo.ClientError.Request.Invalid",
                    "No results to discard"
                )
            return

        extra = fields[0] if fields else {}
        n = extra.get('n', -1)

        if self._session.current_result is not None:
            self._session.current_result.discard(n)

        has_more = self._session.current_result.has_more() if self._session.current_result else False

        metadata = {'has_more': has_more}

        if not has_more:
            if self._session.in_transaction:
                self._state.transition_to(ConnectionState.TX_READY)
            else:
                self._state.transition_to(ConnectionState.READY)
            self._session.clear_result()

        await self._send_success(metadata)

    async def _handle_begin(self, fields: List[Any]) -> None:
        """Handle BEGIN message - start transaction."""
        if self._state.state != ConnectionState.READY:
            if self._state.state == ConnectionState.FAILED:
                await self._send_ignored()
            else:
                await self._send_failure(
                    "Neo.ClientError.Request.Invalid",
                    "Cannot begin transaction in current state"
                )
            return

        extra = fields[0] if fields else {}

        try:
            self._session.begin_transaction()
            self._state.transition_to(ConnectionState.TX_READY)
            await self._send_success({})
        except Exception as e:
            await self._send_failure(
                "Neo.ClientError.Transaction.TransactionStartFailed",
                str(e)
            )

    async def _handle_commit(self, fields: List[Any]) -> None:
        """Handle COMMIT message - commit transaction."""
        if self._state.state != ConnectionState.TX_READY:
            if self._state.state == ConnectionState.FAILED:
                await self._send_ignored()
            else:
                await self._send_failure(
                    "Neo.ClientError.Request.Invalid",
                    "No transaction to commit"
                )
            return

        try:
            self._session.commit_transaction()
            self._state.transition_to(ConnectionState.READY)

            # Update converter with new graph state
            self._converter = ResultConverter(self._graph)

            await self._send_success({})
        except Exception as e:
            await self._send_failure(
                "Neo.ClientError.Transaction.TransactionCommitFailed",
                str(e)
            )

    async def _handle_rollback(self, fields: List[Any]) -> None:
        """Handle ROLLBACK message - rollback transaction."""
        if self._state.state != ConnectionState.TX_READY:
            if self._state.state == ConnectionState.FAILED:
                await self._send_ignored()
            else:
                await self._send_failure(
                    "Neo.ClientError.Request.Invalid",
                    "No transaction to rollback"
                )
            return

        try:
            self._session.rollback_transaction()
            self._state.transition_to(ConnectionState.READY)
            await self._send_success({})
        except Exception as e:
            await self._send_failure(
                "Neo.ClientError.Transaction.TransactionRollbackFailed",
                str(e)
            )

    async def _handle_route(self, fields: List[Any]) -> None:
        """Handle ROUTE message - provide routing table (Bolt 4.3+)."""
        # For single-server mode, return simple routing info
        extra = fields[0] if fields else {}
        bookmarks = fields[1] if len(fields) > 1 else []
        db = fields[2] if len(fields) > 2 else None

        # Simple single-server routing table
        routing_table = {
            'rt': {
                'servers': [
                    {'addresses': ['localhost:7687'], 'role': 'WRITE'},
                    {'addresses': ['localhost:7687'], 'role': 'READ'},
                    {'addresses': ['localhost:7687'], 'role': 'ROUTE'},
                ],
                'ttl': 300,
                'db': db or 'neo4j',
            }
        }

        await self._send_success(routing_table)

    async def _handle_logoff(self, fields: List[Any]) -> None:
        """Handle LOGOFF message (Bolt 5.1+)."""
        await self._send_success({})

    async def _handle_telemetry(self, fields: List[Any]) -> None:
        """Handle TELEMETRY message (Bolt 5.4+)."""
        # Accept and acknowledge telemetry
        await self._send_success({})

    async def _send_success(self, metadata: Dict[str, Any]) -> None:
        """Send a SUCCESS message."""
        msg = create_success(metadata)
        await self._send_message(msg)

    async def _send_failure(self, code: str, message: str) -> None:
        """Send a FAILURE message."""
        msg = create_failure(code, message)
        await self._send_message(msg)

    async def _send_record(self, fields: List[Any]) -> None:
        """Send a RECORD message."""
        msg = create_record(fields)
        await self._send_message(msg)

    async def _send_ignored(self) -> None:
        """Send an IGNORED message."""
        msg = create_ignored()
        await self._send_message(msg)

    async def _send_message(self, message: Structure) -> None:
        """Encode and send a message."""
        encoded = self._encoder.encode(message)
        chunked = self._chunk_writer.write(encoded)
        self._writer.write(chunked)
        await self._writer.drain()

    async def _close(self) -> None:
        """Close the connection."""
        try:
            self._writer.close()
            await self._writer.wait_closed()
        except Exception:
            pass
        logger.debug(f"Connection closed: {self._peer_addr}")
