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
Message chunking for the Bolt protocol.

Bolt messages are framed using chunks:
- Each chunk has a 2-byte big-endian size prefix
- Maximum chunk size is 65535 bytes
- End of message is marked by a zero-length chunk (0x0000)
"""

import struct
from typing import List, Optional


MAX_CHUNK_SIZE = 65535


class ChunkWriter:
    """Splits messages into chunks with size prefixes."""

    def __init__(self, max_chunk_size: int = MAX_CHUNK_SIZE):
        self.max_chunk_size = min(max_chunk_size, MAX_CHUNK_SIZE)

    def write(self, data: bytes) -> bytes:
        """
        Split data into chunks and return the framed bytes.

        Returns bytes including all chunk headers and the end marker.
        """
        result = bytearray()
        offset = 0

        while offset < len(data):
            chunk_size = min(len(data) - offset, self.max_chunk_size)
            result.extend(struct.pack('>H', chunk_size))
            result.extend(data[offset:offset + chunk_size])
            offset += chunk_size

        # End marker
        result.extend(b'\x00\x00')
        return bytes(result)


class ChunkReader:
    """Reassembles chunked messages from a byte stream."""

    def __init__(self):
        self._buffer = bytearray()
        self._message_buffer = bytearray()
        self._expected_chunk_size: Optional[int] = None

    def feed(self, data: bytes) -> List[bytes]:
        """
        Feed data into the reader and return any complete messages.

        Returns a list of complete messages (may be empty).
        """
        self._buffer.extend(data)
        messages = []

        while True:
            message = self._try_read_message()
            if message is None:
                break
            messages.append(message)

        return messages

    def _try_read_message(self) -> Optional[bytes]:
        """Try to read a complete message from the buffer."""
        while True:
            # Need to read chunk size
            if self._expected_chunk_size is None:
                if len(self._buffer) < 2:
                    return None
                self._expected_chunk_size = struct.unpack('>H', self._buffer[:2])[0]
                self._buffer = self._buffer[2:]

            # Zero-length chunk marks end of message
            if self._expected_chunk_size == 0:
                self._expected_chunk_size = None
                if self._message_buffer:
                    message = bytes(self._message_buffer)
                    self._message_buffer = bytearray()
                    return message
                # Empty message - continue to next
                continue

            # Read chunk data
            if len(self._buffer) < self._expected_chunk_size:
                return None

            self._message_buffer.extend(self._buffer[:self._expected_chunk_size])
            self._buffer = self._buffer[self._expected_chunk_size:]
            self._expected_chunk_size = None

    def clear(self) -> None:
        """Clear all buffers."""
        self._buffer = bytearray()
        self._message_buffer = bytearray()
        self._expected_chunk_size = None
