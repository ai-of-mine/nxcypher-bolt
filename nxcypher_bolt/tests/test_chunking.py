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

"""Tests for message chunking."""

import pytest
from nxcypher.bolt.chunking import ChunkWriter, ChunkReader, MAX_CHUNK_SIZE


class TestChunkWriter:
    """Test chunk writing."""

    def test_small_message(self):
        writer = ChunkWriter()
        data = b"hello"
        result = writer.write(data)

        # Should be: [size=5][data][size=0]
        assert result[:2] == b'\x00\x05'  # Size = 5
        assert result[2:7] == b'hello'
        assert result[7:9] == b'\x00\x00'  # End marker

    def test_empty_message(self):
        writer = ChunkWriter()
        result = writer.write(b"")

        # Should be just the end marker
        assert result == b'\x00\x00'

    def test_message_split_into_chunks(self):
        writer = ChunkWriter(max_chunk_size=10)
        data = b"hello world!"  # 12 bytes

        result = writer.write(data)

        # First chunk: size=10, data
        assert result[:2] == b'\x00\x0a'
        assert result[2:12] == b'hello worl'

        # Second chunk: size=2, data
        assert result[12:14] == b'\x00\x02'
        assert result[14:16] == b'd!'

        # End marker
        assert result[16:18] == b'\x00\x00'

    def test_exact_chunk_size(self):
        writer = ChunkWriter(max_chunk_size=5)
        data = b"12345"

        result = writer.write(data)

        assert result[:2] == b'\x00\x05'
        assert result[2:7] == b'12345'
        assert result[7:9] == b'\x00\x00'


class TestChunkReader:
    """Test chunk reading."""

    def test_small_message(self):
        reader = ChunkReader()
        data = b'\x00\x05hello\x00\x00'

        messages = reader.feed(data)

        assert len(messages) == 1
        assert messages[0] == b'hello'

    def test_multiple_messages(self):
        reader = ChunkReader()
        data = b'\x00\x05hello\x00\x00\x00\x05world\x00\x00'

        messages = reader.feed(data)

        assert len(messages) == 2
        assert messages[0] == b'hello'
        assert messages[1] == b'world'

    def test_chunked_message(self):
        reader = ChunkReader()
        data = b'\x00\x05hello\x00\x06 world\x00\x00'

        messages = reader.feed(data)

        assert len(messages) == 1
        assert messages[0] == b'hello world'

    def test_incremental_feed(self):
        reader = ChunkReader()

        # Feed partial data
        messages = reader.feed(b'\x00\x05hel')
        assert len(messages) == 0

        # Feed more
        messages = reader.feed(b'lo\x00')
        assert len(messages) == 0

        # Feed end marker
        messages = reader.feed(b'\x00')
        assert len(messages) == 1
        assert messages[0] == b'hello'

    def test_clear(self):
        reader = ChunkReader()
        reader.feed(b'\x00\x05hel')  # Partial
        reader.clear()

        # Should start fresh
        messages = reader.feed(b'\x00\x03abc\x00\x00')
        assert messages == [b'abc']


class TestChunkRoundTrip:
    """Test round-trip encoding/decoding."""

    def test_round_trip_small(self):
        writer = ChunkWriter()
        reader = ChunkReader()

        original = b"Hello, world!"
        chunked = writer.write(original)
        messages = reader.feed(chunked)

        assert len(messages) == 1
        assert messages[0] == original

    def test_round_trip_large(self):
        writer = ChunkWriter(max_chunk_size=100)
        reader = ChunkReader()

        original = b"x" * 500
        chunked = writer.write(original)
        messages = reader.feed(chunked)

        assert len(messages) == 1
        assert messages[0] == original

    def test_round_trip_multiple(self):
        writer = ChunkWriter()
        reader = ChunkReader()

        data = b""
        originals = [b"first", b"second", b"third"]
        for msg in originals:
            data += writer.write(msg)

        messages = reader.feed(data)
        assert messages == originals
