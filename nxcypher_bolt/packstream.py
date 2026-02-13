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
PackStream serialization for the Bolt protocol.

Implements encoding and decoding of PackStream data types:
- Primitives: null, bool, int, float, string, bytes
- Collections: list, dict (map)
- Structures: tagged structures for nodes, relationships, etc.
"""

import struct
from dataclasses import dataclass
from typing import Any, List, Union


# Marker bytes for PackStream types
TINY_STRING = 0x80
TINY_LIST = 0x90
TINY_MAP = 0xA0
TINY_STRUCT = 0xB0

NULL = 0xC0
FLOAT_64 = 0xC1
FALSE = 0xC2
TRUE = 0xC3

INT_8 = 0xC8
INT_16 = 0xC9
INT_32 = 0xCA
INT_64 = 0xCB

BYTES_8 = 0xCC
BYTES_16 = 0xCD
BYTES_32 = 0xCE

STRING_8 = 0xD0
STRING_16 = 0xD1
STRING_32 = 0xD2

LIST_8 = 0xD4
LIST_16 = 0xD5
LIST_32 = 0xD6

MAP_8 = 0xD8
MAP_16 = 0xD9
MAP_32 = 0xDA

STRUCT_8 = 0xDC
STRUCT_16 = 0xDD


@dataclass
class Structure:
    """A PackStream structure with a tag and fields."""
    tag: int
    fields: List[Any]


class PackStreamEncoder:
    """Encodes Python values to PackStream binary format."""

    def __init__(self):
        self._buffer = bytearray()

    def encode(self, value: Any) -> bytes:
        """Encode a value and return the bytes."""
        self._buffer = bytearray()
        self._encode_value(value)
        return bytes(self._buffer)

    def _encode_value(self, value: Any) -> None:
        """Encode a single value into the buffer."""
        if value is None:
            self._buffer.append(NULL)
        elif isinstance(value, bool):
            self._buffer.append(TRUE if value else FALSE)
        elif isinstance(value, int):
            self._encode_int(value)
        elif isinstance(value, float):
            self._buffer.append(FLOAT_64)
            self._buffer.extend(struct.pack('>d', value))
        elif isinstance(value, str):
            self._encode_string(value)
        elif isinstance(value, bytes):
            self._encode_bytes(value)
        elif isinstance(value, (list, tuple)):
            self._encode_list(value)
        elif isinstance(value, dict):
            self._encode_map(value)
        elif isinstance(value, Structure):
            self._encode_structure(value)
        else:
            raise ValueError(f"Cannot encode type: {type(value)}")

    def _encode_int(self, value: int) -> None:
        """Encode an integer."""
        if -16 <= value < 128:
            # Tiny int: single byte
            self._buffer.append(value & 0xFF)
        elif -128 <= value < 128:
            self._buffer.append(INT_8)
            self._buffer.extend(struct.pack('>b', value))
        elif -32768 <= value < 32768:
            self._buffer.append(INT_16)
            self._buffer.extend(struct.pack('>h', value))
        elif -2147483648 <= value < 2147483648:
            self._buffer.append(INT_32)
            self._buffer.extend(struct.pack('>i', value))
        else:
            self._buffer.append(INT_64)
            self._buffer.extend(struct.pack('>q', value))

    def _encode_string(self, value: str) -> None:
        """Encode a string."""
        encoded = value.encode('utf-8')
        size = len(encoded)
        if size < 16:
            self._buffer.append(TINY_STRING | size)
        elif size < 256:
            self._buffer.append(STRING_8)
            self._buffer.append(size)
        elif size < 65536:
            self._buffer.append(STRING_16)
            self._buffer.extend(struct.pack('>H', size))
        else:
            self._buffer.append(STRING_32)
            self._buffer.extend(struct.pack('>I', size))
        self._buffer.extend(encoded)

    def _encode_bytes(self, value: bytes) -> None:
        """Encode bytes."""
        size = len(value)
        if size < 256:
            self._buffer.append(BYTES_8)
            self._buffer.append(size)
        elif size < 65536:
            self._buffer.append(BYTES_16)
            self._buffer.extend(struct.pack('>H', size))
        else:
            self._buffer.append(BYTES_32)
            self._buffer.extend(struct.pack('>I', size))
        self._buffer.extend(value)

    def _encode_list(self, value: Union[list, tuple]) -> None:
        """Encode a list."""
        size = len(value)
        if size < 16:
            self._buffer.append(TINY_LIST | size)
        elif size < 256:
            self._buffer.append(LIST_8)
            self._buffer.append(size)
        elif size < 65536:
            self._buffer.append(LIST_16)
            self._buffer.extend(struct.pack('>H', size))
        else:
            self._buffer.append(LIST_32)
            self._buffer.extend(struct.pack('>I', size))
        for item in value:
            self._encode_value(item)

    def _encode_map(self, value: dict) -> None:
        """Encode a map/dict."""
        size = len(value)
        if size < 16:
            self._buffer.append(TINY_MAP | size)
        elif size < 256:
            self._buffer.append(MAP_8)
            self._buffer.append(size)
        elif size < 65536:
            self._buffer.append(MAP_16)
            self._buffer.extend(struct.pack('>H', size))
        else:
            self._buffer.append(MAP_32)
            self._buffer.extend(struct.pack('>I', size))
        for k, v in value.items():
            self._encode_string(str(k))
            self._encode_value(v)

    def _encode_structure(self, value: Structure) -> None:
        """Encode a structure."""
        size = len(value.fields)
        if size < 16:
            self._buffer.append(TINY_STRUCT | size)
        elif size < 256:
            self._buffer.append(STRUCT_8)
            self._buffer.append(size)
        else:
            self._buffer.append(STRUCT_16)
            self._buffer.extend(struct.pack('>H', size))
        self._buffer.append(value.tag)
        for field in value.fields:
            self._encode_value(field)


class PackStreamDecoder:
    """Decodes PackStream binary format to Python values."""

    def __init__(self, data: bytes):
        self._data = data
        self._pos = 0

    def decode(self) -> Any:
        """Decode and return the next value."""
        if self._pos >= len(self._data):
            raise ValueError("End of data")
        return self._decode_value()

    def decode_all(self) -> List[Any]:
        """Decode all values in the data."""
        values = []
        while self._pos < len(self._data):
            values.append(self._decode_value())
        return values

    def remaining(self) -> int:
        """Return remaining bytes."""
        return len(self._data) - self._pos

    def _read_byte(self) -> int:
        """Read a single byte."""
        if self._pos >= len(self._data):
            raise ValueError("Unexpected end of data")
        b = self._data[self._pos]
        self._pos += 1
        return b

    def _read_bytes(self, n: int) -> bytes:
        """Read n bytes."""
        if self._pos + n > len(self._data):
            raise ValueError(f"Unexpected end of data: need {n} bytes")
        data = self._data[self._pos:self._pos + n]
        self._pos += n
        return data

    def _decode_value(self) -> Any:
        """Decode a single value."""
        marker = self._read_byte()

        # Tiny int (positive): 0x00-0x7F
        if marker <= 0x7F:
            return marker

        # Tiny string: 0x80-0x8F
        if 0x80 <= marker <= 0x8F:
            size = marker & 0x0F
            return self._read_bytes(size).decode('utf-8')

        # Tiny list: 0x90-0x9F
        if 0x90 <= marker <= 0x9F:
            size = marker & 0x0F
            return [self._decode_value() for _ in range(size)]

        # Tiny map: 0xA0-0xAF
        if 0xA0 <= marker <= 0xAF:
            size = marker & 0x0F
            return {self._decode_value(): self._decode_value() for _ in range(size)}

        # Tiny struct: 0xB0-0xBF
        if 0xB0 <= marker <= 0xBF:
            size = marker & 0x0F
            tag = self._read_byte()
            fields = [self._decode_value() for _ in range(size)]
            return Structure(tag, fields)

        # Null
        if marker == NULL:
            return None

        # Float 64
        if marker == FLOAT_64:
            return struct.unpack('>d', self._read_bytes(8))[0]

        # Boolean
        if marker == FALSE:
            return False
        if marker == TRUE:
            return True

        # Integers
        if marker == INT_8:
            return struct.unpack('>b', self._read_bytes(1))[0]
        if marker == INT_16:
            return struct.unpack('>h', self._read_bytes(2))[0]
        if marker == INT_32:
            return struct.unpack('>i', self._read_bytes(4))[0]
        if marker == INT_64:
            return struct.unpack('>q', self._read_bytes(8))[0]

        # Tiny int (negative): 0xF0-0xFF
        if marker >= 0xF0:
            return marker - 256

        # Bytes
        if marker == BYTES_8:
            size = self._read_byte()
            return self._read_bytes(size)
        if marker == BYTES_16:
            size = struct.unpack('>H', self._read_bytes(2))[0]
            return self._read_bytes(size)
        if marker == BYTES_32:
            size = struct.unpack('>I', self._read_bytes(4))[0]
            return self._read_bytes(size)

        # Strings
        if marker == STRING_8:
            size = self._read_byte()
            return self._read_bytes(size).decode('utf-8')
        if marker == STRING_16:
            size = struct.unpack('>H', self._read_bytes(2))[0]
            return self._read_bytes(size).decode('utf-8')
        if marker == STRING_32:
            size = struct.unpack('>I', self._read_bytes(4))[0]
            return self._read_bytes(size).decode('utf-8')

        # Lists
        if marker == LIST_8:
            size = self._read_byte()
            return [self._decode_value() for _ in range(size)]
        if marker == LIST_16:
            size = struct.unpack('>H', self._read_bytes(2))[0]
            return [self._decode_value() for _ in range(size)]
        if marker == LIST_32:
            size = struct.unpack('>I', self._read_bytes(4))[0]
            return [self._decode_value() for _ in range(size)]

        # Maps
        if marker == MAP_8:
            size = self._read_byte()
            return {self._decode_value(): self._decode_value() for _ in range(size)}
        if marker == MAP_16:
            size = struct.unpack('>H', self._read_bytes(2))[0]
            return {self._decode_value(): self._decode_value() for _ in range(size)}
        if marker == MAP_32:
            size = struct.unpack('>I', self._read_bytes(4))[0]
            return {self._decode_value(): self._decode_value() for _ in range(size)}

        # Structures
        if marker == STRUCT_8:
            size = self._read_byte()
            tag = self._read_byte()
            fields = [self._decode_value() for _ in range(size)]
            return Structure(tag, fields)
        if marker == STRUCT_16:
            size = struct.unpack('>H', self._read_bytes(2))[0]
            tag = self._read_byte()
            fields = [self._decode_value() for _ in range(size)]
            return Structure(tag, fields)

        raise ValueError(f"Unknown marker: 0x{marker:02X}")


def encode(value: Any) -> bytes:
    """Convenience function to encode a value."""
    return PackStreamEncoder().encode(value)


def decode(data: bytes) -> Any:
    """Convenience function to decode a value."""
    return PackStreamDecoder(data).decode()
