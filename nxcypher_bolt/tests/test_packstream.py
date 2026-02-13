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

"""Tests for PackStream encoding and decoding."""

import pytest
from nxcypher.bolt.packstream import (
    PackStreamEncoder,
    PackStreamDecoder,
    Structure,
    encode,
    decode,
)


class TestPackStreamPrimitives:
    """Test encoding/decoding of primitive types."""

    def test_null(self):
        assert decode(encode(None)) is None

    def test_bool_true(self):
        assert decode(encode(True)) is True

    def test_bool_false(self):
        assert decode(encode(False)) is False

    def test_tiny_int_positive(self):
        for i in range(0, 128):
            assert decode(encode(i)) == i

    def test_tiny_int_negative(self):
        for i in range(-16, 0):
            assert decode(encode(i)) == i

    def test_int_8(self):
        for i in [-128, -17, 127]:
            assert decode(encode(i)) == i

    def test_int_16(self):
        for i in [-32768, -129, 128, 32767]:
            assert decode(encode(i)) == i

    def test_int_32(self):
        for i in [-2147483648, -32769, 32768, 2147483647]:
            assert decode(encode(i)) == i

    def test_int_64(self):
        big = 2147483648
        assert decode(encode(big)) == big
        assert decode(encode(-big - 1)) == -big - 1

    def test_float(self):
        assert decode(encode(3.14159)) == pytest.approx(3.14159)
        assert decode(encode(-273.15)) == pytest.approx(-273.15)
        assert decode(encode(0.0)) == 0.0

    def test_tiny_string(self):
        assert decode(encode("")) == ""
        assert decode(encode("hello")) == "hello"
        assert decode(encode("0123456789abcde")) == "0123456789abcde"

    def test_string_8(self):
        s = "a" * 100
        assert decode(encode(s)) == s

    def test_string_16(self):
        s = "b" * 300
        assert decode(encode(s)) == s

    def test_unicode_string(self):
        s = "Hello, 世界! 🌍"
        assert decode(encode(s)) == s


class TestPackStreamCollections:
    """Test encoding/decoding of collections."""

    def test_empty_list(self):
        assert decode(encode([])) == []

    def test_tiny_list(self):
        lst = [1, 2, 3]
        assert decode(encode(lst)) == lst

    def test_list_mixed_types(self):
        lst = [1, "two", 3.0, None, True]
        assert decode(encode(lst)) == lst

    def test_nested_list(self):
        lst = [[1, 2], [3, [4, 5]]]
        assert decode(encode(lst)) == lst

    def test_large_list(self):
        lst = list(range(100))
        assert decode(encode(lst)) == lst

    def test_empty_map(self):
        assert decode(encode({})) == {}

    def test_tiny_map(self):
        m = {"a": 1, "b": 2}
        assert decode(encode(m)) == m

    def test_map_mixed_values(self):
        m = {"int": 1, "str": "hello", "list": [1, 2, 3], "none": None}
        assert decode(encode(m)) == m

    def test_nested_map(self):
        m = {"outer": {"inner": {"deep": 42}}}
        assert decode(encode(m)) == m


class TestPackStreamStructures:
    """Test encoding/decoding of structures."""

    def test_empty_structure(self):
        s = Structure(tag=0x70, fields=[])
        result = decode(encode(s))
        assert isinstance(result, Structure)
        assert result.tag == 0x70
        assert result.fields == []

    def test_structure_with_fields(self):
        s = Structure(tag=0x71, fields=["hello", 42, None])
        result = decode(encode(s))
        assert isinstance(result, Structure)
        assert result.tag == 0x71
        assert result.fields == ["hello", 42, None]

    def test_nested_structure(self):
        inner = Structure(tag=0x4E, fields=[1, ["Person"], {"name": "Alice"}])
        outer = Structure(tag=0x71, fields=[inner])
        result = decode(encode(outer))
        assert isinstance(result, Structure)
        assert result.tag == 0x71
        assert len(result.fields) == 1
        inner_result = result.fields[0]
        assert isinstance(inner_result, Structure)
        assert inner_result.tag == 0x4E


class TestPackStreamBytes:
    """Test encoding/decoding of bytes."""

    def test_small_bytes(self):
        b = b"hello"
        assert decode(encode(b)) == b

    def test_medium_bytes(self):
        b = bytes(range(256))
        assert decode(encode(b)) == b


class TestPackStreamDecodeAll:
    """Test decoding multiple values."""

    def test_decode_all(self):
        encoder = PackStreamEncoder()
        data = encoder.encode(1) + encoder.encode("two") + encoder.encode([3])
        decoder = PackStreamDecoder(data)
        values = decoder.decode_all()
        assert values == [1, "two", [3]]


class TestPackStreamEdgeCases:
    """Test edge cases and error handling."""

    def test_tuple_becomes_list(self):
        # Tuples are encoded as lists
        result = decode(encode((1, 2, 3)))
        assert result == [1, 2, 3]
        assert isinstance(result, list)

    def test_invalid_type_raises(self):
        encoder = PackStreamEncoder()
        with pytest.raises(ValueError):
            encoder.encode(object())

    def test_truncated_data_raises(self):
        data = encode("hello")
        decoder = PackStreamDecoder(data[:-1])
        with pytest.raises(ValueError):
            decoder.decode()
