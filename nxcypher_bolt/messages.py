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
Bolt protocol message types and structures.

Defines message tags for client-server communication and
structure types for nodes, relationships, and paths.
"""

from dataclasses import dataclass
from typing import Any, Dict, List

from .packstream import Structure


# Client -> Server message tags
MSG_HELLO = 0x01      # Initialize connection
MSG_GOODBYE = 0x02    # Close connection
MSG_RESET = 0x0F      # Reset connection state
MSG_RUN = 0x10        # Execute query
MSG_BEGIN = 0x11      # Start transaction
MSG_COMMIT = 0x12     # Commit transaction
MSG_ROLLBACK = 0x13   # Rollback transaction
MSG_DISCARD = 0x2F    # Discard results
MSG_PULL = 0x3F       # Pull results
MSG_ROUTE = 0x66      # Routing table request (Bolt 4.3+)
MSG_LOGON = 0x6A      # Re-authenticate (Bolt 5.1+)
MSG_LOGOFF = 0x6B     # De-authenticate (Bolt 5.1+)
MSG_TELEMETRY = 0x54  # Telemetry (Bolt 5.4+)

# Server -> Client message tags
MSG_SUCCESS = 0x70    # Operation succeeded
MSG_RECORD = 0x71     # Result record
MSG_IGNORED = 0x7E    # Message was ignored
MSG_FAILURE = 0x7F    # Operation failed

# Structure tags for graph entities
STRUCT_NODE = 0x4E           # 'N' - Node
STRUCT_RELATIONSHIP = 0x52   # 'R' - Relationship (full, with start/end)
STRUCT_UNBOUND_REL = 0x72    # 'r' - Unbound relationship (in paths)
STRUCT_PATH = 0x50           # 'P' - Path

# Temporal structure tags
STRUCT_DATE = 0x44           # 'D' - Date
STRUCT_TIME = 0x54           # 'T' - Time
STRUCT_LOCAL_TIME = 0x74     # 't' - LocalTime
STRUCT_DATETIME = 0x49       # 'I' - DateTime (with timezone ID)
STRUCT_DATETIME_OFFSET = 0x46  # 'F' - DateTime (with offset)
STRUCT_LOCAL_DATETIME = 0x64   # 'd' - LocalDateTime
STRUCT_DURATION = 0x45       # 'E' - Duration

# Spatial structure tags
STRUCT_POINT_2D = 0x58       # 'X' - Point2D
STRUCT_POINT_3D = 0x59       # 'Y' - Point3D


@dataclass
class BoltNode:
    """
    A node in Bolt protocol format.

    Attributes:
        id: Unique node identifier
        labels: Set of node labels
        properties: Node properties dict
        element_id: String element ID (Bolt 5+)
    """
    id: int
    labels: List[str]
    properties: Dict[str, Any]
    element_id: str = ""

    def to_structure(self) -> Structure:
        """Convert to PackStream structure."""
        # Bolt 4.x format: [id, labels, properties]
        # Bolt 5.x format: [id, labels, properties, element_id]
        return Structure(STRUCT_NODE, [
            self.id,
            self.labels,
            self.properties,
            self.element_id or str(self.id)
        ])


@dataclass
class BoltRelationship:
    """
    A relationship in Bolt protocol format.

    Attributes:
        id: Unique relationship identifier
        start_node_id: ID of the start node
        end_node_id: ID of the end node
        type: Relationship type
        properties: Relationship properties dict
        element_id: String element ID (Bolt 5+)
        start_element_id: String element ID for start node (Bolt 5+)
        end_element_id: String element ID for end node (Bolt 5+)
    """
    id: int
    start_node_id: int
    end_node_id: int
    type: str
    properties: Dict[str, Any]
    element_id: str = ""
    start_element_id: str = ""
    end_element_id: str = ""

    def to_structure(self) -> Structure:
        """Convert to PackStream structure."""
        # Bolt 5.x format includes element IDs
        return Structure(STRUCT_RELATIONSHIP, [
            self.id,
            self.start_node_id,
            self.end_node_id,
            self.type,
            self.properties,
            self.element_id or str(self.id),
            self.start_element_id or str(self.start_node_id),
            self.end_element_id or str(self.end_node_id)
        ])


@dataclass
class BoltUnboundRelationship:
    """
    An unbound relationship (used in paths) in Bolt protocol format.

    Unlike BoltRelationship, this doesn't include start/end node IDs
    since those are implicit from the path structure.
    """
    id: int
    type: str
    properties: Dict[str, Any]
    element_id: str = ""

    def to_structure(self) -> Structure:
        """Convert to PackStream structure."""
        return Structure(STRUCT_UNBOUND_REL, [
            self.id,
            self.type,
            self.properties,
            self.element_id or str(self.id)
        ])


@dataclass
class BoltPath:
    """
    A path in Bolt protocol format.

    A path is represented as:
    - A list of unique nodes
    - A list of unbound relationships
    - A list of indices that describe the path traversal
    """
    nodes: List[BoltNode]
    relationships: List[BoltUnboundRelationship]
    indices: List[int]

    def to_structure(self) -> Structure:
        """Convert to PackStream structure."""
        return Structure(STRUCT_PATH, [
            [n.to_structure() for n in self.nodes],
            [r.to_structure() for r in self.relationships],
            self.indices
        ])


def create_success(metadata: Dict[str, Any] = None) -> Structure:
    """Create a SUCCESS message."""
    return Structure(MSG_SUCCESS, [metadata or {}])


def create_failure(code: str, message: str) -> Structure:
    """Create a FAILURE message."""
    return Structure(MSG_FAILURE, [{
        'code': code,
        'message': message
    }])


def create_record(fields: List[Any]) -> Structure:
    """Create a RECORD message."""
    return Structure(MSG_RECORD, [fields])


def create_ignored() -> Structure:
    """Create an IGNORED message."""
    return Structure(MSG_IGNORED, [{}])
