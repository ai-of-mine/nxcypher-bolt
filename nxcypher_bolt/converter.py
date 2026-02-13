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
Result converter for Bolt protocol.

Converts nxcypher query results (Dict[str, List]) to Bolt protocol
format (QueryResult with field names and record rows).
"""

from typing import Any, Dict, List, Hashable

import networkx as nx

from .session import QueryResult
from .messages import BoltNode, BoltRelationship, BoltPath, BoltUnboundRelationship


class ResultConverter:
    """
    Converts nxcypher results to Bolt-compatible format.

    nxcypher returns results as Dict[str, List]:
        {'n.name': ['Alice', 'Bob'], 'count': [1, 2]}

    Bolt protocol needs QueryResult with:
        fields = ['n.name', 'count']
        records = [['Alice', 1], ['Bob', 2]]
    """

    def __init__(self, graph: nx.Graph):
        self._graph = graph
        self._node_id_map: Dict[Hashable, int] = {}
        self._edge_id_map: Dict[tuple, int] = {}
        self._next_node_id = 0
        self._next_edge_id = 0

    def convert_result(self, result: Dict[str, List]) -> QueryResult:
        """
        Convert nxcypher result dict to QueryResult.

        Args:
            result: Dict mapping column names to lists of values

        Returns:
            QueryResult with fields and records in row format
        """
        if not result:
            return QueryResult(fields=[], records=[])

        fields = list(result.keys())

        # Get number of rows
        first_col = next(iter(result.values()))
        num_rows = len(first_col) if first_col else 0

        # Convert columns to rows
        records = []
        for i in range(num_rows):
            row = []
            for field in fields:
                value = result[field][i]
                row.append(self._convert_value(value))
            records.append(row)

        return QueryResult(fields=fields, records=records)

    def _convert_value(self, value: Any) -> Any:
        """
        Convert a single value to Bolt-compatible format.

        Handles special nxcypher types like nodes and relationships.
        """
        if value is None:
            return None

        # Check if it's a node dict from nxcypher
        if isinstance(value, dict):
            if '__node_id__' in value:
                return self._convert_node(value)
            if '__rel_type__' in value or '__edge_key__' in value:
                return self._convert_relationship(value)
            if '__path__' in value:
                return self._convert_path(value)
            # Regular dict, recursively convert values
            return {k: self._convert_value(v) for k, v in value.items()}

        if isinstance(value, (list, tuple)):
            return [self._convert_value(v) for v in value]

        # Primitives pass through
        return value

    def _get_node_id(self, node_key: Hashable) -> int:
        """Get or create a numeric ID for a node."""
        if node_key not in self._node_id_map:
            self._node_id_map[node_key] = self._next_node_id
            self._next_node_id += 1
        return self._node_id_map[node_key]

    def _get_edge_id(self, edge_key: tuple) -> int:
        """Get or create a numeric ID for an edge."""
        if edge_key not in self._edge_id_map:
            self._edge_id_map[edge_key] = self._next_edge_id
            self._next_edge_id += 1
        return self._edge_id_map[edge_key]

    def _convert_node(self, node_dict: Dict) -> BoltNode:
        """Convert a nxcypher node dict to BoltNode structure."""
        node_key = node_dict['__node_id__']
        node_id = self._get_node_id(node_key)

        # Get labels
        labels = list(node_dict.get('__labels__', []))

        # Get properties (exclude internal keys)
        properties = {
            k: v for k, v in node_dict.items()
            if not k.startswith('__')
        }

        element_id = str(node_key)
        return BoltNode(
            id=node_id,
            labels=labels,
            properties=properties,
            element_id=element_id
        ).to_structure()

    def _convert_relationship(self, rel_dict: Dict) -> BoltRelationship:
        """Convert a nxcypher relationship dict to BoltRelationship structure."""
        # Extract edge identifiers
        start_node = rel_dict.get('__start_node__')
        end_node = rel_dict.get('__end_node__')
        rel_type = rel_dict.get('__rel_type__', rel_dict.get('__labels__', [''])[0] if isinstance(rel_dict.get('__labels__'), (list, set)) else '')
        edge_key = rel_dict.get('__edge_key__')

        # Create edge tuple for ID lookup
        edge_tuple = (start_node, end_node, edge_key) if edge_key else (start_node, end_node)
        edge_id = self._get_edge_id(edge_tuple)

        start_id = self._get_node_id(start_node) if start_node else 0
        end_id = self._get_node_id(end_node) if end_node else 0

        # Get properties (exclude internal keys)
        properties = {
            k: v for k, v in rel_dict.items()
            if not k.startswith('__')
        }

        return BoltRelationship(
            id=edge_id,
            start_node_id=start_id,
            end_node_id=end_id,
            type=str(rel_type),
            properties=properties,
            element_id=str(edge_tuple),
            start_element_id=str(start_node),
            end_element_id=str(end_node)
        ).to_structure()

    def _convert_path(self, path_dict: Dict) -> BoltPath:
        """Convert a nxcypher path dict to BoltPath structure."""
        path_data = path_dict.get('__path__', [])

        nodes: List[BoltNode] = []
        relationships: List[BoltUnboundRelationship] = []
        indices: List[int] = []

        node_index_map: Dict[Hashable, int] = {}
        rel_index = 0

        for i, element in enumerate(path_data):
            if i % 2 == 0:
                # Node
                if isinstance(element, dict) and '__node_id__' in element:
                    node_key = element['__node_id__']
                    if node_key not in node_index_map:
                        node_index_map[node_key] = len(nodes)
                        node_id = self._get_node_id(node_key)
                        labels = list(element.get('__labels__', []))
                        properties = {k: v for k, v in element.items() if not k.startswith('__')}
                        nodes.append(BoltNode(
                            id=node_id,
                            labels=labels,
                            properties=properties,
                            element_id=str(node_key)
                        ))
                    indices.append(node_index_map[node_key])
            else:
                # Relationship
                if isinstance(element, dict):
                    rel_type = element.get('__rel_type__', '')
                    edge_key = element.get('__edge_key__')
                    start_node = element.get('__start_node__')
                    end_node = element.get('__end_node__')

                    edge_tuple = (start_node, end_node, edge_key) if edge_key else (start_node, end_node)
                    edge_id = self._get_edge_id(edge_tuple)

                    properties = {k: v for k, v in element.items() if not k.startswith('__')}

                    # Determine direction: positive index = forward, negative = backward
                    # Check if the next node in path matches the end_node
                    if i + 1 < len(path_data):
                        next_element = path_data[i + 1]
                        if isinstance(next_element, dict) and '__node_id__' in next_element:
                            next_node_key = next_element['__node_id__']
                            if next_node_key == end_node:
                                # Forward direction
                                indices.append(rel_index + 1)
                            else:
                                # Backward direction
                                indices.append(-(rel_index + 1))
                        else:
                            indices.append(rel_index + 1)
                    else:
                        indices.append(rel_index + 1)

                    relationships.append(BoltUnboundRelationship(
                        id=edge_id,
                        type=str(rel_type),
                        properties=properties,
                        element_id=str(edge_tuple)
                    ))
                    rel_index += 1

        return BoltPath(
            nodes=nodes,
            relationships=relationships,
            indices=indices
        ).to_structure()
