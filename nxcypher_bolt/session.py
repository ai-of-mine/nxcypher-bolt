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
Bolt session management.

Manages query execution state and result streaming for a Bolt connection.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional
import copy

import networkx as nx


@dataclass
class QueryResult:
    """
    Holds query results for streaming.

    Attributes:
        fields: Column names from the query
        records: Row data (list of lists)
        cursor: Current position in records
    """
    fields: List[str]
    records: List[List[Any]]
    cursor: int = 0

    def pull(self, n: int = -1) -> List[List[Any]]:
        """
        Pull up to n records.

        Args:
            n: Number of records to pull. -1 means all remaining.

        Returns:
            List of records pulled.
        """
        if n < 0:
            # Pull all remaining
            result = self.records[self.cursor:]
            self.cursor = len(self.records)
        else:
            end = min(self.cursor + n, len(self.records))
            result = self.records[self.cursor:end]
            self.cursor = end
        return result

    def has_more(self) -> bool:
        """Check if there are more records to pull."""
        return self.cursor < len(self.records)

    def reset(self) -> None:
        """Reset cursor to beginning."""
        self.cursor = 0

    def discard(self, n: int = -1) -> int:
        """
        Discard up to n records.

        Args:
            n: Number of records to discard. -1 means all remaining.

        Returns:
            Number of records discarded.
        """
        if n < 0:
            discarded = len(self.records) - self.cursor
            self.cursor = len(self.records)
        else:
            old_cursor = self.cursor
            self.cursor = min(self.cursor + n, len(self.records))
            discarded = self.cursor - old_cursor
        return discarded


@dataclass
class BoltSession:
    """
    Manages session state for a Bolt connection.

    Attributes:
        graph: The NetworkX graph to query
        current_result: Active query result being streamed
        in_transaction: Whether in an explicit transaction
        tx_graph: Working copy of graph during transaction
        bookmarks: Transaction bookmarks
        database: Selected database name (for compatibility)
    """
    graph: nx.Graph
    current_result: Optional[QueryResult] = None
    in_transaction: bool = False
    tx_graph: Optional[nx.Graph] = None
    bookmarks: List[str] = field(default_factory=list)
    database: str = "neo4j"
    last_qid: int = -1  # Query ID for pipelining

    def begin_transaction(self) -> None:
        """Start a new transaction."""
        if self.in_transaction:
            raise RuntimeError("Already in transaction")
        # Create a working copy of the graph
        self.tx_graph = copy.deepcopy(self.graph)
        self.in_transaction = True

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.in_transaction:
            raise RuntimeError("Not in transaction")
        # Apply changes from tx_graph to main graph
        if self.tx_graph is not None:
            self.graph.clear()
            self.graph.update(self.tx_graph)
        self.tx_graph = None
        self.in_transaction = False

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.in_transaction:
            raise RuntimeError("Not in transaction")
        # Discard working copy
        self.tx_graph = None
        self.in_transaction = False

    def get_working_graph(self) -> nx.Graph:
        """Get the graph to use for queries (tx_graph if in transaction)."""
        if self.in_transaction and self.tx_graph is not None:
            return self.tx_graph
        return self.graph

    def set_result(self, result: QueryResult) -> int:
        """
        Set the current query result.

        Returns:
            Query ID for this result.
        """
        self.current_result = result
        self.last_qid += 1
        return self.last_qid

    def clear_result(self) -> None:
        """Clear the current query result."""
        self.current_result = None
