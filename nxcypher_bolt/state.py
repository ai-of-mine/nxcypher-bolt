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
Bolt connection state machine.

Defines the connection states and valid transitions for the Bolt protocol.
"""

from enum import Enum, auto


class ConnectionState(Enum):
    """
    Bolt connection states.

    State diagram:
        NEGOTIATION → AUTHENTICATION → READY ⇄ STREAMING
                                         ↓
                                    TX_READY ⇄ TX_STREAMING
                                         ↓
                                      FAILED → DEFUNCT
    """
    NEGOTIATION = auto()    # Initial state, version negotiation
    AUTHENTICATION = auto()  # Post-handshake, awaiting HELLO/LOGON
    READY = auto()          # Ready for queries
    STREAMING = auto()      # Streaming results from RUN
    TX_READY = auto()       # In transaction, ready for queries
    TX_STREAMING = auto()   # In transaction, streaming results
    FAILED = auto()         # Error occurred, need RESET
    DEFUNCT = auto()        # Connection is dead


# Valid state transitions
VALID_TRANSITIONS = {
    ConnectionState.NEGOTIATION: {ConnectionState.AUTHENTICATION, ConnectionState.DEFUNCT},
    ConnectionState.AUTHENTICATION: {ConnectionState.READY, ConnectionState.DEFUNCT},
    ConnectionState.READY: {
        ConnectionState.STREAMING,
        ConnectionState.TX_READY,
        ConnectionState.FAILED,
        ConnectionState.DEFUNCT
    },
    ConnectionState.STREAMING: {
        ConnectionState.READY,
        ConnectionState.STREAMING,  # Can continue streaming
        ConnectionState.FAILED,
        ConnectionState.DEFUNCT
    },
    ConnectionState.TX_READY: {
        ConnectionState.TX_STREAMING,
        ConnectionState.READY,  # After COMMIT/ROLLBACK
        ConnectionState.FAILED,
        ConnectionState.DEFUNCT
    },
    ConnectionState.TX_STREAMING: {
        ConnectionState.TX_READY,
        ConnectionState.TX_STREAMING,  # Can continue streaming
        ConnectionState.FAILED,
        ConnectionState.DEFUNCT
    },
    ConnectionState.FAILED: {
        ConnectionState.READY,  # After RESET
        ConnectionState.DEFUNCT
    },
    ConnectionState.DEFUNCT: set()  # Terminal state
}


class StateMachine:
    """
    Manages Bolt connection state transitions.
    """

    def __init__(self):
        self._state = ConnectionState.NEGOTIATION

    @property
    def state(self) -> ConnectionState:
        """Get current state."""
        return self._state

    def can_transition_to(self, new_state: ConnectionState) -> bool:
        """Check if transition to new_state is valid."""
        return new_state in VALID_TRANSITIONS.get(self._state, set())

    def transition_to(self, new_state: ConnectionState) -> None:
        """
        Transition to a new state.

        Raises:
            ValueError: If the transition is not valid.
        """
        if not self.can_transition_to(new_state):
            raise ValueError(
                f"Invalid state transition: {self._state.name} → {new_state.name}"
            )
        self._state = new_state

    def reset(self) -> None:
        """Reset to READY state (used after RESET message in FAILED state)."""
        if self._state == ConnectionState.FAILED:
            self._state = ConnectionState.READY
        elif self._state != ConnectionState.DEFUNCT:
            # RESET from any non-defunct state goes to READY
            self._state = ConnectionState.READY

    def is_defunct(self) -> bool:
        """Check if connection is defunct."""
        return self._state == ConnectionState.DEFUNCT

    def is_in_transaction(self) -> bool:
        """Check if currently in a transaction."""
        return self._state in (ConnectionState.TX_READY, ConnectionState.TX_STREAMING)

    def is_streaming(self) -> bool:
        """Check if currently streaming results."""
        return self._state in (ConnectionState.STREAMING, ConnectionState.TX_STREAMING)

    def mark_defunct(self) -> None:
        """Mark connection as defunct."""
        self._state = ConnectionState.DEFUNCT
