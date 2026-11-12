"""Mission lifecycle state machine. Pure logic, no I/O."""

from fleetforge.contracts.api import MissionStatus as S

ALLOWED_TRANSITIONS: dict[S, frozenset[S]] = {
    S.PENDING: frozenset({S.ASSIGNED, S.CANCELLED}),
    S.ASSIGNED: frozenset({S.RUNNING, S.CANCELLED}),
    S.RUNNING: frozenset({S.COMPLETED, S.FAILED, S.CANCELLED}),
    S.COMPLETED: frozenset(),
    S.FAILED: frozenset(),
    S.CANCELLED: frozenset(),
}

INITIAL_STATUS = S.PENDING


class InvalidTransition(Exception):
    def __init__(self, from_status: S, to_status: S):
        super().__init__(f"transition {from_status} -> {to_status} is not allowed")
        self.from_status = from_status
        self.to_status = to_status


def check_transition(from_status: S, to_status: S) -> None:
    if to_status not in ALLOWED_TRANSITIONS[from_status]:
        raise InvalidTransition(from_status, to_status)
