import pytest

from fleetforge.contracts.api import MissionStatus as S
from fleetforge.domain.missions import InvalidTransition, check_transition


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (S.PENDING, S.ASSIGNED),
        (S.ASSIGNED, S.RUNNING),
        (S.RUNNING, S.COMPLETED),
        (S.RUNNING, S.FAILED),
        (S.PENDING, S.CANCELLED),
        (S.ASSIGNED, S.CANCELLED),
        (S.RUNNING, S.CANCELLED),
    ],
)
def test_allowed(src, dst):
    check_transition(src, dst)


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (S.PENDING, S.RUNNING),  # cannot skip assignment
        (S.COMPLETED, S.RUNNING),  # terminal states are terminal
        (S.CANCELLED, S.ASSIGNED),
        (S.ASSIGNED, S.ASSIGNED),  # self-transitions would create versions without change
    ],
)
def test_forbidden(src, dst):
    with pytest.raises(InvalidTransition):
        check_transition(src, dst)
