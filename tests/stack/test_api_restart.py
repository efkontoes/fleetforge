"""The API must come back after its container restarts (the metrics tmpfs is remounted)."""

from __future__ import annotations

import httpx
import pytest

from .conftest import API, compose, wait_until

pytestmark = pytest.mark.stack


def _ready() -> bool:
    try:
        return httpx.get(f"{API}/readyz", timeout=2).status_code == 200
    except httpx.HTTPError:
        return False


@pytest.mark.disruptive
def test_api_serves_again_after_a_container_restart():
    compose("restart", "api")
    wait_until(_ready, 90, msg="api ready after restart")
    metrics = httpx.get(f"{API}/metrics", timeout=5)
    assert metrics.status_code == 200  # the workers can write their metrics files again
