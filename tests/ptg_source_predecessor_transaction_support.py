# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared transaction fixtures for predecessor-retirement unit tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import source_snapshot_predecessor_retirement as retirement


@pytest.fixture(autouse=True)
def _projection_queue_fakes(monkeypatch):
    """Keep compatibility projection deterministic in transaction tests."""

    monkeypatch.setattr(
        retirement,
        "mark_legacy_global_projection_dirty",
        AsyncMock(),
    )
    monkeypatch.setattr(
        retirement,
        "drain_legacy_global_projection_queue",
        AsyncMock(return_value=SimpleNamespace(reconciled=1)),
    )


class _Transaction:
    """Minimal transaction boundary with observable enter/exit counts."""

    def __init__(self):
        self.session = object()
        self.entered = 0
        self.exited = 0

    async def __aenter__(self):
        self.entered += 1
        return self.session

    async def __aexit__(self, _exc_type, _exc, _traceback):
        self.exited += 1
        return False


__all__ = ["_Transaction", "_projection_queue_fakes"]
