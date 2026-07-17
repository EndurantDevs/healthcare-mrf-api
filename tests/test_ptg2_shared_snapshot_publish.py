# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

import pytest

from process.ptg_parts.ptg2_shared_snapshot_publish import (
    _run_independent_publication_lanes,
)


@pytest.mark.asyncio
async def test_independent_publication_lanes_start_together():
    started: set[str] = set()
    release = asyncio.Event()

    async def lane(name: str) -> str:
        started.add(name)
        if len(started) == 4:
            release.set()
        await asyncio.wait_for(release.wait(), timeout=0.5)
        return name

    results = await _run_independent_publication_lanes(
        finalizer_blocks=lambda: lane("finalizer_blocks"),
        provider_graph=lambda: lane("provider_graph"),
        price=lambda: lane("price"),
        source_witness=lambda: lane("source_witness"),
    )

    assert started == {
        "finalizer_blocks",
        "provider_graph",
        "price",
        "source_witness",
    }
    assert results == (
        "finalizer_blocks",
        "provider_graph",
        "price",
        "source_witness",
    )
