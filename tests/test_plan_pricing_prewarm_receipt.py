# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Terminal receipt coverage for release-scoped plan-pricing prewarm."""

from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_prewarm as prewarm

from .test_plan_pricing_prewarm import (
    PLAN_RELEASE_ID,
    PROJECTION_ID,
    SERVICE_ORIGIN,
    SERVING_REVISION_ID,
    TEST_BEARER,
    _Response,
    _selection,
)


@pytest.mark.asyncio
async def test_post_warm_selection_drift_amends_terminal_receipt(
    monkeypatch,
) -> None:
    selection = _selection()
    shapes = (prewarm.PrewarmShape("HCPCS", "G0439", "10002", 20),)
    complete_receipt = prewarm._receipt(
        selection,
        shapes,
        0,
        (),
        (prewarm._PrewarmResult(cache_key_digest="9" * 64, payload_bytes=123),),
    )
    exact_selection = AsyncMock(
        side_effect=[selection, ValueError("selection changed")]
    )
    monkeypatch.setattr(prewarm, "_exact_ready_selection", exact_selection)
    monkeypatch.setattr(prewarm, "_select_shapes", AsyncMock(return_value=shapes))
    monkeypatch.setattr(
        prewarm,
        "prewarm_http_config",
        lambda: prewarm.PrewarmHttpConfig(
            base_url=SERVICE_ORIGIN,
            token=TEST_BEARER,
            verify_tls=False,
        ),
    )
    monkeypatch.setattr(
        prewarm,
        "_prewarm_shapes",
        AsyncMock(return_value=complete_receipt),
    )
    monkeypatch.setattr(prewarm.db, "transaction", lambda: _Response({}))
    monkeypatch.setattr(
        prewarm.aiohttp,
        "ClientSession",
        lambda **_kwargs: _Response({}),
    )

    receipt = await prewarm.prewarm_plan_pricing(
        plan_release_id=PLAN_RELEASE_ID,
        serving_revision_id=SERVING_REVISION_ID,
        projection_id=PROJECTION_ID,
    )

    assert exact_selection.await_count == 2
    assert receipt["status"] == "partial"
    assert receipt["errors"] == [{"error": "release_identity_changed"}]
    assert receipt["warmed_shape_count"] == 1
    assert receipt["receipt_digest"] != complete_receipt["receipt_digest"]
    assert receipt["terminal_progress"]["message"] == "partial"
