# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pre-resolved canonical release-selection reuse coverage."""

import asyncio
from types import SimpleNamespace

from api import plan_release_serving, ptg2_serving

from .test_plan_release_serving import (
    PLAN_RELEASE_ID,
    _install_single_snapshot_search,
    _network_binding,
    _release_selection,
)


def test_release_query_reuses_supplied_release_selection(monkeypatch):
    selection = _release_selection(
        _network_binding(0, "ptg2:release-old", "aetna-network-a")
    )
    calls = []
    _install_single_snapshot_search(monkeypatch, selection, calls)

    async def fail_release_resolver(*_args, **_kwargs):
        raise AssertionError("supplied release selection must be reused")

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fail_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            release_selection=selection,
        )
    )

    assert response["plan_release_id"] == PLAN_RELEASE_ID
    assert calls[0][0] == "ptg2:release-old"


def test_release_query_rejects_mismatched_supplied_release_selection(
    monkeypatch,
):
    selection = _release_selection(
        _network_binding(0, "ptg2:release-old", "aetna-network-a")
    )
    mismatched_selection = plan_release_serving.PlanReleaseServingSelection(
        **{
            **selection.__dict__,
            "plan_release_id": "hprelease_" + "9" * 26,
        }
    )

    async def fail_release_resolver(*_args, **_kwargs):
        raise AssertionError("mismatched supplied selection must fail closed")

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fail_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            release_selection=mismatched_selection,
        )
    )

    assert response is None


def test_release_query_does_not_reresolve_explicit_resolution_miss(
    monkeypatch,
):
    async def fail_release_resolver(*_args, **_kwargs):
        raise AssertionError("explicit resolution miss must be reused")

    monkeypatch.setattr(
        ptg2_serving,
        "resolve_plan_release_serving",
        fail_release_resolver,
    )

    response = asyncio.run(
        ptg2_serving.search_current_ptg2_index(
            object(),
            {"plan_release_id": PLAN_RELEASE_ID, "code": "99213"},
            SimpleNamespace(limit=25, offset=0, page=1, source="page"),
            release_selection=None,
        )
    )

    assert response is None
