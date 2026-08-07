# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio

from api import plan_release_serving
from api import plan_release_serving_resolution

PLAN_RELEASE_ID = "hprelease_" + "0" * 26
PLAN_ID = "hpplan_" + "1" * 26
PLAN_VERSION_ID = "hpversion_" + "2" * 26
SERVING_REVISION_ID = "hpserve_" + "3" * 26


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), params))
        return list(self.rows)


class _SequenceSession:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), params))
        return list(self.results.pop(0))


def _binding_row(**updates):
    row_by_field = {
        "serving_revision_id": SERVING_REVISION_ID,
        "plan_release_id": PLAN_RELEASE_ID,
        "healthporta_plan_id": PLAN_ID,
        "plan_version_id": PLAN_VERSION_ID,
        "release_month": "2026-07",
        "release_status": "published",
        "expected_binding_count": 1,
        "binding_set_digest": "a" * 64,
        "binding_ordinal": 0,
        "snapshot_id": "ptg2:synthetic-release",
        "source_key": "synthetic-network-a",
        "plan_id": "synthetic-plan-id",
        "plan_market_type": "group",
        "role": "in_network",
        "required": True,
        "snapshot_status": "published",
        "is_pinned": True,
    }
    row_by_field.update(updates)
    return row_by_field


async def _is_serving_binding_ready(
    _session,
    binding,
    **readiness_context,
):
    serving_tables = readiness_context["validated_serving_tables_by_snapshot_id"]
    if binding.role == "in_network":
        serving_tables[binding.snapshot_id] = object()
    return True


def test_typed_resolution_distinguishes_absent_from_unavailable():
    absent = _Session([])
    incomplete = _Session([_binding_row(expected_binding_count=2)])

    absent_resolution = asyncio.run(
        plan_release_serving_resolution.resolve_plan_release_serving_resolution(
            absent,
            PLAN_RELEASE_ID,
        )
    )
    unavailable_resolution = asyncio.run(
        plan_release_serving_resolution.resolve_plan_release_serving_resolution(
            incomplete,
            PLAN_RELEASE_ID,
        )
    )

    assert absent_resolution.state == "not_found"
    assert absent_resolution.selection is None
    assert unavailable_resolution.state == "unavailable"
    assert unavailable_resolution.selection is None
    assert repr(absent_resolution) == (
        "<plan-release-serving-resolution state=not_found>"
    )


def test_typed_resolution_marks_existing_unpublished_release_unavailable():
    session = _SequenceSession([[], [{"release_exists": True}]])

    resolution = asyncio.run(
        plan_release_serving_resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert resolution.state == "unavailable"
    assert resolution.selection is None
    assert "plan_release_snapshot_binding" in session.calls[0][0]
    assert "plan_release_snapshot_binding" not in session.calls[1][0]


def test_typed_resolution_returns_ready_selection(monkeypatch):
    session = _Session([_binding_row()])
    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        _is_serving_binding_ready,
    )

    resolution = asyncio.run(
        plan_release_serving_resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert resolution.state == "ready"
    assert resolution.selection is not None
    assert resolution.selection.plan_release_id == PLAN_RELEASE_ID


def test_typed_resolution_treats_malformed_id_as_absent():
    session = _Session([_binding_row()])

    resolution = asyncio.run(
        plan_release_serving_resolution.resolve_plan_release_serving_resolution(
            session,
            "hprelease_not-canonical",
        )
    )

    assert resolution.state == "not_found"
    assert resolution.selection is None
    assert session.calls == []
