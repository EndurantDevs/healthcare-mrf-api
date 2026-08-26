# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
from unittest.mock import AsyncMock

from api import plan_release_serving_resolution as resolution

PLAN_RELEASE_ID = "hprelease_" + "0" * 26
PLAN_ID = "hpplan_" + "1" * 26
PLAN_VERSION_ID = "hpversion_" + "2" * 26
SERVING_REVISION_ID = "hpserve_" + "3" * 26
SERVING_REVISION_PUBLISHED_AT = "2026-08-25T12:34:56.123456Z"


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
        "serving_revision_published_at": SERVING_REVISION_PUBLISHED_AT,
        "plan_release_id": PLAN_RELEASE_ID,
        "healthporta_plan_id": PLAN_ID,
        "plan_version_id": PLAN_VERSION_ID,
        "release_month": "2026-07",
        "release_status": "published",
        "expected_binding_count": 1,
        "binding_set_digest": "a" * 64,
        "binding_ordinal": 0,
        "snapshot_id": "ptg2:release-old",
        "source_key": "synthetic-network-a",
        "plan_id": "99-0000001",
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
        resolution.resolve_plan_release_serving_resolution(
            absent,
            PLAN_RELEASE_ID,
        )
    )
    unavailable_resolution = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
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
    session = _SequenceSession(
        [
            [],
            [{"release_exists": True}],
        ]
    )

    serving_resolution = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert serving_resolution.state == "unavailable"
    assert serving_resolution.selection is None
    assert len(session.calls) == 2


def test_existence_probe_detects_release_without_snapshot_bindings():
    session = _SequenceSession(
        [
            [],
            [{"release_exists": True}],
        ]
    )

    serving_resolution = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
        )
    )

    ready_sql = session.calls[0][0]
    existence_sql = session.calls[1][0]
    assert "plan_release_snapshot_binding" in ready_sql
    assert "plan_release_snapshot_binding" not in existence_sql
    assert serving_resolution.state == "unavailable"


def test_typed_resolution_returns_ready_selection(monkeypatch):
    session = _Session([_binding_row()])
    monkeypatch.setattr(
        resolution.plan_release_serving,
        "is_release_binding_serving_ready",
        _is_serving_binding_ready,
    )

    serving_resolution = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert serving_resolution.state == "ready"
    assert serving_resolution.selection is not None
    assert serving_resolution.selection.plan_release_id == PLAN_RELEASE_ID


def test_typed_resolution_rejects_release_binding_fanout_before_readiness(
    monkeypatch,
):
    row_limit = resolution.MAX_BILLING_SEARCH_RELEASE_BINDINGS + 1
    release_rows = [
        _binding_row(
            expected_binding_count=row_limit,
            binding_ordinal=ordinal,
            snapshot_id=f"ptg2:release-{ordinal}",
            source_key=f"synthetic-network-{ordinal}",
        )
        for ordinal in range(row_limit)
    ]
    session = _Session(release_rows)
    readiness = AsyncMock()
    monkeypatch.setattr(
        resolution.plan_release_serving,
        "is_release_binding_serving_ready",
        readiness,
    )

    serving_resolution = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert serving_resolution.state == "unavailable"
    assert serving_resolution.selection is None
    assert len(session.calls) == 1
    assert f"LIMIT {row_limit}" in session.calls[0][0]
    readiness.assert_not_awaited()


def test_typed_resolution_pins_billing_source_metadata(monkeypatch):
    session = _Session([_binding_row()])
    readiness_calls = []

    async def is_source_ready(_session, binding, **readiness_context):
        readiness_calls.append(readiness_context)
        readiness_context["validated_serving_tables_by_snapshot_id"][
            binding.snapshot_id
        ] = object()
        return True

    monkeypatch.setattr(
        resolution.plan_release_serving,
        "is_release_binding_serving_ready",
        is_source_ready,
    )

    result = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
            include_billing_tax_identity_source=True,
        )
    )

    assert result.state == "ready"
    assert result.selection is not None
    assert result.selection.includes_billing_tax_identity_source is True
    assert readiness_calls[0]["include_billing_tax_identity_source"] is True


def test_typed_resolution_rejects_non_boolean_source_option():
    session = _Session([_binding_row()])

    result = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            PLAN_RELEASE_ID,
            include_billing_tax_identity_source=1,
        )
    )

    assert result.state == "unavailable"
    assert result.selection is None
    assert session.calls == []


def test_typed_resolution_treats_malformed_id_as_absent():
    session = _Session([_binding_row()])

    serving_resolution = asyncio.run(
        resolution.resolve_plan_release_serving_resolution(
            session,
            "hprelease_not-canonical",
        )
    )

    assert serving_resolution.state == "not_found"
    assert serving_resolution.selection is None
    assert session.calls == []
