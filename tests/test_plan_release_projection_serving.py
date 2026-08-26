# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Release-serving coverage specific to immutable pricing projections."""

import asyncio

from api import plan_release_serving, plan_release_serving_resolution

from .test_plan_release_serving import (
    PLAN_RELEASE_ID,
    _binding_row,
    _is_serving_binding_ready,
    _Session,
)


def test_projection_only_release_resolution_skips_snapshot_readiness(
    monkeypatch,
):
    projection_id = "f" * 64
    session = _Session([_binding_row(pricing_projection_id=projection_id)])

    async def fail_readiness(*_args, **_kwargs):
        raise AssertionError("the immutable projection does not read PTG tables")

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        fail_readiness,
    )

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            session,
            PLAN_RELEASE_ID,
            projection_only=True,
        )
    )

    assert selection is not None
    assert selection.pricing_projection_id == projection_id
    assert selection._validated_serving_tables == ()
    sql = session.calls[-1][0]
    assert "plan_pricing_projection_candidate" in sql
    assert "revision.binding_set_digest" in sql
    assert "pricing_projection.content_digest" in sql


def test_guard_release_resolution_never_reads_projection_metadata():
    session = _Session([_binding_row(pricing_projection_id=None)])

    selection = asyncio.run(
        plan_release_serving_resolution.resolve_plan_release_guard_selection(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert selection is not None
    assert selection.pricing_projection_id is None
    assert len(session.calls) == 1
    sql = session.calls[0][0]
    assert "to_regclass" not in sql
    assert "plan_pricing_projection_candidate" not in sql


def test_release_resolution_without_projection_relation_keeps_full_path(
    monkeypatch,
):
    session = _Session(
        [_binding_row()],
        pricing_projection_relation=False,
    )
    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        _is_serving_binding_ready,
    )

    selection = asyncio.run(
        plan_release_serving.resolve_plan_release_serving(
            session,
            PLAN_RELEASE_ID,
        )
    )

    assert selection is not None
    assert selection.pricing_projection_id is None
    release_sql = session.calls[-1][0]
    assert "NULL::varchar(64) AS pricing_projection_id" in release_sql
    assert "plan_pricing_projection_candidate" not in release_sql
