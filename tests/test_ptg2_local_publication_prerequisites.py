# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native publication admission preserves existing model and coordinate guards."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_expression_adoption import _normalized_expression
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_validation as validation
from tests.ptg2_v4_attempt_migration_postgres_support import FENCE_MIGRATION, migration


@pytest.mark.parametrize(
    "table_name",
    [
        "ptg2_snapshot",
        "ptg2_import_run",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_plan_scope",
        "ptg2_v3_candidate_audit_attestation",
        "ptg2_snapshot_pin",
        "ptg2_current_source_snapshot",
        "ptg2_current_plan_source",
    ],
)
def test_publication_coordinate_projection_matches_actual_historical_guard(table_name):
    """The runtime recognizer cannot silently change the installed coordinate contract."""
    revision = migration(FENCE_MIGRATION)
    attachment = next(
        item
        for item in (*revision.ATTEMPT_STATE_TABLES, *revision.ATTEMPT_ATTACHMENTS)
        if item.table_name == table_name
    )
    historical = revision._trigger_function_sql("synthetic", attachment).split("AS $$", 1)[1].rsplit("$$", 1)[0]
    expected = validation._local_publication_transition_body(
        "synthetic", attachment.snapshot_columns, attachment.run_columns
    )
    assert _normalized_expression(historical) == _normalized_expression(expected)


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "prepared", "installed", "controls"])
async def test_publisher_requires_both_real_views_and_canonical_controls(monkeypatch, refusal):
    """A missing native prerequisite stops before any candidate or pointer write."""
    events = []

    async def attest(_session, *, is_prepared):
        boundary = "prepared" if is_prepared else "installed"
        events.append(boundary)
        if refusal == boundary:
            raise native.PTG2PhysicalBindingError("unavailable")

    async def controls(_session):
        events.append("controls")
        if refusal == "controls":
            raise native.PTG2PhysicalBindingError("unavailable")

    monkeypatch.setattr(native, "require_local_physical_publisher_view", attest)
    monkeypatch.setattr(validation, "require_local_publication_controls", controls)
    if refusal:
        with pytest.raises(native.PTG2PhysicalBindingError):
            await native.require_local_binding_publisher(object())
        assert events[-1] == refusal
    else:
        await native.require_local_binding_publisher(object())
        assert events == ["prepared", "installed", "controls"]


@pytest.mark.asyncio
async def test_direct_local_publisher_rechecks_prerequisite_before_header(monkeypatch):
    """Calling the native cut directly cannot bypass startup/profile admission."""
    prerequisite = AsyncMock(side_effect=native.PTG2PhysicalBindingError("unavailable"))
    header = AsyncMock()
    monkeypatch.setattr(native, "require_local_binding_publisher", prerequisite)
    monkeypatch.setattr(native, "_prepared_local_header", header)
    with pytest.raises(native.PTG2PhysicalBindingError, match="unavailable"):
        await validation.publish_local_data_candidate_in_transaction(
            object(),
            operation={},
            expected_attestation_digest=b"a" * 32,
            rollback_owner_id="synthetic",
        )
    header.assert_not_awaited()


@pytest.mark.parametrize("drift", [None, "disabled", "event", "schema", "transition", "body", "definer"])
def test_native_transition_guard_drift_refuses(drift):
    """Names alone cannot attest a changed executable trigger or transition scope."""
    name = "ptg2_snapshot_attempt_insert_guard"
    expected_by_name = {name: (4, None, "attempt_new_rows", "guard_ptg2_snapshot_attempt", "BEGIN RETURN NULL; END;")}
    guard_by_field = dict(
        tgname=name,
        safe=True,
        tgenabled="O",
        nspname="synthetic",
        tgtype=4,
        tgoldtable=None,
        tgnewtable="attempt_new_rows",
        proname="guard_ptg2_snapshot_attempt",
        prosrc="BEGIN RETURN NULL; END;",
    )
    changed_by_drift = {
        "disabled": ("tgenabled", "D"),
        "event": ("tgtype", 16),
        "schema": ("nspname", "other"),
        "transition": ("tgnewtable", None),
        "body": ("prosrc", "BEGIN RETURN NEW; END;"),
        "definer": ("safe", False),
    }
    if drift:
        field, changed = changed_by_drift[drift]
        guard_by_field[field] = changed
    assert validation._local_publication_guard_matches(guard_by_field, expected_by_name, "synthetic") is (drift is None)


@pytest.mark.asyncio
async def test_control_catalog_refusal_is_not_an_activation_permission(monkeypatch):
    """Canonical catalog failures retain the caller transaction and never write a pointer."""
    events = []

    async def run_sync(callback):
        events.append("catalog")
        raise ValueError("drift")

    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(),
        connection=AsyncMock(return_value=SimpleNamespace(run_sync=run_sync)),
    )
    guards = AsyncMock()
    monkeypatch.setattr(validation, "_require_local_publication_attempt_guards", guards)
    with pytest.raises(native.PTG2PhysicalBindingError, match="catalog differs"):
        await validation.require_local_publication_controls(session)
    assert events == ["catalog"]
    guards.assert_not_awaited()
    assert "ACCESS SHARE MODE NOWAIT" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_transition_catalog_uses_native_text_flags():
    """PostgreSQL internal char flags must be text before matching their native contract."""
    from db import models

    table = models.PTG2Snapshot.__table__
    revision = migration(FENCE_MIGRATION)
    attachment = next(
        candidate_attachment
        for candidate_attachment in revision.ATTEMPT_STATE_TABLES
        if candidate_attachment.table_name == table.name
    )
    body = revision._trigger_function_sql("synthetic", attachment).split("AS $$", 1)[1].rsplit("$$", 1)[0]
    guards = [
        dict(
            tgname=table.name + "_attempt_" + suffix,
            tgtype=mask,
            tgenabled="O",
            tgoldtable=old,
            tgnewtable=new,
            proname=function,
            prosrc=body,
            nspname="synthetic",
            safe=True,
        )
        for suffix, mask, old, new, function in (
            ("lifecycle_lock", 30, None, None, "lock_ptg2_v4_attempt_lifecycle"),
            ("insert_guard", 4, None, "attempt_new_rows", "guard_ptg2_snapshot_attempt"),
            ("update_guard", 16, "attempt_old_rows", "attempt_new_rows", "guard_ptg2_snapshot_attempt"),
            ("delete_guard", 8, "attempt_old_rows", None, "guard_ptg2_snapshot_attempt"),
        )
    ]
    guard_query = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: guards))
    session = SimpleNamespace(execute=AsyncMock(return_value=guard_query))
    await validation._require_local_publication_transition_guards(session, "synthetic", table)
    assert "t.tgenabled::text AS tgenabled" in str(session.execute.await_args.args[0])
