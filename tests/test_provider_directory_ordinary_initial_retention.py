# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initial target custody stays exact while existing stage installation proceeds."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_ordinary_initial_retention as initial
from process import provider_directory_ordinary_retention_capacity as capacity
from process import provider_directory_ordinary_row_retention as rows


@pytest.fixture
def case(monkeypatch):
    stage = SimpleNamespace(
        target_relation="provider_directory_profile", schema="fixture", profile_initial_build=object()
    )
    directory = SimpleNamespace(db=object(), _schema=lambda: "fixture")
    prepared = SimpleNamespace(
        fhir=directory, profile_bundle=SimpleNamespace(stages=[stage]), nonprofile_admission=object()
    )
    actor_by_field = {
        "owner": 11,
        "database_oid": 12,
        "backend": 13,
        "xid": "14",
        "session": "builder",
        "current": "builder",
    }
    original = rows.RetainedRows(
        "family",
        "profile",
        "fixture",
        stage.target_relation,
        21,
        stage.target_relation,
        21,
        "a" * 64,
        (),
        50,
        None,
        0,
        True,
    )
    retained = SimpleNamespace(
        capture=SimpleNamespace(predecessor={"kind": "initial"}), cutover_actor=actor_by_field, families=(original,)
    )
    native_roots_by_relation = {stage.target_relation: {"oid": 21, "relname": stage.target_relation}}
    monkeypatch.setattr(rows, "_require_retained", lambda actual: retained if actual is prepared else None)
    monkeypatch.setattr(initial.receipts, "ordinary", AsyncMock(return_value=(object(), object(), actor_by_field)))
    retain = AsyncMock()
    acknowledge = AsyncMock()
    monkeypatch.setattr(initial.receipts, "retain", retain)
    monkeypatch.setattr(capacity, "acknowledge_initial_retained_relation", acknowledge, raising=False)
    return SimpleNamespace(
        stage=stage,
        directory=directory,
        prepared=prepared,
        actor=actor_by_field,
        original=original,
        retained=retained,
        native_roots=native_roots_by_relation,
        retain=retain,
        acknowledge=acknowledge,
    )


@pytest.mark.asyncio
async def test_initial_original_and_inventory_move_only_to_the_existing_oid_name(case):
    with initial.initial_retention_scope(case.prepared, case.native_roots):
        await initial.retain_initial_stage(case.directory, case.stage)
    case.retain.assert_awaited_once_with(
        case.directory.db, "fixture", {"native": case.native_roots[case.stage.target_relation]}
    )
    case.acknowledge.assert_awaited_once_with(case.prepared.nonprofile_admission, case.directory, family="profile")
    saved = case.retained.families[0]
    assert saved.name == "pdsrv_r_15"
    assert saved.oid == case.original.oid == saved.source_oid
    assert saved.source == case.original.source and saved.rows == case.original.rows and saved.reused_initial
    assert initial._ACTIVE.get() is None


@pytest.mark.asyncio
async def test_default_publication_has_no_initial_retention_side_effects(case):
    await initial.retain_initial_stage(case.directory, case.stage)
    case.retain.assert_not_awaited()
    case.acknowledge.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["actor", "stage", "oid"])
async def test_changed_cut_actor_stage_or_original_oid_fails_before_rename(case, monkeypatch, drift):
    stage = case.stage
    if drift == "actor":
        monkeypatch.setattr(
            initial.receipts, "ordinary", AsyncMock(return_value=(object(), object(), case.actor | {"xid": "15"}))
        )
    elif drift == "stage":
        stage = SimpleNamespace(**vars(case.stage))
    else:
        case.native_roots[stage.target_relation]["oid"] = 22
    with initial.initial_retention_scope(case.prepared, case.native_roots):
        with pytest.raises(ValueError, match="serving_recovery_"):
            await initial.retain_initial_stage(case.directory, stage)
    case.retain.assert_not_awaited()
    assert case.retained.families == (case.original,)


@pytest.mark.asyncio
async def test_copied_async_context_does_not_transfer_initial_cut_custody(case):
    with initial.initial_retention_scope(case.prepared, case.native_roots):
        with pytest.raises(ValueError, match="initial_retention_owner_changed"):
            await asyncio.create_task(initial.retain_initial_stage(case.directory, case.stage))
    case.retain.assert_not_awaited()


@pytest.mark.asyncio
async def test_failed_acknowledgment_preserves_descriptor_for_transaction_rollback(case):
    case.acknowledge.side_effect = RuntimeError("inventory_changed")
    with initial.initial_retention_scope(case.prepared, case.native_roots):
        with pytest.raises(RuntimeError, match="inventory_changed"):
            await initial.retain_initial_stage(case.directory, case.stage)
    assert case.retained.families == (case.original,)
    case.retain.assert_awaited_once()
    assert initial._ACTIVE.get() is None
