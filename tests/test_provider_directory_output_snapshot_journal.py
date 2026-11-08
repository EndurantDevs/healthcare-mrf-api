# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Maintenance history preserves the base plan, exact targets and every predecessor."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import asyncpg
import pytest

from process import provider_directory_output_snapshot_custody as custody
from process import provider_directory_output_snapshot_journal as journal
from process.provider_directory_output_snapshot_catalog import OUTPUT_ROOTS, decoded
from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt, retained_name
from process.provider_directory_output_snapshot_stage import OutputStage
from tests.test_provider_directory_output_snapshot_custody import operation, relation


def _plan():
    return dict(
        complete=True,
        tables={name: {"original_oid": index + 1} for index, name in enumerate(journal.TABLES)},
        keys=["historic"],
        references=[{"oid": 91}],
        relationships=[],
        writers={"old": ["writer"]},
        legacy_adoption={"members": [{"old": 91, "new": 92}]},
    )


def _capability(intent=None):
    intent = intent or operation()
    targets = (OUTPUT_ROOTS[0],)
    return SimpleNamespace(
        operation=intent,
        operation_digest=custody._operation_digest(intent, targets),
        targets=targets,
        reference=intent.get("rollback_of"),
        policy=SimpleNamespace(schema="source_schema"),
        connection=SimpleNamespace(fetchval=AsyncMock(return_value=True)),
    )


def _prepared(capability):
    old = relation(OUTPUT_ROOTS[0], 101)
    new = relation("private_a", 201, descriptor={"phase": "sealed"}, readable=False)
    return custody.PreparedOutputReceipt(1, capability.operation_digest, old, new)


def _forward(capability, preparation):
    return RetainedOutputReceipt(
        1,
        capability.operation_digest,
        preparation.incumbent.schema,
        preparation.incumbent.target,
        retained_name(preparation.incumbent.oid),
        replace(preparation.incumbent, relation=retained_name(preparation.incumbent.oid)),
        replace(preparation.successor, relation=preparation.incumbent.target, acl=preparation.incumbent.acl),
    )


@pytest.mark.asyncio
async def test_progress_and_seal_preserve_every_unrelated_plan_value(monkeypatch):
    plan, capability = _plan(), _capability()
    prior = deepcopy(plan)
    preparation = _prepared(capability)
    stage = OutputStage(
        "source_schema", OUTPUT_ROOTS[0], "private_a", 201, capability.operation_digest, preparation.incumbent
    )
    monkeypatch.setattr(journal, "_plan", AsyncMock(side_effect=lambda cap: deepcopy(plan)))
    monkeypatch.setattr(journal, "_stage_row", AsyncMock())
    monkeypatch.setattr(custody, "_require_capability", AsyncMock())
    monkeypatch.setattr(custody, "capture_prepared_output", AsyncMock(return_value=preparation))

    async def is_cas_applied(query, updated, expected):
        assert decoded(expected) == plan and "AND plan=$2::jsonb" in query
        plan.clear()
        plan.update(decoded(updated))
        return True

    capability.connection.fetchval.side_effect = is_cas_applied
    await journal.record_output_progress(capability, stage, 125)
    await journal.record_output_progress(capability, stage, 513)
    await journal.record_output_prepared(capability, [preparation])
    assert {key: value for key, value in plan.items() if key != journal._KEY} == prior
    entry = plan[journal._KEY]["operations"][capability.operation_digest]
    assert entry["stages"][stage.target]["copied_rows"] == 513 and entry["phase"] == "prepared"
    count = capability.connection.fetchval.await_count
    with pytest.raises(RuntimeError, match="phase_invalid"):
        await journal.record_output_progress(capability, stage, 514)
    assert capability.connection.fetchval.await_count == count


def _prepared_plan(capability):
    plan = _plan()
    preparation = _prepared(capability)
    stage = OutputStage(
        "source_schema", OUTPUT_ROOTS[0], "private_a", 201, capability.operation_digest, preparation.incumbent
    )
    entry = journal._forward_entry(capability, plan)
    entry.update(
        phase="prepared",
        stages={stage.target: {"stage": stage.to_payload(), "copied_rows": 1}},
        prepared=[preparation.to_payload()],
    )
    return plan, preparation


@pytest.mark.asyncio
async def test_forward_inverse_and_redo_append_exact_history_under_original_key(monkeypatch):
    """Every inverse appends the exact reversed pair while prior history stays intact."""
    capability = _capability()
    plan, preparation = _prepared_plan(capability)
    original_digest, original_base = capability.operation_digest, deepcopy(plan)
    monkeypatch.setattr(journal, "_plan", AsyncMock(side_effect=lambda cap: deepcopy(plan)))
    monkeypatch.setattr(custody, "_require_capability", AsyncMock())

    async def is_cas_applied(query, updated, expected):
        assert decoded(expected) == plan
        plan.clear()
        plan.update(decoded(updated))
        return True

    capability.connection.fetchval.side_effect = is_cas_applied
    forward = _forward(capability, preparation)
    monkeypatch.setattr(
        custody, "capture_retained_output", AsyncMock(return_value=(forward.incumbent, forward.successor))
    )
    await journal.append_output_history(capability, [preparation], [forward])
    first = deepcopy(plan[journal._KEY]["operations"][original_digest]["history"][0])
    previous = forward
    for run_id in ("inverse", "redo"):
        rollback_operation_by_field = {
            **operation("rollback"),
            "run_id": run_id,
            "rollback_of": previous.operation_digest,
        }
        rollback = _capability(rollback_operation_by_field)
        rollback.connection = capability.connection
        stored = await journal.read_output_history(rollback, original_digest)
        assert stored == (previous,)
        inverse = RetainedOutputReceipt(
            1,
            rollback.operation_digest,
            previous.schema,
            previous.target,
            retained_name(previous.successor.oid),
            replace(previous.successor, relation=retained_name(previous.successor.oid)),
            replace(previous.incumbent, relation=previous.target),
        )
        monkeypatch.setattr(
            custody, "capture_restored_output", AsyncMock(return_value=(inverse.incumbent, inverse.successor))
        )
        consumed = custody.PreparedOutputReceipt(1, previous.operation_digest, previous.incumbent, previous.successor)
        await journal.append_output_history(rollback, [consumed], [inverse])
        previous = inverse
    histories = plan[journal._KEY]["operations"][original_digest]["history"]
    assert len(histories) == 3 and histories[0] == first
    assert {key: field_value for key, field_value in plan.items() if key != journal._KEY} == {
        key: field_value for key, field_value in original_base.items() if key != journal._KEY
    }
    changed = deepcopy(plan)
    changed[journal._KEY]["operations"][original_digest]["history"][1]["receipts"][0]["successor"]["oid"] = 777
    with pytest.raises(RuntimeError, match="history_continuity_changed"):
        journal._journal(changed)


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [None, "serialization"])
async def test_full_plan_cas_conflict_never_overwrites_other_extensions(monkeypatch, result):
    capability, prior = _capability(), _plan()
    monkeypatch.setattr(custody, "_require_capability", AsyncMock())
    if result == "serialization":
        capability.connection.fetchval.side_effect = asyncpg.SerializationError("concurrent plan")
    else:
        capability.connection.fetchval.return_value = result
    updated = deepcopy(prior)
    journal._forward_entry(capability, updated)
    with pytest.raises(RuntimeError, match="journal_conflict"):
        await journal._cas(capability, prior, updated)
    assert decoded(capability.connection.fetchval.await_args.args[2]) == prior
    updated["legacy_adoption"]["members"].append({"old": 19, "new": 20})
    with pytest.raises(RuntimeError, match="unrelated_plan_changed"):
        await journal._cas(capability, prior, updated)
    assert capability.connection.fetchval.await_count == 1


@pytest.mark.parametrize("change", ["protocol", "targets", "digest", "phase", "stale_reference"])
def test_malformed_or_partial_journal_cannot_become_restore_authority(change):
    capability, plan = _capability(), _plan()
    entry = journal._forward_entry(capability, plan)
    if change == "protocol":
        plan[journal._KEY]["protocol"] = True
    elif change == "targets":
        entry["targets"].append(OUTPUT_ROOTS[1])
    elif change == "digest":
        entry["operation"]["run_id"] = "other"
    elif change == "phase":
        entry["phase"] = "published"
    else:
        entry["history"] = [{"reference": "f" * 64}]
    with pytest.raises(RuntimeError):
        journal._journal(plan)
