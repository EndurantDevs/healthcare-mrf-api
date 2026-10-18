# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sparse compact coordinates remain exact plan and admission inputs."""

import importlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission
from tests.test_provider_directory_profile_control_capacity import _bound_control_wal_projection
from tests.test_provider_directory_profile_control_plan_boundaries import (
    _control_artifact_projection,
    _control_layout_by_oid,
    _control_plan_context,
)
from tests.test_provider_directory_profile_resume_lineage import _dataset, _source_context

fhir = importlib.import_module("process.provider_directory_fhir")


def _ranges():
    """Return the first, adjacent, and final canonical coordinates."""
    canonical = fhir._provider_directory_profile_npi_ranges(fhir.profile_artifact.PROFILE_NPI_BATCH_SIZE)
    return canonical[0], canonical[1], canonical[-1]


def _plan(compact_ranges=None, **options):
    """Build a full-swap plan using the existing default scope."""
    return fhir._provider_directory_profile_batch_plan(
        ("source-a",),
        ("source-a", "source-b"),
        ("dataset-a",),
        has_existing_artifacts=options.pop("has_existing_artifacts", False),
        compact_ranges=compact_ranges,
        **options,
    )


def test_sparse_coordinates_preserve_boundaries_copy_and_waves():
    """Copy remains isolated and sparse coordinates execute in canonical order."""
    ranges = _ranges()
    plan = _plan(ranges, has_existing_artifacts=True, compact_window_size=2)
    assert plan.compact_batches[0] == fhir._ProviderDirectoryProfileCompactBatch(kind="copy")
    assert tuple((batch.npi_start, batch.npi_end) for batch in plan.compact_batches[1:]) == ranges
    assert plan.evidence_batches[0].kind == "copy"
    assert plan.compact_waves == ((0, 1), (1, 3), (3, 4))
    assert ranges[0][0] == fhir.profile_artifact.NPI_MIN
    assert ranges[-1][1] == fhir.profile_artifact.NPI_MAX + 1


def test_none_full_grid_and_empty_inventory_are_distinct():
    """None retains the full fallback while an empty census schedules no NPI work."""
    fallback, empty, copy_only = _plan(), _plan(()), _plan((), has_existing_artifacts=True)
    assert len(fallback.compact_batches) == 400
    assert empty.compact_batches == empty.compact_waves == ()
    assert tuple(batch.kind for batch in copy_only.compact_batches) == ("copy",)
    assert copy_only.compact_waves == ((0, 1),)
    assert fallback.evidence_batches == empty.evidence_batches
    assert len({fallback.fingerprint, empty.fingerprint, copy_only.fingerprint}) == 3


@pytest.mark.parametrize(
    "ranges",
    [
        list(_ranges()),
        (_ranges()[0], _ranges()[0]),
        tuple(reversed(_ranges())),
        ((_ranges()[0][0] + 1, _ranges()[0][1]),),
        ((_ranges()[0][0], _ranges()[0][1] - 1),),
        ((True, _ranges()[0][1]),),
        ((float(_ranges()[0][0]), _ranges()[0][1]),),
        (list(_ranges()[0]),),
        ((_ranges()[0][0],),),
    ],
)
def test_sparse_coordinates_reject_noncanonical_or_mutable_geometry(ranges):
    """Reject drift, duplicate or reordered work, and mutable coordinates."""
    with pytest.raises(ValueError, match="canonical sorted subset"):
        _plan(ranges)


def test_sparse_ranges_require_canonical_batch_size_and_bind_sql_hash(monkeypatch):
    """Only canonical five-million coordinates may enter the sparse identity."""
    with pytest.raises(ValueError, match="canonical sorted subset"):
        _plan(_ranges(), npi_batch_size=10_000_000)
    original = _plan(_ranges())
    assert original.fingerprint != _plan(_ranges()[:-1]).fingerprint
    monkeypatch.setattr(fhir.profile_compact_inventory, "sql_contracts", lambda: {"inventory": "SELECT 1"})
    first = _plan(_ranges())
    assert fhir._provider_directory_profile_sql_contract_hashes()["inventory"]
    monkeypatch.setattr(fhir.profile_compact_inventory, "sql_contracts", lambda: {"inventory": "SELECT 2"})
    assert first.fingerprint != _plan(_ranges()).fingerprint


@pytest.mark.asyncio
async def test_control_wal_counts_exact_sparse_coordinates(monkeypatch):
    """Checkpoint, payload, and progress bounds use the exact signed series."""
    layout_by_oid = _control_layout_by_oid()
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(side_effect=lambda relation_oid, **_: layout_by_oid[relation_oid]),
    )
    monkeypatch.setattr(fhir, "_provider_directory_relation_oid", AsyncMock(return_value=15))
    monkeypatch.setattr(fhir, "_provider_directory_profile_toast_chunk_count", AsyncMock(return_value=0))
    database, _, identity, fence = _control_plan_context()
    for ranges in (_ranges(), ()):
        plan = _plan(ranges)
        plan_input = await fhir._provider_directory_profile_control_wal_plan_input(
            database, _control_artifact_projection(), plan, identity, fence
        )
        assert plan_input.compact_batch_count == len(ranges)
        _, projection = _bound_control_wal_projection(plan_input)
        operation_by_name = {operation.operation_name: operation for operation in projection.operations}
        for operation in ("profile_payload", "profile_checkpoint_advance", "profile_import_run_progress"):
            assert operation_by_name[operation].operation_count == len(ranges)


@pytest.mark.asyncio
async def test_identity_rederives_inventory_and_refuses_admission_drift(monkeypatch):
    """Fresh planning binds exact coordinates into both lineage and admission equality."""
    fence = fhir.ProviderDirectoryArtifactDatasetFence((_dataset(),))
    monkeypatch.setattr(
        fhir,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(return_value=(["source-a"], ["source-a"], (_source_context(),))),
    )
    census = AsyncMock(return_value=_ranges())
    monkeypatch.setattr(fhir.profile_compact_inventory, "compact_ranges", census)
    first = await fhir._profile_build_identity_inputs("example", fence)
    same = await fhir._profile_build_identity_inputs("example", fence)
    assert first == same and first.batch_plan.compact_batches == _plan(_ranges()).compact_batches
    materialization = census.await_args.args[3]
    census.assert_awaited_with(fhir, "example", fence, materialization, has_existing_artifacts=False)
    census.return_value = _ranges()[:-1]
    changed = await fhir._profile_build_identity_inputs("example", fence)
    assert changed.batch_plan.fingerprint != first.batch_plan.fingerprint
    assert changed.resume_lineage_hash != first.resume_lineage_hash
    token = fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
        replace(_wal_tracker_admission(), admitted_identity=first)
    )
    try:
        fhir._validate_profile_build_admission(same, object(), object())
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="admitted_identity_changed"):
            fhir._validate_profile_build_admission(changed, object(), object())
    finally:
        fhir._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


@pytest.mark.asyncio
async def test_final_inventory_is_checked_after_evidence_and_before_compact(monkeypatch):
    """A failed complete census prevents any signed compact coordinate from executing."""
    events = []
    plan = _plan(_ranges())
    build = SimpleNamespace(materialization_mode="source_delta", affected_npi_stage="affected")
    checkpoint = SimpleNamespace(evidence_next_batch=0, evidence_total_batches=1, state="building_evidence")

    async def record(label, *args, **kwargs):
        events.append(label)

    for name, label in (
        ("_populate_provider_directory_profile_evidence_stage", "evidence"),
        ("_populate_provider_directory_profile_affected_npi_stage", "affected"),
    ):
        monkeypatch.setattr(
            fhir, name, AsyncMock(side_effect=lambda *args, _label=label, **kwargs: events.append(_label))
        )
    monkeypatch.setattr(fhir, "_complete_profile_capacity_relation_class", record)
    census = AsyncMock(side_effect=RuntimeError("synthetic_inventory_incomplete"))
    monkeypatch.setattr(fhir.profile_compact_inventory, "require_complete_inventory", census)
    compact = AsyncMock()
    monkeypatch.setattr(fhir, "_populate_provider_directory_profile_compact_stage", compact)
    with pytest.raises(RuntimeError, match="synthetic_inventory_incomplete"):
        await fhir._populate_claimed_provider_directory_profile_stages(build, checkpoint, True, None, None, plan)
    assert events == ["evidence", "evidence_stage", "affected", "affected_npi_stage"]
    census.assert_awaited_once_with(fhir, build, plan)
    compact.assert_not_awaited()
