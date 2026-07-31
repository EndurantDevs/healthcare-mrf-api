# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for control plans and changed-source materialization."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
import math
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution
from .test_provider_directory_profile_capacity import _geometry_payload
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
    _control_wal_plan_input,
)
from .provider_directory_profile_execution_test_support import (
    _capacity_consumption_row,
    _capacity_geometry_identity,
    _profile_serving_state,
    _published_dataset_state,
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")


def _control_artifact_projection():
    """Return artifact batches with one signed terminal zero probe."""
    source_batches = (
        SimpleNamespace(projected_rows=1),
        SimpleNamespace(projected_rows=1),
    )
    resource_batches = (
        SimpleNamespace(projected_rows=4),
        SimpleNamespace(projected_rows=0),
    )
    return SimpleNamespace(
        tables=tuple(
            SimpleNamespace(
                resource_type=resource_type,
                batches=source_batches
                if resource_type == "source"
                else resource_batches
                if resource_type == "InsurancePlan"
                else (),
            )
            for resource_type in capacity.CONTROL_WAL_ARTIFACT_SCOPE_NAMES
        )
    )


def _control_layout(
    relation_oid: int,
    exact_fingerprint: str,
    structural_fingerprint: str,
    index_oid: int,
    toastable_column: str,
):
    """Return one relation storage layout for control-plan metadata."""
    return importer._ProviderDirectoryProfileRelationStorageLayout(
        exact_fingerprint=exact_fingerprint,
        structural_fingerprint=structural_fingerprint,
        relation_oid=relation_oid,
        toast_oid=None,
        main_index_oids=(index_oid,),
        main_index_pages=(1,),
        toast_index_oids=(),
        toast_index_pages=(),
        toastable_columns=(toastable_column,),
        effective_tablespace_oids=(1663,),
    )


def _control_layout_by_oid() -> dict[int, object]:
    """Return checkpoint, import-run, and consumption storage layouts."""
    return {
        11: _control_layout(11, "1" * 64, "a" * 64, 101, "payload"),
        12: _control_layout(12, "2" * 64, "b" * 64, 102, "progress"),
        13: _control_layout(
            13, "3" * 64, "c" * 64, 103, "canonical_lease_json"
        ),
    }


def _control_plan_context():
    """Return database, batch, lineage, and dataset-fence plan inputs."""
    database_identity = SimpleNamespace(
        build_checkpoint_oid=11,
        import_run_oid=12,
        capacity_consumption_oid=13,
        build_checkpoint_storage_fingerprint="1" * 64,
        import_run_storage_fingerprint="2" * 64,
        capacity_consumption_storage_fingerprint="3" * 64,
        postgres_default_toast_compression="pglz",
    )
    batch_plan = SimpleNamespace(
        evidence_batches=(1, 2, 3),
        compact_batches=(1, 2),
    )
    identity = SimpleNamespace(
        resume_lineage_hash="f" * 64,
        source_ids=("source-a",),
        removed_source_ids=("source-b",),
    )
    dataset = importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-a",
    )
    return (
        database_identity,
        batch_plan,
        identity,
        importer.ProviderDirectoryArtifactDatasetFence((dataset,)),
    )


@pytest.mark.asyncio
async def test_control_plan_excludes_signed_terminal_zero_probe(
    monkeypatch,
):
    """Exclude the terminal zero probe from signed DML batch counts."""
    artifact_projection = _control_artifact_projection()
    layout_by_oid = _control_layout_by_oid()
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(side_effect=lambda relation_oid, **_: layout_by_oid[relation_oid]),
    )
    toast_counter = AsyncMock(return_value=0)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_toast_chunk_count",
        toast_counter,
    )
    monkeypatch.setattr(importer, "_schema", lambda: "mrf")
    database_identity, batch_plan, identity, fence = _control_plan_context()
    plan_input = (
        await importer._provider_directory_profile_control_wal_plan_input(
            database_identity,
            artifact_projection,
            batch_plan,
            identity,
            fence,
        )
    )

    batch_count_by_artifact = {
        batch_count.artifact_name: batch_count.batch_count
        for batch_count in plan_input.artifact_batch_counts
    }
    assert batch_count_by_artifact["source"] == 2
    assert batch_count_by_artifact["InsurancePlan"] == 1
    assert plan_input.evidence_batch_count == 3
    assert plan_input.compact_batch_count == 2
    assert plan_input.affected_source_count == 2
    assert plan_input.admission_row_lock_count == 2
    assert plan_input.cutover_row_lock_count == 27
    assert toast_counter.await_count == 1
    assert "build_id" in toast_counter.await_args.kwargs["params"]
    assert plan_input.import_run_update.deleted_toast_chunks == math.ceil(
        capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES
        / capacity.POSTGRES_TOAST_MAX_CHUNK_SIZE_BYTES
    )


def _two_source_profile_scope():
    """Return two source contexts, vectors, fence, and selection execution."""
    datasets = tuple(
        importer.ProviderDirectoryArtifactDataset(
            source_id=f"source-{suffix}",
            endpoint_id=f"endpoint-{suffix}",
            dataset_id=f"dataset-{suffix}",
            evidence_run_id=f"run-{suffix}",
        )
        for suffix in ("a", "b")
    )
    contexts = tuple(
        importer._ProviderDirectoryProfileSourceContext(
            f"source-{suffix}",
            f"endpoint-{suffix}",
            f"https://{suffix}.example/fhir",
            suffix.upper(),
            None,
        )
        for suffix in ("a", "b")
    )
    source_vector_pairs = tuple(
        (dataset.source_id, dataset.dataset_id) for dataset in datasets
    )
    context_vector = (
        importer._provider_directory_profile_source_context_vector(contexts)
    )
    execution = SimpleNamespace(
        attestation=SimpleNamespace(
            pairs=[
                {"source_id": source_id, "dataset_id": dataset_id}
                for source_id, dataset_id in source_vector_pairs
            ]
        )
    )
    return (
        importer.ProviderDirectoryArtifactDatasetFence(datasets),
        contexts,
        source_vector_pairs,
        context_vector,
        execution,
    )


def _patch_profile_scope_resolution(
    monkeypatch,
    contexts,
    serving_state,
) -> None:
    """Install existing-artifact, source-scope, and serving-state seams."""
    monkeypatch.setattr(
        importer,
        "_has_provider_directory_profile_artifacts",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_scope_source_ids",
        AsyncMock(
            return_value=(
                ["source-a", "source-b"],
                ["source-a", "source-b"],
                contexts,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_delta_serving_state",
        AsyncMock(return_value=serving_state),
    )


@pytest.mark.asyncio
async def test_profile_resource_scope_materializes_changed_sources_only(
    monkeypatch,
):
    """Materialize only a source whose semantic context digest changed."""
    fence, contexts, source_vector, context_vector, execution = (
        _two_source_profile_scope()
    )
    serving_state = _profile_serving_state(
        source_vector,
        context_vector,
    )
    _patch_profile_scope_resolution(monkeypatch, contexts, serving_state)
    token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        execution
    )
    try:
        no_op_fence = (
            await importer._provider_directory_profile_resource_scope_fence(
                fence,
                {"profile"},
            )
        )
        assert no_op_fence.datasets == ()

        changed_vector = (
            context_vector[0],
            ("source-b", "4" * 64),
        )
        monkeypatch.setattr(
            importer,
            "_provider_directory_profile_delta_serving_state",
            AsyncMock(
                return_value=_profile_serving_state(
                    source_vector,
                    changed_vector,
                )
            ),
        )
        changed_fence = (
            await importer._provider_directory_profile_resource_scope_fence(
                fence,
                {"profile"},
            )
        )
        assert tuple(
            dataset.source_id for dataset in changed_fence.datasets
        ) == ("source-b",)
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(
            token
        )


@pytest.mark.asyncio
async def test_attested_profile_delta_requires_capacity_before_transaction(
    monkeypatch,
):
    """An official Profile cutover may not reach its first database write."""

    transaction = Mock()
    monkeypatch.setattr(importer.db, "transaction", transaction)
    execution_token = (
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
            SimpleNamespace()
        )
    )
    try:
        with pytest.raises(
            RuntimeError,
            match="provider_directory_profile_capacity_admission_required",
        ):
            await importer._promote_provider_directory_artifact_bundle_transaction(
                (),
                profile_delta=SimpleNamespace(schema="mrf"),
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(
            execution_token
        )
    transaction.assert_not_called()


def _patch_profile_cutover_noops(monkeypatch):
    """Replace work after the lock seam while retaining transaction order."""

    for dependency_name in (
        "_apply_provider_directory_profile_capacity_settings",
        "_lock_provider_directory_artifact_bundle_targets",
        "_verify_active_profile_selection_at_cutover",
        "_assert_provider_directory_profile_delta_identity",
        "_lock_provider_directory_artifact_tables",
        "_promote_provider_directory_artifact_datasets",
        "_apply_provider_directory_profile_delta",
        "_finalize_provider_directory_profile_delta_scratch",
    ):
        monkeypatch.setattr(importer, dependency_name, AsyncMock())
    monkeypatch.setattr(
        importer,
        "_resolve_provider_directory_profile_delta_replay",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_is_artifact_bundle_promotion_committed",
        AsyncMock(return_value=False),
    )


def _patch_profile_cutover_retry_dependencies(
    monkeypatch,
    admission,
):
    """Keep the real transaction ordering while isolating database seams."""

    reservation_counts = []

    class LockNotAvailable(RuntimeError):
        sqlstate = "55P03"

    @asynccontextmanager
    async def transaction():
        yield

    async def lock_dataset_fence(_fence):
        reservation_counts.append(
            admission.wal_tracker.accounted_control_operation_counts[
                "cutover_row_lock"
            ]
        )
        if len(reservation_counts) < (
            importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_ATTEMPTS
        ):
            raise LockNotAvailable("dataset row lock")

    _patch_profile_cutover_noops(monkeypatch)
    monkeypatch.setattr(importer.db, "transaction", transaction)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        lambda: admission,
    )
    monkeypatch.setattr(
        importer,
        "_lock_and_verify_artifact_dataset_fence",
        lock_dataset_fence,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_current_wal_bytes",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(importer.asyncio, "sleep", AsyncMock())
    return reservation_counts


async def _run_profile_cutover_lock_retries(
    monkeypatch,
    admission,
    fence,
):
    """Run the real bounded promotion loop with two lock failures."""

    reservation_counts = _patch_profile_cutover_retry_dependencies(
        monkeypatch,
        admission,
    )
    fence_token = importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.set(
        fence
    )
    try:
        await importer._retry_provider_directory_artifact_bundle_promotion(
            (),
            profile_delta=SimpleNamespace(schema="mrf"),
        )
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.reset(
            fence_token
        )
    return reservation_counts


@pytest.mark.asyncio
async def test_profile_cutover_retry_reserves_every_row_lock_attempt(
    monkeypatch,
):
    """Keep all three post-reservation lock attempts inside signed WAL."""

    per_attempt_count = (
        importer.PROVIDER_DIRECTORY_PROFILE_CUTOVER_FIXED_ROW_LOCK_COUNT
    )
    plan_input = _control_wal_plan_input(
        cutover_row_lock_count=(
            per_attempt_count
            * importer.PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_ATTEMPTS
        )
    )
    geometry, control_projection = _bound_control_wal_projection(plan_input)
    admission = dataclasses.replace(
        _wal_tracker_admission(),
        geometry=geometry,
        control_wal_projection=control_projection,
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence(())
    reservation_counts = await _run_profile_cutover_lock_retries(
        monkeypatch,
        admission,
        fence,
    )
    assert reservation_counts == [6, 12, 18]
    assert admission.wal_tracker.accounted_control_operation_counts[
        "cutover_row_lock"
    ] == 3 * per_attempt_count
