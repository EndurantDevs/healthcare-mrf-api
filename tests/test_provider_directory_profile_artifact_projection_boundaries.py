# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for artifact projections and stage fingerprints."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .test_provider_directory_profile_selection_attestation import _execution
from .test_provider_directory_profile_capacity import _geometry_payload
from .test_provider_directory_profile_control_capacity import (
    _bound_control_wal_projection,
)
from .provider_directory_profile_execution_test_support import (
    _capacity_consumption_row,
    _capacity_geometry_identity,
    _profile_serving_state,
    _published_dataset_state,
    _wal_tracker_admission,
)
from .test_provider_directory_profile_artifact_preflight_boundaries import (
    _evidence_batch_fixture,
)

importer = importlib.import_module("process.provider_directory_fhir")
capacity = importlib.import_module("process.provider_directory_profile_capacity")


def test_owned_artifact_scope_names_retain_exact_run_owner_when_bounded():
    """Long relation names remain reversible without exceeding PostgreSQL."""
    run_id = "run_" + "a" * 32

    for model in (importer.ProviderDirectorySource, *importer.RESOURCE_MODELS):
        base_name = model.__tablename__
        relation_name = importer._owned_artifact_scope_name(
            base_name,
            run_id=run_id,
        )

        assert len(relation_name) <= importer.POSTGRES_IDENTIFIER_MAX_LENGTH
        assert importer._artifact_scope_prior_owner(relation_name) == (
            base_name,
            run_id,
        )


def _artifact_dataset():
    """Return the single retained Practitioner dataset used by projections."""
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-a",
        selected_resources=("Practitioner",),
    )


def _resource_projection_batches(
    dataset,
    *,
    terminal_batch_number: int = 2,
) -> tuple[
    importer._ProviderDirectoryArtifactScopeBatchProjection,
    importer._ProviderDirectoryArtifactScopeBatchProjection,
]:
    """Return one positive batch followed by its exact terminal batch."""
    positive = importer._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=1,
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        evidence_run_id=dataset.evidence_run_id,
        resource_type="Practitioner",
        after_resource_id=None,
        last_resource_id="practitioner-1",
        projected_rows=1,
        projected_logical_bytes=256,
    )
    terminal = importer._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=terminal_batch_number,
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        evidence_run_id=dataset.evidence_run_id,
        resource_type="Practitioner",
        after_resource_id="practitioner-1",
        last_resource_id=None,
        projected_rows=0,
        projected_logical_bytes=0,
    )
    return positive, terminal


def _exact_resource_projection_probe() -> AsyncMock:
    """Return positive then terminal exact projection observations."""
    return AsyncMock(
        side_effect=(
            SimpleNamespace(
                _mapping={
                    "projected_rows": 1,
                    "projected_logical_bytes": 256,
                    "last_cursor": "practitioner-1",
                }
            ),
            SimpleNamespace(
                _mapping={
                    "projected_rows": 0,
                    "projected_logical_bytes": 0,
                    "last_cursor": None,
                }
            ),
        )
    )


@pytest.mark.asyncio
async def test_projection_driven_artifact_batch_consumes_hashed_terminal(
    monkeypatch,
):
    """Consume the positive batch and verify its hashed terminal projection."""
    dataset = _artifact_dataset()
    projection_batches = _resource_projection_batches(dataset)
    monkeypatch.setattr(
        importer.db,
        "status",
        AsyncMock(return_value=1),
    )
    monkeypatch.setattr(
        importer.db,
        "scalar",
        AsyncMock(return_value="practitioner-1"),
    )
    exact_probe = _exact_resource_projection_probe()
    monkeypatch.setattr(importer.db, "first", exact_probe)

    inserted = (
        await importer._materialize_provider_directory_artifact_resource_dataset(
            "mrf",
            "provider_directory_practitioner_artifact_scope_test",
            importer.ProviderDirectoryPractitioner,
            dataset,
            "Practitioner",
            batch_size=100,
            projection_batches=projection_batches,
        )
    )

    assert inserted == 1
    importer.db.status.assert_awaited_once()
    assert exact_probe.await_count == 2
    importer.db.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_artifact_source_byte_drift_refuses_before_insert(
    monkeypatch,
):
    source_id = "source-a"
    projection = importer._ProviderDirectoryArtifactScopeTableProjection(
        table_name=importer.ProviderDirectorySource.__tablename__,
        resource_type="source",
        projected_rows=1,
        projected_logical_bytes=256,
        batches=(
            importer._ProviderDirectoryArtifactScopeBatchProjection(
                batch_number=1,
                source_id=source_id,
                dataset_id=None,
                evidence_run_id=None,
                resource_type="source",
                after_resource_id=None,
                last_resource_id=None,
                projected_rows=1,
                projected_logical_bytes=256,
            ),
        ),
    )
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value=SimpleNamespace(
                _mapping={
                    "projected_rows": 1,
                    "projected_logical_bytes": 257,
                    "last_cursor": None,
                }
            )
        ),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_projection_changed",
    ):
        await importer._materialize_provider_directory_artifact_source_scope(
            "mrf",
            "provider_directory_source_artifact_scope_test",
            [source_id],
            projection=projection,
        )

    importer.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_artifact_resource_byte_drift_refuses_before_insert(
    monkeypatch,
):
    """Refuse a resource batch whose observed bytes exceed projection."""
    dataset = _artifact_dataset()
    projection_batches = _resource_projection_batches(dataset)
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value=SimpleNamespace(
                _mapping={
                    "projected_rows": 1,
                    "projected_logical_bytes": 257,
                    "last_cursor": "practitioner-1",
                }
            )
        ),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="resource_projection_changed",
    ):
        await importer._materialize_provider_directory_artifact_resource_dataset(
            "mrf",
            "provider_directory_practitioner_artifact_scope_test",
            importer.ProviderDirectoryPractitioner,
            dataset,
            "Practitioner",
            batch_size=100,
            projection_batches=projection_batches,
        )

    importer.db.status.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_batch_number", [None, 1, 3])
async def test_artifact_resource_projection_requires_ordered_terminal_before_dml(
    monkeypatch,
    terminal_batch_number,
):
    """Refuse missing, early, or nonconsecutive terminal projections."""
    dataset = _artifact_dataset()
    positive, terminal = _resource_projection_batches(
        dataset,
        terminal_batch_number=terminal_batch_number or 2,
    )
    projection_batches = (
        (positive,)
        if terminal_batch_number is None
        else (
            (terminal, positive)
            if terminal_batch_number == 1
            else (positive, terminal)
        )
    )
    monkeypatch.setattr(importer.db, "first", AsyncMock())
    monkeypatch.setattr(importer.db, "status", AsyncMock())

    with pytest.raises(
        RuntimeError,
        match="batch_projection_invalid|terminal_projection_invalid",
    ):
        await importer._materialize_provider_directory_artifact_resource_dataset(
            "mrf",
            "provider_directory_practitioner_artifact_scope_test",
            importer.ProviderDirectoryPractitioner,
            dataset,
            "Practitioner",
            batch_size=100,
            projection_batches=projection_batches,
        )

    importer.db.first.assert_not_awaited()
    importer.db.status.assert_not_awaited()


def test_profile_empty_layout_index_ddl_has_no_collision_skip_path():
    evidence_statements = (
        importer.profile_artifact.profile_index_statements(
            "mrf",
            "evidence_stage",
            evidence=True,
        )
    )
    profile_statements = (
        importer.profile_artifact.profile_index_statements(
            "mrf",
            "profile_stage",
            evidence=False,
        )
    )
    _index_name, bucket_statement = (
        importer._provider_directory_profile_bucket_index_sql(
            "mrf",
            "role_scope",
        )
    )

    assert len(evidence_statements) == 4
    assert len(profile_statements) == 1
    assert all(
        statement.startswith("CREATE INDEX ")
        and "IF NOT EXISTS" not in statement
        for statement in (
            *evidence_statements,
            *profile_statements,
            bucket_statement,
        )
    )


@pytest.mark.asyncio
async def test_stage_layout_drift_refuses_capacity_write_before_dml(
    monkeypatch,
):
    """Refuse a changed stage fingerprint before admitted payload DML."""
    build, batch = _evidence_batch_fixture()

    @asynccontextmanager
    async def capacity_transaction():
        yield

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_transaction",
        capacity_transaction,
    )
    verify_layout = AsyncMock(
        side_effect=importer.ProviderDirectoryArtifactBuildStale(
            "provider_directory_profile_stage_storage_layout_changed"
        )
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_profile_stage_storage_identity",
        verify_layout,
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    capacity_token = (
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
            object()
        )
    )
    try:
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="stage_storage_layout_changed",
        ):
            await importer._execute_profile_evidence_batch(
                build,
                batch,
                "COPY",
                {},
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(
            capacity_token
        )

    verify_layout.assert_awaited_once()
    importer.db.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_resumed_stage_fingerprint_drift_is_not_reusable(
    monkeypatch,
):
    checkpoint_by_field = {
        "evidence_stage_oid": 21,
        "evidence_stage_storage_fingerprint": "a" * 64,
        "profile_stage_oid": 22,
        "profile_stage_storage_fingerprint": "b" * 64,
    }
    observed_fingerprint = AsyncMock(return_value="c" * 64)
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_storage_fingerprint",
        observed_fingerprint,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_stage_relation_identity",
        AsyncMock(return_value=(21, "r", "p")),
    )

    assert not await importer._is_profile_stage_pair_matching(
        "mrf",
        checkpoint_by_field,
        "evidence_stage",
        "profile_stage",
    )
    observed_fingerprint.assert_awaited_once_with(
        "mrf",
        "evidence_stage",
        expected_oid=21,
        lock_relation=False,
    )
