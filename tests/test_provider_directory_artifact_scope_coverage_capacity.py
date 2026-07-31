# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Capacity-bound coverage for Provider Directory artifact scope."""

from __future__ import annotations

import dataclasses
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from .provider_directory_profile_execution_test_support import (
    _wal_tracker_admission,
)

importer = importlib.import_module("process.provider_directory_fhir")


def _dataset():
    """Return one immutable Practitioner dataset coordinate."""
    return importer.ProviderDirectoryArtifactDataset(
        source_id="source-a",
        endpoint_id="endpoint-a",
        dataset_id="dataset-a",
        evidence_run_id="run-evidence-a",
        selected_resources=("Practitioner",),
    )


def _source_batch():
    """Return one immutable source projection coordinate."""
    return importer._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=1,
        source_id="source-a",
        dataset_id=None,
        evidence_run_id=None,
        resource_type="source",
        after_resource_id=None,
        last_resource_id=None,
        projected_rows=1,
        projected_logical_bytes=128,
    )


def test_legacy_capacity_check_binds_signed_projection():
    """An admitted projection cannot drift from its exact row ceiling."""
    admission = _wal_tracker_admission()
    token = importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.set(
        admission
    )
    try:
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="artifact_projection_changed",
        ):
            importer._assert_provider_directory_artifact_scope_capacity(
                {"projected_rows": 0}
            )
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_CAPACITY_ADMISSION.reset(token)


@pytest.mark.asyncio
async def test_unprojected_source_insert_and_unadmitted_usage_are_direct(
    monkeypatch,
):
    """Legacy source writes insert directly; usage checks need admission."""
    status = AsyncMock(return_value="INSERT 0 1")
    scalar = AsyncMock()
    monkeypatch.setattr(importer.db, "status", status)
    monkeypatch.setattr(importer.db, "scalar", scalar)
    assert await importer._insert_artifact_source_batch(
        _source_batch(),
        None,
        "SELECT projected",
        "INSERT source",
    ) == 1
    status.assert_awaited_once()

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        Mock(return_value=None),
    )
    await importer._assert_artifact_scope_usage(
        "mrf",
        {"source": "source_scope"},
        importer._ProviderDirectoryArtifactScopeExactProjection(()),
    )
    scalar.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("relation_oid", (0, 11))
async def test_scratch_projection_rejects_oid_or_tablespace_drift(
    monkeypatch,
    relation_oid,
):
    """Scratch projection requires a real OID in the signed tablespace."""
    admission = _wal_tracker_admission()
    monkeypatch.setattr(
        importer.db,
        "scalar",
        AsyncMock(return_value=relation_oid),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_relation_storage_fingerprint",
        AsyncMock(
            return_value=SimpleNamespace(
                effective_tablespace_oids=(999_999,),
                toastable_columns=(),
                main_index_pages=(),
                toast_index_pages=(),
            )
        ),
    )
    table_projection = importer._ProviderDirectoryArtifactScopeTableProjection(
        table_name="base",
        resource_type="source",
        projected_rows=1,
        projected_logical_bytes=128,
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="scratch_oid_changed|scratch_tablespace_changed",
    ):
        await importer._artifact_scope_scratch_projection(
            "mrf",
            "source_scope",
            table_projection,
            admission,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(("row_count", "byte_count"), ((6, 1), (1, 101)))
async def test_scope_usage_rejects_row_or_byte_growth(
    monkeypatch,
    row_count,
    byte_count,
):
    """Observed scope use remains below signed row and byte ceilings."""
    admission = _wal_tracker_admission()
    projection = importer._ProviderDirectoryArtifactScopeExactProjection(
        tables=(
            importer._ProviderDirectoryArtifactScopeTableProjection(
                table_name="base",
                resource_type="source",
                projected_rows=5,
                projected_logical_bytes=100,
            ),
        )
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        Mock(return_value=admission),
    )
    monkeypatch.setattr(
        importer.db,
        "scalar",
        AsyncMock(return_value=row_count),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_relation_cap",
        Mock(return_value=SimpleNamespace(max_scratch_bytes=100)),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_relation_bytes",
        AsyncMock(return_value=byte_count),
    )
    error = "observed_rows_exceeded" if row_count > 5 else "bytes_exceeded"
    with pytest.raises(
        (importer.ProviderDirectoryArtifactBuildStale, RuntimeError),
        match=error,
    ):
        await importer._assert_artifact_scope_usage(
            "mrf",
            {"base": "scope"},
            projection,
        )


@pytest.mark.asyncio
async def test_capacity_preflight_handles_unadmitted_and_bad_table_set(
    monkeypatch,
):
    """Preflight skips legacy runs and rejects a signed table-set mismatch."""
    projection = importer._ProviderDirectoryArtifactScopeExactProjection(())
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        Mock(return_value=None),
    )
    await importer._preflight_provider_directory_artifact_scope_capacity(
        "mrf",
        {"base": "scope"},
        projection,
    )

    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_capacity_admission",
        Mock(return_value=_wal_tracker_admission()),
    )
    with pytest.raises(RuntimeError, match="projection_tables_invalid"):
        await importer._preflight_provider_directory_artifact_scope_capacity(
            "mrf",
            {"base": "scope"},
            projection,
        )


def test_exact_projection_metrics_include_bytes_hash_and_resources():
    """Exact metrics expose every bounded projection identity."""
    projection = importer._ProviderDirectoryArtifactScopeExactProjection(
        tables=(
            importer._ProviderDirectoryArtifactScopeTableProjection(
                table_name="source",
                resource_type="source",
                projected_rows=2,
                projected_logical_bytes=256,
            ),
        )
    )
    metrics_by_name = {}
    importer._record_artifact_scope_metrics(
        metrics_by_name,
        projection,
        ["retired"],
    )
    assert metrics_by_name == {
        "artifact_scope_projected_rows": 2,
        "artifact_scope_projected_logical_bytes": 256,
        "artifact_scope_projection_hash": projection.projection_hash,
        "artifact_scope_projected_rows_by_resource": {"source": 2},
        "artifact_scope_projected_bytes_by_resource": {"source": 256},
        "artifact_scope_reaped_table_count": 1,
    }


@pytest.mark.asyncio
async def test_exact_projection_plan_uses_admitted_batch_geometry(monkeypatch):
    """Admitted planning binds exact projection and omits legacy reaping."""
    admission = _wal_tracker_admission()
    projection = importer._ProviderDirectoryArtifactScopeExactProjection(())
    exact = AsyncMock(return_value=projection)
    capacity_check = Mock()
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_scope_exact_projection",
        exact,
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_scope_exact_capacity",
        capacity_check,
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence((_dataset(),))
    assert await importer._artifact_scope_projection_plan(
        "mrf",
        fence,
        fence,
        frozenset({"Practitioner"}),
        admission,
    ) == (projection, projection, [])
    assert exact.call_args.kwargs["batch_size"] == (
        admission.geometry.artifact_scope_batch_size
    )
    capacity_check.assert_called_once_with(projection)


def _payload_plan_and_projection():
    """Return a one-resource scope plan and its exact projection."""
    plan = importer._ArtifactScopeMaterializationPlan(
        source_table="source_scope",
        created_tables=["source_scope"],
        relation_by_table={
            importer.ProviderDirectorySource.__tablename__: "source_scope",
            importer.ProviderDirectoryPractitioner.__tablename__: (
                "practitioner_scope"
            ),
        },
        resource_scope_jobs=(
            (importer.ProviderDirectoryPractitioner, "practitioner_scope"),
        ),
        model_by_table_name={},
    )
    source_projection = importer._ProviderDirectoryArtifactScopeTableProjection(
        table_name=importer.ProviderDirectorySource.__tablename__,
        resource_type="source",
        projected_rows=1,
        projected_logical_bytes=128,
    )
    practitioner_projection = dataclasses.replace(
        source_projection,
        table_name=importer.ProviderDirectoryPractitioner.__tablename__,
        resource_type="Practitioner",
    )
    exact_projection = importer._ProviderDirectoryArtifactScopeExactProjection(
        (source_projection, practitioner_projection)
    )
    return plan, exact_projection


@pytest.mark.asyncio
async def test_payload_checks_capacity_after_source_and_worker_wave(monkeypatch):
    """Materialization rechecks exact usage after source and resource writes."""
    plan, projection = _payload_plan_and_projection()
    observed = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_preflight_provider_directory_artifact_scope_capacity",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_source_scope",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_resource_scope",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_scope_observed_capacity",
        observed,
    )
    fence = importer.ProviderDirectoryArtifactDatasetFence((_dataset(),))
    await importer._materialize_artifact_scope_payload(
        "mrf",
        plan,
        fence,
        fence,
        frozenset({"Practitioner"}),
        projection,
        2,
        1,
    )
    assert observed.await_count == 2
