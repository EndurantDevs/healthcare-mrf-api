# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed coverage for bounded artifact-scope materialization."""

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

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


def _batch(
    *,
    number=1,
    rows=1,
    after=None,
    last="practitioner-1",
    resource_type="Practitioner",
):
    """Return one signed resource projection coordinate."""
    dataset = _dataset()
    return importer._ProviderDirectoryArtifactScopeBatchProjection(
        batch_number=number,
        source_id=dataset.source_id,
        dataset_id=dataset.dataset_id,
        evidence_run_id=dataset.evidence_run_id,
        resource_type=resource_type,
        after_resource_id=after,
        last_resource_id=last,
        projected_rows=rows,
        projected_logical_bytes=rows * 128,
    )


def _source_projection(*, projected_rows=1, batch=None):
    """Return a signed source-table projection."""
    source_batch = batch or importer._ProviderDirectoryArtifactScopeBatchProjection(
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
    return importer._ProviderDirectoryArtifactScopeTableProjection(
        table_name=importer.ProviderDirectorySource.__tablename__,
        resource_type="source",
        projected_rows=projected_rows,
        projected_logical_bytes=128,
        batches=(source_batch,),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("projection_result", "error"),
    (
        ((3, 128, "p3"), "batch_row_count_invalid"),
        ((0, 1, None), "terminal_projection_invalid"),
        ((1, 128, None), "batch_cursor_invalid"),
        ((1, 128, "same"), "batch_cursor_invalid"),
    ),
)
async def test_resource_projection_rejects_invalid_observation(
    monkeypatch,
    projection_result,
    error,
):
    """Projection refuses oversized, malformed terminal, or stuck batches."""
    projected_rows, projected_bytes, cursor = projection_result
    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            return_value=SimpleNamespace(
                _mapping={
                    "projected_rows": projected_rows,
                    "projected_logical_bytes": projected_bytes,
                    "last_cursor": cursor,
                }
            )
        ),
    )

    with pytest.raises(RuntimeError, match=error):
        await importer._artifact_resource_projection_batch(
            "mrf",
            importer.ProviderDirectoryPractitioner,
            _dataset(),
            "Practitioner",
            batch_size=2,
            after_resource_id="same" if cursor == "same" else None,
            batch_number=1,
        )


@pytest.mark.parametrize(
    "projection",
    (
        importer._ProviderDirectoryArtifactScopeTableProjection(
            table_name="provider_directory_source",
            resource_type="Practitioner",
            projected_rows=0,
            projected_logical_bytes=0,
        ),
        _source_projection(
            batch=importer._ProviderDirectoryArtifactScopeBatchProjection(
                batch_number=2,
                source_id="source-a",
                dataset_id=None,
                evidence_run_id=None,
                resource_type="source",
                after_resource_id=None,
                last_resource_id=None,
                projected_rows=1,
                projected_logical_bytes=128,
            )
        ),
    ),
)
def test_source_batch_plan_rejects_shape_or_coordinate_drift(projection):
    """Source materialization requires the exact signed batch sequence."""
    with pytest.raises(RuntimeError, match="batch_projection_invalid"):
        importer._assert_artifact_source_batches(["source-a"], projection)


def test_resource_batch_plan_requires_terminal_and_positive_batches():
    """Resource plans require a positive batch and signed zero probe."""
    with pytest.raises(RuntimeError, match="batch_projection_invalid"):
        importer._assert_artifact_resource_batches(
            _dataset(),
            "Practitioner",
            (),
            batch_size=2,
        )

    invalid_positive = _batch(rows=0, last=None)
    terminal = _batch(number=2, rows=0, after=None, last=None)
    with pytest.raises(RuntimeError, match="batch_projection_invalid"):
        importer._assert_artifact_resource_batches(
            _dataset(),
            "Practitioner",
            (invalid_positive, terminal),
            batch_size=2,
        )


def test_batch_coordinate_validators_reject_reordered_writes():
    """Per-batch validators reject reordered source and resource writes."""
    source_batch = _source_projection().batches[0]
    with pytest.raises(RuntimeError, match="batch_projection_invalid"):
        importer._validate_artifact_source_batch(source_batch, 2)

    state = importer._ArtifactResourceMaterializationState(
        after_resource_id="practitioner-0",
        expected_batch_number=1,
    )
    with pytest.raises(RuntimeError, match="batch_projection_invalid"):
        importer._validate_artifact_resource_batch_coordinate(
            _batch(after=None),
            state,
            _dataset(),
            "Practitioner",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("inserted_rows", "projected_rows"),
    ((-1, 1), (1, 2)),
)
async def test_source_materialization_rejects_rowcount_drift(
    monkeypatch,
    inserted_rows,
    projected_rows,
):
    """Source writes reject negative and aggregate row-count drift."""
    projection = _source_projection(projected_rows=projected_rows)
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_source_projection_batches",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "_execute_artifact_source_batch",
        AsyncMock(return_value=inserted_rows),
    )
    monkeypatch.setattr(
        importer,
        "_analyze_artifact_scope_table",
        AsyncMock(),
    )

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="source_projection_changed",
    ):
        await importer._materialize_provider_directory_artifact_source_scope(
            "mrf",
            "source_scope",
            ["source-a"],
            projection=projection,
        )


@pytest.mark.asyncio
async def test_resource_iteration_requires_cursor_and_advances_legacy_batch(
    monkeypatch,
):
    """Projected writes require a cursor; full legacy batches advance it."""
    context = importer._artifact_resource_materialization_context(
        "mrf",
        "practitioner_scope",
        importer.ProviderDirectoryPractitioner,
        _dataset(),
        "Practitioner",
        2,
    )
    monkeypatch.setattr(
        importer,
        "_execute_artifact_resource_batch",
        AsyncMock(return_value=1),
    )
    with pytest.raises(RuntimeError, match="batch_projection_invalid"):
        await importer._is_artifact_resource_iteration_continuing(
            context,
            importer._ArtifactResourceMaterializationState(
                expected_batch_number=1
            ),
            _batch(last=None),
        )

    monkeypatch.setattr(
        importer,
        "_execute_artifact_resource_batch",
        AsyncMock(return_value=2),
    )
    next_cursor = AsyncMock(return_value="practitioner-2")
    monkeypatch.setattr(importer, "_next_artifact_resource_id", next_cursor)
    state = importer._ArtifactResourceMaterializationState()
    assert await importer._is_artifact_resource_iteration_continuing(
        context,
        state,
        None,
    )
    assert state.after_resource_id == "practitioner-2"
    next_cursor.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("iterations", "message"),
    (
        ((True,), "resource_projection_changed"),
        ((False,), "resource_projection_changed"),
    ),
)
async def test_resource_dataset_rejects_incomplete_projection_consumption(
    monkeypatch,
    iterations,
    message,
):
    """A signed resource plan must consume its terminal batch exactly once."""
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_resource_projection_batches",
        Mock(),
    )
    monkeypatch.setattr(
        importer,
        "_is_artifact_resource_iteration_continuing",
        AsyncMock(side_effect=iterations),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match=message,
    ):
        await importer._materialize_provider_directory_artifact_resource_dataset(
            "mrf",
            "practitioner_scope",
            importer.ProviderDirectoryPractitioner,
            _dataset(),
            "Practitioner",
            batch_size=2,
            projection_batches=(_batch(),),
        )


@pytest.mark.asyncio
async def test_resource_dataset_accepts_explicit_terminal_consumption(
    monkeypatch,
):
    """A consumed terminal probe ends the projected write deterministically."""
    monkeypatch.setattr(
        importer,
        "_assert_provider_directory_artifact_resource_projection_batches",
        Mock(),
    )

    async def should_continue(_context, state, expected_batch):
        state.is_terminal_projection_observed = True
        return expected_batch is not None

    monkeypatch.setattr(
        importer,
        "_is_artifact_resource_iteration_continuing",
        should_continue,
    )
    assert (
        await importer._materialize_provider_directory_artifact_resource_dataset(
            "mrf",
            "practitioner_scope",
            importer.ProviderDirectoryPractitioner,
            _dataset(),
            "Practitioner",
            batch_size=2,
            projection_batches=(_batch(number=1, rows=0, last=None),),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_resource_scope_rejects_projection_total_drift(monkeypatch):
    """A relation refuses totals that diverge from the signed projection."""
    projection = importer._ProviderDirectoryArtifactScopeTableProjection(
        table_name=importer.ProviderDirectoryPractitioner.__tablename__,
        resource_type="Practitioner",
        projected_rows=2,
        projected_logical_bytes=256,
    )
    monkeypatch.setattr(
        importer,
        "_materialize_provider_directory_artifact_resource_dataset",
        AsyncMock(return_value=1),
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="resource_projection_changed",
    ):
        await importer._materialize_provider_directory_artifact_resource_scope(
            "mrf",
            "practitioner_scope",
            importer.ProviderDirectoryPractitioner,
            importer.ProviderDirectoryArtifactDatasetFence((_dataset(),)),
            frozenset({"Practitioner"}),
            projection=projection,
        )
