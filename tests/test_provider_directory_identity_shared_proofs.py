# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


fhir = importlib.import_module("process.provider_directory_fhir")


def _checkpoint_context(*, dataset_id: str | None = "dataset-a"):
    return fhir.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope",
        source_ids=("source-a",),
        owner_run_id="run",
        acquisition_root_run_id="root",
        dataset_id=dataset_id,
        lineage_verified=True,
    )


def _partition_options(**overrides):
    fields_by_name = {
        "timeout": 1,
        "run_id": "run",
        "row_batch_handler": None,
        "row_batch_size": 10,
        "retain_rows": True,
        "deadline_at": None,
        "max_pages": 0,
    }
    fields_by_name.update(overrides)
    return fhir.PartitionFetchOptions(**fields_by_name)


def _partition_page_request(**overrides):
    state = overrides.pop(
        "state",
        fhir.PartitionFetchState(
            result_model=fhir.ProviderDirectoryOrganization,
            retained_resource_rows=[],
        ),
    )
    fields_by_name = {
        "source_record": {
            "source_id": "source-a",
            "api_base": fhir.UHC_PROVIDER_DIRECTORY_BASE,
        },
        "resource_type": "Organization",
        "start_url_queue": asyncio.Queue(),
        "state": state,
        "partition_start_url": f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Organization",
        "current_url": f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Organization",
        "is_residual_only": False,
        "pending_resource_rows": [],
        "fetch_options": _partition_options(),
    }
    fields_by_name.update(overrides)
    return fhir.PartitionPageRequest(**fields_by_name)


def _alohr_options(**overrides):
    fields_by_name = {
        "per_resource_limit": 0,
        "page_limit": 0,
        "timeout": 1,
        "run_id": "run",
        "stale_cleanup": True,
        "seen_table": None,
        "checkpoint_context": None,
        "resume_required_entries": None,
    }
    fields_by_name.update(overrides)
    return fhir.AlohrGraphQLImportOptions(**fields_by_name)


def _alohr_import_state(**overrides):
    fields_by_name = {
        "source_ids": ["source-a"],
        "compatibility_source_id": "source-a",
        "selected_resource_types": {"Organization"},
        "canonical_api_base": fhir.ALOHR_FHIR_PROVIDER_DIRECTORY_BASE,
        "options": _alohr_options(),
        "source_counts_by_resource": {"Organization": 0},
    }
    fields_by_name.update(overrides)
    return fhir.AlohrGraphQLImportState(**fields_by_name)


@pytest.mark.asyncio
async def test_location_archive_and_network_catalog_prerequisites_skip(monkeypatch):
    monkeypatch.setattr(fhir, "_has_address_canon_functions", AsyncMock(return_value=False))
    assert await fhir.publish_provider_directory_location_archive("mrf") == {
        "skipped": True,
        "reason": "canonical_functions_unavailable",
    }

    fhir._has_address_canon_functions.return_value = True
    monkeypatch.setattr(
        fhir,
        "_is_table_present",
        AsyncMock(side_effect=[True, False]),
    )
    assert await fhir.publish_provider_directory_location_archive("mrf") == {
        "skipped": True,
        "reason": "provider_directory_location_unavailable",
    }

    monkeypatch.setattr(
        fhir,
        "_network_catalog_missing_requirement",
        AsyncMock(return_value="provider_directory_source_unavailable"),
    )
    monkeypatch.setattr(
        fhir,
        "_ensure_provider_directory_network_catalog_table",
        AsyncMock(),
    )
    ensured = await fhir._ensure_provider_directory_network_catalog_populated("mrf")
    assert ensured["reason"] == "provider_directory_source_unavailable"
    published = await fhir.publish_provider_directory_network_catalog("mrf")
    assert published["reason"] == "provider_directory_source_unavailable"
    assert await fhir._copy_existing_network_catalog("stage", "target", "x", []) == 0
    assert "source_id = ANY" in fhir._provider_directory_network_catalog_scope_filter(
        "network",
        run_id=None,
        source_ids=["source-a"],
    )


@pytest.mark.asyncio
async def test_address_overlay_filter_owner_and_precreate_failure(monkeypatch):
    assert fhir._address_overlay_query_params("run", []) == {"run_id": "run"}
    monkeypatch.setattr(
        fhir.db,
        "status",
        AsyncMock(side_effect=["DELETE 2", RuntimeError("create failed")]),
    )
    assert await fhir._dedupe_address_overlay_stage(
        '"mrf"."stage"',
        source_ids=["source-a"],
        resource_types=["Organization"],
    ) == 2

    monkeypatch.setattr(
        fhir.db,
        "all",
        AsyncMock(return_value=[(None, "failed", True), ("run", "failed", True)]),
    )
    owners = await fhir._address_overlay_stage_owners("mrf")
    assert len(owners) == 1

    monkeypatch.setattr(
        fhir,
        "_cleanup_orphan_address_overlay_stages",
        AsyncMock(return_value=fhir._address_overlay_stage_cleanup_counts()),
    )
    drop = AsyncMock()
    monkeypatch.setattr(fhir, "_drop_address_overlay_stage_best_effort", drop)
    with pytest.raises(RuntimeError, match="create failed"):
        await fhir._build_provider_directory_address_overlay_stage(
            "mrf",
            "run",
            ["source-a"],
            None,
            '"mrf"."provider_directory_address_overlay"',
            fhir.ProviderDirectoryArtifactBuildFence(target_oid=1),
            False,
        )
    drop.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_fingerprint_writes_preserve_transaction_boundary(monkeypatch):
    fingerprint_rows = [{"resource_id": "practitioner-1"}]
    default_writer = AsyncMock()
    connection_writer = AsyncMock()
    monkeypatch.setattr(fhir, "_upsert_rows", default_writer)
    monkeypatch.setattr(
        fhir,
        "_upsert_dataset_resource_rows_on_connection",
        connection_writer,
    )

    await fhir._write_last_updated_partition_fingerprint_rows(
        fingerprint_rows,
        database_connection=None,
    )
    default_writer.assert_awaited_once_with(
        fhir.ProviderDirectoryDatasetResource,
        fingerprint_rows,
    )
    connection_writer.assert_not_awaited()

    connection = object()
    await fhir._write_last_updated_partition_fingerprint_rows(
        fingerprint_rows,
        database_connection=connection,
    )
    connection_writer.assert_awaited_once_with(connection, fingerprint_rows)

    empty_connection = SimpleNamespace(status=AsyncMock())
    await fhir._upsert_dataset_resource_rows_on_connection(empty_connection, [])
    empty_connection.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_pass_proof_rejects_drift_and_accepts_both_passes(
    monkeypatch,
):
    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    plan = fhir.PartitionPlan.create(
        now,
        now + datetime.timedelta(seconds=1),
        ceiling=10,
        minimum_width=datetime.timedelta(microseconds=1),
    )
    plan.observe_count("root", fhir.CountObservation.exact(1))
    window = plan.windows["root"]
    window.passes = {
        1: fhir.WindowPass({"practitioner-1": "fingerprint-1"}),
        2: fhir.WindowPass({"practitioner-1": "fingerprint-1"}),
    }
    stored = AsyncMock()
    monkeypatch.setattr(
        fhir,
        "_store_last_updated_partition_window_fingerprints",
        stored,
    )
    pass_one = AsyncMock(return_value={"practitioner-1": "drifted"})
    monkeypatch.setattr(
        fhir,
        "_load_last_updated_partition_window_fingerprints",
        pass_one,
    )

    with pytest.raises(RuntimeError, match="twin_pass_mismatch"):
        await fhir._store_partition_pass_proof(
            _checkpoint_context(),
            "Practitioner",
            window,
            2,
        )
    stored.assert_not_awaited()

    pass_one.return_value = {"practitioner-1": "fingerprint-1"}
    await fhir._store_partition_pass_proof(
        _checkpoint_context(),
        "Practitioner",
        window,
        2,
    )
    await fhir._store_partition_pass_proof(
        _checkpoint_context(),
        "Practitioner",
        window,
        1,
    )
    assert stored.await_count == 2


@pytest.mark.asyncio
async def test_partition_candidate_proof_and_stream_fail_closed(monkeypatch):
    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    plan = fhir.PartitionPlan.create(
        now,
        now + datetime.timedelta(seconds=1),
        ceiling=10,
        minimum_width=datetime.timedelta(microseconds=1),
    )
    plan.observe_count("root", fhir.CountObservation.exact(1))
    incomplete = fhir.LastUpdatedPartitionProofCounts(
        leaf_count_sum=0,
        pass1_unique=1,
        pass2_unique=0,
        staged_candidate_count=1,
        invalid_candidate_count=0,
        orphan_proof_count=0,
    )
    monkeypatch.setattr(
        fhir,
        "_last_updated_partition_candidate_proof_counts",
        AsyncMock(return_value=incomplete),
    )
    with pytest.raises(RuntimeError, match="staged_dataset_mismatch"):
        await fhir._assert_last_updated_partition_candidate_proof(
            _checkpoint_context(),
            "Practitioner",
            plan,
        )

    verified = fhir.LastUpdatedPartitionProofCounts(
        leaf_count_sum=1,
        pass1_unique=1,
        pass2_unique=1,
        staged_candidate_count=1,
        invalid_candidate_count=0,
        orphan_proof_count=0,
    )
    monkeypatch.setattr(
        fhir,
        "_assert_last_updated_partition_candidate_proof",
        AsyncMock(return_value=verified),
    )
    monkeypatch.setattr(fhir.db, "all", AsyncMock(return_value=[]))
    with pytest.raises(RuntimeError, match="stream_count_mismatch"):
        await fhir._stream_last_updated_partition_staged_rows(
            _checkpoint_context(),
            {"source_id": "source-a"},
            "Practitioner",
            object,
            plan,
            run_id="run",
            row_batch_handler=AsyncMock(return_value=0),
            row_batch_size=10,
        )


def test_partition_output_rows_reject_missing_or_non_object_payloads():
    with pytest.raises(RuntimeError, match="staged_dataset_invalid"):
        fhir._partition_output_rows(
            [{"resource_id": None, "payload_json": {}}],
            {"source_id": "source-a"},
            "run",
        )
    with pytest.raises(RuntimeError, match="staged_dataset_invalid"):
        fhir._partition_output_rows(
            [{"resource_id": "practitioner-1", "payload_json": "not-an-object"}],
            {"source_id": "source-a"},
            "run",
        )


@pytest.mark.asyncio
async def test_location_coordinate_backfill_skips_absence_and_stalled_cursor(
    monkeypatch,
):
    table_present = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(fhir, "_is_table_present", table_present)
    assert await fhir.backfill_provider_directory_location_coordinates("mrf") == 0

    repeated_cursor_by_field = {
        "candidate_rows": 1,
        "updated_rows": 1,
        "last_source_id": "source-a",
        "last_resource_id": "location-1",
    }
    monkeypatch.setattr(
        fhir.db,
        "first",
        AsyncMock(side_effect=[repeated_cursor_by_field, repeated_cursor_by_field]),
    )
    assert await fhir.backfill_provider_directory_location_coordinates(
        "mrf",
        run_id="run",
        source_ids=["source-a"],
        batch_size=10,
    ) == 2


@pytest.mark.asyncio
async def test_artifact_fence_and_relation_only_publish_keep_selected_identity(
    monkeypatch,
):
    expected = fhir.ProviderDirectoryArtifactDatasetFence(())
    selected = fhir.ProviderDirectoryArtifactDatasetFence(
        (
            fhir.ProviderDirectoryArtifactDataset(
                source_id="source-a",
                endpoint_id="endpoint-a",
                dataset_id="dataset-a",
                evidence_run_id="run",
            ),
        )
    )
    with pytest.raises(
        fhir.ProviderDirectoryArtifactBuildStale,
        match="dataset_selection_changed",
    ):
        fhir._assert_artifact_fence_selection_unchanged(expected, selected)

    monkeypatch.setattr(
        fhir,
        "_prepare_artifact_publication_fence",
        AsyncMock(return_value=selected),
    )
    metrics_by_field: dict[str, object] = {}
    assert await fhir._publish_provider_directory_dataset_artifacts(
        run_id="run",
        metrics=metrics_by_field,
        source_ids=["source-a"],
        publish_corroboration=False,
        publish_artifacts_targets={"dataset_network_plan"},
    ) == {"publish_artifacts_targets": ["dataset_network_plan"]}


@pytest.mark.asyncio
async def test_partition_checkpoint_save_rejects_lost_owner(monkeypatch):
    now = datetime.datetime(2026, 7, 22, tzinfo=datetime.UTC)
    config = fhir.LastUpdatedPartitionConfig(
        start=now,
        end=now + datetime.timedelta(seconds=1),
        end_mode="fixed",
        ceiling=10,
        minimum_width=datetime.timedelta(microseconds=1),
        boundary_precision=datetime.timedelta(microseconds=1),
        page_count=10,
        volatile_metadata_paths=(),
    )
    plan = fhir.PartitionPlan.create(
        config.start,
        config.end,
        ceiling=config.ceiling,
        minimum_width=config.minimum_width,
        boundary_precision=config.boundary_precision,
    )
    monkeypatch.setattr(
        fhir,
        "_update_last_updated_partition_checkpoint",
        AsyncMock(return_value="UPDATE 0"),
    )
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await fhir._save_last_updated_partition_plan(
            _checkpoint_context(),
            "Practitioner",
            config,
            plan,
            census=fhir.LastUpdatedCompletenessCensus(),
            pages_processed=2,
            rows_processed=3,
        )
