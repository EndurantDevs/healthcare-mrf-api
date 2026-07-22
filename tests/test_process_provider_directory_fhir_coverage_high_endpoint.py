# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.provider_directory_fhir_coverage_high_support import (
    endpoint_dataset_candidate,
    importer,
    pagination_checkpoint_context,
)


@pytest.mark.asyncio
async def test_endpoint_scope_state_and_orphan_guards(monkeypatch):
    with pytest.raises(RuntimeError, match="endpoint_scope_invalid"):
        importer._validate_provider_directory_endpoint_scope([], "/")
    monkeypatch.setattr(
        importer,
        "PROVIDER_DIRECTORY_ENDPOINT_ACQUISITION_MANIFEST_PATH",
        SimpleNamespace(read_text=Mock(side_effect=OSError("missing-manifest"))),
    )
    with pytest.raises(RuntimeError, match="acquisition_manifest_unavailable"):
        importer._configured_catalog_protected_source_ids({})

    monkeypatch.setattr(
        importer.db,
        "first",
        AsyncMock(
            side_effect=[
                {"dataset_id": "dataset-1", "status": "validated"},
                {"dataset_id": "dataset-current"},
            ]
        ),
    )
    dataset_state = await importer._endpoint_dataset_state("dataset-1")
    current_id = await importer._current_endpoint_dataset_id(
        "endpoint-1",
        exclude_dataset_id="dataset-1",
    )
    assert dataset_state["status"] == "validated"
    assert current_id == "dataset-current"

    connection = SimpleNamespace(first=AsyncMock(return_value={}))
    repair_candidate = endpoint_dataset_candidate(repair_empty_orphan=True)
    await importer._assert_empty_endpoint_dataset_orphan(connection, repair_candidate)
    connection.first.return_value = {"resource_count": 1}
    with pytest.raises(RuntimeError, match="orphan_not_empty"):
        await importer._assert_empty_endpoint_dataset_orphan(
            connection,
            repair_candidate,
        )


def test_twin_root_and_terminal_proof_guards_reject_mismatches(monkeypatch):
    baseline_metadata_map = {
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "result": "wrong-result",
        },
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: None,
    }
    monkeypatch.setattr(
        importer,
        "_twin_root_dataset_proof",
        Mock(return_value={"endpoint_id": "wrong-endpoint"}),
    )
    with pytest.raises(RuntimeError, match="verification_baseline_invalid"):
        importer._twin_root_baseline_proof(
            {"publication_metadata_json": baseline_metadata_map}
        )

    matched_metadata_map = {
        importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
            "result": "wrong-result",
            "mismatch_fields": [],
            "baseline_dataset_id": "baseline-1",
        },
        importer.TWIN_ROOT_VERIFICATION_BASELINE_DATASET_KEY: "baseline-1",
    }
    with pytest.raises(RuntimeError, match="verification_match_invalid"):
        importer._assert_matched_twin_root_dataset_proof(
            {"publication_metadata_json": matched_metadata_map}
        )

    monkeypatch.setattr(
        importer,
        "_twin_root_baseline_proof",
        Mock(return_value={"endpoint_id": "different"}),
    )
    with pytest.raises(RuntimeError, match="baseline_incompatible"):
        importer._assert_compatible_twin_root_baseline(
            endpoint_dataset_candidate(),
            {},
        )

    with pytest.raises(RuntimeError, match="verification_mismatch_invalid"):
        importer._assert_terminal_endpoint_dataset_proof(
            {},
            importer.ENDPOINT_DATASET_VERIFICATION_MISMATCH,
            {
                importer.TWIN_ROOT_VERIFICATION_METADATA_KEY: {
                    "result": "matched",
                    "mismatch_fields": [],
                }
            },
            {"verification_baseline_dataset_id": "baseline-1"},
        )


@pytest.mark.asyncio
async def test_resumable_candidate_rejects_unproven_lineage(monkeypatch):
    profile = importer.EndpointDatasetVerificationProfile(False)
    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        AsyncMock(return_value="other-dataset"),
    )
    with pytest.raises(RuntimeError, match="dataset_lineage_mismatch"):
        await importer._select_resumable_endpoint_dataset_candidate(
            {},
            "dataset-1",
            "endpoint-1",
            ("Practitioner",),
            "root-run",
            "run-old",
            pagination_checkpoint_context(),
            profile,
        )

    monkeypatch.setattr(
        importer,
        "_checkpoint_candidate_dataset_id",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        importer,
        "_should_repair_empty_endpoint_dataset_orphan",
        Mock(return_value=False),
    )
    with pytest.raises(RuntimeError, match="dataset_retry_unverified"):
        await importer._select_resumable_endpoint_dataset_candidate(
            {},
            "dataset-1",
            "endpoint-1",
            ("Practitioner",),
            "root-run",
            "run-old",
            pagination_checkpoint_context(),
            profile,
        )


@pytest.mark.asyncio
async def test_pagination_adoption_rejects_unproven_lineage(monkeypatch):
    checkpoint_map = {"owner_run_id": "run-old"}
    with pytest.raises(RuntimeError, match="checkpoint_lineage_mismatch"):
        await importer._adopt_pagination_checkpoint_owner(
            pagination_checkpoint_context(
                retry_of_run_id="different-run",
                lineage_verified=False,
            ),
            "Practitioner",
            "run-old",
            checkpoint_map,
            "start-hash",
        )

    monkeypatch.setattr(
        importer,
        "_compatible_pagination_resume_state",
        Mock(return_value=None),
    )
    with pytest.raises(RuntimeError, match="checkpoint_lineage_mismatch"):
        await importer._adopt_pagination_checkpoint_owner(
            pagination_checkpoint_context(),
            "Practitioner",
            "run-old",
            checkpoint_map,
            "start-hash",
        )


@pytest.mark.asyncio
async def test_pagination_checkpoint_write_fences(monkeypatch):
    monkeypatch.setattr(
        importer.db,
        "status",
        AsyncMock(side_effect=["UPDATE 1", "UPDATE 0"]),
    )
    checkpoint_context = pagination_checkpoint_context()
    await importer._save_pagination_checkpoint(
        checkpoint_context,
        "Practitioner",
        next_url=None,
        pages_processed=2,
        rows_processed=3,
        recent_url_hashes=["one", "two"],
    )
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await importer._save_pagination_checkpoint(
            checkpoint_context,
            "Practitioner",
            next_url="https://example.test/next",
            pages_processed=2,
            rows_processed=3,
            recent_url_hashes=[],
        )


@pytest.mark.asyncio
async def test_pagination_completeness_write_fences(monkeypatch):
    monkeypatch.setattr(
        importer.db,
        "status",
        AsyncMock(side_effect=["UPDATE 1", "UPDATE 0"]),
    )
    checkpoint_context = pagination_checkpoint_context()
    await importer._save_pagination_checkpoint_completeness(
        checkpoint_context,
        "Practitioner",
        {"verified": True},
    )
    with pytest.raises(RuntimeError, match="checkpoint_ownership_lost"):
        await importer._save_pagination_checkpoint_completeness(
            checkpoint_context,
            "Practitioner",
            {"verified": False},
        )


def test_serving_relation_and_projection_proof_guards():
    with pytest.raises(RuntimeError, match="dataset_missing"):
        importer._validated_serving_relation_root(
            {"target_dataset_count": 0},
            "dataset-1",
            "root-run",
            "proof",
        )
    with pytest.raises(RuntimeError, match="root_mismatch"):
        importer._validated_serving_relation_root(
            {
                "target_dataset_count": 1,
                "acquisition_root_run_id": "different-root",
            },
            "dataset-1",
            "root-run",
            "proof",
        )
    with pytest.raises(RuntimeError, match="insurance_plan_incomplete"):
        importer._validated_dataset_insurance_plan_projection_proof(
            {
                "target_dataset_count": 1,
                "acquisition_root_run_id": "root-run",
                "insurance_plan_resource_count": 2,
                "plan_projection_count": 1,
            },
            dataset_id="dataset-1",
            expected_acquisition_root_run_id="root-run",
            replaced_plan_projection_count=0,
            inserted_plan_projection_count=1,
        )

    should_verify, query_params = importer._dataset_serving_relation_query_options(
        "dataset-1",
        "root-run",
    )
    assert should_verify is True
    assert query_params["acquisition_root_run_id"] == "root-run"


@pytest.mark.asyncio
async def test_dataset_content_and_twin_admission_guards(monkeypatch):
    assert await importer._endpoint_dataset_bulk_transaction_times(object(), None) == {}
    content_proof = importer.EndpointDatasetContentProof("hash", 3, {}, {})
    monkeypatch.setattr(
        importer,
        "_endpoint_dataset_content_proof",
        AsyncMock(return_value=content_proof),
    )
    assert await importer._endpoint_dataset_content_hash(
        object(),
        "dataset-1",
    ) == ("hash", 3)

    candidate = endpoint_dataset_candidate(
        requires_twin_root_verification=True,
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
    )
    monkeypatch.setattr(
        importer,
        "_locked_endpoint_verification_state",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        importer,
        "_candidate_with_locked_twin_root_admission",
        Mock(return_value=endpoint_dataset_candidate()),
    )
    with pytest.raises(RuntimeError, match="verification_admission_missing"):
        await importer._locked_twin_root_verification_decision(
            object(),
            candidate,
            content_proof,
        )

    with pytest.raises(RuntimeError, match="verification_metadata_invalid"):
        importer._record_baseline_payload_retirement({}, "baseline-1", 3)
    connection = SimpleNamespace(status=AsyncMock(return_value="UPDATE 0"))
    with pytest.raises(RuntimeError, match="payload_retirement_lost"):
        await importer._store_baseline_payload_retirement(
            connection,
            endpoint_dataset_candidate(),
            "baseline-1",
            {},
        )


@pytest.mark.asyncio
async def test_small_branch_only_helpers_remain_source_neutral(monkeypatch):
    assert importer._reference_resource_key(None, "Practitioner") is None
    assert importer._row_reference_values(
        {"references": ["", "Practitioner/one"]},
        ("references",),
    ) == ["Practitioner/one"]

    monkeypatch.setattr(
        importer,
        "_attach_provider_directory_endpoint_ids",
        Mock(return_value=[]),
    )
    upsert = AsyncMock(return_value=1)
    monkeypatch.setattr(importer, "_upsert_rows", upsert)
    assert await importer._upsert_provider_directory_source_rows(
        [{"source_id": "source-1"}]
    ) == 0
    assert upsert.await_count == 1

    monkeypatch.setattr(
        importer,
        "_address_overlay_index_name",
        Mock(return_value="same-index"),
    )
    monkeypatch.setattr(importer.db, "status", AsyncMock())
    await importer._rename_address_overlay_stage_indexes("mrf", "stage")
    importer.db.status.assert_not_awaited()
