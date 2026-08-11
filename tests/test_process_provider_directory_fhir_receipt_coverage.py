# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Pure branch coverage for UHC/Profile importer boundary helpers."""

from __future__ import annotations

import datetime
import json
import types
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_profile_capacity_preflight_contract as contract
from tests.provider_directory_profile_capacity_signing_guard_test_support import (
    synthetic_profile_execution,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    EXPIRES_AT,
    ISSUED_AT,
    _geometry,
    _quiescence,
    _receipt,
    _receipt_storage,
    _request,
    _runtime_observation,
    _serving_state,
    _transaction,
    importer,
)


@pytest.fixture(autouse=True)
def _import_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _workload():
    request = _request()
    return types.SimpleNamespace(
        limits_payload=request.limits_payload,
        limits_sha256=request.limits_sha256,
        artifact_projection=types.SimpleNamespace(
            projected_rows=45,
            projected_logical_bytes=67_890,
            projection_hash="8" * 64,
        ),
    )


def _lease(receipt):
    return types.SimpleNamespace(
        nonce=receipt["receipt_sha256"],
        expires_at=EXPIRES_AT,
        observed_at=ISSUED_AT,
        issued_at=ISSUED_AT,
        attestation_id="attestation-1",
        signing_preflight_guard={
            "healthcare_receipt": dict(receipt),
            "healthcare_receipt_sha256": receipt["receipt_sha256"],
            "healthcare_request_sha256": receipt["request_sha256"],
            "control_plane_receipt_sha256": (receipt["control_plane_receipt_sha256"]),
            "capacity_limits_sha256": receipt["capacity_limits_sha256"],
        },
    )


def _receipt_row(receipt):
    values = importer._profile_capacity_preflight_receipt_values(
        _request(),
        receipt,
        issued_at=ISSUED_AT,
    )
    return {
        **values,
        "receipt_json": receipt,
        "consumed_at": None,
    }


@pytest.mark.asyncio
async def test_receipt_layout_missing_guard(monkeypatch):
    """Reject a missing durable preflight receipt relation."""
    relation_oid = AsyncMock(return_value=None)
    monkeypatch.setattr(importer, "_provider_directory_relation_oid", relation_oid)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="preflight_relation_missing",
    ):
        await importer._profile_capacity_preflight_receipt_layout("mrf")


@pytest.mark.asyncio
async def test_receipt_clock_guard(monkeypatch):
    """Reject an absent database clock and accept its aware timestamp."""
    clock_row = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", clock_row)
    with pytest.raises(
        contract.ProviderDirectoryProfileCapacityPreflightError,
        match="clock_invalid",
    ):
        await importer._profile_capacity_preflight_clock()
    clock_row.return_value = {"observed_at": ISSUED_AT}
    assert await importer._profile_capacity_preflight_clock() == ISSUED_AT


@pytest.mark.asyncio
async def test_existing_receipt_ledger_guards(monkeypatch):
    """Reject ambiguous replay rows and return one exact receipt row."""
    request = _request()
    receipt_rows = AsyncMock(return_value=[])
    monkeypatch.setattr(importer.db, "all", receipt_rows)
    assert (
        await importer._profile_capacity_preflight_existing_receipt("mrf", request)
        is None
    )
    receipt_rows.return_value = [{}, {}]
    with pytest.raises(RuntimeError, match="ledger_corrupt"):
        await importer._profile_capacity_preflight_existing_receipt("mrf", request)
    receipt_rows.return_value = [
        {
            "request_sha256": request.request_sha256,
            "request_nonce": request.request_nonce,
        }
    ]
    assert (
        await importer._profile_capacity_preflight_existing_receipt("mrf", request)
        == receipt_rows.return_value[0]
    )


@pytest.mark.asyncio
async def test_quiescence_ledger_guards(monkeypatch):
    """Reject missing or active work and hash the exact zero snapshot."""
    count_row = AsyncMock(return_value=None)
    monkeypatch.setattr(importer.db, "first", count_row)
    with pytest.raises(RuntimeError, match="quiescence_missing"):
        await importer._profile_capacity_quiescence(
            "mrf",
            observed_at=ISSUED_AT,
            request_sha256="a" * 64,
        )
    count_row.return_value = {"active_profile_run_count": 1}
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="not_quiescent",
    ):
        await importer._profile_capacity_quiescence(
            "mrf",
            observed_at=ISSUED_AT,
            request_sha256="a" * 64,
        )
    count_row.return_value = {}
    quiescence, digest = await importer._profile_capacity_quiescence(
        "mrf",
        observed_at=ISSUED_AT,
        request_sha256="a" * 64,
    )
    assert not any(
        quiescence[field]
        for field in (
            "active_profile_run_count",
            "claimed_profile_checkpoint_count",
            "unexpired_capacity_consumption_count",
            "outstanding_preflight_receipt_count",
        )
    )
    assert digest == contract.preflight_domain_sha256(
        contract.CAPACITY_QUIESCENCE_DIGEST_DOMAIN,
        quiescence,
    )


@pytest.mark.asyncio
async def test_existing_receipt_replay_guards():
    """Replay one open exact row and reject a consumed row."""
    request = _request()
    receipt = _receipt()
    value_by_field = importer._profile_capacity_preflight_receipt_values(
        request, receipt, issued_at=ISSUED_AT
    )
    existing_row_by_field = {
        **value_by_field,
        "receipt_json": json.dumps(receipt),
        "consumed_at": None,
    }
    assert (
        await importer._persist_or_replay_capacity_receipt(
            "mrf",
            request,
            receipt,
            existing_row_by_field,
            issued_at=ISSUED_AT,
            observed_at=ISSUED_AT,
        )
        == receipt
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="replay_changed",
    ):
        await importer._persist_or_replay_capacity_receipt(
            "mrf",
            request,
            receipt,
            {**existing_row_by_field, "consumed_at": ISSUED_AT},
            issued_at=ISSUED_AT,
            observed_at=ISSUED_AT,
        )


@pytest.mark.asyncio
async def test_new_receipt_insert_guards(monkeypatch):
    """Reject a lost insert and return one durably inserted receipt."""
    request = _request()
    receipt = _receipt()
    status = AsyncMock(return_value="INSERT 0 0")
    monkeypatch.setattr(importer.db, "status", status)
    with pytest.raises(RuntimeError, match="insert_failed"):
        await importer._persist_or_replay_capacity_receipt(
            "mrf",
            request,
            receipt,
            None,
            issued_at=ISSUED_AT,
            observed_at=ISSUED_AT,
        )
    status.return_value = "INSERT 0 1"
    assert (
        await importer._persist_or_replay_capacity_receipt(
            "mrf",
            request,
            receipt,
            None,
            issued_at=ISSUED_AT,
            observed_at=ISSUED_AT,
        )
        == receipt
    )


def test_receipt_issued_at_and_public_preflight_type_guards():
    assert importer._profile_capacity_receipt_issued_at(None, ISSUED_AT) == ISSUED_AT
    assert (
        importer._profile_capacity_receipt_issued_at(
            {"issued_at": ISSUED_AT},
            ISSUED_AT + datetime.timedelta(seconds=1),
        )
        == ISSUED_AT
    )
    with pytest.raises(RuntimeError, match="ledger_corrupt"):
        importer._profile_capacity_receipt_issued_at(
            {"issued_at": "invalid"},
            ISSUED_AT,
        )


@pytest.mark.asyncio
async def test_public_preflight_rejects_untyped_request():
    with pytest.raises(
        contract.ProviderDirectoryProfileCapacityPreflightError,
        match="request_invalid",
    ):
        await importer.provider_directory_profile_capacity_preflight({})


def test_dataset_materialization_unsupported_and_invalid(monkeypatch):
    monkeypatch.setattr(importer, "parse_fhir_resource", lambda *_args, **_kwargs: None)
    with pytest.raises(ValueError, match="resource_unsupported"):
        importer.materialize_provider_directory_dataset_fhir_resource(
            source_id="source",
            dataset_id="dataset",
            resource={"resourceType": "Unsupported"},
            run_id="run",
            semantic_projection_as_of="2026-08-10",
        )

    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (object(), {}),
    )
    monkeypatch.setattr(
        importer, "_endpoint_dataset_resource_rows", lambda *_a, **_k: []
    )
    with pytest.raises(ValueError, match="resource_invalid"):
        importer.materialize_provider_directory_dataset_fhir_resource(
            source_id="source",
            dataset_id="dataset",
            resource={"resourceType": "Practitioner"},
            run_id="run",
            semantic_projection_as_of="2026-08-10",
        )


@pytest.mark.asyncio
async def test_dataset_serving_and_final_uhc_publication_guards(monkeypatch):
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_network_plan",
        AsyncMock(return_value={"complete": False}),
    )
    monkeypatch.setattr(
        importer,
        "_build_provider_directory_dataset_affiliation_organization",
        AsyncMock(return_value={"complete": True}),
    )
    with pytest.raises(RuntimeError, match="serving_relations_incomplete"):
        await importer.build_provider_directory_dataset_serving_relations(
            object(),
            "dataset",
            build_run_id="run",
            expected_acquisition_root_run_id="root",
        )

    monkeypatch.setattr(importer, "_endpoint_dataset_state", AsyncMock(return_value={}))
    candidate = types.SimpleNamespace(dataset_id="dataset", source_ids=["a", "b"])
    with pytest.raises(RuntimeError, match="current_publication_proof_invalid"):
        await importer._assert_final_uhc_publication(candidate)


def test_remaining_flex_boundary_branches(monkeypatch):
    """Accept exact Flex metadata and reject a drifted proof."""
    resources = ("Organization", "Practitioner")
    monkeypatch.setattr(
        importer,
        "uhc_flex_profile_expected_resources",
        lambda _dataset_id: resources,
    )
    metadata_valid_by_field = {"value": True}
    monkeypatch.setattr(
        importer,
        "is_uhc_flex_publication_metadata_valid",
        lambda *_args, **_kwargs: metadata_valid_by_field["value"],
    )
    keyword_by_name = {
        "endpoint_id": "endpoint",
        "dataset_id": "dataset",
        "evidence_run_id": "run",
        "selected_resources": resources,
        "publication_metadata": {},
        "dataset_scoped_ready": True,
    }
    assert importer._artifact_flex_retained_resources(**keyword_by_name) == resources
    metadata_valid_by_field["value"] = False
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="dataset_scoped_proof_invalid",
    ):
        importer._artifact_flex_retained_resources(**keyword_by_name)


def test_remaining_profile_control_boundary_branches():
    """Reject layout, strategy, trigger, and index contract drift."""
    control_identity = types.SimpleNamespace(
        build_checkpoint_storage_fingerprint="a",
        import_run_storage_fingerprint="b",
        capacity_consumption_storage_fingerprint="c",
        serving_generation_storage_fingerprint="d",
    )
    control_layouts = tuple(
        types.SimpleNamespace(exact_fingerprint=fingerprint)
        for fingerprint in ("a", "b", "c", "changed")
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="control_metadata_layout_changed",
    ):
        importer._assert_profile_control_layout_identities(
            control_identity,
            control_layouts,
        )

    invalid_execution = types.SimpleNamespace(
        attestation=types.SimpleNamespace(
            profile_schema_version=-1,
            profile_strategy_version="invalid",
        )
    )
    run_id = "run_" + "a" * 32
    with pytest.raises(RuntimeError, match="capacity_strategy_mismatch"):
        importer._validated_admission_run_id(run_id, run_id, invalid_execution)

    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="trigger_shape_changed",
    ):
        importer._assert_profile_capacity_trigger_shape([], 1, "immutable")
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="index_shape_unsupported",
    ):
        importer._profile_capacity_structural_indexes([], 100)


def test_profile_resource_scope_boundary_branch():
    """Reject a materialized source scope that differs from the fence."""
    materialization = types.SimpleNamespace(
        materialization_mode="source_delta",
        source_ids=["source-a"],
    )
    scope_token = importer._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.set(
        ("source-b",)
    )
    try:
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="resource_scope_changed",
        ):
            importer._validate_profile_materialized_resource_scope(materialization)
    finally:
        importer._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.reset(
            scope_token
        )
