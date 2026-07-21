# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import importlib
import types
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import BadRequest, SanicException

from api import control
from api import control_imports
from api import provider_directory_profile_selection_attestation as selection_api
from process import provider_directory_profile_selection as selection


importer = importlib.import_module("process.provider_directory_fhir")


def _catalog() -> dict:
    return {
        "catalog_digest": "a" * 64,
        "items": [
            {
                "entry_id": "payer",
                "runnable": True,
                "profile_enabled": True,
                "source_ids": ["pdfhir_payer"],
            },
            {
                "entry_id": "probe",
                "runnable": False,
                "profile_enabled": False,
                "source_ids": ["pdfhir_probe"],
            },
        ],
    }


def _computed() -> selection._ComputedProfileSelection:
    return selection._computed_selection_from_rows(
        _catalog(),
        node_id="dev-node",
        source_rows=[
            {
                "source_id": "pdfhir_payer",
                "endpoint_id": "endpoint-1",
                "canonical_api_base": "https://payer.example/fhir",
                "org_name": "Payer",
                "plan_name": "Payer Plan",
            }
        ],
        dataset_rows=[
            {
                "endpoint_id": "endpoint-1",
                "dataset_id": "dataset-1",
                "acquisition_root_run_id": "run-root-1",
                "dataset_hash": "b" * 64,
                "status": "published",
                "is_current": True,
                "resource_count": 42,
                "validated_at": "2026-07-20 10:00:00",
                "published_at": "2026-07-20 11:00:00",
                "superseded_at": None,
                "publication_metadata_json": {
                    "source_ids": ["pdfhir_payer"],
                    "selected_resources": ["Practitioner"],
                },
            }
        ],
    )


def _attestation() -> selection.ProviderDirectoryProfileSelectionAttestation:
    computed = _computed()
    identity_with_revision_map = {
        **computed.identity_payload,
        "authority_revision": 7,
    }
    attestation_map = {
        **identity_with_revision_map,
        "proof_id": selection._proof_id(identity_with_revision_map),
    }
    return selection.validated_profile_selection_attestation(attestation_map)


def _execution() -> selection.ProviderDirectoryProfileExecution:
    return selection.ProviderDirectoryProfileExecution(
        attestation=_attestation(),
        generation=11,
    )


def _artifact_fence() -> importer.ProviderDirectoryArtifactDatasetFence:
    return importer.ProviderDirectoryArtifactDatasetFence(
        (
            importer.ProviderDirectoryArtifactDataset(
                source_id="pdfhir_payer",
                endpoint_id="endpoint-1",
                dataset_id="dataset-1",
                evidence_run_id="run-root-1",
                dataset_hash="b" * 64,
            ),
        )
    )


def _stub_profile_publish(monkeypatch, fence) -> None:
    monkeypatch.setattr(
        importer,
        "assert_registered_profile_selection_current",
        AsyncMock(),
    )
    monkeypatch.setattr(
        importer,
        "_prepare_artifact_publication_fence",
        AsyncMock(return_value=fence),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_artifact_resource_types",
        lambda *_args, **_kwargs: frozenset({"Practitioner"}),
    )
    monkeypatch.setattr(
        importer,
        "_publish_artifact_bundle_from_fence",
        AsyncMock(
            return_value={
                "profile": {
                    "generation_id": "pdpb_generation",
                    "profile_rows": 3,
                    "evidence_rows": 5,
                    "selected_evidence_rows": 5,
                }
            }
        ),
    )


def test_current_selection_matches_peer_fingerprint_and_exact_pairs():
    computed = _computed()

    assert list(computed.request_projection) == [
        {"source_id": "pdfhir_payer", "dataset_id": "dataset-1"}
    ]
    assert computed.identity_payload["selection_fingerprint"] == selection.stable_hash(
        {
            "node_id": "dev-node",
            "catalog_digest": "a" * 64,
            "datasets": [["pdfhir_payer", "dataset-1"]],
        },
        domain="provider_directory_profile_current_selection",
    )
    assert computed.identity_payload["pairs"] == [
        {
            "source_id": "pdfhir_payer",
            "endpoint_id": "endpoint-1",
            "dataset_id": "dataset-1",
            "dataset_hash": "b" * 64,
            "acquisition_root_run_id": "run-root-1",
            "publication_status": "published",
            "is_current": True,
            "lineage_authority": selection.PROFILE_SELECTION_LINEAGE_AUTHORITY,
        }
    ]


def test_profile_input_digest_changes_with_source_context_and_dataset_metadata():
    baseline = _computed()
    changed_context = selection._computed_selection_from_rows(
        _catalog(),
        node_id="dev-node",
        source_rows=[
            {
                "source_id": "pdfhir_payer",
                "endpoint_id": "endpoint-1",
                "canonical_api_base": "https://payer.example/fhir",
                "org_name": "Renamed Payer",
                "plan_name": "Payer Plan",
            }
        ],
        dataset_rows=[
            {
                "endpoint_id": "endpoint-1",
                "dataset_id": "dataset-1",
                "acquisition_root_run_id": "run-root-1",
                "dataset_hash": "b" * 64,
                "resource_count": 42,
                "validated_at": "2026-07-20 10:00:00",
                "published_at": "2026-07-20 11:00:00",
                "publication_metadata_json": {
                    "source_ids": ["pdfhir_payer"],
                    "selected_resources": ["Practitioner"],
                    "proof_version": 2,
                },
            }
        ],
    )

    assert (
        changed_context.identity_payload["source_context_digest"]
        != baseline.identity_payload["source_context_digest"]
    )
    assert (
        changed_context.identity_payload["profile_input_digest"]
        != baseline.identity_payload["profile_input_digest"]
    )


def test_attestation_rejects_tampering_and_purge_is_explicit():
    attestation = _attestation()
    tampered_map = dict(attestation.payload)
    tampered_map["catalog_digest"] = "c" * 64

    with pytest.raises(
        selection.ProviderDirectoryProfileSelectionError,
        match="proof_id",
    ):
        selection.validated_profile_selection_attestation(tampered_map)

    computed = _computed()
    empty_identity_map = {
        **computed.identity_payload,
        "selection_fingerprint": selection.stable_hash(
            {
                "node_id": "dev-node",
                "catalog_digest": "a" * 64,
                "datasets": [],
            },
            domain="provider_directory_profile_current_selection",
        ),
        "operation": "purge",
        "pairs": [],
        "authority_revision": 8,
    }
    empty_attestation_map = {
        **empty_identity_map,
        "proof_id": selection._proof_id(empty_identity_map),
    }
    purge = selection.validated_profile_selection_attestation(
        empty_attestation_map
    )

    assert purge.operation == "purge"
    assert purge.pairs == ()


@pytest.mark.asyncio
async def test_attestation_endpoint_authenticates_and_never_echoes_request(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    attestation = _attestation()
    attester = AsyncMock(return_value=attestation.payload)
    monkeypatch.setattr(selection_api, "attest_profile_selection", attester)
    monkeypatch.setattr(selection_api, "provider_directory_source_catalog", _catalog)
    request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={"untrusted": "payload"},
    )

    response = await selection_api.control_provider_directory_profile_selection_attestation(
        request
    )

    assert json.loads(response.body) == attestation.payload
    attester.assert_awaited_once_with(request.json, _catalog())


@pytest.mark.asyncio
async def test_attestation_endpoint_maps_validation_and_drift(monkeypatch):
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    monkeypatch.setattr(selection_api, "provider_directory_source_catalog", _catalog)
    request = types.SimpleNamespace(
        headers={"Authorization": "Bearer secret"},
        json={},
    )
    monkeypatch.setattr(
        selection_api,
        "attest_profile_selection",
        AsyncMock(
            side_effect=selection.ProviderDirectoryProfileSelectionError(
                "bad proof"
            )
        ),
    )
    with pytest.raises(BadRequest, match="bad proof"):
        await selection_api.control_provider_directory_profile_selection_attestation(
            request
        )

    monkeypatch.setattr(
        selection_api,
        "attest_profile_selection",
        AsyncMock(
            side_effect=selection.ProviderDirectoryProfileSelectionDrift(
                "provider_directory_profile_selection_drift"
            )
        ),
    )
    with pytest.raises(SanicException) as drift:
        await selection_api.control_provider_directory_profile_selection_attestation(
            request
        )
    assert drift.value.status_code == 409
    assert str(drift.value) == "provider_directory_profile_selection_drift"


def test_admission_requires_exact_proof_and_generation(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    attestation = _attestation()
    profile_params_map = {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": 11,
        "provider_directory_profile_selection_attestation": attestation.payload,
    }

    control_imports._validate_provider_directory_profile_execution_params(
        "provider-directory-fhir",
        profile_params_map,
    )
    profile_params_map["provider_directory_profile_generation"] = 0
    with pytest.raises(ValueError, match="generation"):
        control_imports._validate_provider_directory_profile_execution_params(
            "provider-directory-fhir",
            profile_params_map,
        )


def test_global_profile_and_independent_acquisition_do_not_block_each_other(
    monkeypatch,
):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    profile_params_map = {
        **selection._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": 11,
        "provider_directory_profile_selection_attestation": _attestation().payload,
    }
    acquisition_params_map = {
        "import_resources": True,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "source_ids": ["pdfhir_other"],
        "source_concurrency": 1,
        "provider_directory_endpoint_scope": "https://other.example/fhir",
    }
    active_acquisition_map = {
        "run_id": "run-acquisition",
        "params": acquisition_params_map,
        "metrics": {},
    }
    active_profile_map = {
        "run_id": "run-profile",
        "params": profile_params_map,
        "metrics": {},
    }

    assert control_imports._provider_directory_blocking_run(
        profile_params_map,
        [active_acquisition_map],
    ) is None
    assert control_imports._provider_directory_blocking_run(
        acquisition_params_map,
        [active_profile_map],
    ) is None


def test_artifact_fence_must_match_every_attested_pair():
    execution = _execution()
    fence = _artifact_fence()

    importer._assert_profile_selection_matches_artifact_fence(execution, fence)
    drifted_fence = importer.ProviderDirectoryArtifactDatasetFence(
        (
            importer.ProviderDirectoryArtifactDataset(
                source_id="pdfhir_payer",
                endpoint_id="endpoint-1",
                dataset_id="dataset-2",
                evidence_run_id="run-root-1",
                dataset_hash="b" * 64,
            ),
        )
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="selection_fence_changed",
    ):
        importer._assert_profile_selection_matches_artifact_fence(
            execution,
            drifted_fence,
        )


@pytest.mark.asyncio
async def test_attested_publish_emits_strict_result_and_profile_generation(
    monkeypatch,
):
    """Bind the terminal result to the exact published Profile generation."""

    execution = _execution()
    fence = _artifact_fence()
    _stub_profile_publish(monkeypatch, fence)

    metrics = await importer._publish_attested_provider_directory_profile(
        run_id="run-profile",
        metrics={},
        execution=execution,
    )

    selection_result_map = metrics[selection.PROFILE_SELECTION_RESULT_METRIC]
    assert selection_result_map["generation"] == 11
    assert selection_result_map["profile_generation_id"] == "pdpb_generation"
    assert selection_result_map["status"] == "published"
    assert selection_result_map["row_counts"] == {
        "profile_rows": 3,
        "profile_source_evidence_rows": 5,
        "source_count": 1,
        "dataset_count": 1,
    }
    assert (
        metrics["profile"]["generation_id"]
        == selection_result_map["profile_generation_id"]
    )


@pytest.mark.asyncio
async def test_cutover_recomputes_attestation_inside_transaction(monkeypatch):
    execution = _execution()
    verifier = AsyncMock()
    monkeypatch.setattr(
        importer,
        "assert_profile_selection_current_in_transaction",
        verifier,
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_selection_catalog",
        _catalog,
    )
    token = importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.set(
        execution
    )
    try:
        await importer._verify_active_profile_selection_at_cutover()
    finally:
        importer._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION.reset(token)

    verifier.assert_awaited_once_with(execution.attestation, _catalog())
