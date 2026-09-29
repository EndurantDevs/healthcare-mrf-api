# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact desired CMS selection, date identity, and legacy-lane safety."""

from __future__ import annotations

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_selection_contract as contract
from process import provider_directory_profile_selection_snapshot as snapshot

from .test_provider_directory_profile_selection_attestation import _variant_registry_rows


def _pair(dataset_id="cms-next", *, current=False):
    return {
        "source_id": "cms-npd",
        "endpoint_id": "cms-endpoint",
        "dataset_id": dataset_id,
        "dataset_hash": "c" * 64,
        "acquisition_root_run_id": "cms-root",
        "publication_status": "published" if current else "validated",
        "is_current": current,
        "lineage_authority": contract.PROFILE_SELECTION_LINEAGE_AUTHORITY,
    }


def _desired(*, current=False, day="2026-09-29", incumbent=True):
    return {
        "desired_cms_dataset": _pair("cms-current" if current else "cms-next", current=current),
        "expected_cms_incumbent": _pair("cms-current", current=True) if incumbent else None,
        "desired_profile_as_of": day,
    }


def _rows(pair):
    return {
        "endpoint_id": pair["endpoint_id"],
        "dataset_id": pair["dataset_id"],
        "dataset_hash": pair["dataset_hash"],
        "acquisition_root_run_id": pair["acquisition_root_run_id"],
        "status": pair["publication_status"],
        "is_current": pair["is_current"],
        "resource_count": 12,
        "validated_at": "2026-09-28 10:00:00",
        "published_at": "2026-09-28 11:00:00" if pair["is_current"] else None,
        "superseded_at": None,
        "publication_metadata_json": {"source_ids": [pair["source_id"]]},
    }


def _selection_rows(desired=None):
    desired = desired or _desired()
    catalog_map = {
        "catalog_digest": "a" * 64,
        "items": [
            {"runnable": True, "profile_enabled": True, "source_ids": [source_id]}
            for source_id in ("cms-npd", "pdfhir_synthetic_retained")
        ],
    }
    sources = [
        *(_variant_registry_rows()),
        {"source_id": "cms-npd", "endpoint_id": "cms-endpoint", "org_name": "Synthetic directory"},
        {"source_id": "pdfhir_synthetic_retained", "endpoint_id": "payer-endpoint", "org_name": "Synthetic payer"},
    ]
    payer_pair_map = {
        **_pair("payer-current", current=True),
        "source_id": "pdfhir_synthetic_retained",
        "endpoint_id": "payer-endpoint",
    }
    current_rows = [_rows(payer_pair_map)]
    if desired["expected_cms_incumbent"] is not None:
        current_rows.append(_rows(desired["expected_cms_incumbent"]))
    return catalog_map, sources, current_rows, _rows(desired["desired_cms_dataset"])


def _computed(desired=None):
    desired = desired or _desired()
    catalog, sources, rows, candidate = _selection_rows(desired)
    return snapshot._computed_desired_selection_from_rows(
        catalog,
        node_id="synthetic-node",
        source_rows=sources,
        dataset_rows=rows,
        desired_dataset_row=candidate,
        desired_selection=desired,
    )


def _proof(desired=None):
    payload = {**_computed(desired).identity_payload, "authority_revision": 1}
    payload["proof_id"] = contract._proof_id(payload)
    return contract.validated_profile_selection_attestation(payload)


def test_profile_spec_enables_the_complete_cms_resource_family():
    from process.provider_directory_profile import load_profile_source_spec

    source_spec = load_profile_source_spec()
    assert "cms-npd" in source_spec["entry_ids"] and "cms-npd" in source_spec["source_ids"]
    matrix = source_spec["verification_matrix"]
    cms_profile = matrix["resource_profiles"]["CMS_NPD_R4"]
    assert cms_profile["transport"] == "cms_npd_bulk_files"
    assert set(cms_profile["resources"]) == {
        "Organization",
        "Location",
        "Endpoint",
        "HealthcareService",
        "InsurancePlan",
        "Practitioner",
        "PractitionerRole",
        "OrganizationAffiliation",
    }
    assert [source for source in matrix["sources"] if source["source_id"] == "cms-npd"] == [
        {"entry_id": "cms-npd", "source_id": "cms-npd", "resource_profile": "CMS_NPD_R4"}
    ]


def test_replacement_preserves_other_retained_sources_and_real_candidate_state():
    computed = _computed()
    assert list(computed.request_projection) == [
        {"source_id": "cms-npd", "dataset_id": "cms-next"},
        {"source_id": "pdfhir_synthetic_retained", "dataset_id": "payer-current"},
    ]
    cms, retained = computed.identity_payload["pairs"]
    assert cms == _pair()
    assert retained["dataset_id"] == "payer-current" and retained["is_current"] is True
    assert _proof().desired_profile_as_of == "2026-09-29"


def test_first_cms_publication_is_explicit_and_cannot_be_a_purge():
    proof = _proof(_desired(incumbent=False))
    assert proof.expected_cms_incumbent is None
    assert proof.operation == "publish" and proof.desired_cms_dataset == _pair()
    payload = deepcopy(proof.payload)
    payload["pairs"] = []
    payload["operation"] = "purge"
    payload["proof_id"] = contract._proof_id(payload)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_selection_attestation(payload)


def test_same_byte_current_date_refresh_changes_all_desired_identity_hashes():
    earlier = _proof(_desired(current=True, day="2026-09-29"))
    later = _proof(_desired(current=True, day="2026-09-30"))
    assert earlier.pairs == later.pairs
    assert earlier.desired_cms_dataset == earlier.expected_cms_incumbent
    for name in ("selection_fingerprint", "profile_input_digest", "proof_id"):
        assert getattr(earlier, name) != getattr(later, name)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda desired: desired.update(desired_profile_as_of="2026-9-29"),
        lambda desired: desired["desired_cms_dataset"].update(source_id="pdfhir_other"),
        lambda desired: desired["desired_cms_dataset"].update(publication_status="superseded"),
        lambda desired: desired["desired_cms_dataset"].update(is_current=True),
        lambda desired: desired["expected_cms_incumbent"].update(is_current=False),
        lambda desired: desired["expected_cms_incumbent"].update(endpoint_id="other-endpoint"),
        lambda desired: desired["desired_cms_dataset"].update(dataset_id="cms-current"),
    ],
)
def test_desired_contract_rejects_every_other_state_and_incumbent_shape(mutate):
    desired = _desired()
    mutate(desired)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._validated_desired_selection(desired)


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_hash", "d" * 64),
        ("status", "published"),
        ("is_current", True),
        ("validated_at", None),
        ("published_at", "2026-09-29 01:00:00"),
        ("superseded_at", "2026-09-29 01:00:00"),
        ("resource_count", 0),
        ("publication_metadata_json", {"source_ids": ["pdfhir_other"]}),
    ],
)
def test_locked_dataset_read_rejects_drift(field, value):
    desired = _desired()
    catalog, sources, rows, candidate = _selection_rows(desired)
    candidate[field] = value
    with pytest.raises(contract.ProviderDirectoryProfileSelectionDrift):
        snapshot._computed_desired_selection_from_rows(
            catalog,
            node_id="synthetic-node",
            source_rows=sources,
            dataset_rows=rows,
            desired_dataset_row=candidate,
            desired_selection=desired,
        )


def test_incumbent_replacement_and_current_refresh_must_match_exact_source_tuple():
    desired = _desired()
    catalog, sources, rows, candidate = _selection_rows(desired)
    rows[-1]["dataset_hash"] = "d" * 64
    with pytest.raises(contract.ProviderDirectoryProfileSelectionDrift):
        snapshot._computed_desired_selection_from_rows(
            catalog,
            node_id="synthetic-node",
            source_rows=sources,
            dataset_rows=rows,
            desired_dataset_row=candidate,
            desired_selection=desired,
        )
    current = _desired(current=True)
    current["expected_cms_incumbent"]["dataset_hash"] = "d" * 64
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._validated_desired_selection(current)


def test_legacy_default_cannot_accept_a_validated_desired_pair_or_new_fields():
    proof = _proof()
    payload = {name: value for name, value in proof.payload.items() if name not in contract._DESIRED_SELECTION_FIELDS}
    payload["contract_id"] = contract.PROFILE_SELECTION_ATTESTATION_CONTRACT_ID
    payload["proof_id"] = contract._proof_id(payload)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_selection_attestation(payload)
    payload = deepcopy(proof.payload)
    payload["contract_id"] = contract.PROFILE_SELECTION_ATTESTATION_CONTRACT_ID
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_selection_attestation(payload)


@pytest.mark.asyncio
async def test_registry_current_recheck_recomputes_desired_lane_and_rejects_drift(monkeypatch):
    proof = _proof()
    computed = _computed()
    desired_read = AsyncMock(return_value=computed)
    monkeypatch.setattr(selection, "_compute_desired_selection", desired_read)
    monkeypatch.setattr(selection, "configured_node_id", lambda: "synthetic-node")
    monkeypatch.setattr(
        selection,
        "_registered_proof",
        AsyncMock(return_value={"proof_id": proof.proof_id, "identity_json": computed.identity_payload}),
    )
    monkeypatch.setattr(
        selection,
        "_latest_registered_observation",
        AsyncMock(
            return_value={
                "input_identity_digest": selection._input_identity_digest(computed.identity_payload),
                "payload_json": proof.payload,
            }
        ),
    )
    await selection._assert_registered_current_in_transaction(proof, {})
    assert desired_read.call_args.kwargs["lock_selection"] is True
    assert desired_read.call_args.kwargs["desired_selection"] == _desired()
    desired_read.side_effect = contract.ProviderDirectoryProfileSelectionDrift("changed")
    with pytest.raises(contract.ProviderDirectoryProfileSelectionStale):
        await selection._assert_registered_current_in_transaction(proof, {})


def test_desired_result_and_capacity_date_binding(monkeypatch):
    proof = _proof()
    execution = contract.ProviderDirectoryProfileExecution(proof, 1)
    selection_result_map = contract.profile_selection_result(
        execution,
        profile_generation_id="synthetic-profile",
        profile_as_of="2026-09-29",
        profile_rows=2,
        profile_source_evidence_rows=3,
    )
    assert selection_result_map["contract_id"] == contract.PROFILE_SELECTION_DESIRED_RESULT_CONTRACT_ID
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.profile_selection_result(
            execution,
            profile_generation_id="synthetic-profile",
            profile_as_of="2026-09-30",
            profile_rows=2,
            profile_source_evidence_rows=3,
        )
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")
    task_map = {
        **contract._GLOBAL_PROFILE_PARAMS,
        "provider_directory_profile_generation": 1,
        "provider_directory_profile_selection_attestation": proof.payload,
        "provider_directory_profile_capacity_attestation": {},
    }
    assert contract.validated_profile_execution(task_map).attestation.desired_profile_as_of == "2026-09-29"
    task_map["profile_as_of"] = "2026-09-30"
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_execution(task_map)


def test_capacity_preflight_binds_desired_proof_and_date(monkeypatch):
    from process import provider_directory_profile_capacity_preflight_contract as preflight

    from .test_provider_directory_profile_capacity_preflight import _request_payload

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")
    request_maps = []
    for day in ("2026-09-29", "2026-09-30"):
        payload = _request_payload()
        payload["profile_execution"]["provider_directory_profile_selection_attestation"] = _proof(
            _desired(current=True, day=day)
        ).payload
        request_maps.append(preflight.validated_capacity_preflight_request(payload))
    first_identity = preflight.profile_execution_identity_payload(request_maps[0])
    later_identity = preflight.profile_execution_identity_payload(request_maps[1])
    assert first_identity.keys() == later_identity.keys()
    for name in ("selection_proof_id", "selection_fingerprint", "profile_input_digest"):
        assert first_identity[name] != later_identity[name]
    assert request_maps[0].request_sha256 != request_maps[1].request_sha256
