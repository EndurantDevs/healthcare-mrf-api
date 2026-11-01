# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact dated ordinary and CMS selections, with legacy-lane safety."""

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


def _ordinary_desired(*, day="2026-09-29"):
    return {"desired_profile_as_of": day, "desired_cms_dataset": None, "expected_cms_incumbent": None}


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
    candidate = desired["desired_cms_dataset"]
    return catalog_map, sources, current_rows, _rows(candidate) if candidate is not None else None


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


@pytest.mark.parametrize("ordinary", [False, True])
def test_same_byte_current_date_refresh_changes_all_desired_identity_hashes(ordinary):
    earlier = _proof(_ordinary_desired() if ordinary else _desired(current=True))
    later = _proof(_ordinary_desired(day="2026-09-30") if ordinary else _desired(current=True, day="2026-09-30"))
    assert earlier.pairs == later.pairs
    assert earlier.desired_cms_dataset == earlier.expected_cms_incumbent
    for name in ("selection_fingerprint", "profile_input_digest", "proof_id"):
        assert getattr(earlier, name) != getattr(later, name)


def test_ordinary_date_preserves_the_complete_current_vector():
    desired = _ordinary_desired()
    catalog, source_rows, dataset_rows, _candidate = _selection_rows(desired)
    catalog["items"].append({"runnable": True, "profile_enabled": True, "source_ids": ["pdfhir_synthetic_second"]})
    source_rows.append(
        {"source_id": "pdfhir_synthetic_second", "endpoint_id": "second-endpoint", "org_name": "Second payer"}
    )
    dataset_rows.append(
        _rows(
            {
                **_pair("second-current", current=True),
                "source_id": "pdfhir_synthetic_second",
                "endpoint_id": "second-endpoint",
            }
        )
    )
    current = snapshot._computed_selection_from_rows(
        catalog, node_id="synthetic-node", source_rows=source_rows, dataset_rows=dataset_rows
    )
    dated = snapshot._computed_desired_selection_from_rows(
        catalog,
        node_id="synthetic-node",
        source_rows=source_rows,
        dataset_rows=dataset_rows,
        desired_dataset_row=None,
        desired_selection=desired,
    )
    assert len(dated.request_projection) == 2
    assert dated.request_projection == current.request_projection
    for name in ("pairs", "source_context_digest", "operation"):
        assert dated.identity_payload[name] == current.identity_payload[name]
    assert _proof(desired).desired_cms_dataset is None
    assert selection._validated_request(selection._expected_request(dated, "synthetic-node")) == {
        "contract_id": contract.PROFILE_SELECTION_DESIRED_REQUEST_CONTRACT_ID,
        "node_id": "synthetic-node",
        "catalog_digest": catalog["catalog_digest"],
        "selection_fingerprint": dated.identity_payload["selection_fingerprint"],
        "datasets": list(current.request_projection),
        **desired,
    }


@pytest.mark.parametrize(
    "change",
    [
        {"desired_profile_as_of": None},
        {"desired_profile_as_of": "2026-9-29"},
        {"desired_profile_as_of": "2026-02-30"},
        {"expected_cms_incumbent": _pair("cms-current", current=True)},
    ],
)
def test_ordinary_date_requires_a_canonical_date_without_an_incumbent(change):
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract._validated_desired_selection({**_ordinary_desired(), **change})


@pytest.mark.parametrize("mutation", ["cms", "unpublished", "duplicate_source", "date"])
def test_ordinary_dated_proof_cannot_bypass_selection_guards(mutation):
    payload = deepcopy(_proof(_ordinary_desired()).payload)
    if mutation == "cms":
        payload["pairs"].insert(0, _pair("cms-current", current=True))
    elif mutation == "unpublished":
        payload["pairs"][0].update(publication_status="validated", is_current=False)
    elif mutation == "duplicate_source":
        payload["pairs"].append({**payload["pairs"][0], "dataset_id": "payer-other"})
    else:
        payload["desired_profile_as_of"] = "2026-09-30"
    payload["proof_id"] = contract._proof_id(payload)
    with pytest.raises(contract.ProviderDirectoryProfileSelectionError):
        contract.validated_profile_selection_attestation(payload)


def test_ordinary_dated_snapshot_refuses_a_current_cms_pair():
    catalog, sources, rows, _candidate = _selection_rows()
    with pytest.raises(contract.ProviderDirectoryProfileSelectionDrift, match="cms_requires_desired_dataset"):
        snapshot._computed_desired_selection_from_rows(
            catalog,
            node_id="synthetic-node",
            source_rows=sources,
            dataset_rows=rows,
            desired_dataset_row=None,
            desired_selection=_ordinary_desired(),
        )


@pytest.mark.parametrize("exact_readiness", [False, True])
@pytest.mark.asyncio
async def test_ordinary_dated_read_preserves_current_readiness_and_locks(monkeypatch, exact_readiness):
    desired = _ordinary_desired()
    catalog, sources, rows, _candidate = _selection_rows(desired)
    lock = AsyncMock()
    dataset_read = AsyncMock(return_value=rows)
    cms_read = AsyncMock(side_effect=AssertionError("ordinary selection must not read a CMS candidate"))
    monkeypatch.setattr(snapshot, "_lock_profile_selection_tables", lock)
    monkeypatch.setattr(snapshot, "_selection_source_rows", AsyncMock(return_value=sources))
    monkeypatch.setattr(snapshot, "_selection_dataset_rows", dataset_read)
    monkeypatch.setattr(snapshot.db, "first", cms_read)
    monkeypatch.setattr(snapshot.db, "all", cms_read)
    result = await snapshot._compute_desired_selection(
        catalog,
        node_id="synthetic-node",
        lock_selection=exact_readiness,
        desired_selection=desired,
        exact_readiness=exact_readiness,
    )
    assert result.request_projection == _computed(desired).request_projection
    dataset_read.assert_awaited_once_with(exact_readiness=exact_readiness)
    assert lock.await_count == int(exact_readiness)
    cms_read.assert_not_awaited()


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


@pytest.mark.parametrize("desired", [_desired(), _ordinary_desired()])
@pytest.mark.asyncio
async def test_registry_current_recheck_recomputes_desired_lane_and_rejects_drift(monkeypatch, desired):
    proof = _proof(desired)
    computed = _computed(desired)
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
    assert desired_read.call_args.kwargs["desired_selection"] == desired
    desired_read.side_effect = contract.ProviderDirectoryProfileSelectionDrift("changed")
    with pytest.raises(contract.ProviderDirectoryProfileSelectionStale):
        await selection._assert_registered_current_in_transaction(proof, {})


@pytest.mark.parametrize("desired", [_desired(), _ordinary_desired()])
def test_desired_result_and_capacity_date_binding(monkeypatch, desired):
    proof = _proof(desired)
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


@pytest.mark.parametrize("ordinary", [False, True])
def test_capacity_preflight_binds_desired_proof_and_date(monkeypatch, ordinary):
    from process import provider_directory_profile_capacity_preflight_contract as preflight

    from .test_provider_directory_profile_capacity_preflight import _request_payload

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")
    request_maps = []
    for day in ("2026-09-29", "2026-09-30"):
        payload = _request_payload()
        payload["profile_execution"]["provider_directory_profile_selection_attestation"] = _proof(
            _ordinary_desired(day=day) if ordinary else _desired(current=True, day=day)
        ).payload
        request_maps.append(preflight.validated_capacity_preflight_request(payload))
    first_identity = preflight.profile_execution_identity_payload(request_maps[0])
    later_identity = preflight.profile_execution_identity_payload(request_maps[1])
    assert first_identity.keys() == later_identity.keys()
    for name in ("selection_proof_id", "selection_fingerprint", "profile_input_digest"):
        assert first_identity[name] != later_identity[name]
    assert request_maps[0].request_sha256 != request_maps[1].request_sha256


@pytest.mark.parametrize("mode", ["publish", "purge", "legacy"])
def test_initial_preflight_requires_a_dated_ordinary_publish(monkeypatch, mode):
    from process import provider_directory_profile_capacity_preflight_contract as preflight
    from process import provider_directory_profile_initial_contract as initial

    from .test_provider_directory_profile_capacity_preflight import _request_payload

    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")
    request_map = _request_payload()
    request_map.update(contract_id=initial.REQUEST_CONTRACT, profile_materialization=initial.MATERIALIZATION)
    desired = _ordinary_desired()
    catalog, source_rows, dataset_rows, _candidate = _selection_rows(desired)
    if mode == "legacy":
        computed = snapshot._computed_selection_from_rows(
            catalog, node_id="synthetic-node", source_rows=source_rows, dataset_rows=dataset_rows
        )
    else:
        computed = snapshot._computed_desired_selection_from_rows(
            catalog,
            node_id="synthetic-node",
            source_rows=source_rows,
            dataset_rows=[] if mode == "purge" else dataset_rows,
            desired_dataset_row=None,
            desired_selection=desired,
        )
    proof_by_field = {**computed.identity_payload, "authority_revision": 1}
    proof_by_field["proof_id"] = contract._proof_id(proof_by_field)
    request_map["profile_execution"]["provider_directory_profile_selection_attestation"] = proof_by_field
    if mode != "publish":
        with pytest.raises(preflight.ProviderDirectoryProfileCapacityPreflightError, match="explicit_date_required"):
            preflight.validated_capacity_preflight_request(request_map)
        return
    request = preflight.validated_capacity_preflight_request(request_map)
    assert request.execution.attestation.desired_profile_as_of == "2026-09-29"
    assert request.execution.attestation.desired_cms_dataset is None
    assert initial.initial_profile_for_request(request)
    assert preflight.profile_execution_identity_payload(request)["materialization_mode"] == "full_swap"
