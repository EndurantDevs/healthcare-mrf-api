# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Versioned semantic-proof coverage for volatile FHIR observations."""

from __future__ import annotations

import datetime as dt
import importlib
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryPractitioner
from process import provider_directory_dataset_rehydrate as rehydrate
from process import provider_directory_proof_store as proof_store
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID,
    ProviderDirectoryProofStoreError,
)
from process.provider_directory_resource_hash import (
    DEFAULT_RESOURCE_HASH_CONTRACT,
    LEGACY_RESOURCE_HASH_CONTRACT,
    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    canonical_practitioner_payload,
    is_resource_payload_hash_match,
    merge_practitioner_semantic_payloads,
    persisted_resource_hash_contract,
    resource_payload_sha256,
    semantic_resource_content_hash_payload,
)
from tests.test_provider_directory_proof_store import (
    DATASET_ID as PROOF_DATASET_ID,
    ENDPOINT_ID as PROOF_ENDPOINT_ID,
    ROOT_RUN_ID as PROOF_ROOT_RUN_ID,
    SOURCE_IDS as PROOF_SOURCE_IDS,
    _MemoryProofConnection,
    _dataset_resource as _legacy_dataset_resource,
    _persist_rows_by_resource,
    _sample_dataset_resources,
)


importer = importlib.import_module("process.provider_directory_fhir")
PROJECTION_AS_OF = "2026-08-09"
PROOF_RESOURCE_SCOPE = ["Practitioner"]


def _observation(
    *,
    last_updated: str | None = "2026-08-09T13:14:52.638Z",
    version_id: str = "7",
    full_name: str = "Example Practitioner",
) -> dict[str, object]:
    """Return one mapped resource with retained FHIR provenance."""

    fhir_meta = {
        "versionId": version_id,
        "source": "https://directory.example.test/fhir",
    }
    if last_updated is not None:
        fhir_meta["lastUpdated"] = last_updated
    name_parts = full_name.split()
    family_name = name_parts[-1]
    given_names = name_parts[:-1]
    return {
        "source_id": "source-a",
        "resource_id": "practitioner-1",
        "full_name": full_name,
        "family_name": family_name,
        "given_names": given_names,
        "names": [
            {
                "use": "official",
                "text": full_name,
                "family": family_name,
                "given": given_names,
            }
        ],
        "npi": 1215387113,
        "resource_url": (
            "https://directory.example.test/fhir/Practitioner/practitioner-1"
        ),
        "fhir_self_url": (
            "https://directory.example.test/fhir/Practitioner/practitioner-1"
        ),
        "fhir_fetch_url": (
            "https://directory.example.test/fhir/Practitioner?page=1"
        ),
        "fhir_fetch_mode": "rest_bundle",
        "fhir_meta": fhir_meta,
    }


def _dataset_row(
    observation_by_field: dict[str, object],
    resource_hash_contract: str = DEFAULT_RESOURCE_HASH_CONTRACT,
) -> dict[str, object]:
    return importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitioner,
        [observation_by_field],
        dataset_id="dataset-1",
        resource_hash_contract=resource_hash_contract,
    )[0]


def _canonical_row(
    observation_by_field: dict[str, object],
    resource_hash_contract: str = DEFAULT_RESOURCE_HASH_CONTRACT,
) -> dict[str, object]:
    return importer._canonical_resource_rows(
        ProviderDirectoryPractitioner,
        [observation_by_field],
        canonical_api_base="https://directory.example.test/fhir",
        run_id="run-1",
        resource_hash_contract=resource_hash_contract,
    )[0]


def _merged_resource_count(tmp_path, dataset_rows) -> int:
    record_directory = tmp_path / "records"
    npi_directory = tmp_path / "npis"
    record_directory.mkdir(parents=True)
    npi_directory.mkdir()
    record_spool = proof_store._RecordSpool(record_directory)
    for dataset_row_by_field in dataset_rows:
        record_spool.add(
            json.dumps(
                proof_store._proof_record(
                    dataset_row_by_field,
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
                ),
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
    summary = proof_store._merged_resource_proof(
        record_spool,
        proof_store._RecordSpool(npi_directory),
    )
    return summary[1]


def test_v3_excludes_only_volatile_fhir_observation_time():
    first_observation = _observation()
    later_observation = _observation(
        last_updated="2026-08-09T13:17:34.202Z"
    )

    first_v1 = _dataset_row(
        first_observation,
        LEGACY_RESOURCE_HASH_CONTRACT,
    )
    later_v1 = _dataset_row(
        later_observation,
        LEGACY_RESOURCE_HASH_CONTRACT,
    )
    first_v2 = _dataset_row(
        first_observation,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    later_v2 = _dataset_row(
        later_observation,
        TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    )
    first_v3 = _dataset_row(
        first_observation,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    later_v3 = _dataset_row(
        later_observation,
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )

    assert first_v1["payload_hash"] != later_v1["payload_hash"]
    assert first_v2["payload_hash"] != later_v2["payload_hash"]
    assert first_v3["payload_hash"] == later_v3["payload_hash"]
    assert first_v3["payload_json"]["fhir_meta"]["lastUpdated"] == (
        "2026-08-09T13:14:52.638Z"
    )
    assert later_v3["payload_json"]["fhir_meta"]["lastUpdated"] == (
        "2026-08-09T13:17:34.202Z"
    )


def test_v3_hash_view_preserves_other_meta_without_mutating_payload():
    observation_by_field = _observation()
    mapped_payload = importer._canonical_resource_payload(
        observation_by_field
    )

    hash_payload = semantic_resource_content_hash_payload(mapped_payload)

    assert "lastUpdated" not in hash_payload["fhir_meta"]
    assert hash_payload["fhir_meta"]["versionId"] == "7"
    assert hash_payload["fhir_meta"]["source"] == (
        "https://directory.example.test/fhir"
    )
    assert mapped_payload["fhir_meta"]["lastUpdated"] == (
        "2026-08-09T13:14:52.638Z"
    )
    assert semantic_resource_content_hash_payload(
        {**mapped_payload, "fhir_meta": {"lastUpdated": "later"}}
    )["fhir_meta"] is None
    assert semantic_resource_content_hash_payload(
        {**mapped_payload, "fhir_meta": None}
    )["fhir_meta"] is None


def test_v3_keeps_fhir_version_and_semantic_changes_in_proof():
    baseline = _dataset_row(_observation())
    changed_version = _dataset_row(_observation(version_id="8"))
    changed_name = _dataset_row(
        _observation(full_name="Changed Practitioner")
    )

    assert changed_version["payload_hash"] != baseline["payload_hash"]
    assert changed_name["payload_hash"] != baseline["payload_hash"]


def test_v3_merges_time_and_name_variants_but_rejects_identity_drift(
    tmp_path,
):
    first_row = _dataset_row(_observation())
    later_row = _dataset_row(
        _observation(last_updated="2026-08-09T13:17:34.202Z")
    )
    name_variant = _dataset_row(
        _observation(full_name="Example Alternate")
    )
    identity_drift = _observation()
    identity_drift["npi"] = 1000000001
    changed_row = _dataset_row(
        identity_drift
    )

    assert _merged_resource_count(tmp_path, [first_row, later_row]) == 1
    assert (
        _merged_resource_count(
            tmp_path / "name-variant",
            [first_row, name_variant],
        )
        == 1
    )
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="proof shards conflict",
    ):
        _merged_resource_count(
            tmp_path / "semantic-drift",
            [first_row, changed_row],
        )


def test_v3_practitioner_name_union_is_lossless_and_permutation_stable():
    first = importer._canonical_resource_payload(
        _observation(full_name="Example Practitioner")
    )
    second = importer._canonical_resource_payload(
        _observation(full_name="Example Alternate")
    )

    forward = merge_practitioner_semantic_payloads(first, second)
    reverse = merge_practitioner_semantic_payloads(second, first)

    assert forward == reverse
    assert len(forward["names"]) == 2
    assert set(
        json.dumps(name, sort_keys=True) for name in forward["names"]
    ) == {
        json.dumps(first["names"][0], sort_keys=True),
        json.dumps(second["names"][0], sort_keys=True),
    }
    assert forward["full_name"] in {
        "Example Practitioner",
        "Example Alternate",
    }


def test_v3_name_union_selects_one_complete_provenance_observation():
    first = importer._canonical_resource_payload(_observation())
    second = importer._canonical_resource_payload(
        _observation(last_updated="2026-08-09T13:17:34.202Z")
    )
    first.update(
        resource_url="https://z.example.test/resource",
        fhir_self_url="https://a.example.test/self",
        fhir_fetch_url="https://z.example.test/page",
        fhir_fetch_mode="rest_bundle",
    )
    second.update(
        resource_url="https://a.example.test/resource",
        fhir_self_url="https://z.example.test/self",
        fhir_fetch_url="https://a.example.test/page",
        fhir_fetch_mode="graphql",
    )

    merged = merge_practitioner_semantic_payloads(first, second)
    provenance_fields = (
        "resource_url",
        "fhir_self_url",
        "fhir_fetch_url",
        "fhir_fetch_mode",
    )
    observed_provenance = tuple(
        merged.get(field_name) for field_name in provenance_fields
    ) + (merged["fhir_meta"]["lastUpdated"],)
    source_provenance = {
        tuple(payload.get(field_name) for field_name in provenance_fields)
        + (payload["fhir_meta"]["lastUpdated"],)
        for payload in (first, second)
    }

    assert observed_provenance in source_provenance
    assert merge_practitioner_semantic_payloads(second, first) == merged


def test_v3_within_batch_name_union_precedes_identity_deduplication():
    forward = importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitioner,
        [
            _observation(full_name="Example Practitioner"),
            _observation(full_name="Example Alternate"),
        ],
        dataset_id="dataset-1",
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    reverse = importer._endpoint_dataset_resource_rows(
        ProviderDirectoryPractitioner,
        [
            _observation(full_name="Example Alternate"),
            _observation(full_name="Example Practitioner"),
        ],
        dataset_id="dataset-1",
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )

    assert forward == reverse
    assert len(forward) == 1
    assert len(forward[0]["payload_json"]["names"]) == 2


def test_v3_union_rejects_non_name_drift_and_projection_tampering():
    baseline = importer._canonical_resource_payload(_observation())
    changed = dict(baseline)
    changed["active"] = True

    with pytest.raises(
        ValueError,
        match="practitioner_identity_payload_conflict",
    ):
        merge_practitioner_semantic_payloads(baseline, changed)

    canonical = canonical_practitioner_payload(baseline)
    stored_hash = _dataset_row(_observation())["payload_hash"]
    tampered = {**canonical, "full_name": "Unobserved Projection"}
    assert not is_resource_payload_hash_match(tampered, stored_hash)

    bool_payload = {**baseline, "active": True}
    integer_payload = {**baseline, "active": 1}
    with pytest.raises(
        ValueError,
        match="practitioner_identity_payload_conflict",
    ):
        merge_practitioner_semantic_payloads(
            bool_payload,
            integer_payload,
        )


def test_v2_hash_remains_readable_after_v3_becomes_default():
    observation_by_field = _observation()
    mapped_payload = importer._canonical_resource_payload(
        observation_by_field
    )
    stored_v2_hash = resource_payload_sha256(mapped_payload)
    default_canonical = _canonical_row(observation_by_field)

    assert DEFAULT_RESOURCE_HASH_CONTRACT == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    assert stored_v2_hash != default_canonical["payload_hash"]
    importer._assert_endpoint_dataset_resource_payload_hash(
        {"payload_json": mapped_payload, "payload_hash": stored_v2_hash}
    )
    assert (
        rehydrate._validate_payload(
            ProviderDirectoryPractitioner,
            "practitioner-1",
            stored_v2_hash,
            mapped_payload,
            resource_hash_contract=(
                TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            ),
        )
        is None
    )


def test_contract_reader_keeps_markerless_v1_and_explicit_v2():
    assert persisted_resource_hash_contract(None) == (
        LEGACY_RESOURCE_HASH_CONTRACT
    )
    assert persisted_resource_hash_contract(
        {"resource_hash_contract": TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT}
    ) == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
    assert persisted_resource_hash_contract(
        {"resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT}
    ) == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT


def test_v3_projection_date_is_created_once_and_reused(monkeypatch):
    selection = importer.EndpointDatasetCandidateSelection(
        dataset_id="dataset-1",
        acquisition_root_run_id="root-1",
        previous_dataset_id=None,
        reused_from_checkpoint=False,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    monkeypatch.setattr(
        importer,
        "_now",
        lambda: dt.datetime(2026, 8, 9, tzinfo=dt.UTC),
    )
    fresh = importer._selection_with_semantic_projection_as_of(
        selection,
        {},
        ("Practitioner",),
    )
    assert fresh.semantic_projection_as_of == PROJECTION_AS_OF
    assert fresh.proof_resource_scope == ("Practitioner",)

    monkeypatch.setattr(
        importer,
        "_now",
        lambda: dt.datetime(2026, 8, 10, tzinfo=dt.UTC),
    )
    persisted_state = {
        "publication_metadata_json": {
            "resource_hash_contract": (
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            "semantic_projection_as_of": PROJECTION_AS_OF,
            "selected_resources": ["Practitioner"],
            "proof_resource_scope": PROOF_RESOURCE_SCOPE,
        }
    }
    resumed = importer._selection_with_semantic_projection_as_of(
        selection,
        persisted_state,
        ("Practitioner",),
    )
    assert resumed.semantic_projection_as_of == PROJECTION_AS_OF

    for invalid_value in (None, " 2026-08-09", "2026-8-9", dt.date(2026, 8, 9)):
        invalid_state = {
            "publication_metadata_json": {
                "resource_hash_contract": (
                    SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                ),
                "semantic_projection_as_of": invalid_value,
                "selected_resources": ["Practitioner"],
                "proof_resource_scope": PROOF_RESOURCE_SCOPE,
            }
        }
        with pytest.raises(
            RuntimeError,
            match="semantic_projection_as_of",
        ):
            importer._selection_with_semantic_projection_as_of(
                selection,
                invalid_state,
                ("Practitioner",),
            )


def test_twin_successor_inherits_and_fences_projection_date(monkeypatch):
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-successor",
        acquisition_root_run_id="root-successor",
        source_ids=("source-1",),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
        import_run_id="root-successor",
        previous_dataset_id=None,
        requires_twin_root_verification=True,
        verification_campaign_id="campaign-1",
        verification_source_scope_hash="scope-1",
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-10",
        proof_resource_scope=("Practitioner",),
    )
    baseline = {
        "dataset_id": "dataset-baseline",
        "publication_metadata_json": {
            "resource_hash_contract": (
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            "semantic_projection_as_of": PROJECTION_AS_OF,
            "selected_resources": ["Practitioner"],
            "proof_resource_scope": PROOF_RESOURCE_SCOPE,
        },
    }
    monkeypatch.setattr(
        importer,
        "_compatible_twin_root_baseline",
        lambda _candidate, _state: baseline,
    )

    admitted = importer._candidate_with_locked_twin_root_admission(
        candidate,
        baseline,
    )
    assert admitted.semantic_projection_as_of == PROJECTION_AS_OF

    resumed = importer.replace(
        candidate,
        verification_role=importer.TWIN_ROOT_VERIFICATION_CANDIDATE_ROLE,
        verification_baseline_dataset_id="dataset-baseline",
    )
    with pytest.raises(RuntimeError, match="baseline_incompatible"):
        importer._candidate_with_locked_twin_root_admission(
            resumed,
            baseline,
        )


@pytest.mark.parametrize(
    ("baseline_contract", "baseline_projection_date", "baseline_scope"),
    (
        (LEGACY_RESOURCE_HASH_CONTRACT, None, None),
        (TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT, None, None),
        (
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
            PROJECTION_AS_OF,
            ("PractitionerRole",),
        ),
    ),
)
def test_fresh_twin_successor_inherits_persisted_baseline_contract(
    monkeypatch,
    baseline_contract,
    baseline_projection_date,
    baseline_scope,
):
    selected_resources = ("PractitionerRole",)
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-successor",
        acquisition_root_run_id="root-successor",
        source_ids=("source-1",),
        selected_resources=selected_resources,
        expected_resources=selected_resources,
        import_run_id="root-successor",
        previous_dataset_id=None,
        requires_twin_root_verification=True,
        verification_campaign_id="campaign-1",
        verification_source_scope_hash="scope-1",
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of="2026-08-10",
        proof_resource_scope=importer._provider_directory_proof_resource_scope(
            selected_resources
        ),
    )
    metadata = {
        "source_ids": ["source-1"],
        "selected_resources": list(selected_resources),
        "expected_resources": list(selected_resources),
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: "campaign-1",
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: "scope-1",
    }
    if baseline_contract != LEGACY_RESOURCE_HASH_CONTRACT:
        metadata["resource_hash_contract"] = baseline_contract
    if baseline_projection_date is not None:
        metadata["semantic_projection_as_of"] = baseline_projection_date
    if baseline_scope is not None:
        metadata["proof_resource_scope"] = list(baseline_scope)
    baseline = {
        "dataset_id": "dataset-baseline",
        "acquisition_root_run_id": "root-baseline",
        "status": importer.ENDPOINT_DATASET_VERIFICATION_BASELINE,
        "verification_baseline_count": 1,
        "completion_proof_required_version": None,
        "publication_metadata_json": metadata,
    }
    baseline_proof = {
        "endpoint_id": "endpoint-1",
        "source_ids": ["source-1"],
        "selected_resources": list(selected_resources),
        "expected_resources": list(selected_resources),
        importer.TWIN_ROOT_VERIFICATION_CAMPAIGN_KEY: "campaign-1",
        importer.TWIN_ROOT_VERIFICATION_SOURCE_SCOPE_KEY: "scope-1",
    }
    if baseline_projection_date is not None:
        baseline_proof["semantic_projection_as_of"] = (
            baseline_projection_date
        )
    if baseline_scope is not None:
        baseline_proof["proof_resource_scope"] = list(baseline_scope)
    monkeypatch.setattr(
        importer,
        "_twin_root_baseline_proof",
        lambda _dataset_map: baseline_proof,
    )

    admitted = importer._candidate_with_locked_twin_root_admission(
        candidate,
        baseline,
    )

    assert admitted.resource_hash_contract == baseline_contract
    assert admitted.semantic_projection_as_of == baseline_projection_date
    assert admitted.proof_resource_scope == baseline_scope


@pytest.mark.asyncio
async def test_finalization_lock_rejects_projection_date_tamper():
    candidate = importer.EndpointDatasetCandidate(
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        acquisition_root_run_id="root-1",
        source_ids=("source-1",),
        selected_resources=("Practitioner",),
        expected_resources=("Practitioner",),
        import_run_id="root-1",
        previous_dataset_id=None,
        resource_hash_contract=SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        semantic_projection_as_of=PROJECTION_AS_OF,
        proof_resource_scope=("Practitioner",),
    )
    connection = SimpleNamespace(
        first=AsyncMock(
            side_effect=[
                {"endpoint_id": "endpoint-1"},
                {
                    "dataset_id": "dataset-1",
                    "acquisition_root_run_id": "root-1",
                    "is_current": False,
                    "status": importer.ENDPOINT_DATASET_ACQUIRING,
                    "previous_dataset_id": None,
                    "completion_proof_required_version": None,
                    "completion_proof_json": None,
                    "completion_proof_sha256": None,
                    "publication_metadata_json": {
                        "resource_hash_contract": (
                            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                        ),
                        "semantic_projection_as_of": "2026-08-10",
                        "selected_resources": ["Practitioner"],
                        "proof_resource_scope": PROOF_RESOURCE_SCOPE,
                    },
                },
            ]
        )
    )

    with pytest.raises(
        RuntimeError,
        match="candidate_stale",
    ):
        await importer._lock_endpoint_dataset_for_validation(
            connection,
            candidate,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "projection_fields",
    [
        {"age_years": 40, "age_as_of": None},
        {"age_years": 40, "age_as_of": "2026-08-10"},
        {
            "years_of_practice": 10,
            "years_of_practice_as_of": PROJECTION_AS_OF,
            "years_of_practice_basis": None,
            "years_of_practice_start_date": "2016-08-09",
        },
    ],
)
async def test_v3_accumulator_rejects_partial_or_wrong_date_projection(
    projection_fields,
):
    observation = _observation()
    observation.update(projection_fields)
    incoming_row = _dataset_row(observation)
    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "dataset_id": "dataset-1",
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
                "is_current": False,
                "publication_metadata_json": {
                    "selected_resources": ["Practitioner"],
                    "resource_hash_contract": (
                        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
                    ),
                    "semantic_projection_as_of": PROJECTION_AS_OF,
                    "proof_resource_scope": PROOF_RESOURCE_SCOPE,
                },
            }
        ),
        all=AsyncMock(return_value=[]),
        scalar=AsyncMock(return_value=None),
    )

    with pytest.raises(ValueError, match="semantic_.*projection"):
        await importer._accumulated_endpoint_dataset_rows(
            connection,
            [incoming_row],
            dataset_id="dataset-1",
            resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            semantic_projection_as_of=PROJECTION_AS_OF,
        )
    connection.all.assert_not_awaited()
    connection.scalar.assert_awaited_once()


@pytest.mark.asyncio
async def test_accumulator_rejects_resource_outside_parent_scope(
):
    resource_hash_contract = SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    incoming_row = _dataset_row(_observation(), resource_hash_contract)
    semantic_projection_as_of = (
        PROJECTION_AS_OF
        if resource_hash_contract == SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        else None
    )
    connection = SimpleNamespace(
        first=AsyncMock(
            return_value={
                "dataset_id": "dataset-1",
                "status": importer.ENDPOINT_DATASET_ACQUIRING,
                "is_current": False,
                "publication_metadata_json": {
                    "selected_resources": ["Organization"],
                    "resource_hash_contract": resource_hash_contract,
                    "proof_resource_scope": ["Endpoint", "Organization"],
                    **(
                        {
                            "semantic_projection_as_of": (
                                semantic_projection_as_of
                            )
                        }
                        if semantic_projection_as_of is not None
                        else {}
                    ),
                },
            }
        ),
        all=AsyncMock(return_value=[]),
        scalar=AsyncMock(return_value=None),
    )

    with pytest.raises(RuntimeError, match="resource_scope_changed"):
        await importer._accumulated_endpoint_dataset_rows(
            connection,
            [incoming_row],
            dataset_id="dataset-1",
            resource_hash_contract=(
                resource_hash_contract
            ),
            semantic_projection_as_of=semantic_projection_as_of,
        )
    connection.all.assert_not_awaited()
    connection.scalar.assert_not_awaited()


def _semantic_proof_connection() -> _MemoryProofConnection:
    connection = _MemoryProofConnection()
    connection.parent["publication_metadata_json"] = {
        "source_ids": PROOF_SOURCE_IDS,
        "selected_resources": ["Practitioner"],
        "proof_resource_scope": PROOF_RESOURCE_SCOPE,
        "resource_hash_contract": SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        "semantic_projection_as_of": PROJECTION_AS_OF,
    }
    return connection


def _semantic_proof_row(**observation_changes) -> dict[str, object]:
    return {
        **_dataset_row(_observation(**observation_changes)),
        "dataset_id": PROOF_DATASET_ID,
    }


async def _semantic_stored_proof(connection: _MemoryProofConnection):
    return await proof_store.build_stored_dataset_proof(
        connection,
        "mrf",
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=["Practitioner"],
        proof_resource_scope=PROOF_RESOURCE_SCOPE,
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
        expected_semantic_projection_as_of=PROJECTION_AS_OF,
    )


@pytest.mark.asyncio
async def test_v3_sealed_proof_binds_contract_and_projection_date():
    connection = _semantic_proof_connection()
    await proof_store.persist_dataset_proof_shard(
        connection,
        "mrf",
        [_semantic_proof_row()],
        dataset_id=PROOF_DATASET_ID,
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
    )

    stored_proof = await _semantic_stored_proof(connection)
    sealed = stored_proof.metadata

    assert sealed["contract_id"] == (
        PROVIDER_DIRECTORY_SEMANTIC_CONTENT_PROOF_CONTRACT_ID
    )
    assert sealed["resource_hash_contract"] == (
        SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
    )
    assert sealed["semantic_projection_as_of"] == PROJECTION_AS_OF
    assert PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY == (
        "provider_directory_content_proof_v1"
    )
    assert proof_store.validate_stored_dataset_proof_metadata(
        sealed,
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=["Practitioner"],
        proof_resource_scope=PROOF_RESOURCE_SCOPE,
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
        expected_semantic_projection_as_of=PROJECTION_AS_OF,
    ) == sealed

    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="projection date changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            sealed,
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            selected_resources=["Practitioner"],
            proof_resource_scope=PROOF_RESOURCE_SCOPE,
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of="2026-08-10",
        )


@pytest.mark.asyncio
async def test_proof_contracts_reject_cross_version_and_mixed_shards():
    semantic_connection = _semantic_proof_connection()
    await proof_store.persist_dataset_proof_shard(
        semantic_connection,
        "mrf",
        [_semantic_proof_row()],
        dataset_id=PROOF_DATASET_ID,
        expected_resource_hash_contract=(
            SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
        ),
    )
    semantic_proof = await _semantic_stored_proof(semantic_connection)
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="contract changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            semantic_proof.metadata,
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            selected_resources=["Practitioner"],
            proof_resource_scope=PROOF_RESOURCE_SCOPE,
            expected_resource_hash_contract=(
                TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            ),
        )

    legacy_descriptor, legacy_payload = proof_store.build_dataset_proof_shard(
        [
            _legacy_dataset_resource(
                "Organization",
                "organization-1",
                {"resource_id": "organization-1", "name": "Example"},
            )
        ],
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    )
    await semantic_connection.status(
        "INSERT INTO provider_directory_dataset_proof_shard",
        **proof_store._proof_shard_insert_params(
            legacy_descriptor,
            legacy_payload,
        ),
    )
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="contract changed",
    ):
        await proof_store.build_stored_dataset_proof(
            semantic_connection,
            "mrf",
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            selected_resources=["Practitioner"],
            proof_resource_scope=["Organization", "Practitioner"],
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=PROJECTION_AS_OF,
        )


@pytest.mark.asyncio
async def test_historical_proof_shape_remains_readable_for_v1_and_v2():
    legacy_connection = _MemoryProofConnection()
    await _persist_rows_by_resource(
        legacy_connection,
        _sample_dataset_resources(),
    )
    legacy_proof = await proof_store.build_stored_dataset_proof(
        legacy_connection,
        "mrf",
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=[
            "InsurancePlan",
            "Location",
            "Organization",
            "OrganizationAffiliation",
            "Practitioner",
        ],
        expected_resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    )
    proof_store.validate_stored_dataset_proof_metadata(
        legacy_proof.metadata,
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=[
            "InsurancePlan",
            "Location",
            "Organization",
            "OrganizationAffiliation",
            "Practitioner",
        ],
        expected_resource_hash_contract=LEGACY_RESOURCE_HASH_CONTRACT,
    )
    proof_store.validate_stored_dataset_proof_metadata(
        legacy_proof.metadata,
        dataset_id=PROOF_DATASET_ID,
        endpoint_id=PROOF_ENDPOINT_ID,
        acquisition_root_run_id=PROOF_ROOT_RUN_ID,
        source_ids=PROOF_SOURCE_IDS,
        selected_resources=[
            "InsurancePlan",
            "Location",
            "Organization",
            "OrganizationAffiliation",
            "Practitioner",
        ],
        expected_resource_hash_contract=(
            TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
        ),
    )
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="contract changed",
    ):
        proof_store.validate_stored_dataset_proof_metadata(
            legacy_proof.metadata,
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            selected_resources=[
                "InsurancePlan",
                "Location",
                "Organization",
                "OrganizationAffiliation",
                "Practitioner",
            ],
            expected_resource_hash_contract=(
                SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT
            ),
            expected_semantic_projection_as_of=PROJECTION_AS_OF,
        )


def test_proof_shard_rejects_unknown_hash_contract():
    with pytest.raises(
        ProviderDirectoryProofStoreError,
        match="hash contract is invalid",
    ):
        proof_store.build_dataset_proof_shard(
            [_semantic_proof_row()],
            dataset_id=PROOF_DATASET_ID,
            endpoint_id=PROOF_ENDPOINT_ID,
            acquisition_root_run_id=PROOF_ROOT_RUN_ID,
            source_ids=PROOF_SOURCE_IDS,
            resource_hash_contract="unknown-v4",
        )
