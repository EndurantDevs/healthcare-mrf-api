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

    fhir_metadata_by_field = {
        "versionId": version_id,
        "source": "https://directory.example.test/fhir",
    }
    if last_updated is not None:
        fhir_metadata_by_field["lastUpdated"] = last_updated
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
        "fhir_meta": fhir_metadata_by_field,
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
    observed_source_provenances = {
        tuple(
            observation_payload_by_field.get(field_name)
            for field_name in provenance_fields
        )
        + (observation_payload_by_field["fhir_meta"]["lastUpdated"],)
        for observation_payload_by_field in (first, second)
    }

    assert observed_provenance in observed_source_provenances
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
    changed_payload_by_field = dict(baseline)
    changed_payload_by_field["active"] = True

    with pytest.raises(
        ValueError,
        match="practitioner_identity_payload_conflict",
    ):
        merge_practitioner_semantic_payloads(
            baseline,
            changed_payload_by_field,
        )

    canonical = canonical_practitioner_payload(baseline)
    stored_hash = _dataset_row(_observation())["payload_hash"]
    tampered_payload_by_field = {
        **canonical,
        "full_name": "Unobserved Projection",
    }
    assert not is_resource_payload_hash_match(
        tampered_payload_by_field,
        stored_hash,
    )

    boolean_payload_by_field = {**baseline, "active": True}
    integer_payload_by_field = {**baseline, "active": 1}
    with pytest.raises(
        ValueError,
        match="practitioner_identity_payload_conflict",
    ):
        merge_practitioner_semantic_payloads(
            boolean_payload_by_field,
            integer_payload_by_field,
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
