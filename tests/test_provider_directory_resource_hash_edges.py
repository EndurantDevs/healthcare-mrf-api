# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import datetime
import importlib
import json
from types import SimpleNamespace

import pytest

from db.models import ProviderDirectoryPractitioner
from process.provider_directory_resource_hash import (
    RESOURCE_HASH_CONTRACT_METADATA_KEY,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    _json_default,
    resource_payload_sha256_for_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


def test_hash_encoding_and_unknown_contract_are_explicit():
    """Cover both JSON scalar encodings and reject an unknown write policy."""

    assert _json_default(datetime.date(2026, 8, 9)) == "2026-08-09"
    assert _json_default(SimpleNamespace(name="fallback")) == (
        "namespace(name='fallback')"
    )
    with pytest.raises(ValueError, match="resource_hash_contract_invalid"):
        resource_payload_sha256_for_contract({}, "unknown-contract")


@pytest.mark.parametrize(
    "raw_metadata",
    ("{", "[]", 7),
)
def test_dataset_contract_rejects_malformed_serialized_metadata(raw_metadata):
    """Reject invalid JSON, non-object JSON, and non-JSON metadata values."""

    with pytest.raises(RuntimeError, match="resource_hash_contract_invalid"):
        importer._dataset_resource_hash_contract(
            {"publication_metadata_json": raw_metadata}
        )


def test_dataset_contract_accepts_serialized_metadata_object():
    """Read the same explicit contract from stored JSON text and mappings."""

    raw_metadata = json.dumps(
        {
            RESOURCE_HASH_CONTRACT_METADATA_KEY: (
                TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            )
        }
    )
    assert importer._dataset_resource_hash_contract(
        {"publication_metadata_json": raw_metadata}
    ) == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT


@pytest.mark.asyncio
async def test_deferred_dataset_write_requires_hash_contract():
    """Fail before persistence when a deferred dataset write lacks its policy."""

    with pytest.raises(ValueError, match="resource_hash_contract_required"):
        await importer._upsert_deferred_resource_rows(
            ProviderDirectoryPractitioner,
            [{"resource_id": "practitioner-1"}],
            dataset_id="dataset-1",
            track_seen=False,
        )


_COMPONENT_PAYLOAD = {
    "resource_id": "practitioner-synthetic",
    "npi": None,
    "names": [{"family": "Example", "given": ["Sample"]}],
    "family_name": "Example",
    "given_names": ["Sample"],
    "full_name": "Sample Example",
    "fhir_meta": {"versionId": "1", "lastUpdated": "2026-08-01T00:00:00Z"},
    "fhir_fetch_url": "https://example.test/fhir/Practitioner/practitioner-synthetic",
}
_COMPONENT_VECTOR = (
    "50726a7c81573b6d515451b4fbb16fa64844f5d7ec7aedaf5a82616318b1d430",
    ("3b104c256485529ccf44dc572208c80fa1b26b0db7d3984f865d742ca3011678",),
    "bd9b201517eacdeea844a39b8c8b93db3a765470f987559786c7975dd0b83001",
)


def test_practitioner_components_preserve_hashes_with_one_canonicalization(monkeypatch):
    from process import provider_directory_resource_hash as hashes

    payload = copy.deepcopy(_COMPONENT_PAYLOAD)
    original = hashes._canonical_practitioner_hash_view
    calls = []

    def canonical(value):
        calls.append(1)
        return original(value)

    monkeypatch.setattr(hashes, "_canonical_practitioner_hash_view", canonical)
    assert hashes.practitioner_semantic_hash_components(payload) == _COMPONENT_VECTOR
    assert len(calls) == 1
    assert payload == _COMPONENT_PAYLOAD


def test_practitioner_proof_reuses_validated_components(monkeypatch):
    from process import provider_directory_proof_store as proof
    from process import provider_directory_resource_hash as hashes

    original = hashes._canonical_practitioner_hash_view
    calls = []

    def canonical(value):
        calls.append(1)
        return original(value)

    monkeypatch.setattr(hashes, "_canonical_practitioner_hash_view", canonical)
    payload = copy.deepcopy(_COMPONENT_PAYLOAD)
    record = proof.build_dataset_proof_record(
        {
            "resource_type": "Practitioner",
            "resource_id": "practitioner-synthetic",
            "payload_hash": _COMPONENT_VECTOR[2],
            "payload_json": payload,
        },
        hashes.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
    )
    assert record[2] == _COMPONENT_VECTOR[2]
    assert record[8:] == [_COMPONENT_VECTOR[0], list(_COMPONENT_VECTOR[1])]
    assert len(calls) == 1 and payload == _COMPONENT_PAYLOAD


@pytest.mark.parametrize("field", ("family_name", "given_names", "full_name", "names"))
def test_practitioner_components_reject_projection_changes(field):
    from process import provider_directory_resource_hash as hashes

    payload = copy.deepcopy(_COMPONENT_PAYLOAD)
    payload[field] = None
    with pytest.raises(ValueError, match="practitioner_name_projection_invalid"):
        hashes.practitioner_semantic_hash_components(payload)


def test_practitioner_hash_view_leaves_nested_content_and_public_copy_unchanged():
    from process import provider_directory_resource_hash as hashes

    payload = copy.deepcopy(_COMPONENT_PAYLOAD)
    payload["qualifications"] = [{"code": {"text": "Example"}}]
    before = copy.deepcopy(payload)
    hashes.practitioner_semantic_payload_sha256(payload)
    hashes.practitioner_semantic_hash_components(payload)
    assert payload == before
    canonical = hashes.canonical_practitioner_payload(payload)
    canonical["qualifications"][0]["code"]["text"] = "Changed"
    canonical["names"][0]["given"].append("Changed")
    assert payload == before


@pytest.mark.parametrize(
    "ordered",
    (
        [{"family": "Alpha", "given": ["Sample"]}, {"family": "Beta", "given": ["Sample"]}],
        [{"family": "Zulu", "given": ["Sample"]}, {"family": "Ångström", "given": ["İ"]}],
    ),
)
def test_practitioner_names_reuse_exact_ordering_keys(monkeypatch, ordered):
    from process import provider_directory_resource_hash as hashes

    names = [copy.deepcopy(ordered[1]), copy.deepcopy(ordered[0]), copy.deepcopy(ordered[1])]
    before = copy.deepcopy(names)
    original = hashes._stable_json
    calls = []

    def stable_json(value):
        calls.append(1)
        return original(value)

    monkeypatch.setattr(hashes, "_stable_json", stable_json)
    assert hashes.canonical_practitioner_names(names) == ordered
    assert len(calls) == len(names)
    assert names == before


def test_practitioner_hash_and_proof_preserve_error_order():
    from process import provider_directory_proof_store as proof
    from process import provider_directory_resource_hash as hashes

    payload = copy.deepcopy(_COMPONENT_PAYLOAD)
    payload["family_name"] = "changed"
    payload["non_name"] = {("unsupported",): "synthetic"}
    with pytest.raises(ValueError, match="practitioner_name_projection_invalid"):
        hashes.practitioner_semantic_payload_sha256(payload)
    with pytest.raises(TypeError, match="keys must be"):
        proof.build_dataset_proof_record(
            {
                "resource_type": "Practitioner",
                "resource_id": "practitioner-synthetic",
                "payload_hash": _COMPONENT_VECTOR[2],
                "payload_json": payload,
            },
            hashes.SEMANTIC_CONTENT_RESOURCE_HASH_CONTRACT,
        )
