# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observation receipts bind logical publication counts without inventing old proofs."""

import hashlib
from copy import deepcopy

import pytest

from process.registry_fhir_catalog_extraction import RESOURCE_TYPES, fhir_catalog_json_bytes
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    FHIRCatalogObservationError,
    catalog_observation_descriptor_digest,
    validate_catalog_observation_receipt,
)


def _families(counts=None, *, with_sha256=False):
    counts_by_type = counts or {}
    return {
        resource_type: (
            {"rows": counts_by_type.get(resource_type, 0), "sha256": "a" * 64}
            if with_sha256
            else {"rows": counts_by_type.get(resource_type, 0)}
        )
        for resource_type in RESOURCE_TYPES
    }


def _receipt(*, with_sha256=False):
    return {
        "version": 1,
        "kind": "catalog_observation",
        "contract": CATALOG_OBSERVATION_CONTRACT,
        "publication": {
            "root_run_id": "publication-root",
            "published_at": "2026-01-01T12:00:00+00:00",
            "resource_hash_contract": "historical-hash-v1",
            "claimed_resource_count": 75,
        },
        "families": _families({"Organization": 1}, with_sha256=with_sha256),
    }


def test_receipt_accepts_count_only_and_legacy_families():
    count_only = _receipt()
    legacy = _receipt(with_sha256=True)
    assert validate_catalog_observation_receipt(count_only) == count_only
    assert validate_catalog_observation_receipt(legacy) == legacy
    mixed = _receipt(with_sha256=True)
    del mixed["families"]["Organization"]["sha256"]
    with pytest.raises(FHIRCatalogObservationError):
        validate_catalog_observation_receipt(mixed)
    bad_digest = _receipt(with_sha256=True)
    bad_digest["families"]["Organization"]["sha256"] = "invalid"
    with pytest.raises(FHIRCatalogObservationError):
        validate_catalog_observation_receipt(bad_digest)


@pytest.mark.parametrize(
    "missing_field",
    [
        "resource_hash_contract",
    ],
)
def test_missing_historical_proofs_remain_missing_independently(missing_field):
    receipt = _receipt()
    for field in receipt["publication"]:
        if field == missing_field:
            receipt["publication"][field] = None
    validated = validate_catalog_observation_receipt(receipt)
    assert validated == receipt
    assert validated is not receipt and validated["publication"] is not receipt["publication"]
    assert validated["publication"]["claimed_resource_count"] == 75
    assert sum(family["rows"] for family in validated["families"].values()) == 1


@pytest.mark.parametrize(
    "path,value",
    [
        (("version",), True),
        (("version",), 2),
        (("kind",), "generic"),
        (("contract",), "other"),
        (("publication", "root_run_id"), ""),
        (("publication", "root_run_id"), " root "),
        (("publication", "root_run_id"), "a\nroot"),
        (("publication", "root_run_id"), "é" * 33),
        (("publication", "resource_hash_contract"), "x" * 129),
        (("publication", "published_at"), "2026-01-01T12:00:00"),
        (("publication", "published_at"), "2026-01-01T12:00:00+01:00"),
        (("publication", "published_at"), "invalid"),
        (("publication", "claimed_resource_count"), True),
        (("publication", "claimed_resource_count"), -1),
        (("publication", "claimed_resource_count"), 2**63),
        (("families", "Organization", "rows"), False),
        (("families", "Organization", "rows"), 1.0),
        (("families", "Organization", "rows"), -1),
        (("families", "Organization", "rows"), 2**63),
    ],
)
def test_receipt_rejects_invalid_provenance_and_family_counts(path, value):
    receipt = _receipt()
    target = receipt
    for field in path[:-1]:
        target = target[field]
    target[path[-1]] = value
    with pytest.raises(FHIRCatalogObservationError):
        validate_catalog_observation_receipt(receipt)


@pytest.mark.parametrize("path", [(), ("publication",), ("families",), ("families", "Organization")])
@pytest.mark.parametrize("change", ["extra", "missing", "wrong_type"])
def test_receipt_objects_have_closed_exact_field_sets(path, change):
    receipt = _receipt()
    target = receipt
    for field in path:
        target = target[field]
    if change == "extra":
        target["extra"] = None
    elif change == "missing":
        target.pop(next(iter(target)))
    elif path:
        parent = receipt
        for field in path[:-1]:
            parent = parent[field]
        parent[path[-1]] = []
    else:
        with pytest.raises(FHIRCatalogObservationError):
            validate_catalog_observation_receipt([])
        return
    with pytest.raises(FHIRCatalogObservationError):
        validate_catalog_observation_receipt(receipt)


def test_receipt_accepts_exact_text_and_count_bounds_and_utc_iso_spelling():
    receipt = _receipt()
    receipt["publication"].update(
        root_run_id="é" * 32,
        resource_hash_contract="h" * 128,
        published_at="2026-01-01T12:00:00Z",
        claimed_resource_count=2**63 - 1,
    )
    receipt["families"]["Organization"]["rows"] = 2**63 - 1
    assert validate_catalog_observation_receipt(receipt) == receipt


def test_descriptor_digest_excludes_only_circular_receipt_fields():
    descriptor_by_field = {
        "coordinates": {"source_id": "example", "dataset_id": "edition"},
        "policy": {"schema_name": "example_source", "catalog_schema_name": "example_catalog"},
        "admission": _receipt(),
        "accepted_receipt_id": "old-id",
        "accepted_receipt_sha256": "old-digest",
    }
    expected_by_field = {
        field: value for field, value in descriptor_by_field.items() if not field.startswith("accepted_receipt_")
    }
    digest = catalog_observation_descriptor_digest(descriptor_by_field)
    assert digest == hashlib.sha256(fhir_catalog_json_bytes(expected_by_field)).hexdigest()
    replay = deepcopy(descriptor_by_field)
    replay.update(accepted_receipt_id=CATALOG_OBSERVATION_CONTRACT + ":" + digest, accepted_receipt_sha256=digest)
    assert catalog_observation_descriptor_digest(dict(reversed(tuple(replay.items())))) == digest
    for field, value in expected_by_field.items():
        changed_by_field = {**descriptor_by_field, field: {"changed": value}}
        assert catalog_observation_descriptor_digest(changed_by_field) != digest
    assert catalog_observation_descriptor_digest({**descriptor_by_field, "accepted_receipt_extra": "bound"}) != digest


@pytest.mark.parametrize("descriptor", [None, [], {"fact": float("nan")}, {"fact": (1,)}, {1: "invalid key"}])
def test_descriptor_digest_rejects_malformed_json(descriptor):
    with pytest.raises(FHIRCatalogObservationError):
        catalog_observation_descriptor_digest(descriptor)


@pytest.mark.parametrize("tamper", ["receipt", "complete", "digest"])
def test_source_descriptor_refuses_a_corrupted_or_incomplete_observation_receipt(tamper):
    from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor
    from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor

    selected = observed_descriptor()
    if tamper == "receipt":
        selected["admission"]["publication"]["status"] = "unpublished"
    elif tamper == "complete":
        selected["complete"] = False
    else:
        selected["accepted_receipt_sha256"] = "f" * 64
        selected["accepted_receipt_id"] = CATALOG_OBSERVATION_CONTRACT + ":" + "f" * 64
    with pytest.raises(ValueError, match="invalid"):
        validate_fhir_catalog_source_descriptor(selected)
