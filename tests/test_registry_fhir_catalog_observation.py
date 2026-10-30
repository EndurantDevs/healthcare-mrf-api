# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observation digests bind actual normalized bytes without inventing old proofs."""

import hashlib
from copy import deepcopy

import pytest

from process.registry_fhir_catalog_extraction import MAX_BYTES, RESOURCE_TYPES, fhir_catalog_json_bytes
from process.registry_fhir_catalog_observation import (
    CATALOG_OBSERVATION_CONTRACT,
    FHIRCatalogObservationDigests,
    FHIRCatalogObservationError,
    catalog_observation_descriptor_digest,
    validate_catalog_observation_receipt,
)


def _row(resource_type="Organization", resource_id="example", **changes):
    return {
        "resource_type": resource_type,
        "resource_id": resource_id,
        "payload_hash": "a" * 64,
        "payload_json": {"name": "Example", "nested": {"active": True, "aliases": [None, "Example"]}},
        **changes,
    }


def _families(rows=()):
    digests = FHIRCatalogObservationDigests()
    for row in rows:
        digests.add(row)
    return digests.document()


def _receipt():
    return {
        "version": 1,
        "kind": "catalog_observation",
        "contract": CATALOG_OBSERVATION_CONTRACT,
        "publication": {
            "root_run_id": "publication-root",
            "published_at": "2026-01-01T12:00:00+00:00",
            "resource_hash_contract": "historical-hash-v1",
            "original_admission_version": 7,
            "original_admission_kind": "historical",
            "original_admission_sha256": "b" * 64,
            "original_metadata_sha256": "c" * 64,
            "claimed_resource_count": 75,
        },
        "families": _families([_row()]),
    }


def test_family_hashes_match_framed_canonical_rows_including_empty_families():
    rows = [_row("Organization", "A"), _row("Organization", "a")]
    observed = _families(rows)
    for resource_type in RESOURCE_TYPES:
        framed = fhir_catalog_json_bytes({"contract": CATALOG_OBSERVATION_CONTRACT, "resource_type": resource_type})
        selected_rows = [row for row in rows if row["resource_type"] == resource_type]
        expected = framed + b"\n" + b"".join(fhir_catalog_json_bytes(row) + b"\n" for row in selected_rows)
        assert observed[resource_type] == {"rows": len(selected_rows), "sha256": hashlib.sha256(expected).hexdigest()}


@pytest.mark.parametrize("chunk_size", [1, 3, 8])
def test_page_boundaries_and_repeated_documents_do_not_change_identity(chunk_size):
    rows = [_row(resource_type, resource_id) for resource_type in RESOURCE_TYPES for resource_id in ("A", "a")]
    expected = _families(rows)
    digests = FHIRCatalogObservationDigests()
    for offset in range(0, len(rows), chunk_size):
        for row in rows[offset : offset + chunk_size]:
            digests.add(dict(reversed(tuple(row.items()))))
        checkpoint = digests.document()
        assert checkpoint == digests.document()
        checkpoint["Organization"]["rows"] = -1
    assert digests.document() == expected
    assert _families(deepcopy(rows)) == expected


@pytest.mark.parametrize("resource_type", RESOURCE_TYPES)
def test_each_family_binds_actual_payload_even_when_historical_hash_is_unchanged(resource_type):
    rows = [_row(family) for family in RESOURCE_TYPES]
    baseline = _families(rows)
    changed = deepcopy(rows)
    selected = next(row for row in changed if row["resource_type"] == resource_type)
    selected["payload_json"]["unconsumed_normalized_field"] = "changed"
    assert selected["payload_hash"] == rows[0]["payload_hash"]
    observed = _families(changed)
    assert [family for family in RESOURCE_TYPES if observed[family] != baseline[family]] == [resource_type]
    selected["payload_json"].pop("unconsumed_normalized_field")
    selected["payload_hash"] = "d" * 64
    assert _families(changed)[resource_type] != baseline[resource_type]


@pytest.mark.parametrize(
    "changes",
    [
        {"extra": None},
        {"resource_type": "Practitioner"},
        {"resource_type": None},
        {"resource_id": "bad/id"},
        {"resource_id": ""},
        {"resource_id": "x" * 65},
        {"resource_id": 1},
        {"payload_hash": "A" * 64},
        {"payload_hash": "a" * 63},
        {"payload_json": []},
        {"payload_json": {"value": float("nan")}},
        {"payload_json": {"value": float("inf")}},
        {"payload_json": {"value": float("-inf")}},
        {"payload_json": {"value": (1, 2)}},
        {"payload_json": {1: "non-JSON key"}},
        {"payload_json": {"value": b"bytes"}},
        {"payload_json": {"value": "\ud800"}},
    ],
)
def test_malformed_rows_are_rejected_without_advancing_the_stream(changes):
    digests = FHIRCatalogObservationDigests()
    before = digests.document()
    with pytest.raises(FHIRCatalogObservationError):
        digests.add(_row(**changes))
    assert digests.document() == before
    digests.add(_row())
    assert digests.document() == _families([_row()])


@pytest.mark.parametrize("row", [None, {}, {"resource_type": "Organization"}])
def test_missing_row_fields_are_rejected(row):
    with pytest.raises(FHIRCatalogObservationError):
        FHIRCatalogObservationDigests().add(row)


@pytest.mark.parametrize(
    "rows",
    [
        [_row(), _row()],
        [_row("Organization", "a"), _row("Organization", "A")],
        [_row("Organization"), _row("InsurancePlan")],
    ],
)
def test_duplicates_and_out_of_order_rows_cannot_be_hashed(rows):
    digests = FHIRCatalogObservationDigests()
    digests.add(rows[0])
    before = digests.document()
    with pytest.raises(FHIRCatalogObservationError, match="order_invalid"):
        digests.add(rows[1])
    assert digests.document() == before


def test_row_bound_applies_to_canonical_utf8_bytes():
    prefix = _row(payload_json={"value": ""})
    budget = MAX_BYTES - len(fhir_catalog_json_bytes(prefix))
    accepted = _row(payload_json={"value": "x" * budget})
    assert _families([accepted])["Organization"]["rows"] == 1
    with pytest.raises(FHIRCatalogObservationError, match="bounds_invalid"):
        _families([_row(payload_json={"value": "x" * (budget + 1)})])


@pytest.mark.parametrize(
    "missing_field",
    [
        "resource_hash_contract",
        "original_admission_version",
        "original_admission_kind",
        "original_admission_sha256",
        "original_metadata_sha256",
        "all",
    ],
)
def test_missing_historical_proofs_remain_missing_independently(missing_field):
    receipt = _receipt()
    for field in receipt["publication"]:
        if field == missing_field or (missing_field == "all" and field.startswith(("resource_hash", "original_"))):
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
        (("publication", "original_admission_kind"), "x" * 65),
        (("publication", "original_admission_kind"), False),
        (("publication", "original_admission_version"), True),
        (("publication", "original_admission_version"), 0),
        (("publication", "original_admission_version"), 2**63),
        (("publication", "original_admission_sha256"), "no-proof"),
        (("publication", "original_metadata_sha256"), "A" * 64),
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
        (("families", "Organization", "sha256"), "invalid"),
    ],
)
def test_receipt_rejects_invalid_provenance_counts_and_family_digests(path, value):
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
        original_admission_kind="k" * 64,
        published_at="2026-01-01T12:00:00Z",
        original_admission_version=2**63 - 1,
        claimed_resource_count=2**63 - 1,
    )
    receipt["families"]["Organization"]["rows"] = 2**63 - 1
    assert validate_catalog_observation_receipt(receipt) == receipt


def test_descriptor_digest_excludes_only_circular_receipt_fields():
    descriptor_by_field = {
        "coordinates": {"source_id": "example", "dataset_id": "edition"},
        "policy": {"schema_name": "example_source", "runtime_roles": ["example_reader"]},
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
    with pytest.raises(ValueError, match="observation_invalid"):
        validate_fhir_catalog_source_descriptor(selected)
