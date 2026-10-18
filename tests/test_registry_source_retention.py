# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed source retention metadata and unchanged legacy candidate contracts."""

import json

import pytest

from process.registry_source_retention import (
    SUMMARY_KEY,
    RegistrySourceRetentionError,
    registry_source_retention_sha256,
    validate_registry_source_retention_receipt,
    verify_registry_source_retention,
)


def retention_receipt(**changes):
    return {
        "component": SUMMARY_KEY,
        "revision": 1,
        "table_name": "network_membership_source",
        "source_generation_sha256": "a" * 64,
        "source_rows": 2,
        "add_rows": 1,
        "remove_rows": 1,
        "suppressed_source_rows": 1,
        "effective_rows": 2,
        **changes,
    }


def candidate_receipt(receipt=None):
    receipt = retention_receipt() if receipt is None else receipt
    return {
        "source_generations": {SUMMARY_KEY: registry_source_retention_sha256(receipt)},
        "validation_json": {SUMMARY_KEY: receipt},
        "expected_rows": receipt["effective_rows"],
    }


def test_source_retention_canonical_roundtrip_and_legacy():
    receipt = retention_receipt()
    assert validate_registry_source_retention_receipt(json.dumps(receipt)) == receipt
    assert registry_source_retention_sha256(dict(reversed(list(receipt.items())))) == registry_source_retention_sha256(
        receipt
    )
    assert verify_registry_source_retention(candidate_receipt()) == receipt
    assert verify_registry_source_retention({"source_generations": {}, "validation_json": None}) is None
    assert (
        verify_registry_source_retention({"source_generations": "{}", "validation_json": '{"unrelated":true}'}) is None
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"component": "other"},
        {"revision": True},
        {"revision": 2},
        {"table_name": "network_membership"},
        {"source_generation_sha256": "A" * 64},
        {"source_rows": True},
        {"source_rows": -1},
        {"add_rows": 2**63},
        {"remove_rows": False},
        {"effective_rows": 3},
        {"suppressed_source_rows": 3},
        {"extra": 1},
    ],
)
def test_source_retention_rejects_invalid_closed_receipts(changes):
    with pytest.raises(RegistrySourceRetentionError):
        validate_registry_source_retention_receipt(retention_receipt(**changes))


def test_source_retention_rejects_missing_duplicate_and_oversized_fields():
    receipt = retention_receipt()
    for field in receipt:
        incomplete_by_field = dict(receipt)
        incomplete_by_field.pop(field)
        with pytest.raises(RegistrySourceRetentionError):
            validate_registry_source_retention_receipt(incomplete_by_field)
    duplicate = json.dumps(receipt)[:-1] + ', "revision": 1}'
    for invalid in (duplicate, " " * 4097, [], None):
        with pytest.raises(RegistrySourceRetentionError):
            validate_registry_source_retention_receipt(invalid)


@pytest.mark.parametrize("mutation", ["missing_hash", "missing_receipt", "hash", "expected", "count_bool"])
def test_source_retention_rejects_orphan_or_changed_candidate_metadata(mutation):
    candidate = candidate_receipt()
    if mutation == "missing_hash":
        candidate["source_generations"].clear()
    elif mutation == "missing_receipt":
        candidate["validation_json"].clear()
    elif mutation == "hash":
        candidate["source_generations"][SUMMARY_KEY] = "b" * 64
    else:
        candidate["expected_rows"] = True if mutation == "count_bool" else 3
    with pytest.raises(RegistrySourceRetentionError):
        verify_registry_source_retention(candidate)
