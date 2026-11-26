# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical observation receipts; authenticated readers and grants confer authority."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta

from process.registry_fhir_catalog_extraction import (
    MAX_BYTES,
    RESOURCE_TYPES,
    fhir_catalog_json_bytes,
    fhir_catalog_text,
)

CATALOG_OBSERVATION_CONTRACT = "registry-fhir-catalog-observation-v1"
_RECEIPT_FIELDS = {"version", "kind", "contract", "publication", "families"}
_PUBLICATION_FIELDS = {
    "root_run_id",
    "published_at",
    "resource_hash_contract",
    "claimed_resource_count",
}


class FHIRCatalogObservationError(ValueError):
    """Refuse malformed observation evidence without supplying missing provenance."""


def _fail(reason="invalid"):
    raise FHIRCatalogObservationError("registry_fhir_catalog_observation_" + reason)


def _canonical(value):
    try:
        encoded = fhir_catalog_json_bytes(value)
    except ValueError:
        _fail()
    if len(encoded) > MAX_BYTES:
        _fail("bounds_invalid")
    pending_values = [value]
    while pending_values:
        current = pending_values.pop()
        if type(current) is dict:
            if any(type(key) is not str for key in current):
                _fail()
            pending_values.extend(current.values())
        elif type(current) is list:
            pending_values.extend(current)
        elif current is not None and type(current) not in {str, int, float, bool}:
            _fail()
    return encoded


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        _fail()


def _count(value):
    if type(value) is not int or not 0 <= value < 2**63:
        _fail()


def _text(value, maximum):
    try:
        valid = bool(value) and fhir_catalog_text(value, maximum, strip=False) == value
    except ValueError:
        _fail()
    if not valid or value.strip() != value:
        _fail()


def _published_at(value):
    _text(value, 64)
    try:
        instant = datetime.fromisoformat(value)
    except ValueError:
        _fail()
    if instant.tzinfo is None or instant.utcoffset() != timedelta(0):
        _fail()


def validate_catalog_observation_receipt(receipt):
    """Validate the closed receipt while preserving independently absent old proofs."""
    from process.delta_dental_directory_producer import METADATA_KEY

    if type(receipt) is not dict or set(receipt) not in (_RECEIPT_FIELDS, _RECEIPT_FIELDS | {METADATA_KEY}):
        _fail()
    if (
        type(receipt["version"]) is not int
        or receipt["version"] != 1
        or receipt["kind"] != "catalog_observation"
        or receipt["contract"] != CATALOG_OBSERVATION_CONTRACT
    ):
        _fail()
    publication = receipt["publication"]
    if type(publication) is not dict or set(publication) != _PUBLICATION_FIELDS:
        _fail()
    _text(publication["root_run_id"], 64)
    _published_at(publication["published_at"])
    for field, maximum in (("resource_hash_contract", 128),):
        if publication[field] is not None:
            _text(publication[field], maximum)
    _count(publication["claimed_resource_count"])
    families = receipt["families"]
    if type(families) is not dict or set(families) != set(RESOURCE_TYPES):
        _fail()
    family_fields = None
    for family in families.values():
        if type(family) is not dict or set(family) not in ({"rows"}, {"rows", "sha256"}):
            _fail()
        if family_fields is None:
            family_fields = set(family)
        elif set(family) != family_fields:
            _fail()
        _count(family["rows"])
        if "sha256" in family:
            _digest(family["sha256"])
    if METADATA_KEY in receipt:
        from process.delta_dental_directory_producer import validate_delta_positive_receipt

        coverage_by_field = receipt[METADATA_KEY]
        validate_delta_positive_receipt(coverage_by_field, resource_count=publication["claimed_resource_count"])
        if any(
            count["rows"] != coverage_by_field["resource_counts"].get(family, 0) for family, count in families.items()
        ):
            _fail("delta_observed_resource_counts_changed")
    return json.loads(_canonical(receipt))


def catalog_observation_descriptor_digest(descriptor):
    """Hash every descriptor field except its two circular receipt identity fields."""
    if type(descriptor) is not dict:
        _fail()
    identity_by_field = {
        field: value
        for field, value in descriptor.items()
        if field not in {"accepted_receipt_id", "accepted_receipt_sha256"}
    }
    return hashlib.sha256(_canonical(identity_by_field)).hexdigest()
