# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical observation checksums; authenticated readers and grants confer authority."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime, timedelta

from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    RESOURCE_TYPES,
    _check_fhir_catalog_page_bounds,
    _decode_fhir_catalog_page,
    fhir_catalog_json_bytes,
    fhir_catalog_text,
)
from process.registry_fhir_catalog_reference_rollup import MAX_PAGE_REFERENCES

try:
    from ptg2_address_canon import encode_catalog_observation_page as _native_page
except ImportError:
    _native_page = None

CATALOG_OBSERVATION_CONTRACT = "registry-fhir-catalog-observation-v1"
_ROW_FIELDS = {"resource_type", "resource_id", "payload_hash", "payload_json"}
_RECEIPT_FIELDS = {"version", "kind", "contract", "publication", "families"}
_PUBLICATION_FIELDS = {
    "root_run_id",
    "published_at",
    "resource_hash_contract",
    "original_admission_version",
    "original_admission_kind",
    "original_admission_sha256",
    "original_metadata_sha256",
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


class FHIRCatalogObservationDigests:
    """Hash an ordered row stream with fixed memory and explicit empty families."""

    def __init__(self):
        self._hashes_by_type = {
            resource_type: hashlib.sha256(
                _canonical({"contract": CATALOG_OBSERVATION_CONTRACT, "resource_type": resource_type}) + b"\n"
            )
            for resource_type in RESOURCE_TYPES
        }
        self._counts_by_type = dict.fromkeys(RESOURCE_TYPES, 0)
        self._last_key = None

    def add(self, row):
        """Accept one exact source row, rejecting duplicates and non-bytewise order."""
        if type(row) is not dict or set(row) != _ROW_FIELDS:
            _fail()
        resource_type, resource_id = row["resource_type"], row["resource_id"]
        if (
            type(resource_type) is not str
            or resource_type not in RESOURCE_TYPES
            or type(resource_id) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", resource_id) is None
            or type(row["payload_json"]) is not dict
        ):
            _fail()
        _digest(row["payload_hash"])
        encoded = _canonical(row)
        key = (resource_type.encode("ascii"), resource_id.encode("ascii"))
        if self._last_key is not None and key <= self._last_key:
            _fail("order_invalid")
        count = self._counts_by_type[resource_type] + 1
        _count(count)
        self._hashes_by_type[resource_type].update(encoded + b"\n")
        self._counts_by_type[resource_type] = count
        self._last_key = key

    def document(self):
        """Return a detached four-family receipt fragment without ending the stream."""
        return {
            resource_type: {
                "rows": self._counts_by_type[resource_type],
                "sha256": self._hashes_by_type[resource_type].hexdigest(),
            }
            for resource_type in RESOURCE_TYPES
        }

    def add_page(self, source_records, validate_metadata, *, page_bytes=DEFAULT_BYTES):
        """Observe a complete bounded SQL page atomically, with a reference encoding fallback."""
        _check_fhir_catalog_page_bounds(source_records, page_bytes)
        observation_rows = tuple(
            tuple(source_record[field] for field in ("resource_type", "resource_id", "payload_hash", "payload_json"))
            for source_record in source_records
        )
        encoded = None
        if _native_page is not None and all(
            type(field_value) is str for observation_row in observation_rows for field_value in observation_row
        ):
            try:
                encoded = _native_page(observation_rows, page_bytes, MAX_PAGE_REFERENCES, sys.get_int_max_str_digits())
            except UnicodeError:
                encoded = None
        if encoded is None:
            page = _decode_fhir_catalog_page(source_records, page_bytes)
            validate_metadata(page)
            candidate = FHIRCatalogObservationDigests()
            candidate._hashes_by_type = {family: digest.copy() for family, digest in self._hashes_by_type.items()}
            candidate._counts_by_type = self._counts_by_type.copy()
            candidate._last_key = self._last_key
            for observation_row in page:
                candidate.add(observation_row)
            self._hashes_by_type = candidate._hashes_by_type
            self._counts_by_type = candidate._counts_by_type
            self._last_key = candidate._last_key
            return
        validate_metadata(
            tuple(
                json.loads(frame)
                for observation_row, frame in zip(observation_rows, encoded)
                if observation_row[0] in {"Organization", "InsurancePlan"}
            )
        )
        last_key = self._last_key
        counts = self._counts_by_type.copy()
        for resource_type, resource_id, _, _ in observation_rows:
            key = (resource_type.encode("ascii"), resource_id.encode("ascii"))
            if last_key is not None and key <= last_key:
                _fail("order_invalid")
            counts[resource_type] += 1
            _count(counts[resource_type])
            last_key = key
        for observation_row, frame in zip(observation_rows, encoded):
            self._hashes_by_type[observation_row[0]].update(frame + b"\n")
        self._counts_by_type = counts
        self._last_key = last_key


def validate_catalog_observation_receipt(receipt):
    """Validate the closed receipt while preserving independently absent old proofs."""
    if type(receipt) is not dict or set(receipt) != _RECEIPT_FIELDS:
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
    for field, maximum in (("resource_hash_contract", 128), ("original_admission_kind", 64)):
        if publication[field] is not None:
            _text(publication[field], maximum)
    version = publication["original_admission_version"]
    if version is not None:
        _count(version)
        if version == 0:
            _fail()
    for field in ("original_admission_sha256", "original_metadata_sha256"):
        if publication[field] is not None:
            _digest(publication[field])
    _count(publication["claimed_resource_count"])
    families = receipt["families"]
    if type(families) is not dict or set(families) != set(RESOURCE_TYPES):
        _fail()
    for family in families.values():
        if type(family) is not dict or set(family) != {"rows", "sha256"}:
            _fail()
        _count(family["rows"])
        _digest(family["sha256"])
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
