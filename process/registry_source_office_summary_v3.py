# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed office-summary metadata; parsing never grants source or address authority."""

import hashlib
import json
import re
from uuid import UUID

MAX_V3_RECEIPT_BYTES = 128 * 1024
SUMMARY_KEY = "registry_initial_source_offices"
_MAX_COUNTER = 2**63 - 1
_FIELDS = {
    "component",
    "revision",
    "recipe_sha256",
    "approved_revision",
    "approved_generation_sha256",
    "address_generation_sha256",
    "origin_manifest_sha256",
    "recipe_count",
    "office_rows",
    "page_count",
    "correspondence_sha256",
    "derivations",
}
_DERIVATION_FIELDS = {
    "recipe_sha256",
    "mode",
    "mapping_sha256",
    "physical_source_sha256",
    "fact_artifact",
    "office_rows",
    "page_count",
    "correspondence_sha256",
}
_ARTIFACT_FIELDS = {
    "fact_candidate_id",
    "fact_table_oid",
    "fact_seal_sha256",
    "capture_sha256",
    "transform_version",
    "fact_sha256",
    "fact_rows",
}


def canonical(document):
    """Encode deterministic JSON used by office summary commitments."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(digest):
    if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("registry_source_office_v3_invalid")


def _counter(counter):
    if type(counter) is not int or not 0 <= counter <= _MAX_COUNTER:
        raise ValueError("registry_source_office_v3_invalid")


def _artifact(document):
    if type(document) is not dict or set(document) != _ARTIFACT_FIELDS:
        raise ValueError("registry_source_office_v3_invalid")
    identity = UUID(document["fact_candidate_id"]) if type(document["fact_candidate_id"]) is str else None
    if (
        identity is None
        or not identity.int
        or str(identity) != document["fact_candidate_id"]
        or type(document["fact_table_oid"]) is not int
        or not 0 < document["fact_table_oid"] < 2**32
        or document["transform_version"] not in {"fhir-source-facts-v1", "generic-fhir-source-facts-v1"}
    ):
        raise ValueError("registry_source_office_v3_invalid")
    for name in ("fact_seal_sha256", "capture_sha256", "fact_sha256"):
        _sha(document[name])
    _counter(document["fact_rows"])


def validate_derivation(document):
    """Validate one bounded recipe derivation without granting source authority."""
    if type(document) is not dict or set(document) != _DERIVATION_FIELDS or document["mode"] not in {"facts", "raw"}:
        raise ValueError("registry_source_office_v3_invalid")
    for name in ("recipe_sha256", "mapping_sha256", "physical_source_sha256", "correspondence_sha256"):
        _sha(document[name])
    for name in ("office_rows", "page_count"):
        _counter(document[name])
    if document["mode"] == "facts":
        _artifact(document["fact_artifact"])
    elif document["fact_artifact"] is not None:
        raise ValueError("registry_source_office_v3_invalid")
    if document["office_rows"] + document["page_count"] > _MAX_COUNTER:
        raise ValueError("registry_source_office_v3_invalid")
    return document


def validate_office_summary_v3(document):
    """Bound every field and bind aggregate counters to explicit recipe derivations."""
    try:
        if type(document) is not dict or set(document) != _FIELDS:
            raise ValueError
        if document["component"] != SUMMARY_KEY or type(document["revision"]) is not int or document["revision"] != 3:
            raise ValueError
        for name in ("approved_revision", "recipe_count", "office_rows", "page_count"):
            _counter(document[name])
        for name in (
            "recipe_sha256",
            "approved_generation_sha256",
            "address_generation_sha256",
            "correspondence_sha256",
        ):
            _sha(document[name])
        if document["origin_manifest_sha256"] is not None:
            _sha(document["origin_manifest_sha256"])
        derivations = document["derivations"]
        if type(derivations) is not list or not 1 <= len(derivations) <= 100:
            raise ValueError
        for derivation in derivations:
            validate_derivation(derivation)
        identities = [derivation["recipe_sha256"] for derivation in derivations]
        if identities != sorted(set(identities)) or not any(
            derivation["mode"] == "facts" for derivation in derivations
        ):
            raise ValueError
        if (
            document["recipe_count"] != len(derivations)
            or document["office_rows"] != sum(derivation["office_rows"] for derivation in derivations)
            or document["page_count"] != sum(derivation["page_count"] for derivation in derivations)
            or document["office_rows"] + document["page_count"] > _MAX_COUNTER
            or document["correspondence_sha256"] != hashlib.sha256(canonical(derivations).encode()).hexdigest()
            or len(canonical(document).encode()) > MAX_V3_RECEIPT_BYTES
        ):
            raise ValueError
        return json.loads(canonical(document))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise ValueError("registry_source_office_v3_invalid") from None


def office_summary_v3(recipes_sha256, approved, address_source, derivations, origin_manifest_sha256):
    """Build the closed v3 aggregate from canonically ordered derivations."""
    ordered = sorted(derivations, key=lambda source: source["recipe_sha256"])
    return validate_office_summary_v3(
        {
            "component": SUMMARY_KEY,
            "revision": 3,
            "recipe_sha256": recipes_sha256,
            "approved_revision": approved.approved_revision,
            "approved_generation_sha256": approved.generation_id,
            "address_generation_sha256": address_source.generation_id,
            "origin_manifest_sha256": origin_manifest_sha256,
            "recipe_count": len(ordered),
            "office_rows": sum(source["office_rows"] for source in ordered),
            "page_count": sum(source["page_count"] for source in ordered),
            "correspondence_sha256": hashlib.sha256(canonical(ordered).encode()).hexdigest(),
            "derivations": ordered,
        }
    )
