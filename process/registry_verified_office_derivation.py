# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed witness-policy metadata; parsing grants no native source or office authority."""

import hashlib
import json

from process.network_address_projection import PinnedAddressSource
from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2
from process.registry_source_contribution_store import FACT_ANCHOR_FIELDS, _validate_fact_anchor
from process.registry_source_office_summary_v3 import MAX_V3_RECEIPT_BYTES, _counter, _sha, canonical

VERIFIED_OFFICE_POLICY_V2 = "retained-fhir-verified-office-v2"
_CONTEXT_FIELDS = {
    "policy_version",
    "approved_revision",
    "approved_generation_sha256",
    "address_source",
    "address_identity",
    "origin_manifest_sha256",
    "canonicalizer_version",
}
_DERIVATION_FIELDS = _CONTEXT_FIELDS | {
    "derivation_version",
    "recipe_sha256",
    "source_binding",
    "contribution_sha256",
    "fact_artifact",
    "mapping_sha256",
    "input_membership_rows",
    "accepted_membership_rows",
    "omitted_membership_rows",
    "office_rows",
    "page_count",
    "correspondence_sha256",
}


def _closed(document, fields):
    if type(document) is not dict or set(document) != fields:
        raise ValueError


def _context(context):
    _closed(context, _CONTEXT_FIELDS)
    if context["policy_version"] != VERIFIED_OFFICE_POLICY_V2:
        raise ValueError
    _counter(context["approved_revision"])
    _sha(context["approved_generation_sha256"])
    if context["origin_manifest_sha256"] is not None:
        _sha(context["origin_manifest_sha256"])
    source = context["address_source"]
    _closed(source, {"schema_name", "table_name", "generation_id"})
    PinnedAddressSource(**source)
    identity = context["address_identity"]
    _closed(identity, {"schema_oid", "table_oid", "owner_oid", "identity_sha256"})
    for name in ("schema_oid", "table_oid", "owner_oid"):
        if type(identity[name]) is not int or not 0 < identity[name] < 2**32:
            raise ValueError
    _sha(identity["identity_sha256"])
    version = context["canonicalizer_version"]
    _closed(version, {"identity_version", "identity_prefix", "ruleset_version", "pub28_sha256"})
    for name in ("identity_version", "ruleset_version"):
        _counter(version[name])
        if version[name] == 0:
            raise ValueError
    if version["identity_prefix"] != f"v{version['identity_version']}":
        raise ValueError
    _sha(version["pub28_sha256"])


def _defensive(document):
    encoded = canonical(document)
    if len(encoded.encode()) > MAX_V3_RECEIPT_BYTES:
        raise ValueError
    return json.loads(encoded)


def validate_verified_office_context_v2(context):
    """Validate the closed expected context without authenticating any declared native identity."""
    try:
        _context(context)
        return _defensive(context)
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise ValueError("registry_verified_office_derivation_invalid") from None


def verified_office_context_sha256(context):
    """Commit the complete canonical context, including address origin and canonicalizer versions."""
    return hashlib.sha256(canonical(validate_verified_office_context_v2(context)).encode()).hexdigest()


def validate_verified_office_derivation_v2(document):
    """Validate bounded policy output and its full witness binding; execution needs independent proof."""
    try:
        _closed(document, _DERIVATION_FIELDS)
        if type(document["derivation_version"]) is not int or document["derivation_version"] != 2:
            raise ValueError
        _context({field: document[field] for field in _CONTEXT_FIELDS})
        for name in ("recipe_sha256", "contribution_sha256", "mapping_sha256", "correspondence_sha256"):
            _sha(document[name])
        binding = document["source_binding"]
        _closed(binding, {"logical_generation_id", "original_generation_id", "physical_generation_id"})
        for digest in binding.values():
            _sha(digest)
        artifact = document["fact_artifact"]
        _closed(artifact, FACT_ANCHOR_FIELDS | {"capture_sha256", "transform_version", "fact_sha256", "fact_rows"})
        _validate_fact_anchor(artifact)
        if artifact["transform_version"] != FHIR_SOURCE_WITNESS_TRANSFORM_V2:
            raise ValueError
        for name in ("capture_sha256", "fact_sha256"):
            _sha(artifact[name])
        _counter(artifact["fact_rows"])
        for name in (
            "input_membership_rows",
            "accepted_membership_rows",
            "omitted_membership_rows",
            "office_rows",
            "page_count",
        ):
            _counter(document[name])
        if (
            document["input_membership_rows"]
            != document["accepted_membership_rows"] + document["omitted_membership_rows"]
            or document["office_rows"] > document["accepted_membership_rows"]
            or document["office_rows"] + document["page_count"] > 2**63 - 1
        ):
            raise ValueError
        return _defensive(document)
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise ValueError("registry_verified_office_derivation_invalid") from None
