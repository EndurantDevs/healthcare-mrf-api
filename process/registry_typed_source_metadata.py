# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed contribution and office metadata, distinct from every legacy witness transcript."""

import hashlib
import json

from process.network_address_projection import PinnedAddressSource
from process.registry_source_office_summary_v3 import _counter, _sha, canonical

COUNTERS = (
    "source_rows",
    "projected_rows",
    "strict_pair_rows",
    "reference_rows",
    "provider_only_roles",
    "mapped_pair_rows",
    "mapped_reference_rows",
    "unapproved_pair_rows",
    "unapproved_reference_rows",
    "unmatched_office_pair_rows",
    "unmatched_office_reference_rows",
    "matched_offices",
    "missing_offices",
)
_CONTRIBUTION_FIELDS = {
    "component",
    "descriptor_version",
    "recipe_sha256",
    "capture_sha256",
    "catalog_generation",
    "mapping_sha256",
    "counters",
}
_OFFICE_CONTEXT_FIELDS = (
    "office_policy",
    "canonicalizer_version",
    "approved_revision",
    "approved_generation_sha256",
    "address_source",
)
_DERIVATION_FIELDS = {
    "derivation_version",
    "recipe_sha256",
    "capture_sha256",
    "contribution_sha256",
    "mapping_sha256",
    "office_policy",
    "canonicalizer_version",
    "approved_revision",
    "approved_generation_sha256",
    "address_source",
    "matched_offices",
    "missing_offices",
}


def _closed(document, fields):
    if type(document) is not dict or set(document) != fields:
        raise ValueError("registry_typed_metadata_invalid")


def _defensive(document):
    encoded = canonical(document)
    if len(encoded.encode()) > 128 * 1024:
        raise ValueError("registry_typed_metadata_invalid")
    return json.loads(encoded)


def _counts(counts):
    _closed(counts, set(COUNTERS))
    for value in counts.values():
        _counter(value)
    if (
        counts["projected_rows"] > counts["source_rows"]
        or counts["provider_only_roles"] > counts["projected_rows"]
        or counts["strict_pair_rows"]
        != sum(counts[name] for name in ("mapped_pair_rows", "unapproved_pair_rows", "unmatched_office_pair_rows"))
        or counts["reference_rows"]
        != sum(
            counts[name]
            for name in ("mapped_reference_rows", "unapproved_reference_rows", "unmatched_office_reference_rows")
        )
        or any(
            counts[pairs] > counts[references]
            for pairs, references in (
                ("strict_pair_rows", "reference_rows"),
                ("mapped_pair_rows", "mapped_reference_rows"),
                ("unapproved_pair_rows", "unapproved_reference_rows"),
                ("unmatched_office_pair_rows", "unmatched_office_reference_rows"),
            )
        )
        or counts["matched_offices"] + counts["missing_offices"] > counts["strict_pair_rows"]
    ):
        raise ValueError("registry_typed_metadata_invalid")


def validate_typed_contribution(document):
    """Validate complete accounting without old fact anchors or Source authority."""
    _closed(document, _CONTRIBUTION_FIELDS)
    if (
        document["component"] != "registry_fhir_typed_contribution"
        or type(document["descriptor_version"]) is not int
        or document["descriptor_version"] != 4
    ):
        raise ValueError("registry_typed_metadata_invalid")
    for name in ("recipe_sha256", "capture_sha256", "catalog_generation", "mapping_sha256"):
        _sha(document[name])
    _counts(document["counters"])
    return _defensive(document)


def validate_typed_derivation(document):
    """Keep named address generations and exact logical approval."""
    from process.network_fhir_typed_contract import POLICY

    document = (
        {key: field_value for key, field_value in document.items() if key != "address_identity"}
        if type(document) is dict
        else document
    )
    _closed(document, _DERIVATION_FIELDS)
    if (
        type(document["derivation_version"]) is not int
        or document["derivation_version"] != 3
        or type(document["canonicalizer_version"]) is not int
        or document["canonicalizer_version"] != 2
        or document["office_policy"] != POLICY
    ):
        raise ValueError("registry_typed_metadata_invalid")
    for name in (
        "recipe_sha256",
        "capture_sha256",
        "contribution_sha256",
        "mapping_sha256",
        "approved_generation_sha256",
    ):
        _sha(document[name])
    for name in ("approved_revision", "matched_offices", "missing_offices"):
        _counter(document[name])
    _office_context(document)
    return _defensive(document)


def _office_context(document):
    from process.network_fhir_typed_contract import POLICY

    if (
        document["office_policy"] != POLICY
        or type(document["canonicalizer_version"]) is not int
        or document["canonicalizer_version"] != 2
    ):
        raise ValueError("registry_typed_metadata_invalid")
    _counter(document["approved_revision"])
    _sha(document["approved_generation_sha256"])
    _closed(document["address_source"], {"schema_name", "table_name", "generation_id"})
    PinnedAddressSource(**document["address_source"])


def _legacy_fields(version, offices):
    if offices:
        if version == 1:
            return ("office_rows", "page_count")
        if version == 2:
            return (
                "input_membership_rows",
                "accepted_membership_rows",
                "omitted_membership_rows",
                "office_rows",
                "page_count",
            )
    else:
        from process.registry_source_contribution_store import _COUNTERS, _MAPPING_COUNTERS_V3, _WITNESS_COUNTERS_V2

        if version in (1, 2, 3):
            return {1: _COUNTERS, 2: _WITNESS_COUNTERS_V2, 3: _MAPPING_COUNTERS_V3}[version]
    raise ValueError("registry_typed_metadata_invalid")


def _child(document, offices):
    if offices:
        version = document.get("derivation_version", 1)
        if version == 3:
            return validate_typed_derivation(document), None
        from process.registry_source_office_summary_v3 import validate_derivation
        from process.registry_verified_office_derivation import validate_verified_office_derivation_v2

        return (
            validate_verified_office_derivation_v2(document) if version == 2 else validate_derivation(document)
        ), version
    version = document.get("descriptor_version", 1)
    if version == 4:
        return validate_typed_contribution(document), None
    from process.registry_source_contribution_store import validate_source_contribution_descriptor

    return validate_source_contribution_descriptor(document), version


def _summary(root, documents, *, offices):
    from process.registry_source_inventory_contract import SourceInventoryRoot

    if type(root) is not SourceInventoryRoot:
        raise ValueError("registry_typed_metadata_invalid")
    selected_documents = [_child(document, offices) for document in documents]
    documents = [document for document, _version in selected_documents]
    typed_documents = [document for document, version in selected_documents if version is None]
    expected = root.derivation_count if offices else root.contribution_count
    identities = [document["recipe_sha256"] for document in documents]
    if not typed_documents or len(documents) != expected or identities != sorted(set(identities)):
        raise ValueError("registry_typed_metadata_invalid")
    counters = ("matched_offices", "missing_offices") if offices else COUNTERS
    totals_by_name = {
        name: sum((document if offices else document["counters"])[name] for document in typed_documents)
        for name in counters
    }
    legacy_summaries = []
    for version in sorted({version for _document, version in selected_documents if version is not None}):
        child_documents = [document for document, selected_version in selected_documents if selected_version == version]
        legacy_summaries.append(
            {
                "version": version,
                "source_count": len(child_documents),
                "counters": {
                    name: sum(document[name] for document in child_documents)
                    for name in _legacy_fields(version, offices)
                },
            }
        )
    summary_by_field = {
        "component": "registry_initial_source_offices" if offices else "registry_source_contributions",
        "revision": 6,
        "inventory_entries_sha256": root.entries_sha256,
        "source_count": len(documents),
        "typed_source_count": len(typed_documents),
        "sources_sha256": hashlib.sha256(canonical(documents).encode()).hexdigest(),
        "counters": totals_by_name,
        "legacy": legacy_summaries,
    }
    if offices:
        context_by_name = {name: typed_documents[0][name] for name in _OFFICE_CONTEXT_FIELDS}
        if any(
            any(document[name] != context_value for name, context_value in context_by_name.items())
            for document in typed_documents
        ):
            raise ValueError("registry_typed_metadata_invalid")
        summary_by_field["context"] = context_by_name
    return validate_typed_summary(summary_by_field, root, offices=offices)


def typed_contribution_summary(root, descriptors):
    """Summarize exact mixed contribution formats under the inventory root."""
    return _summary(root, descriptors, offices=False)


def typed_office_summary(root, derivations):
    """Summarize office derivations with their shared approval and address context."""
    return _summary(root, derivations, offices=True)


def _validate_legacy_summary(document, expected, offices):
    legacy = document["legacy"]
    if type(legacy) is not list or len(legacy) > (2 if offices else 3):
        raise ValueError("registry_typed_metadata_invalid")
    versions, count = [], document["typed_source_count"]
    for child in legacy:
        _closed(child, {"version", "source_count", "counters"})
        if type(child["version"]) is not int:
            raise ValueError("registry_typed_metadata_invalid")
        fields = _legacy_fields(child["version"], offices)
        _closed(child["counters"], set(fields))
        _counter(child["source_count"])
        if not child["source_count"]:
            raise ValueError("registry_typed_metadata_invalid")
        for counter_value in child["counters"].values():
            _counter(counter_value)
        versions.append(child["version"])
        count += child["source_count"]
    if versions != sorted(set(versions)) or count != expected:
        raise ValueError("registry_typed_metadata_invalid")


def validate_typed_summary(document, root, *, offices=False):
    """Validate complete mixed-format accounting and logical office context."""
    from process.registry_source_inventory_contract import SourceInventoryRoot

    if type(root) is not SourceInventoryRoot or type(offices) is not bool:
        raise ValueError("registry_typed_metadata_invalid")
    fields = {
        "component",
        "revision",
        "inventory_entries_sha256",
        "source_count",
        "typed_source_count",
        "sources_sha256",
        "counters",
        "legacy",
    }
    document_by_field = document
    if offices and type(document_by_field) is dict and type(document_by_field.get("context")) is dict:
        document_by_field = {
            **document_by_field,
            "context": {
                key: context_value
                for key, context_value in document_by_field["context"].items()
                if key != "address_identity"
            },
        }
    _closed(document_by_field, fields | ({"context"} if offices else set()))
    expected = root.derivation_count if offices else root.contribution_count
    if (
        type(document_by_field["revision"]) is not int
        or document_by_field["revision"] != 6
        or document_by_field["component"]
        != ("registry_initial_source_offices" if offices else "registry_source_contributions")
        or type(document_by_field["source_count"]) is not int
        or document_by_field["source_count"] != expected
        or expected == 0
        or document_by_field["inventory_entries_sha256"] != root.entries_sha256
        or len(canonical(document_by_field).encode()) > 4096
    ):
        raise ValueError("registry_typed_metadata_invalid")
    _sha(document_by_field["sources_sha256"])
    _counter(document_by_field["typed_source_count"])
    if not 0 < document_by_field["typed_source_count"] <= expected:
        raise ValueError("registry_typed_metadata_invalid")
    _validate_legacy_summary(document_by_field, expected, offices)
    if offices:
        _closed(document_by_field["counters"], {"matched_offices", "missing_offices"})
        context = document_by_field["context"]
        _closed(context, set(_OFFICE_CONTEXT_FIELDS))
        _office_context(context)
        for counter_value in document_by_field["counters"].values():
            _counter(counter_value)
    else:
        _counts(document_by_field["counters"])
    return _defensive(document_by_field)
