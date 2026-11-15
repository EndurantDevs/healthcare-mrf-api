# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete synthetic published candidates so their manifests carry the real retained digest."""

import copy
import json

from process.network_address_projection import PinnedAddressSource
from process.network_compact_readiness import COMPONENT
from process.network_membership_publication import _manifest_digest, _publication_readiness
from process.network_membership_validation import _scope_report

_IDENTITY = "00000000-0000-0000-0000-0000000000{:02x}"
_COUNTS = ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")


def _document(value):
    return json.loads(value) if isinstance(value, str) else copy.deepcopy(value)


def _complete_identity(candidate):
    """Fill the logical identity and accounting fields a published candidate always records."""
    for index, field in enumerate(("dataset_id", "schema_id", "producer_id"), start=1):
        candidate.setdefault(field, _IDENTITY.format(index))
    for field in ("expected_head", "expected_rows", "accepted_rows"):
        candidate.setdefault(field, 0)
    candidate.setdefault("source_recipes_json", [])
    candidate["index_ready"] = True
    candidate["source_generations"].setdefault("unified_address", "b" * 64)


def _validated_report(candidate):
    """Record the address scope and complete accounting exactly as validation does."""
    report_by_field = _document(candidate.get("validation_json") or {})
    report_by_field.setdefault(
        "address_source",
        {"schema_name": "published_addresses", "table_name": "entity_address_unified", "generation_id": ""},
    )
    report_by_field["address_source"]["generation_id"] = candidate["source_generations"]["unified_address"]
    candidate["validation_json"] = report_by_field
    scope_by_field = _scope_report(candidate, PinnedAddressSource(**report_by_field["address_source"]))
    row_count = candidate["expected_rows"]
    report_by_field.update(scope_by_field)
    report_by_field.update(
        valid=True,
        raw_rows=row_count,
        batch_rows=row_count,
        batch_count=int(row_count > 0),
        membership_rows=row_count,
        distinct_memberships=row_count,
        projected_locations=row_count,
        orphan_bindings=0,
        unknown_network_rows=0,
        accounting_errors=[],
    )
    return report_by_field, scope_by_field


def _canonical_readiness(candidate, readiness_by_field, count_by_field, *, compact):
    """Build the canonical receipt the compact or flat serving branch expects."""
    if compact:
        schema = candidate["schema_name"]
        return {
            "component": "compact_office_slices",
            **readiness_by_field,
            **count_by_field,
            "ref_map": {"schema_name": schema, "table_name": "network_slice_ref"},
            "edge_heap": {"schema_name": schema, "table_name": "network_office_edge"},
        }
    row_count = candidate["expected_rows"]
    zero_counts = ("missing_locations", "extra_locations", "changed_address_rows", "changed_network_arrays")
    return {
        "component": "canonical_address_projection",
        **readiness_by_field,
        **count_by_field,
        "index_checks": dict.fromkeys(("has_canonical_column", "has_location_pk", "has_canonical_gin"), True),
        "source_rows": row_count,
        "address_rows": row_count,
        **dict.fromkeys(zero_counts, 0),
    }


def published_candidate(candidate):
    """Return a copy whose validation, readiness and serving reports match its immutable scope."""
    candidate = copy.deepcopy(candidate)
    _complete_identity(candidate)
    report_by_field, scope_by_field = _validated_report(candidate)
    serving_by_field = report_by_field.get("serving_readiness") or {}
    serving_by_field.setdefault("component", "unified_address_serving")
    report_by_field["serving_readiness"] = serving_by_field
    is_compact = serving_by_field["component"] == COMPONENT
    readiness_by_field = {"readiness_revision": 1, "ready": True, "scope": scope_by_field}
    count_by_field = {field: report_by_field[field] for field in _COUNTS}
    report_by_field["candidate_readiness"] = _canonical_readiness(
        candidate, readiness_by_field, count_by_field, compact=is_compact
    )
    if is_compact:
        serving_by_field.update(readiness_by_field, index_profile="compact_membership", slice_ref_sha256="e" * 64)
        serving_by_field.setdefault("address_artifact", {})
        serving_by_field.setdefault("edge_artifacts", [])
    else:
        serving_by_field.update(readiness_by_field, index_profile="serving")
    serving_by_field.setdefault("index_definition_sha256", "d" * 64)
    return candidate


def published_digest(candidate, generation_id):
    """Compute the manifest digest exactly as publication records it."""
    return _manifest_digest(generation_id, *_publication_readiness(candidate))


def published_entry(entry):
    """Rewrite one stored manifest row so its candidate is complete and its digest is genuine."""
    original = _document(entry["candidate_json"])
    candidate = published_candidate(original)
    manifest = _document(entry["manifest_json"])
    if manifest.get("source_generations") == original["source_generations"]:
        manifest["source_generations"] = candidate["source_generations"]
    manifest.setdefault("eligible", True)
    manifest["manifest_sha256"] = published_digest(candidate, manifest["generation_id"])
    return {**entry, "candidate_json": json.dumps(candidate), "manifest_json": json.dumps(manifest)}
