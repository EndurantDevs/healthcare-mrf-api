"""Publication and storage evidence checks for the PTG V4 canary."""

from __future__ import annotations

import hashlib
import re
import struct
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
)
from process.ptg_parts.canonical import canonical_json_dumps
from process.ptg_parts.ptg2_v4_graph_compiler import (
    validate_v4_adaptive_layout_decision,
)
from process.ptg_parts.ptg2_v4_taxonomy_candidates import (
    PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
    PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
    validate_v4_inferred_taxonomy_projection_manifest,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_GRAPH_RESOURCE_FIELDS,
)
from scripts.ptg_v4_dev_canary_cas import REFERENCE_POPULATION
from scripts.ptg_v4_dev_canary_storage_budget import (
    MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS,
    UNAPPROVED_STORAGE_CEILING_FAILURE,
    StorageBudget,
)
from scripts.ptg_v4_dev_canary_retained_artifacts import (
    RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT,
)


STORAGE_EVIDENCE_CONTRACT = "ptg_v4_physical_storage_v1"
_TAX_IDENTITY_CONTRACT = "ptg2_provider_group_tax_identity_v1"
_TAX_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_TAX_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_TAX_CANDIDATE_PREFIX_CONTRACT = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
_TAX_AUTHORITY_CONTRACT = "tin_hmac_sha256_full_32_bytes_authoritative"
_TAX_SOURCE_ORDINAL_CONTRACT = "snapshot_shard_id_sorted_lsb0_bitmap_v1"
_TAX_POLICY_DESCRIPTOR_HASH_DOMAIN = b"PTG2V4TINPOLICY\x01"
_TAX_SOURCE_ORDINAL_HASH_DOMAIN = b"PTG2V4TAXORD\x01"
_TAX_POLICY_ID = re.compile(r"^ptg-tin-hmac-sha256-v1:[a-z0-9][a-z0-9._-]{0,31}$")
REQUIRED_PHYSICAL_RELATIONS = frozenset(
    {
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_provider_set",
        "ptg2_v3_provider_group",
        "ptg2_v4_snapshot_map_root",
        "ptg2_v4_snapshot_map_pack",
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_heavy_owner",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_provider_graph_diagnostic",
        "ptg2_v4_inferred_taxonomy_candidate",
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
    }
)
WHOLE_SNAPSHOT_PHYSICAL_RELATIONS = frozenset(
    {
        "ptg2_v3_snapshot_layout",
        "ptg2_v3_layout_fingerprint",
        "ptg2_v3_snapshot_binding",
        "ptg2_v3_snapshot_scope",
        "ptg2_v3_snapshot_plan_scope",
        "ptg2_v3_snapshot_source",
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_graph_owner",
        "ptg2_v3_code",
        "ptg2_v3_provider_group",
        "ptg2_v3_provider_set",
        "ptg2_v3_price_attr",
        "ptg2_v3_npi_scope",
        "ptg2_v3_audit_occurrence",
        "ptg2_v3_source_audit_witness_part",
        "ptg2_v3_source_audit_witness",
        "ptg2_v3_candidate_audit_attestation",
        "ptg2_v3_gc_candidate",
        "ptg2_artifact_manifest",
        "ptg2_artifact_blob_chunk",
        "ptg2_provider_tax_identity_legacy_layout",
        *REQUIRED_PHYSICAL_RELATIONS,
    }
)


@dataclass(frozen=True)
class _V4Evidence:
    snapshot: dict[str, Any]
    root: dict[str, Any]
    exact_counts: dict[str, Any]
    relations: list[dict[str, Any]]
    provider_graph_diagnostic: dict[str, Any]
    inferred_taxonomy_candidates: dict[str, Any]
    provider_tax_identity: dict[str, Any]
    physical_storage: dict[str, Any]


def _normalized_v4_evidence(
    evidence_by_field: Mapping[str, Any],
) -> _V4Evidence:
    return _V4Evidence(
        snapshot=_mapping(evidence_by_field.get("snapshot")),
        root=_mapping(evidence_by_field.get("root")),
        exact_counts=_mapping(evidence_by_field.get("exact_counts")),
        relations=_mapping_rows(evidence_by_field.get("relations")),
        provider_graph_diagnostic=_mapping(
            evidence_by_field.get("provider_graph_diagnostic")
        ),
        inferred_taxonomy_candidates=_mapping(
            evidence_by_field.get("inferred_taxonomy_candidates")
        ),
        provider_tax_identity=_mapping(evidence_by_field.get("provider_tax_identity")),
        physical_storage=_mapping(evidence_by_field.get("physical_storage")),
    )


def _validate_v4_evidence_parts(
    evidence: _V4Evidence,
    *,
    storage_budget: StorageBudget,
    expected_root_counts: Mapping[str, int],
    expected_relation_counts: Mapping[str, int],
    failures: list[str],
) -> dict[str, Any]:
    _validate_v4_state(evidence.snapshot, evidence.root, failures)
    _reconcile_exact_counts(evidence.root, evidence.exact_counts, failures)
    _validate_provider_graph_diagnostic(
        evidence.provider_graph_diagnostic,
        exact_counts=evidence.exact_counts,
        relations=evidence.relations,
        failures=failures,
    )
    _validate_declared_counts(
        evidence.root,
        evidence.relations,
        expected_root_counts,
        expected_relation_counts,
        failures,
    )
    inferred_taxonomy_summary = _validate_inferred_taxonomy_candidates(
        evidence.snapshot,
        evidence.inferred_taxonomy_candidates,
        evidence.exact_counts,
        failures,
        expected_representation=str(evidence.root.get("representation") or ""),
    )
    _validate_provider_tax_identity(
        evidence.provider_tax_identity,
        evidence.exact_counts,
        failures,
    )
    _validate_physical_storage(
        evidence.physical_storage,
        storage_budget,
        failures,
    )
    return _manifest_summary(
        evidence.snapshot,
        evidence.root,
        evidence.provider_graph_diagnostic,
        inferred_taxonomy_summary,
        failures,
    )


def evaluate_v4_evidence(
    evidence_by_field: Mapping[str, Any],
    *,
    storage_budget: StorageBudget,
    measurement_image_identity: str,
    expected_root_counts: Mapping[str, int],
    expected_relation_counts: Mapping[str, int],
) -> dict[str, Any]:
    """Reconcile exact V4 rows and gate attributable physical PostgreSQL bytes."""

    failures: list[str] = []
    evidence = _normalized_v4_evidence(evidence_by_field)
    manifest_summary = _validate_v4_evidence_parts(
        evidence,
        storage_budget=storage_budget,
        expected_root_counts=expected_root_counts,
        expected_relation_counts=expected_relation_counts,
        failures=failures,
    )
    return {
        "passed": not failures,
        "failures": failures,
        "snapshot": _safe_snapshot_summary(evidence.snapshot),
        "root": dict(evidence.root),
        "exact_counts": dict(evidence.exact_counts),
        "relations": evidence.relations,
        "heavy_owner_diagnostics": evidence_by_field.get("heavy_owners", []),
        "provider_graph_diagnostic": evidence.provider_graph_diagnostic,
        "inferred_taxonomy_candidates": (evidence.inferred_taxonomy_candidates),
        "provider_tax_identity": evidence.provider_tax_identity,
        "manifest": manifest_summary,
        "physical_storage": evidence.physical_storage,
        "storage_budget": storage_budget.report(
            graph_gate_bytes=_optional_int(
                evidence.physical_storage.get("graph_gate_bytes")
            ),
            snapshot_gate_bytes=_optional_int(
                evidence.physical_storage.get("snapshot_gate_bytes")
            ),
            measurement_image_identity=measurement_image_identity,
        ),
    }


def _validate_v4_state(
    snapshot: Mapping[str, Any],
    root: Mapping[str, Any],
    failures: list[str],
) -> None:
    expected_by_field = {
        "snapshot_status": "published",
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v4",
    }
    for field_name, expected_value in expected_by_field.items():
        if snapshot.get(field_name) != expected_value:
            failures.append(f"{field_name} is not {expected_value}")
    if root.get("state") != "complete":
        failures.append("V4 snapshot-map root is not complete")


def _reconcile_exact_counts(
    root: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
    failures: list[str],
) -> None:
    exact_field_by_root_field = {
        "map_pack_count": "map_pack_count",
        "coordinate_count": "map_coordinate_count",
        "entry_count": "map_entry_count",
        "logical_byte_count": "map_logical_byte_count",
        "npi_count": "npi_count",
        "component_count": "component_count",
        "pattern_count": "pattern_count",
        "relation_count": "relation_count",
        "heavy_owner_count": "heavy_owner_count",
    }
    for root_field, exact_field in exact_field_by_root_field.items():
        if _optional_int(root.get(root_field)) != _optional_int(
            exact_counts.get(exact_field)
        ):
            failures.append(f"root {root_field} differs from exact snapshot rows")


def _validate_provider_graph_diagnostic(
    diagnostic_evidence: Mapping[str, Any],
    *,
    exact_counts: Mapping[str, Any],
    relations: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> None:
    """Reconcile compiler-selected owners, prefix rows, and relation geometry."""

    diagnostic = _mapping(diagnostic_evidence.get("fields"))
    resources = _mapping(diagnostic_evidence.get("resources"))
    prefix = _mapping(diagnostic_evidence.get("prefix"))
    selected_prefixes = _mapping_rows(prefix.get("selected_owners"))
    if (
        _optional_int(diagnostic_evidence.get("row_count")) != 1
        or _optional_int(exact_counts.get("diagnostic_count")) != 1
        or set(diagnostic) != set(PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS)
    ):
        failures.append("provider-graph diagnostic singleton is invalid")
        return
    if (
        set(resources) != set(PTG2_V4_GRAPH_RESOURCE_FIELDS)
        or (_optional_int(resources.get("compressed_acquisition_bytes")) or 0) <= 0
        or _optional_int(resources.get("input_factor_bytes")) is None
        or _optional_int(resources.get("input_factor_bytes")) < 0
        or _optional_int(resources.get("factor_edge_count")) is None
        or _optional_int(resources.get("factor_edge_count")) < 0
        or _optional_int(resources.get("empty_npi_tin_only_normalization_count"))
        is None
        or _optional_int(resources.get("empty_npi_tin_only_normalization_count")) < 0
    ):
        failures.append("provider-graph sealed resource admission is invalid")
    _validate_prefix_totals(diagnostic, prefix, exact_counts, failures)
    _validate_selected_prefixes(diagnostic, selected_prefixes, failures)
    _validate_prefix_relation(diagnostic, relations, failures)


def _validate_prefix_totals(
    diagnostic: Mapping[str, Any],
    prefix: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
    failures: list[str],
) -> None:
    """Require exact metadata owner/member totals and valid persisted digests."""

    expected_owner_count = _optional_int(diagnostic.get("override_owner_count"))
    expected_member_count = _optional_int(diagnostic.get("override_member_count"))
    observed_owner_counts = {
        _optional_int(prefix.get("owner_count")),
        _optional_int(exact_counts.get("prefix_owner_count")),
    }
    observed_member_counts = {
        _optional_int(prefix.get("member_count")),
        _optional_int(exact_counts.get("prefix_member_count")),
    }
    if (
        observed_owner_counts != {expected_owner_count}
        or observed_member_counts != {expected_member_count}
        or prefix.get("all_rows_valid") is not True
    ):
        failures.append("NPI-prefix metadata totals differ from compiler diagnostics")


def _validate_selected_prefixes(
    diagnostic: Mapping[str, Any],
    selected_prefixes: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> None:
    """Authenticate override presence and digest for both deterministic owners."""

    prefix_by_owner = {
        _optional_int(prefix_record.get("provider_set_key")): prefix_record
        for prefix_record in selected_prefixes
    }
    worst_key = _optional_int(diagnostic.get("worst_provider_set_key"))
    worst_prefix = prefix_by_owner.get(worst_key)
    expected_worst_prefix = (
        _optional_int(diagnostic.get("worst_member_count")),
        diagnostic.get("worst_member_digest"),
    )
    actual_worst_prefix = (
        (
            _optional_int(worst_prefix.get("member_count")),
            worst_prefix.get("member_digest"),
        )
        if worst_prefix is not None
        else None
    )
    if bool(diagnostic.get("worst_uses_override")) != (worst_prefix is not None) or (
        bool(diagnostic.get("worst_uses_override"))
        and actual_worst_prefix != expected_worst_prefix
    ):
        failures.append("worst-owner NPI-prefix digest or override mode is invalid")
    online_key = _optional_int(diagnostic.get("worst_online_provider_set_key"))
    if online_key is not None and online_key in prefix_by_owner:
        failures.append("worst online owner unexpectedly uses a prefix override")


def _validate_prefix_relation(
    diagnostic: Mapping[str, Any],
    relations: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> None:
    """Reconcile the exact prefix vector with its compiler diagnostic totals."""

    relation_by_name = {
        str(relation.get("relation") or ""): relation for relation in relations
    }
    override_relation = relation_by_name.get("set_npi_prefix_override")
    expected_members = _optional_int(diagnostic.get("override_member_count"))
    if (
        override_relation is None
        or _optional_int(override_relation.get("logical_member_count"))
        != expected_members
        or _optional_int(override_relation.get("vector_member_count"))
        != expected_members
    ):
        failures.append("NPI-prefix relation geometry differs from diagnostics")


def _validate_declared_counts(
    root: Mapping[str, Any],
    relations: Sequence[Mapping[str, Any]],
    expected_root_counts: Mapping[str, int],
    expected_relation_counts: Mapping[str, int],
    failures: list[str],
) -> None:
    for field_name, expected_count in expected_root_counts.items():
        if _optional_int(root.get(field_name)) != expected_count:
            failures.append(f"root {field_name} differs from declared expectation")
    relation_by_name = {str(row.get("relation") or ""): row for row in relations}
    for scoped_field, expected_count in expected_relation_counts.items():
        relation_name, field_name = scoped_field.rsplit(".", 1)
        relation = relation_by_name.get(relation_name)
        if (
            relation is None
            or _optional_int(relation.get(field_name)) != expected_count
        ):
            failures.append(
                f"relation {scoped_field} differs from declared expectation"
            )


def _manifest_summary(
    snapshot: Mapping[str, Any],
    root: Mapping[str, Any],
    diagnostic_evidence: Mapping[str, Any],
    inferred_taxonomy_summary: Mapping[str, Any],
    failures: list[str],
) -> dict[str, Any]:
    layout_manifest = _mapping(snapshot.get("layout_manifest"))
    serving_index = _mapping(layout_manifest.get("serving_index"))
    snapshot_map = _mapping(serving_index.get("snapshot_map"))
    serving_binary = _mapping(serving_index.get("serving_binary"))
    provider_graph = _mapping(serving_binary.get("provider_graph_v4"))
    expected_marker_by_field = {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v4",
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
    }
    for field_name, expected_value in expected_marker_by_field.items():
        if serving_index.get(field_name) != expected_value:
            failures.append(f"V4 manifest marker {field_name} is invalid")
    representation = root.get("representation")
    if snapshot_map.get("representation") != representation:
        failures.append("snapshot_map manifest representation differs from root")
    if provider_graph.get("representation") != representation:
        failures.append("serving provider-graph representation differs from root")
    try:
        adaptive_layout_map = validate_v4_adaptive_layout_decision(
            provider_graph.get("adaptive_layout")
        )
    except RuntimeError:
        failures.append("serving adaptive-layout decision is invalid")
        adaptive_layout_map = {}
    if adaptive_layout_map.get("selected_representation") != representation:
        failures.append("serving adaptive-layout decision differs from root")
    hot_prefix = _mapping(provider_graph.get("hot_prefix"))
    resource_admission = _mapping(provider_graph.get("resource_admission"))
    diagnostic = _mapping(diagnostic_evidence.get("fields"))
    diagnostic_resources = _mapping(diagnostic_evidence.get("resources"))
    if hot_prefix != diagnostic:
        failures.append("V4 manifest hot-prefix diagnostics differ from database")
    if resource_admission != diagnostic_resources:
        failures.append("V4 manifest resource admission differs from database")
    return {
        **expected_marker_by_field,
        "representation": representation,
        "adaptive_layout": adaptive_layout_map,
        "hot_prefix": hot_prefix,
        "resource_admission": resource_admission,
        "inferred_taxonomy_candidates": dict(inferred_taxonomy_summary),
        "manifest_storage_bytes_informational": _optional_int(
            serving_index.get("storage_bytes")
        ),
        "layout_logical_byte_count_informational": _optional_int(
            snapshot.get("layout_logical_byte_count")
        ),
    }


def _inferred_taxonomy_exact_counts(
    exact_counts: Mapping[str, Any],
) -> dict[str, int | None]:
    return {
        "rule_count": _optional_int(exact_counts.get("inferred_taxonomy_rule_count")),
        "observe_only_rule_count": _optional_int(
            exact_counts.get("inferred_taxonomy_observe_only_rule_count")
        ),
        "member_count": _optional_int(
            exact_counts.get("inferred_taxonomy_member_count")
        ),
        "packed_byte_count": _optional_int(
            exact_counts.get("inferred_taxonomy_packed_byte_count")
        ),
        "pattern_count": _optional_int(
            exact_counts.get("inferred_taxonomy_pattern_count")
        ),
        "pattern_member_count": _optional_int(
            exact_counts.get("inferred_taxonomy_pattern_member_count")
        ),
        "pattern_member_bytes": _optional_int(
            exact_counts.get("inferred_taxonomy_pattern_payload_byte_count")
        ),
    }


def _validated_taxonomy_projection(
    projection_value: Any,
    *,
    invalid_message: str,
    failures: list[str],
) -> dict[str, Any]:
    try:
        return validate_v4_inferred_taxonomy_projection_manifest(projection_value)
    except PTG2ManifestArtifactError:
        failures.append(invalid_message)
        return {}


def _validate_taxonomy_exact_counts(
    exact_by_manifest_field: Mapping[str, int | None],
    evidence_projection_map: Mapping[str, Any],
    failures: list[str],
) -> None:
    if (
        any(count is None for count in exact_by_manifest_field.values())
        or not evidence_projection_map
        or any(
            exact_count != _optional_int(evidence_projection_map.get(field_name))
            for field_name, exact_count in exact_by_manifest_field.items()
        )
    ):
        failures.append(
            "inferred-taxonomy projection counts differ from exact snapshot rows"
        )


def _validate_taxonomy_representation(
    evidence_projection_map: Mapping[str, Any],
    *,
    expected_representation: str,
    failures: list[str],
) -> None:
    nonempty_rules = tuple(
        rule
        for rule in evidence_projection_map.get("rules", ())
        if _optional_int(rule.get("member_count")) not in (None, 0)
    )
    if expected_representation in {
        PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
    } and any(
        rule.get("representation") != expected_representation for rule in nonempty_rules
    ):
        failures.append(
            "inferred-taxonomy nonempty rules do not use the selected graph representation"
        )
    if expected_representation == (
        PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
    ) and any(
        _optional_int(rule.get(field_name)) in (None, 0)
        for rule in nonempty_rules
        for field_name in (
            "pattern_count",
            "pattern_member_count",
            "pattern_member_bytes",
        )
    ):
        failures.append(
            "inferred-taxonomy pattern layout lacks a positive pattern projection"
        )


def _inferred_taxonomy_provider_graph(
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    layout_manifest = _mapping(snapshot.get("layout_manifest"))
    serving_index = _mapping(layout_manifest.get("serving_index"))
    serving_binary = _mapping(serving_index.get("serving_binary"))
    return _mapping(serving_binary.get("provider_graph_v4"))


def _unadvertised_taxonomy_summary(
    provider_graph: Mapping[str, Any],
    *,
    exact_by_manifest_field: Mapping[str, int | None],
    projection_evidence: Mapping[str, Any],
    failures: list[str],
) -> dict[str, Any] | None:
    if "inferred_taxonomy_candidates" in provider_graph:
        return None
    if any(count is None or count != 0 for count in exact_by_manifest_field.values()):
        failures.append("inferred-taxonomy rows exist without a serving manifest")
    if projection_evidence:
        failures.append("inferred-taxonomy database evidence exists without rows")
    return {"advertised": False}


def _advertised_taxonomy_summary(
    evidence_projection_map: Mapping[str, Any],
    projection_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "advertised": True,
        **(
            evidence_projection_map
            if evidence_projection_map
            else dict(projection_evidence)
        ),
    }


def _validate_inferred_taxonomy_candidates(
    snapshot: Mapping[str, Any],
    projection_evidence: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
    failures: list[str],
    *,
    expected_representation: str | None = None,
) -> dict[str, Any]:
    """Bind an advertised projection to exact authenticated database rows."""

    provider_graph = _inferred_taxonomy_provider_graph(snapshot)
    exact_by_manifest_field = _inferred_taxonomy_exact_counts(exact_counts)
    absent_summary = _unadvertised_taxonomy_summary(
        provider_graph,
        exact_by_manifest_field=exact_by_manifest_field,
        projection_evidence=projection_evidence,
        failures=failures,
    )
    if absent_summary is not None:
        return absent_summary

    advertised_value = provider_graph.get("inferred_taxonomy_candidates")
    if not isinstance(advertised_value, Mapping):
        failures.append("inferred-taxonomy serving manifest is malformed")
        advertised_projection_map: dict[str, Any] = {}
    else:
        advertised_projection_map = _validated_taxonomy_projection(
            advertised_value,
            invalid_message="inferred-taxonomy serving manifest is invalid",
            failures=failures,
        )
    evidence_projection_map = _validated_taxonomy_projection(
        projection_evidence,
        invalid_message="inferred-taxonomy database projection is invalid",
        failures=failures,
    )
    _validate_taxonomy_exact_counts(
        exact_by_manifest_field,
        evidence_projection_map,
        failures,
    )
    if (
        advertised_projection_map
        and evidence_projection_map
        and advertised_projection_map != evidence_projection_map
    ):
        failures.append(
            "inferred-taxonomy serving manifest differs from database projection"
        )
    _validate_taxonomy_representation(
        evidence_projection_map,
        expected_representation=str(
            expected_representation or provider_graph.get("representation") or ""
        ),
        failures=failures,
    )
    return _advertised_taxonomy_summary(evidence_projection_map, projection_evidence)


def _validate_physical_storage(
    storage: Mapping[str, Any],
    storage_budget: StorageBudget,
    failures: list[str],
) -> None:
    """Validate complete physical relations, attribution, and storage gates."""

    if storage.get("contract") != STORAGE_EVIDENCE_CONTRACT:
        failures.append("physical storage evidence contract is missing")
        return
    _validate_physical_relations(storage, failures)
    _validate_cas_attribution(storage, failures)
    _validate_retained_raw_artifacts(storage, storage_budget, failures)
    _validate_storage_gate_limits(storage, storage_budget, failures)


def _validate_physical_relations(
    storage: Mapping[str, Any],
    failures: list[str],
) -> None:
    """Require every whole-snapshot relation and reconciled allocation."""

    relation_records = _mapping_rows(storage.get("relations"))
    relation_by_name = {
        str(relation_record.get("relation") or ""): relation_record
        for relation_record in relation_records
    }
    missing_relations = WHOLE_SNAPSHOT_PHYSICAL_RELATIONS - set(relation_by_name)
    if missing_relations:
        failures.append("physical storage evidence lacks required relations")
    for relation_name in WHOLE_SNAPSHOT_PHYSICAL_RELATIONS & set(relation_by_name):
        relation_record = relation_by_name[relation_name]
        if relation_record.get("exists") is not True:
            failures.append(f"physical relation is absent: {relation_name}")
        if _optional_int(relation_record.get("total_bytes")) is None:
            failures.append(f"physical relation size is absent: {relation_name}")
        if _optional_int(relation_record.get("attributed_bytes")) is None:
            failures.append(f"snapshot attribution is absent: {relation_name}")
    if storage.get("baseline_captured") is not True:
        failures.append("pre-import physical relation-size baseline is missing")
    if storage.get("allocation_reconciled") is not True:
        failures.append("physical allocation does not reconcile to global size")
    if storage.get("missing_required_object_kinds"):
        failures.append("owner, locator, or coordinate-map CAS blocks are missing")


def _validate_storage_gate_limits(
    storage: Mapping[str, Any],
    storage_budget: StorageBudget,
    failures: list[str],
) -> None:
    """Apply approved graph and whole-snapshot physical ceilings."""

    graph_gate_bytes = _optional_int(storage.get("graph_gate_bytes"))
    snapshot_gate_bytes = _optional_int(storage.get("snapshot_gate_bytes"))
    if graph_gate_bytes is None:
        failures.append("attributable V4 graph storage measurement is missing")
    if snapshot_gate_bytes is None:
        failures.append("whole-snapshot physical storage measurement is missing")
    if graph_gate_bytes is not None and graph_gate_bytes * 10_000 > (
        storage_budget.encoded_persistent_projection_bytes
        * MAX_GRAPH_PHYSICAL_TO_ENCODED_PROJECTION_BASIS_POINTS
    ):
        failures.append(
            "physical graph storage exceeds encoded-projection drift budget"
        )
    if not storage_budget.is_promotion_approved:
        failures.append(UNAPPROVED_STORAGE_CEILING_FAILURE)
    elif graph_gate_bytes is not None and graph_gate_bytes > int(
        storage_budget.maximum_graph_physical_storage_bytes or 0
    ):
        failures.append(
            "attributable V4 graph storage exceeds its source-controlled maximum"
        )
    if (
        storage_budget.is_promotion_approved
        and snapshot_gate_bytes is not None
        and snapshot_gate_bytes
        > int(storage_budget.maximum_snapshot_physical_storage_bytes or 0)
    ):
        failures.append(
            "whole-snapshot physical storage exceeds its source-controlled maximum"
        )
    if storage.get("storage_claim_scope") != (
        "whole_snapshot_v4_graph_and_retained_raw"
    ):
        failures.append("physical storage evidence is graph-only or ambiguously scoped")


def _validate_retained_raw_artifacts(
    storage: Mapping[str, Any],
    storage_budget: StorageBudget,
    failures: list[str],
) -> None:
    """Validate retained compressed files and their physical allocation."""

    evidence_by_field = _mapping(storage.get("retained_raw_artifacts"))
    artifact_records = _mapping_rows(evidence_by_field.get("artifacts"))
    evidence_by_field_without_digest = {
        field_name: field_value
        for field_name, field_value in evidence_by_field.items()
        if field_name != "evidence_sha256"
    }
    expected_digest = hashlib.sha256(
        canonical_json_dumps(evidence_by_field_without_digest).encode("utf-8")
    ).hexdigest()
    version_ids = {
        str(artifact.get("source_file_version_id") or "")
        for artifact in artifact_records
    }
    raw_hashes = {
        str(artifact.get("raw_sha256") or "") for artifact in artifact_records
    }
    ordinals = {_optional_int(artifact.get("ordinal")) for artifact in artifact_records}
    referenced_raw_bytes = sum(
        _optional_int(artifact.get("raw_byte_count")) or 0
        for artifact in artifact_records
    )
    physical_bytes = sum(
        _optional_int(artifact.get("physical_allocated_bytes")) or 0
        for artifact in artifact_records
    )
    artifact_count = len(artifact_records)
    if not _is_valid_retained_identity(
        evidence_by_field,
        storage_budget=storage_budget,
        expected_digest=expected_digest,
        artifact_count=artifact_count,
        version_ids=version_ids,
        raw_hashes=raw_hashes,
        ordinals=ordinals,
    ):
        failures.append("retained raw-artifact identity evidence is invalid")
    _validate_retained_raw_storage_totals(
        storage,
        storage_budget,
        evidence_by_field,
        referenced_raw_bytes=referenced_raw_bytes,
        physical_bytes=physical_bytes,
        failures=failures,
    )
    _validate_retained_artifact_records(artifact_records, failures)


def _validate_retained_raw_storage_totals(
    storage: Mapping[str, Any],
    storage_budget: StorageBudget,
    evidence_by_field: Mapping[str, Any],
    *,
    referenced_raw_bytes: int,
    physical_bytes: int,
    failures: list[str],
) -> None:
    """Reconcile retained-file bytes with acquisition and snapshot storage."""

    if (
        referenced_raw_bytes != storage_budget.compressed_acquisition_bytes
        or _optional_int(evidence_by_field.get("referenced_raw_bytes"))
        != referenced_raw_bytes
        or _optional_int(evidence_by_field.get("referenced_physical_bytes"))
        != physical_bytes
        or _optional_int(storage.get("retained_raw_artifact_physical_bytes"))
        != physical_bytes
        or (_optional_int(storage.get("snapshot_gate_bytes")) or 0) < physical_bytes
        or physical_bytes <= 0
    ):
        failures.append("retained raw-artifact physical storage is invalid")


def _validate_retained_artifact_records(
    artifact_records: Sequence[Mapping[str, Any]],
    failures: list[str],
) -> None:
    """Require every retained file to have one manifest and live reference."""

    if any(
        (_optional_int(artifact.get("raw_byte_count")) or 0) <= 0
        or (_optional_int(artifact.get("physical_allocated_bytes")) or 0) <= 0
        or _optional_int(artifact.get("artifact_manifest_count")) != 1
        or (_optional_int(artifact.get("source_version_reference_count")) or 0) < 1
        for artifact in artifact_records
    ):
        failures.append("retained raw-artifact file evidence is incomplete")


def _is_valid_retained_identity(
    evidence_by_field: Mapping[str, Any],
    *,
    storage_budget: StorageBudget,
    expected_digest: str,
    artifact_count: int,
    version_ids: set[str],
    raw_hashes: set[str],
    ordinals: set[int | None],
) -> bool:
    """Return whether retained-file identity and cardinality are exact."""

    return not (
        evidence_by_field.get("contract") != RETAINED_RAW_ARTIFACT_STORAGE_CONTRACT
        or evidence_by_field.get("snapshot_id") != storage_budget.snapshot_id
        or evidence_by_field.get("all_files_verified") is not True
        or evidence_by_field.get("attribution")
        != "full_referenced_physical_bytes_conservative"
        or evidence_by_field.get("evidence_sha256") != expected_digest
        or _optional_int(evidence_by_field.get("source_file_version_count"))
        != artifact_count
        or _optional_int(evidence_by_field.get("distinct_artifact_count"))
        != artifact_count
        or artifact_count < 2
        or len(version_ids) != artifact_count
        or len(raw_hashes) != artifact_count
        or "" in version_ids
        or "" in raw_hashes
        or ordinals != set(range(1, artifact_count + 1))
    )


def _validate_provider_tax_identity(
    evidence: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
    failures: list[str],
) -> None:
    """Reconcile the token-only sidecar manifest with exact snapshot rows."""

    fields = _mapping(evidence.get("fields"))
    if _optional_int(evidence.get("row_count")) != 1:
        failures.append("provider tax-identity manifest is missing or duplicated")
        return
    _validate_tax_identity_contract(fields, failures)
    _validate_tax_identity_source_map(fields, failures)
    _validate_tax_identity_counts(fields, exact_counts, failures)


def _validate_tax_identity_contract(
    fields: Mapping[str, Any],
    failures: list[str],
) -> None:
    """Validate the public token-policy and digest manifest fields."""

    expected_contract_by_field = {
        "contract": _TAX_IDENTITY_CONTRACT,
        "normalization_contract": _TAX_NORMALIZATION_CONTRACT,
        "hmac_contract": _TAX_HMAC_CONTRACT,
        "source_ordinal_contract": _TAX_SOURCE_ORDINAL_CONTRACT,
    }
    if any(
        fields.get(field_name) != expected_value
        for field_name, expected_value in expected_contract_by_field.items()
    ):
        failures.append("provider tax-identity manifest contract is invalid")
    token_policy_id = fields.get("token_policy_id")
    if (
        not isinstance(token_policy_id, str)
        or len(token_policy_id.encode("utf-8")) > 55
        or _TAX_POLICY_ID.fullmatch(token_policy_id) is None
    ):
        failures.append("provider tax-identity token policy is invalid")
    elif fields.get("token_policy_descriptor_sha256") != (
        _tax_policy_descriptor_digest(token_policy_id)
    ):
        failures.append("provider tax-identity token policy descriptor is invalid")
    for digest_field in (
        "token_policy_descriptor_sha256",
        "source_ordinal_map_digest",
        "content_digest",
    ):
        digest = fields.get(digest_field)
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            failures.append(f"provider tax-identity {digest_field} is invalid")


def _tax_policy_descriptor_digest(token_policy_id: str) -> str:
    """Rebuild the policy descriptor without secret or snapshot material."""

    descriptor = hashlib.sha256()
    descriptor.update(_TAX_POLICY_DESCRIPTOR_HASH_DOMAIN)
    for value in (
        token_policy_id,
        _TAX_NORMALIZATION_CONTRACT,
        _TAX_HMAC_CONTRACT,
        _TAX_CANDIDATE_PREFIX_CONTRACT,
        _TAX_AUTHORITY_CONTRACT,
    ):
        encoded_value = value.encode("ascii")
        descriptor.update(struct.pack(">I", len(encoded_value)))
        descriptor.update(encoded_value)
    return descriptor.hexdigest()


def _validate_tax_identity_source_map(
    fields: Mapping[str, Any],
    failures: list[str],
) -> None:
    """Validate the authenticated source-shard ordinal map."""

    source_ordinal_map = fields.get("source_ordinal_map")
    source_shard_count = _optional_int(fields.get("source_shard_count"))
    if not _is_valid_tax_source_ordinal_map(
        source_ordinal_map,
        source_shard_count,
        str(fields.get("source_ordinal_map_digest") or ""),
    ):
        failures.append("provider tax-identity source ordinal map is invalid")


def _validate_tax_identity_counts(
    fields: Mapping[str, Any],
    exact_counts: Mapping[str, Any],
    failures: list[str],
) -> None:
    """Reconcile manifest counts against exact snapshot-owned rows."""

    count_field_by_exact_field = {
        "provider_group_count": "provider_group_count",
        "tax_identity_count": "provider_tax_identity_count",
        "matched_ein_count": "provider_tax_matched_ein_count",
        "missing_count": "provider_tax_missing_count",
        "malformed_count": "provider_tax_malformed_count",
        "unsupported_type_count": "provider_tax_unsupported_type_count",
    }
    if any(
        _optional_int(fields.get(manifest_field))
        != _optional_int(exact_counts.get(exact_field))
        for manifest_field, exact_field in count_field_by_exact_field.items()
    ):
        failures.append("provider tax-identity manifest counts differ from rows")
    if (
        _optional_int(exact_counts.get("provider_tax_identity_manifest_count")) != 1
        or _optional_int(exact_counts.get("provider_group_tax_identity_count"))
        != _optional_int(exact_counts.get("provider_group_count"))
        or _optional_int(exact_counts.get("provider_tax_referenced_identity_count"))
        != _optional_int(exact_counts.get("provider_tax_identity_count"))
    ):
        failures.append("provider tax-identity exact cardinality is invalid")


def _is_valid_tax_source_ordinal_map(
    source_ordinal_map: Any,
    source_shard_count: int | None,
    expected_digest: str,
) -> bool:
    """Authenticate the sorted contiguous source-shard ordinal map."""

    if (
        not isinstance(source_ordinal_map, list)
        or source_shard_count is None
        or source_shard_count <= 0
        or len(source_ordinal_map) != source_shard_count
    ):
        return False
    shard_ids: list[str] = []
    for ordinal, entry in enumerate(source_ordinal_map):
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"shard_id", "ordinal"}
            or entry.get("ordinal") != ordinal
            or not isinstance(entry.get("shard_id"), str)
            or not entry["shard_id"]
        ):
            return False
        shard_ids.append(str(entry["shard_id"]))
    if shard_ids != sorted(set(shard_ids)):
        return False
    digest = hashlib.sha256()
    digest.update(_TAX_SOURCE_ORDINAL_HASH_DOMAIN)
    digest.update(struct.pack(">I", len(shard_ids)))
    for ordinal, shard_id in enumerate(shard_ids):
        encoded_shard_id = shard_id.encode("utf-8")
        digest.update(struct.pack(">I", len(encoded_shard_id)))
        digest.update(encoded_shard_id)
        digest.update(struct.pack(">I", ordinal))
    return digest.hexdigest() == expected_digest


def _validate_cas_attribution(
    storage: Mapping[str, Any],
    failures: list[str],
) -> None:
    cas = _mapping(storage.get("cas"))
    if (
        cas.get("reference_source") != "direct_rows_plus_authenticated_v4_map_payloads"
        or cas.get("reference_population") != REFERENCE_POPULATION
    ):
        failures.append("CAS attribution population or reachability source is invalid")
    for field_name in (
        "distinct_referenced_block_count",
        "new_during_import_block_count",
        "preexisting_reused_block_count",
        "shared_block_count",
    ):
        if _optional_int(cas.get(field_name)) is None:
            failures.append(f"CAS attribution lacks {field_name}")


def _safe_snapshot_summary(snapshot: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field_name: snapshot.get(field_name)
        for field_name in (
            "snapshot_id",
            "import_run_id",
            "snapshot_status",
            "published_at",
            "snapshot_key",
            "layout_state",
            "layout_generation",
            "layout_logical_byte_count",
        )
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [dict(row) for row in value or [] if isinstance(row, Mapping)]


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None
