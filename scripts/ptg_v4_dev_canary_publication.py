"""Publication and storage evidence checks for the PTG V4 canary."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_GRAPH_RESOURCE_FIELDS,
)
from scripts.ptg_v4_dev_canary_cas import REFERENCE_POPULATION


STORAGE_EVIDENCE_CONTRACT = "ptg_v4_physical_storage_v1"
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
        *REQUIRED_PHYSICAL_RELATIONS,
    }
)


def evaluate_v4_evidence(
    evidence_by_field: Mapping[str, Any],
    *,
    expected_representation: str,
    expected_root_counts: Mapping[str, int],
    expected_relation_counts: Mapping[str, int],
    maximum_graph_physical_storage_bytes: int,
    maximum_snapshot_physical_storage_bytes: int,
) -> dict[str, Any]:
    """Reconcile exact V4 rows and gate attributable physical PostgreSQL bytes."""

    failures: list[str] = []
    snapshot = _mapping(evidence_by_field.get("snapshot"))
    root = _mapping(evidence_by_field.get("root"))
    exact_counts = _mapping(evidence_by_field.get("exact_counts"))
    relations = _mapping_rows(evidence_by_field.get("relations"))
    provider_graph_diagnostic = _mapping(
        evidence_by_field.get("provider_graph_diagnostic")
    )
    physical_storage = _mapping(evidence_by_field.get("physical_storage"))
    _validate_v4_state(snapshot, root, expected_representation, failures)
    _reconcile_exact_counts(root, exact_counts, failures)
    _validate_provider_graph_diagnostic(
        provider_graph_diagnostic,
        exact_counts=exact_counts,
        relations=relations,
        failures=failures,
    )
    _validate_declared_counts(
        root,
        relations,
        expected_root_counts,
        expected_relation_counts,
        failures,
    )
    manifest_summary = _manifest_summary(
        snapshot,
        root,
        provider_graph_diagnostic,
        failures,
    )
    _validate_physical_storage(
        physical_storage,
        maximum_graph_physical_storage_bytes,
        maximum_snapshot_physical_storage_bytes,
        failures,
    )
    return {
        "passed": not failures,
        "failures": failures,
        "snapshot": _safe_snapshot_summary(snapshot),
        "root": dict(root),
        "exact_counts": dict(exact_counts),
        "relations": relations,
        "heavy_owner_diagnostics": evidence_by_field.get("heavy_owners", []),
        "provider_graph_diagnostic": provider_graph_diagnostic,
        "manifest": manifest_summary,
        "physical_storage": physical_storage,
    }


def _validate_v4_state(
    snapshot: Mapping[str, Any],
    root: Mapping[str, Any],
    expected_representation: str,
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
    if root.get("representation") != expected_representation:
        failures.append("V4 representation differs from the expected layout")


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
        or (_optional_int(resources.get("compressed_acquisition_bytes")) or 0)
        <= 0
        or _optional_int(resources.get("input_factor_bytes")) is None
        or _optional_int(resources.get("input_factor_bytes")) < 0
        or _optional_int(resources.get("factor_edge_count")) is None
        or _optional_int(resources.get("factor_edge_count")) < 0
        or _optional_int(
            resources.get("empty_npi_tin_only_normalization_count")
        )
        is None
        or _optional_int(
            resources.get("empty_npi_tin_only_normalization_count")
        )
        < 0
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
        _optional_int(worst_prefix.get("member_count")),
        worst_prefix.get("member_digest"),
    ) if worst_prefix is not None else None
    if (
        bool(diagnostic.get("worst_uses_override"))
        != (worst_prefix is not None)
        or (
            bool(diagnostic.get("worst_uses_override"))
            and actual_worst_prefix != expected_worst_prefix
        )
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
        if relation is None or _optional_int(relation.get(field_name)) != expected_count:
            failures.append(f"relation {scoped_field} differs from declared expectation")


def _manifest_summary(
    snapshot: Mapping[str, Any],
    root: Mapping[str, Any],
    diagnostic_evidence: Mapping[str, Any],
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
        "hot_prefix": hot_prefix,
        "resource_admission": resource_admission,
        "manifest_storage_bytes_informational": _optional_int(
            serving_index.get("storage_bytes")
        ),
        "layout_logical_byte_count_informational": _optional_int(
            snapshot.get("layout_logical_byte_count")
        ),
    }


def _validate_physical_storage(
    storage: Mapping[str, Any],
    maximum_graph_bytes: int,
    maximum_snapshot_bytes: int,
    failures: list[str],
) -> None:
    if storage.get("contract") != STORAGE_EVIDENCE_CONTRACT:
        failures.append("physical storage evidence contract is missing")
        return
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
    _validate_cas_attribution(storage, failures)
    graph_gate_bytes = _optional_int(storage.get("graph_gate_bytes"))
    if graph_gate_bytes is None or graph_gate_bytes > maximum_graph_bytes:
        failures.append("attributable V4 graph storage exceeds its configured maximum")
    snapshot_gate_bytes = _optional_int(storage.get("snapshot_gate_bytes"))
    if snapshot_gate_bytes is None or snapshot_gate_bytes > maximum_snapshot_bytes:
        failures.append("whole-snapshot physical storage exceeds its configured maximum")
    if storage.get("storage_claim_scope") != "whole_snapshot_and_v4_graph":
        failures.append("physical storage evidence is graph-only or ambiguously scoped")


def _validate_cas_attribution(
    storage: Mapping[str, Any],
    failures: list[str],
) -> None:
    cas = _mapping(storage.get("cas"))
    if (
        cas.get("reference_source")
        != "direct_rows_plus_authenticated_v4_map_payloads"
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
