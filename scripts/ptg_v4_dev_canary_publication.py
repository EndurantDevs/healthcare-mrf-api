"""Publication and storage evidence checks for the PTG V4 canary."""

from __future__ import annotations

import hashlib
import re
import struct
from typing import Any, Mapping, Sequence

from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2ManifestArtifactError,
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
    UNAPPROVED_STORAGE_CEILING_FAILURE,
    StorageBudget,
)


STORAGE_EVIDENCE_CONTRACT = "ptg_v4_physical_storage_v1"
_TAX_IDENTITY_CONTRACT = "ptg2_provider_group_tax_identity_v1"
_TAX_NORMALIZATION_CONTRACT = "ein_ascii_digits_or_2_7_hyphen_v1"
_TAX_HMAC_CONTRACT = "hmac_sha256_ptg_tin_v1"
_TAX_CANDIDATE_PREFIX_CONTRACT = (
    "tin_id_128=first_16_bytes(tin_hmac_sha256)"
)
_TAX_AUTHORITY_CONTRACT = "tin_hmac_sha256_full_32_bytes_authoritative"
_TAX_SOURCE_ORDINAL_CONTRACT = "snapshot_shard_id_sorted_lsb0_bitmap_v1"
_TAX_POLICY_DESCRIPTOR_HASH_DOMAIN = b"PTG2V4TINPOLICY\x01"
_TAX_SOURCE_ORDINAL_HASH_DOMAIN = b"PTG2V4TAXORD\x01"
_TAX_POLICY_ID = re.compile(
    r"^ptg-tin-hmac-sha256-v1:[a-z0-9][a-z0-9._-]{0,31}$"
)
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
        "ptg2_provider_tax_identity_legacy_layout",
        *REQUIRED_PHYSICAL_RELATIONS,
    }
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
    snapshot = _mapping(evidence_by_field.get("snapshot"))
    root = _mapping(evidence_by_field.get("root"))
    exact_counts = _mapping(evidence_by_field.get("exact_counts"))
    relations = _mapping_rows(evidence_by_field.get("relations"))
    provider_graph_diagnostic = _mapping(
        evidence_by_field.get("provider_graph_diagnostic")
    )
    inferred_taxonomy_candidates = _mapping(
        evidence_by_field.get("inferred_taxonomy_candidates")
    )
    provider_tax_identity = _mapping(
        evidence_by_field.get("provider_tax_identity")
    )
    physical_storage = _mapping(evidence_by_field.get("physical_storage"))
    _validate_v4_state(
        snapshot,
        root,
        storage_budget.case.expected_representation,
        failures,
    )
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
    inferred_taxonomy_summary = _validate_inferred_taxonomy_candidates(
        snapshot,
        inferred_taxonomy_candidates,
        exact_counts,
        failures,
        expected_representation=storage_budget.case.expected_representation,
    )
    _validate_provider_tax_identity(
        provider_tax_identity,
        exact_counts,
        failures,
    )
    manifest_summary = _manifest_summary(
        snapshot,
        root,
        provider_graph_diagnostic,
        inferred_taxonomy_summary,
        failures,
    )
    _validate_physical_storage(
        physical_storage,
        storage_budget,
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
        "inferred_taxonomy_candidates": inferred_taxonomy_candidates,
        "provider_tax_identity": provider_tax_identity,
        "manifest": manifest_summary,
        "physical_storage": physical_storage,
        "storage_budget": storage_budget.report(
            graph_gate_bytes=_optional_int(
                physical_storage.get("graph_gate_bytes")
            ),
            snapshot_gate_bytes=_optional_int(
                physical_storage.get("snapshot_gate_bytes")
            ),
            measurement_image_identity=measurement_image_identity,
        ),
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
        "inferred_taxonomy_candidates": dict(inferred_taxonomy_summary),
        "manifest_storage_bytes_informational": _optional_int(
            serving_index.get("storage_bytes")
        ),
        "layout_logical_byte_count_informational": _optional_int(
            snapshot.get("layout_logical_byte_count")
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

    layout_manifest = _mapping(snapshot.get("layout_manifest"))
    serving_index = _mapping(layout_manifest.get("serving_index"))
    serving_binary = _mapping(serving_index.get("serving_binary"))
    provider_graph = _mapping(serving_binary.get("provider_graph_v4"))
    has_advertised_projection = (
        "inferred_taxonomy_candidates" in provider_graph
    )
    advertised_projection_value = provider_graph.get(
        "inferred_taxonomy_candidates"
    )
    exact_by_manifest_field = {
        "rule_count": _optional_int(
            exact_counts.get("inferred_taxonomy_rule_count")
        ),
        "observe_only_rule_count": _optional_int(
            exact_counts.get(
                "inferred_taxonomy_observe_only_rule_count"
            )
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
            exact_counts.get(
                "inferred_taxonomy_pattern_payload_byte_count"
            )
        ),
    }
    exact_counts_are_complete = all(
        count is not None for count in exact_by_manifest_field.values()
    )

    if not has_advertised_projection:
        if not exact_counts_are_complete or any(
            count != 0 for count in exact_by_manifest_field.values()
        ):
            failures.append(
                "inferred-taxonomy rows exist without a serving manifest"
            )
        if projection_evidence:
            failures.append(
                "inferred-taxonomy database evidence exists without rows"
            )
        return {"advertised": False}

    if not isinstance(advertised_projection_value, Mapping):
        failures.append("inferred-taxonomy serving manifest is malformed")
        advertised_projection_map: dict[str, Any] = {}
    else:
        try:
            advertised_projection_map = (
                validate_v4_inferred_taxonomy_projection_manifest(
                    advertised_projection_value
                )
            )
        except PTG2ManifestArtifactError:
            failures.append(
                "inferred-taxonomy serving manifest is invalid"
            )
            advertised_projection_map = {}

    try:
        evidence_projection_map = (
            validate_v4_inferred_taxonomy_projection_manifest(
                projection_evidence
            )
        )
    except PTG2ManifestArtifactError:
        failures.append("inferred-taxonomy database projection is invalid")
        evidence_projection_map = {}

    if (
        not exact_counts_are_complete
        or not evidence_projection_map
        or any(
            exact_by_manifest_field[field_name]
            != _optional_int(evidence_projection_map.get(field_name))
            for field_name in exact_by_manifest_field
        )
    ):
        failures.append(
            "inferred-taxonomy projection counts differ from exact snapshot rows"
        )

    if (
        advertised_projection_map
        and evidence_projection_map
        and advertised_projection_map != evidence_projection_map
    ):
        failures.append(
            "inferred-taxonomy serving manifest differs from database projection"
        )

    expected_rule_representation = (
        str(expected_representation or provider_graph.get("representation") or "")
    )
    nonempty_rules = tuple(
        rule
        for rule in evidence_projection_map.get("rules", ())
        if _optional_int(rule.get("member_count")) not in (None, 0)
    )
    if expected_rule_representation in {
        PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
        PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
    } and any(
        rule.get("representation") != expected_rule_representation
        for rule in nonempty_rules
    ):
        failures.append(
            "inferred-taxonomy nonempty rules do not use the selected graph representation"
        )
    if expected_rule_representation == (
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

    return {
        "advertised": True,
        **(
            evidence_projection_map
            if evidence_projection_map
            else dict(projection_evidence)
        ),
    }


def _validate_physical_storage(
    storage: Mapping[str, Any],
    storage_budget: StorageBudget,
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
    snapshot_gate_bytes = _optional_int(storage.get("snapshot_gate_bytes"))
    if graph_gate_bytes is None:
        failures.append("attributable V4 graph storage measurement is missing")
    if snapshot_gate_bytes is None:
        failures.append("whole-snapshot physical storage measurement is missing")
    if not storage_budget.is_promotion_approved:
        failures.append(UNAPPROVED_STORAGE_CEILING_FAILURE)
    elif (
        graph_gate_bytes is not None
        and graph_gate_bytes
        > int(storage_budget.maximum_graph_physical_storage_bytes or 0)
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
    if storage.get("storage_claim_scope") != "whole_snapshot_and_v4_graph":
        failures.append("physical storage evidence is graph-only or ambiguously scoped")


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
        _optional_int(exact_counts.get("provider_tax_identity_manifest_count"))
        != 1
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
