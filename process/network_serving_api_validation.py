# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate bounded API metadata without replaying historical edge semantics."""

import hashlib
import json
import re
from datetime import datetime

from process import entity_address_snapshot_receipt as shape
from process import network_candidate_custody as custody
from process import network_compact_readiness as compact
from process import network_indexed_address_artifact as indexed
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_compact_custom_preparation import compact_carrier_receipt, compact_custom_receipt
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_membership_copy import MembershipCopyTarget
from process.network_membership_serving_indexes import _serving_plan
from process.network_membership_writer_closure import _protected_owner, _receipt
from process.network_serving_read import NetworkServingReadUnavailable, _pinned_manifest
from process.registry_management_permissions import _TABLES
from process.registry_source_contribution_store import CONTRIBUTION_TABLE, verify_registry_source_contributions
from process.registry_source_inventory_reference import source_inventory_heaps
from process.registry_source_retention import SOURCE_TABLE, verify_registry_source_retention


def _require(condition):
    if condition is not True:
        raise NetworkServingReadUnavailable("Protected API serving metadata is unavailable")


def _document(value):
    if type(value) is str:
        return json.loads(value)
    _require(type(value) is dict)
    return value


def _candidate(document, created_at=None):
    candidate = _document(document)
    created_at = candidate["created_at"] if created_at is None else created_at
    candidate["created_at"] = datetime.fromisoformat(created_at) if type(created_at) is str else created_at
    return candidate


def _candidate_catalog(family, control, candidate=None):
    current = family["catalog"]
    relations = current["relations"]
    _require(0 < len(relations) <= 128 and current["has_routines"] is False and current["heaps_safe"] is True)
    _require(current["schema_owner_oid"] == current["owner_oid"] == control["owner_oid"])
    _require(_protected_owner(current["owner"]))
    heap_oids_by_name = {
        relation_row["relname"]: relation_row["oid"] for relation_row in relations if relation_row["relkind"] == "r"
    }
    if candidate is None:
        _require(
            {"network_membership", "provider_location_binding", "network_slice_ref", "network_office_edge"}
            <= set(heap_oids_by_name)
        )
    else:
        required = {"network_membership", "provider_location_binding"} | source_inventory_heaps(candidate)
        if verify_registry_source_retention(candidate) is not None:
            required.add(SOURCE_TABLE)
        if verify_registry_source_contributions(candidate) is not None:
            required.add(CONTRIBUTION_TABLE)
        _require(set(heap_oids_by_name) == required | compact.compact_serving_heaps(candidate))
    for relation in relations:
        _require(relation["relkind"] in ("r", "i") and relation["relpersistence"] == "p")
        _require(relation["relowner"] == control["owner_oid"])
        if relation["relkind"] == "i":
            _require(relation["indrelid"] in heap_oids_by_name.values() and relation["relacl"] is None)
    converted_relations = [
        relation_row
        | {"relkind": relation_row["relkind"].encode(), "relpersistence": relation_row["relpersistence"].encode()}
        for relation_row in relations
    ]
    return {
        "namespace": {"oid": current["schema_oid"], "nspowner": current["schema_owner_oid"]},
        "relations": converted_relations,
        "heaps": heap_oids_by_name,
    }


def _full_candidate_custody(family, control):
    candidate = _candidate(family["candidate"])
    _require(candidate["state"] in ("ready", "published"))
    receipt = custody._sealed_receipt(
        {"receipt_json": json.dumps(family["receipt_json"]), "seal_sha256": family["seal_sha256"]}
    )
    _require(receipt["revision"] in (4, 5) and compact.is_compact_candidate(candidate))
    if receipt["revision"] == 5:
        from process.network_sampled_custody import validate_sampled_custody

        validate_sampled_custody(receipt["semantic_validation"], candidate_id=str(candidate["candidate_id"]))
    _require(receipt["control"] == custody._control_identity(control, receipt["revision"]))
    _require(receipt["candidate_sha256"] == custody._digest(custody._evidence(candidate, revision=receipt["revision"])))
    roles = receipt["runtime_roles"]
    _require(type(roles) is list and 0 < len(roles) <= 128 and roles == sorted(set(roles)))
    for role in roles:
        _identifier(role)
    closure = receipt["closure"]
    _require(type(closure) is dict and type(closure["reader_roles"]) is list and closure["loader_roles"] == [])
    _require(closure["owner_role"] == control["rolname"] and set(closure["reader_roles"]) <= set(roles))
    copy_target = MembershipCopyTarget(*(str(candidate[field]) for field in custody._STABLE[:5]))
    _require(
        _receipt(
            copy_target,
            custody._final_roles(control["rolname"], tuple(closure["reader_roles"])),
            _candidate_catalog(family, control, candidate),
        )
        == closure
    )
    _require(all(family[field] is True for field in ("closed", "q_reach_closed", "census_closed")))
    indexes = family["indexes"]
    _require(
        bool(indexes)
        and all(all(index[flag] is True for flag in ("indisvalid", "indisready", "indislive")) for index in indexes)
    )
    compact._require_index_paths(indexes, candidate)
    _require(compact._digest(indexes) == receipt["index_definition_sha256"])
    return candidate, {"receipt": receipt, "seal_sha256": family["seal_sha256"]}


def _retained_edge_metadata(family, control, expected):
    """Authenticate an API-only ledger projection, not a newly rehashed historical seal."""
    candidate = _document(family["candidate"])
    _require(set(candidate) == set(custody._STABLE[:5]) | {"state"})
    _require(candidate["state"] in ("ready", "published") and family["projected_edge"] is True)
    _require(candidate["candidate_id"] == str(family["candidate_id"]) == expected["candidate_id"])
    _require(candidate["schema_name"] == expected["schema_name"])
    _require(type(expected["seal_sha256"]) is str and re.fullmatch("[0-9a-f]{64}", expected["seal_sha256"]) is not None)
    _require(family["seal_sha256"] == expected["seal_sha256"])
    receipt = _document(family["receipt_json"])
    _require(
        set(receipt) == {"component", "revision", "control", "closure", "index_definition_sha256", "runtime_roles"}
    )
    _require(
        receipt["component"] == "network_candidate_custody"
        and type(receipt["revision"]) is int
        and receipt["revision"] in (4, 5)
    )
    _require(receipt["control"] == custody._control_identity(control, receipt["revision"]))
    _require(
        type(receipt["index_definition_sha256"]) is str
        and re.fullmatch("[0-9a-f]{64}", receipt["index_definition_sha256"]) is not None
    )
    roles = receipt["runtime_roles"]
    _require(type(roles) is list and 0 < len(roles) <= 128 and roles == sorted(set(roles)))
    for role in roles:
        _identifier(role)
    closure = receipt["closure"]
    _require(type(closure) is dict and type(closure["reader_roles"]) is list and closure["loader_roles"] == [])
    _require(closure["owner_role"] == control["rolname"] and set(closure["reader_roles"]) <= set(roles))
    copy_target = MembershipCopyTarget(*(str(candidate[field]) for field in custody._STABLE[:5]))
    _require(
        _receipt(
            copy_target,
            custody._final_roles(control["rolname"], tuple(closure["reader_roles"])),
            _candidate_catalog(family, control),
        )
        == closure
    )
    _require(closure["relation_oids"]["network_office_edge"] == expected["table_oid"])
    _require(all(family[field] is True for field in ("closed", "q_reach_closed", "census_closed")))
    indexes = family["indexes"]
    _require(
        bool(indexes)
        and all(all(index[flag] is True for flag in ("indisvalid", "indisready", "indislive")) for index in indexes)
    )
    compact._require_index_paths(indexes, None)
    _require(compact._digest(indexes) == receipt["index_definition_sha256"])
    return candidate, {"receipt": receipt, "seal_sha256": family["seal_sha256"], "api_projected_edge": True}


def _verified_control_pins(metadata_entry, control, schema):
    _require(control["owned"] is True and control["closed"] is True and _protected_owner(control))
    tables = (
        _document(metadata_entry["control_tables_json"])
        if type(metadata_entry["control_tables_json"]) is dict
        else json.loads(metadata_entry["control_tables_json"])
    )
    expected_table_names = {
        "network_membership_candidate",
        "network_serving_manifest",
        "registry_approved_record",
        "network_candidate_custody",
        "network_serving_control",
    }
    _require(len(tables) == 5 and {table["relname"] for table in tables} == expected_table_names)
    for table in tables:
        expected_owner = (
            control["owner_oid"] if table["relname"] == "network_candidate_custody" else control["schema_owner_oid"]
        )
        _require(table["relowner"] == expected_owner)
        _require(table["relkind"] == "r" and table["relpersistence"] == "p" and table["heap"] is True)
        _require(
            not any(
                table[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "inherited", "rules")
            )
        )
        _require(tuple(table["columns"]) == _TABLES[table["relname"]])
    _require(
        next(table["oid"] for table in tables if table["relname"] == "network_membership_candidate")
        == control["candidate_table_oid"]
    )
    _require(
        next(table["oid"] for table in tables if table["relname"] == "network_candidate_custody")
        == control["seal_table_oid"]
    )
    _require(metadata_entry["control_consumed"] is True)
    return CompactAddressRelation(
        schema,
        "registry_approved_record",
        next(table["oid"] for table in tables if table["relname"] == "registry_approved_record"),
    )


def _serving_digest(records, schema):
    by_name = {row["relname"]: row for row in records}
    _require(all(name in by_name for name, _ in _serving_plan(schema)))
    for record in records:
        _require(record["indrelid"] == record["table_oid"] and record["index_oid"] > 0)
        _require(all(record[flag] is True for flag in ("indisvalid", "indisready", "indislive")))
    definitions = sorted(row["index_definition"] for row in records)
    return hashlib.sha256(json.dumps(definitions, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _validated_indexed_base(metadata_entry, descriptor, candidate):
    evidence = _document(metadata_entry["indexed_base_json"])
    safety = _document(metadata_entry["indexed_actor_safety_json"])
    _require(
        all(
            safety[key] is True
            for key in ("descriptor_roles_safe", "ordinary_mutation_closed", "owner_safe", "runtime_closure_safe")
        )
    )
    _require(
        all(evidence[key] is True for key in ("native_structure_safe", "native_execution_safe", "native_subjects_safe"))
    )
    _require(1 <= len(evidence["subjects"]) <= 33 and evidence["subjects"] == sorted(set(evidence["subjects"])))
    _require(evidence["original_path"] == evidence["restored_path"])
    physical = evidence["physical"]
    _require(physical["relkind"] == "r" and physical["relpersistence"] == "p")
    _require(
        not any(
            physical[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition", "rules", "inherited")
        )
    )
    _require(physical["relname"] == "entity_address_unified")
    _require(descriptor["database"] == {"oid": metadata_entry["database_oid"], "name": metadata_entry["database_name"]})
    _require(
        descriptor["namespace"]
        == {"name": physical["schema_name"], "oid": physical["schema_oid"], "owner_oid": physical["owner_oid"]}
    )
    _require(descriptor["table"] == {"oid": physical["oid"], "relfilenode": physical["relfilenode"]})
    report = _document(candidate["validation_json"])
    closure = report["writer_closure"]
    _require(descriptor["runtime_roles"] == sorted(set(closure["loader_roles"] + closure["reader_roles"])))
    _require_indexed_catalog(evidence, descriptor)
    _require(
        descriptor["index_definition_sha256"]
        in {
            _serving_digest(evidence["qualified_index_records"], physical["schema_name"]),
            _serving_digest(evidence["legacy_index_records"], physical["schema_name"]),
        }
    )
    return CompactAddressRelation(physical["schema_name"], physical["relname"], physical["oid"])


def _require_indexed_catalog(evidence, descriptor):
    columns, constraints, indexes = (evidence[key] for key in ("shape_columns", "shape_constraints", "shape_indexes"))
    _require(bool(columns))
    indexes = sorted(
        indexes, key=lambda entry: json.dumps(entry, sort_keys=True, separators=(",", ":"), default=shape._json_scalar)
    )
    shape._reject_schema_qualified_expressions(evidence["schema_name"], columns, constraints, indexes)
    digest = shape._canonical_digest(
        {"table_name": evidence["table_name"], "columns": columns, "constraints": constraints, "indexes": indexes}
    )
    catalog_by_kind = {
        "shapes": [[evidence["table_name"], evidence["relation_oid"], digest]],
        **{kind: evidence["catalog_" + kind] for kind in ("indexes", "columns", "constraints")},
    }
    _require(catalog_by_kind == descriptor["catalog"])


def _verified_compact_layout(candidate, base, descriptor, families):
    report = _document(candidate["validation_json"])
    serving_by_field = report["serving_readiness"]
    canonical_by_field = report["candidate_readiness"]
    closure_by_field = report["writer_closure"]
    _require(closure_by_field["relation_oids"].get("network_slice_ref") == canonical_by_field["ref_map_oid"])
    _require(closure_by_field["relation_oids"].get("network_office_edge") == canonical_by_field["edge_heap_oid"])
    _require(base.table_oid == descriptor["table"]["oid"])
    artifacts = []
    for expected in serving_by_field["edge_artifacts"]:
        _require(
            type(expected) is dict and set(expected) == {"candidate_id", "schema_name", "table_oid", "seal_sha256"}
        )
        artifact = CompactEdgeArtifact(expected["candidate_id"], expected["schema_name"], expected["table_oid"])
        if artifact.candidate_id == str(candidate["candidate_id"]):
            _require(expected["seal_sha256"] is None and artifact.table_oid == canonical_by_field["edge_heap_oid"])
        else:
            retained, seal = families[artifact.candidate_id]
            _require(artifact.schema_name == retained["schema_name"] and expected["seal_sha256"] == seal["seal_sha256"])
            _require(artifact.table_oid == seal["receipt"]["closure"]["relation_oids"]["network_office_edge"])
        artifacts.append(artifact)
    custom = _custom_address_relation(candidate, descriptor)
    return CompactNetworkLayout(
        CompactAddressRelation(candidate["schema_name"], "network_slice_ref", canonical_by_field["ref_map_oid"]),
        base,
        tuple(artifacts),
        custom,
    )


def _custom_address_relation(candidate, descriptor):
    report = _document(candidate["validation_json"])
    closure = report["writer_closure"]
    custom, carrier = compact_custom_receipt(candidate), compact_carrier_receipt(candidate)
    if custom is None:
        return None
    _require(custom.base_source == PinnedAddressSource(**report["address_source"]))
    _require(custom.base_artifact_sha256 == descriptor["descriptor_sha256"])
    _require(custom.binding_table_oid == closure["relation_oids"].get("provider_location_binding"))
    _require(custom.runtime_roles == tuple(sorted(set(closure["loader_roles"] + closure["reader_roles"]))))
    address_oid = custom.address_table_oid
    if carrier is not None:
        _require(carrier.base_source == custom.base_source and carrier.runtime_roles == custom.runtime_roles)
        _require(
            carrier.base_artifact_sha256 == custom.base_artifact_sha256
            and carrier.binding_table_oid == custom.binding_table_oid
        )
        _require(not custom.custom_pair_count or address_oid == carrier.address_table_oid)
        address_oid = carrier.address_table_oid
    if custom.custom_pair_count or carrier is not None and carrier.materialized_address_rows:
        _require(address_oid == closure["relation_oids"].get("entity_address_unified"))
        return CompactAddressRelation(candidate["schema_name"], "entity_address_unified", address_oid)
    return None


def validate_api_serving_pin(metadata_entry, *, schema, backend_pid, reader_identity, generation_id=None):
    """Authenticate one result against a separately verified original Reader identity."""
    _require(metadata_entry is not None and metadata_entry["backend_pid"] == backend_pid)
    _require(metadata_entry["isolation"] in ("repeatable read", "serializable") and metadata_entry["read_only"] == "on")
    _require(metadata_entry["session_user"] == metadata_entry["current_user"])
    identity_fields = ("role_oid", "database_oid", "server_address", "server_port", "backend_pid")
    _require(type(reader_identity) is dict and set(reader_identity) == set(identity_fields))
    _require(all(metadata_entry[field] == reader_identity[field] for field in identity_fields))
    control = _document(metadata_entry["control_json"])
    relation = _verified_control_pins(metadata_entry, control, schema)
    candidate = _candidate(metadata_entry["candidate_json"], metadata_entry["created_at"])
    _require(candidate["state"] == "published")
    descriptor = indexed._descriptor(_document(metadata_entry["indexed_descriptor_json"]))
    family_rows = json.loads(metadata_entry["families_json"])
    _require(len(family_rows) <= 129 and metadata_entry["edge_candidate_count"] <= 128)
    by_id = {str(family["candidate_id"]): family for family in family_rows}
    current_id = str(candidate["candidate_id"])
    _require(len(by_id) == len(family_rows) and current_id in by_id)
    _require(by_id[current_id]["projected_edge"] is False)
    verified_families_by_id = {current_id: _full_candidate_custody(by_id[current_id], control)}
    _require(verified_families_by_id[current_id][0] == candidate)
    edge_artifacts_by_id = {
        artifact["candidate_id"]: artifact
        for artifact in _document(candidate["validation_json"])["serving_readiness"]["edge_artifacts"]
    }
    _require(set(by_id) == set(edge_artifacts_by_id) | {current_id})
    for retained_id in set(by_id) - {current_id}:
        verified_families_by_id[retained_id] = _retained_edge_metadata(
            by_id[retained_id], control, edge_artifacts_by_id[retained_id]
        )
    base = _validated_indexed_base(metadata_entry, descriptor, candidate)
    layout = _verified_compact_layout(candidate, base, descriptor, verified_families_by_id)
    seal = verified_families_by_id[str(candidate["candidate_id"])][1]
    manifest = _pinned_manifest(metadata_entry, custody=seal | {"compact_layout": layout})
    _require(generation_id is None or manifest.generation_id == generation_id)
    _require(_document(metadata_entry["manifest_json"])["eligible"] is True)
    approved = _document(metadata_entry["approved_json"])
    _require(
        approved["is_bounded"] is True and approved["generation_id"] == manifest.source_generations["custom_membership"]
    )
    approved_source = ApprovedMembershipSource(
        manifest.approved_custom_revision, approved["generation_id"], approved["total_rows"] or 0
    )
    return manifest, approved_source, relation
