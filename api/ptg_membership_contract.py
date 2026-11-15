# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed physical-source/code identity for the derived membership family."""

import hashlib
import json
import re
from dataclasses import asdict
from uuid import UUID

from api.code_systems import canonical_catalog_code, normalize_code_system
from db.models.ptg_membership_projection import (
    PTGMembershipCode,
    PTGMembershipCodeCoverage,
    PTGMembershipFirst,
    PTGMembershipManifest,
    PTGMembershipSecond,
)
from process.reference_family_archive import ReferenceFamilySpec

CONTRACT = "ptg_provider_membership_v1"
IMPORTER = "ptg-provider-membership-projection"
SPEC = ReferenceFamilySpec(
    IMPORTER,
    (PTGMembershipManifest, PTGMembershipFirst, PTGMembershipSecond, PTGMembershipCode, PTGMembershipCodeCoverage),
)
MAX_BUILD_EDGES = 32_000_000
MAX_BUILD_BYTES = 512 * 1024 * 1024
MAX_CODE_OCCURRENCES = 200_000
MAX_BUILD_OWNERS = 8_000_000
MAX_MAP_BYTES = 512 * 1024 * 1024
MAX_FACTOR_BATCH_MEMBERS = 1_000_000


def canonical_code(code_system, code):
    """Reject aliases and return the exact canonical procedure selector."""
    system = normalize_code_system(code_system)
    normalized = canonical_catalog_code(system, code)
    if not system or not normalized or system != code_system or normalized != code:
        raise ValueError("membership index requires a canonical procedure code")
    return system, normalized


def validate_params(params):
    """Accept only the four nonempty canonical physical-source selectors."""
    names = {"source_key", "snapshot_id", "code_system", "code"}
    if (
        type(params) is not dict
        or set(params) != names
        or any(type(value) is not str or not value or value != value.strip() for value in params.values())
    ):
        raise ValueError("membership projection params must be exactly source_key, snapshot_id, code_system, code")
    canonical_code(params["code_system"], params["code"])


def encoded(value):
    """Serialize identity and accounting data deterministically without NaN."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def digest(value):
    """Hash the canonical serialization of an immutable identity."""
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def logical_receipt(receipt_by_field):
    """Project saved storage coordinates to names without changing the raw receipt."""
    named_by_field = logical_source_identity(receipt_by_field)
    attachment = receipt_by_field["active_attachment"]
    name = attachment["name"]
    namespace = attachment.get("schema_name", "hp_snapshot_retention")
    if (
        namespace != "hp_snapshot_retention"
        or not isinstance(name, str)
        or re.fullmatch(r"(?:pm|pc)_[a-z2-7]{52}", name) is None
    ):
        raise ValueError("projection attachment name is invalid")
    stored = receipt_by_field["storage"]
    dataset = UUID(str(stored["dataset_id"]))
    schema = "reference_family_archive_" + dataset.hex
    names = stored.get("relation_names")
    if names is None:
        names = [entry[0] for entry in stored["relation_oids"]]
    if (
        stored.get("schema_name", schema) != schema
        or not isinstance(names, (list, tuple))
        or not names
        or any(
            not isinstance(relation_name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", relation_name) is None
            for relation_name in names
        )
        or len(set(names)) != len(names)
    ):
        raise ValueError("projection storage names are invalid")
    named_by_field["active_attachment"] = {"schema_name": namespace, "name": name}
    named_by_field["storage"] = {"dataset_id": str(dataset), "schema_name": schema, "relation_names": sorted(names)}
    if "projection_id" in named_by_field:
        receipt_fields = {
            "state",
            "projection_id",
            "content_digest",
            "counts",
            "code_coverage",
            "active_attachment",
            "source_pin",
            "storage",
        }
        named_by_field["projection_id"] = digest(
            {field: saved for field, saved in named_by_field.items() if field not in receipt_fields}
        )
    return named_by_field


def _logical_source_binding(binding):
    from process.ptg_parts.ptg2_physical_binding import PTG2PhysicalBinding, PTG2PhysicalBindingError

    if not isinstance(binding, dict):
        raise ValueError("membership source binding names are invalid")
    has_legacy_coordinates = "schema_oid" in binding or "relation_oids" in binding
    binding_by_field = {
        field: saved
        for field, saved in binding.items()
        if field not in {"database_oid", "schema_oid", "owner_oid", "schema_owner_oid", "relfilenode"}
    }
    try:
        dataset = UUID(str(binding_by_field["dataset_id"]))
        schema = binding_by_field.setdefault("schema_name", "reference_family_archive_" + dataset.hex)
        relations = binding_by_field.get("relations", binding_by_field.pop("relation_oids", ()))
        sequences = binding_by_field.get("sequences", binding_by_field.pop("sequence_oids", ()))
        binding_by_field["relations"] = tuple(
            (schema, first) if has_legacy_coordinates else (first, second) for first, second in relations
        )
        binding_by_field["sequences"] = tuple(
            (schema, first, table, column) if has_legacy_coordinates else (first, second, table, column)
            for first, second, table, column in sequences
        )
        binding_by_field["dataset_id"] = dataset
        named_binding = PTG2PhysicalBinding(**binding_by_field)
    except (KeyError, IndexError, TypeError, ValueError, PTG2PhysicalBindingError) as error:
        raise ValueError("membership source binding names are invalid") from error
    return json.loads(encoded(asdict(named_binding) | {"dataset_id": str(named_binding.dataset_id)}))


def _logical_source_storage(stored, schema_name):
    if schema_name is None:
        from api.ptg2_serving import PTG2_SCHEMA

        schema_name = PTG2_SCHEMA
    schema = stored.get("schema_name", schema_name)
    names = stored.get("relation_names")
    if names is None:
        names = [entry[0] for entry in stored["relation_oids"]]
    if (
        not isinstance(schema, str)
        or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is None
        or not isinstance(names, (list, tuple))
        or not names
        or any(
            not isinstance(name, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", name) is None for name in names
        )
        or len(set(names)) != len(names)
    ):
        raise ValueError("membership source storage names are invalid")
    return {"schema_name": schema, "relation_names": sorted(names)}


def logical_source_identity(source_by_field, schema_name=None):
    """Keep source/data commitments while ignoring saved physical catalog tokens."""
    named_by_field = dict(source_by_field)
    binding = source_by_field.get("physical_binding")
    if binding is not None:
        named_by_field["physical_binding"] = _logical_source_binding(binding)
        schema_name = named_by_field["physical_binding"]["schema_name"]
    stored = source_by_field.get("source_storage")
    if stored is not None:
        named_by_field["source_storage"] = _logical_source_storage(stored, schema_name)
    return named_by_field


def physical_identity(tables):
    """Preserve the complete physical binding, or genuine legacy absence."""
    binding = getattr(tables, "physical_binding", None)
    if binding is None:
        return None
    document = asdict(binding)
    document["dataset_id"] = str(binding.dataset_id)
    return json.loads(encoded(document))


def identity(tables, root, finalizer, code_system, code):
    """Bind the named source, graph and finalizer roots to one canonical code."""
    system, normalized = canonical_code(code_system, code)
    return {
        "contract": CONTRACT,
        "source_key": tables.source_key,
        "snapshot_id": tables.snapshot_id,
        "physical_binding": physical_identity(tables),
        "graph_root": asdict(root) | {"map_digest": root.map_digest.hex()},
        "finalizer": finalizer,
        "code_system": system,
        "code": normalized,
    }
