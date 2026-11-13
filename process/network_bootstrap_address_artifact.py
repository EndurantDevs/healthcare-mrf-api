# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named immutable bootstrap inputs and their retained data accounting."""

import json
import re
from dataclasses import asdict
from uuid import UUID

from process import network_address_artifact as artifact
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation

CONTRACT = "network_bootstrap_address_artifact.postgres.v1"
_HEAPS = {"entity_address_unified", "npi"}
_FIELDS = {
    "contract",
    "revision",
    "namespace",
    "table",
    "heaps",
    "bootstrap_receipt",
    "generation_id",
    "descriptor_sha256",
}
_LEGACY_FIELDS = {
    "contract",
    "database",
    "namespace",
    "protected_owner_oid",
    "table",
    "heaps",
    "bootstrap_receipt",
    "generation_id",
    "native_sha256",
    "dormant_sequence_oid",
    "dependency_oids",
    "runtime_roles",
    "descriptor_sha256",
}
_RECEIPT_FIELDS = {"component", "revision", "bootstrap_id", "captured_at", "parent_receipts", "generation_sha256"}
_LEGACY_RECEIPT_FIELDS = {
    "component",
    "revision",
    "specification",
    "owner_role",
    "runtime_roles",
    "parent_receipts",
    "capture_snapshot",
    "captured_at",
    "schema_oid",
    "owner_oid",
    "relation_oids",
    "index_oids",
    "generation_sha256",
}
_PARENT_FIELDS = {"schema_name", "table_name", "row_count", "rows_sha256"}
_LEGACY_PARENT_FIELDS = _PARENT_FIELDS | {
    "schema_oid",
    "table_oid",
    "owner_oid",
    "authority_table_oid",
    "authority_json",
    "shape_sha256",
}


def is_bootstrap_source(source):
    """Select the existing immutable bootstrap namespace convention."""
    return re.fullmatch(r"network_bootstrap_[0-9a-f]{32}", source.schema_name) is not None


def _project_legacy_bootstrap_receipt(receipt_by_field):
    """Project the exact retained bootstrap encoding to logical names and counts."""
    specification = receipt_by_field["specification"]
    artifact._require(
        type(specification) is dict
        and set(specification)
        == {"bootstrap_id", "address_schema", "npi_schema", "address_table_oid", "npi_table_oid"},
        "bootstrap_receipt_invalid",
    )
    parents = []
    for parent in receipt_by_field["parent_receipts"]:
        artifact._require(type(parent) is dict and set(parent) == _LEGACY_PARENT_FIELDS, "bootstrap_receipt_invalid")
        parents.append({name: parent[name] for name in _PARENT_FIELDS})
    artifact._require(
        [parent["schema_name"] for parent in parents] == [specification["address_schema"], specification["npi_schema"]],
        "bootstrap_receipt_invalid",
    )
    receipt_by_field = {
        "component": "network_bootstrap_sources",
        "revision": 2,
        "bootstrap_id": specification["bootstrap_id"],
        "captured_at": receipt_by_field["captured_at"],
        "parent_receipts": parents,
        "generation_sha256": receipt_by_field["generation_sha256"],
    }
    return receipt_by_field


def _receipt(receipt_by_field):
    """Validate bounded parent names, logical edition and complete data accounting."""
    artifact._require(type(receipt_by_field) is dict, "bootstrap_receipt_invalid")
    if set(receipt_by_field) == _LEGACY_RECEIPT_FIELDS:
        receipt_by_field = _project_legacy_bootstrap_receipt(receipt_by_field)
    artifact._require(
        set(receipt_by_field) == _RECEIPT_FIELDS
        and receipt_by_field["component"] == "network_bootstrap_sources"
        and type(receipt_by_field["revision"]) is int
        and receipt_by_field["revision"] == 2,
        "bootstrap_receipt_invalid",
    )
    identity = receipt_by_field["bootstrap_id"]
    artifact._require(
        type(identity) is str and str(UUID(identity)) == identity and UUID(identity).int, "bootstrap_receipt_invalid"
    )
    artifact._require(
        type(receipt_by_field["captured_at"]) is str and bool(receipt_by_field["captured_at"]),
        "bootstrap_receipt_invalid",
    )
    artifact._sha(receipt_by_field["generation_sha256"])
    parents = receipt_by_field["parent_receipts"]
    artifact._require(type(parents) is list and len(parents) == 2, "bootstrap_receipt_invalid")
    for parent in parents:
        artifact._require(type(parent) is dict and set(parent) == _PARENT_FIELDS, "bootstrap_receipt_invalid")
        CompactAddressRelation(parent["schema_name"], parent["table_name"])
        artifact._require(
            type(parent["row_count"]) is int and 0 <= parent["row_count"] < 2**63, "bootstrap_receipt_invalid"
        )
        artifact._sha(parent["rows_sha256"])
        artifact._require(
            parent["schema_name"] != "network_bootstrap_" + UUID(identity).hex, "bootstrap_receipt_invalid"
        )
    artifact._require(
        [parent["table_name"] for parent in parents] == ["entity_address_unified", "npi"], "bootstrap_receipt_invalid"
    )
    return json.loads(artifact._json(receipt_by_field))


def _descriptor(binding_by_field):
    artifact._require(type(binding_by_field) is dict, "descriptor_invalid")
    if set(binding_by_field) == _LEGACY_FIELDS:
        receipt = _receipt(binding_by_field["bootstrap_receipt"])
        schema = "network_bootstrap_" + UUID(receipt["bootstrap_id"]).hex
        artifact._require(binding_by_field["namespace"]["name"] == schema, "descriptor_invalid")
        binding_by_field = {
            "contract": CONTRACT,
            "revision": 1,
            "namespace": {"name": schema},
            "table": asdict(CompactAddressRelation(schema, "entity_address_unified")),
            "heaps": {name: asdict(CompactAddressRelation(schema, name)) for name in sorted(_HEAPS)},
            "bootstrap_receipt": receipt,
            **{name: binding_by_field[name] for name in ("generation_id", "descriptor_sha256")},
        }
    artifact._require(
        set(binding_by_field) == _FIELDS and binding_by_field["contract"] == CONTRACT, "descriptor_invalid"
    )
    artifact._require(
        type(binding_by_field["revision"]) is int and binding_by_field["revision"] in (1, 2), "descriptor_invalid"
    )
    receipt = _receipt(binding_by_field["bootstrap_receipt"])
    artifact._require(receipt == binding_by_field["bootstrap_receipt"], "bootstrap_receipt_invalid")
    schema = "network_bootstrap_" + UUID(receipt["bootstrap_id"]).hex
    artifact._require(
        binding_by_field["namespace"] == {"name": schema}
        and binding_by_field["generation_id"] == receipt["generation_sha256"],
        "generation_invalid",
    )
    artifact._require(
        binding_by_field["heaps"] == {name: asdict(CompactAddressRelation(schema, name)) for name in sorted(_HEAPS)}
        and binding_by_field["table"] == binding_by_field["heaps"]["entity_address_unified"],
        "descriptor_invalid",
    )
    artifact._sha(binding_by_field["descriptor_sha256"])
    if binding_by_field["revision"] == 2:
        artifact._require(
            artifact._digest({key: entry for key, entry in binding_by_field.items() if key != "descriptor_sha256"})
            == binding_by_field["descriptor_sha256"],
            "descriptor_digest_invalid",
        )
    artifact._require(
        len(artifact._json(binding_by_field).encode()) <= artifact._MAX_DESCRIPTOR_BYTES, "descriptor_invalid"
    )
    return json.loads(artifact._json(binding_by_field))


async def capture_bootstrap_address_artifact(connection, address_source, *, runtime_roles=()):
    """Read the declared bootstrap edition and its original content counts."""
    artifact._require(
        type(address_source) is PinnedAddressSource
        and address_source.table_name == "entity_address_unified"
        and is_bootstrap_source(address_source),
        "source_invalid",
    )
    await artifact._require_capture_snapshot(connection)
    metadata = await connection.fetchval(
        "SELECT obj_description(oid,'pg_namespace') FROM pg_namespace WHERE nspname=$1", address_source.schema_name
    )
    artifact._require(
        type(metadata) is str and len(metadata.encode()) <= artifact._MAX_DESCRIPTOR_BYTES, "bootstrap_receipt_invalid"
    )
    receipt = _receipt(json.loads(metadata))
    artifact._require(
        address_source.schema_name == "network_bootstrap_" + UUID(receipt["bootstrap_id"]).hex
        and address_source.generation_id == receipt["generation_sha256"],
        "source_generation_changed",
    )
    descriptor_by_field = {
        "contract": CONTRACT,
        "revision": 2,
        "namespace": {"name": address_source.schema_name},
        "table": asdict(CompactAddressRelation(address_source.schema_name, address_source.table_name)),
        "heaps": {name: asdict(CompactAddressRelation(address_source.schema_name, name)) for name in sorted(_HEAPS)},
        "bootstrap_receipt": receipt,
        "generation_id": address_source.generation_id,
    }
    descriptor_by_field["descriptor_sha256"] = artifact._digest(descriptor_by_field)
    descriptor_by_field = _descriptor(descriptor_by_field)
    await verify_bootstrap_address_artifact(connection, descriptor_by_field)
    return descriptor_by_field


async def verify_bootstrap_address_artifact(connection, descriptor, *, runtime_roles=(), api_read=False):
    """Read only the qualified names belonging to this retained logical edition."""
    await artifact._require_api_read_context(connection, api_read)
    await artifact._require_capture_snapshot(connection)
    descriptor = _descriptor(descriptor)
    for relation in descriptor["heaps"].values():
        await connection.fetch(
            f"SELECT * FROM {_identifier(relation['schema_name'])}.{_identifier(relation['table_name'])} LIMIT 0"
        )
    return CompactAddressRelation(**descriptor["table"])
