# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named address editions with logical publication and data provenance."""

import hashlib
import json
import re
from dataclasses import asdict
from uuid import UUID

from process import entity_address_result_generation as generation
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_cms_registry_address_capture import NativeReceiptSession, _NativeMappings
from process.network_compact_layout import CompactAddressRelation

CONTRACT = "network_address_artifact.postgres.v1"
_MAX_DESCRIPTOR_BYTES = 256 * 1024
_TABLE = "entity_address_unified"
_AUTHORITY = "entity_address_result_generation"
_LEGACY_FIELDS = {
    "contract",
    "database",
    "namespace",
    "protected_owner_oid",
    "table",
    "authority_table_oid",
    "authority",
    "generation_id",
    "alias_generation",
    "alias_catalog_sha256",
    "catalog",
    "runtime_roles",
    "descriptor_sha256",
}
_FIELDS = {
    "contract",
    "revision",
    "namespace",
    "table",
    "authority",
    "generation_id",
    "alias_generation",
    "descriptor_sha256",
}


class NetworkAddressArtifactError(ValueError):
    """The declared named address edition or its logical source scope differs."""


def _require(condition, reason):
    if not condition:
        raise NetworkAddressArtifactError("network_address_artifact_" + reason)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)


def _digest(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _sha(value):
    _require(type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None, "descriptor_invalid")
    return value


class _CatalogMappings(_NativeMappings):
    def all(self):
        """Complete the existing receipt adapter's bounded result interface."""
        return self

    def one(self):
        """Require one result row for the existing SQLAlchemy data interface."""
        _require(len(self) == 1, "catalog_row_invalid")
        return self[0]


class _CatalogRows(_CatalogMappings):
    def mappings(self):
        """Preserve SQLAlchemy's explicit mapping view for existing receipt checks."""
        return _CatalogMappings(self)

    def all(self):
        """Unmapped receipt comparisons use ordered tuples, as SQLAlchemy does."""
        return [tuple(row.values()) for row in self]

    def one(self):
        """Expose one ordered native row for existing positional catalog readers."""
        return tuple(super().one().values())

    def scalars(self):
        """Keep the existing receipt reader's scalar-list comparisons intact."""
        return _CatalogMappings(next(iter(row.values())) for row in self)


class _CatalogSession(NativeReceiptSession):
    async def execute(self, statement, parameters=None):
        """Reuse native receipt binds/JSON decoding on the caller's transaction."""
        return _CatalogRows(await super().execute(statement, parameters))


def _roles(runtime_roles):
    _require(type(runtime_roles) is tuple and 0 < len(runtime_roles) <= 128, "roles_invalid")
    _require(tuple(sorted(set(runtime_roles))) == runtime_roles, "roles_invalid")
    for role in runtime_roles:
        _identifier(role)
    return runtime_roles


def _authority_document(value):
    fields = {"local_lineage_id", "local_generation", "origin_lineage_id", "origin_generation", "published_at"}
    _require(type(value) is dict and set(value) in (fields, fields | {"relation_oids"}), "descriptor_invalid")
    serving = generation.validate_entity_address_serving_generation(
        {field: value[field] for field in ("origin_lineage_id", "origin_generation", "published_at")}
    )
    lineage = value["local_lineage_id"]
    _require(type(lineage) is str and str(UUID(lineage)) == lineage and UUID(lineage).int, "generation_invalid")
    _require(type(value["local_generation"]) is int and 0 < value["local_generation"] < 2**63, "generation_invalid")
    return {"local_lineage_id": lineage, "local_generation": value["local_generation"], **serving.as_dict()}


def _descriptor(binding_by_field):
    from process import network_bootstrap_address_artifact as bootstrap
    from process import network_indexed_address_artifact as indexed

    if type(binding_by_field) is dict and binding_by_field.get("contract") == bootstrap.CONTRACT:
        return bootstrap._descriptor(binding_by_field)
    if type(binding_by_field) is dict and binding_by_field.get("contract") == indexed.CONTRACT:
        return indexed._descriptor(binding_by_field)
    _require(type(binding_by_field) is dict, "descriptor_invalid")
    if set(binding_by_field) == _LEGACY_FIELDS:
        schema = binding_by_field["namespace"]["name"]
        key = binding_by_field["table"]["oid"]
        _require(type(key) is int, "descriptor_invalid")
        binding_by_field = {
            "contract": CONTRACT,
            "revision": 1,
            "namespace": {"name": schema},
            "table": asdict(CompactAddressRelation(schema, _TABLE)),
            "retained_table": asdict(CompactAddressRelation(schema, _TABLE + "_retained_" + format(key, "x"))),
            "authority": _authority_document(binding_by_field["authority"]),
            **{field: binding_by_field[field] for field in ("generation_id", "alias_generation", "descriptor_sha256")},
        }
    fields = _FIELDS | ({"retained_table"} if binding_by_field.get("revision") == 1 else set())
    _require(set(binding_by_field) == fields and binding_by_field["contract"] == CONTRACT, "descriptor_invalid")
    _require(type(binding_by_field["revision"]) is int and binding_by_field["revision"] in (1, 2), "descriptor_invalid")
    _require(
        type(binding_by_field["namespace"]) is dict and set(binding_by_field["namespace"]) == {"name"},
        "descriptor_invalid",
    )
    relation = CompactAddressRelation(**binding_by_field["table"])
    _require(
        relation.schema_name == binding_by_field["namespace"]["name"] and relation.table_name == _TABLE,
        "descriptor_invalid",
    )
    _authority_document(binding_by_field["authority"])
    _sha(binding_by_field["generation_id"])
    _sha(binding_by_field["descriptor_sha256"])
    _require(
        type(binding_by_field["alias_generation"]) is int and binding_by_field["alias_generation"] >= 0,
        "descriptor_invalid",
    )
    if binding_by_field["revision"] == 1:
        retained = CompactAddressRelation(**binding_by_field["retained_table"])
        _require(
            retained.schema_name == relation.schema_name and retained.table_name.startswith(_TABLE + "_retained_"),
            "descriptor_invalid",
        )
    else:
        _require(
            _digest({key: field_value for key, field_value in binding_by_field.items() if key != "descriptor_sha256"})
            == binding_by_field["descriptor_sha256"],
            "descriptor_digest_invalid",
        )
    _require(len(_json(binding_by_field).encode()) <= _MAX_DESCRIPTOR_BYTES, "descriptor_invalid")
    return json.loads(_json(binding_by_field))


def address_artifact_data(binding):
    """Compare named Source data across the supported historical descriptor encodings."""
    descriptor = _descriptor(binding)
    document_by_field = {
        key: value
        for key, value in descriptor.items()
        if key not in {"revision", "descriptor_sha256", "retained_table"}
    }
    if "origin_artifact" in document_by_field:
        document_by_field["origin_artifact"] = address_artifact_data(document_by_field["origin_artifact"])
    return document_by_field


async def _require_api_read_context(connection, api_read):
    _require(type(api_read) is bool, "read_mode_invalid")
    if api_read:
        _require(connection.is_in_transaction(), "transaction_required")
        modes = await connection.fetchrow(
            "SELECT current_setting('transaction_isolation'),current_setting('transaction_read_only')"
        )
        _require(tuple(modes) in (("repeatable read", "on"), ("serializable", "on")), "read_snapshot_required")


async def _require_capture_snapshot(connection):
    _require(connection.is_in_transaction(), "transaction_required")
    _require(
        await connection.fetchval("SHOW transaction_isolation") in {"repeatable read", "serializable"},
        "snapshot_required",
    )


async def _current_authority(connection, schema):
    record = await connection.fetchrow(
        "SELECT local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,relation_oids "
        f"FROM {_identifier(schema)}.{_AUTHORITY} WHERE singleton IS TRUE"
    )
    _require(record is not None, "generation_unpublished")
    authority_by_field = dict(record)
    for name in ("local_lineage_id", "origin_lineage_id"):
        authority_by_field[name] = str(authority_by_field[name])
    timestamp = authority_by_field["published_at"]
    if hasattr(timestamp, "isoformat"):
        authority_by_field["published_at"] = timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")
    logical = _authority_document(authority_by_field)
    legacy_by_field = {
        **logical,
        "published_at": authority_by_field["published_at"],
        "relation_oids": authority_by_field["relation_oids"],
    }
    return logical, _digest(legacy_by_field)


async def capture_network_address_artifact(connection, source, *, runtime_roles=()):
    """Read the configured current edition while its logical transaction remains pinned."""
    from process import entity_address_snapshot_alias as alias
    from process import network_bootstrap_address_artifact as bootstrap

    _require(type(source) is PinnedAddressSource and source.table_name == _TABLE, "source_invalid")
    if bootstrap.is_bootstrap_source(source):
        return await bootstrap.capture_bootstrap_address_artifact(connection, source, runtime_roles=runtime_roles)
    await _require_capture_snapshot(connection)
    authority, legacy_key = await _current_authority(connection, source.schema_name)
    _require(source.generation_id in {_digest(authority), legacy_key}, "source_generation_changed")
    _, _, alias_generation = await alias._alias_state(_CatalogSession(connection), source.schema_name)
    document_by_field = {
        "contract": CONTRACT,
        "revision": 2,
        "namespace": {"name": source.schema_name},
        "table": asdict(CompactAddressRelation(source.schema_name, source.table_name)),
        "authority": authority,
        "generation_id": source.generation_id,
        "alias_generation": alias_generation,
    }
    document_by_field["descriptor_sha256"] = _digest(document_by_field)
    return _descriptor(document_by_field)


async def verify_network_address_artifact(connection, descriptor, *, runtime_roles=(), api_read=False):
    """Resolve named metadata and its supported historical rename alias."""
    from process import network_bootstrap_address_artifact as bootstrap
    from process import network_indexed_address_artifact as indexed

    await _require_api_read_context(connection, api_read)
    await _require_capture_snapshot(connection)
    descriptor = _descriptor(descriptor)
    if descriptor["contract"] == bootstrap.CONTRACT:
        return await bootstrap.verify_bootstrap_address_artifact(connection, descriptor, runtime_roles=runtime_roles)
    if descriptor["contract"] == indexed.CONTRACT:
        return await indexed.verify_indexed_address_artifact(
            connection, descriptor, runtime_roles=runtime_roles, api_read=api_read
        )
    relation = CompactAddressRelation(**descriptor["table"])
    if descriptor["revision"] == 2:
        await connection.fetch(
            f"SELECT * FROM {_identifier(relation.schema_name)}.{_identifier(relation.table_name)} LIMIT 0"
        )
        return relation
    current = None
    authority_name = _identifier(relation.schema_name) + "." + _AUTHORITY
    if await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", authority_name):
        current, _legacy = await _current_authority(connection, relation.schema_name)
    candidates = []
    for name in (descriptor["table"], descriptor["retained_table"]):
        qualified = _identifier(name["schema_name"]) + "." + _identifier(name["table_name"])
        present = await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", qualified)
        if present and (name != descriptor["table"] or current is None or current == descriptor["authority"]):
            candidates.append(CompactAddressRelation(**name))
    _require(len(candidates) == 1, "retained_table_unavailable")
    relation = candidates[0]
    await connection.fetch(
        f"SELECT * FROM {_identifier(relation.schema_name)}.{_identifier(relation.table_name)} LIMIT 0"
    )
    return relation
