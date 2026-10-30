# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Versioned readiness for compact office memberships and retained addresses."""

import hashlib
import json
import re

from process.network_address_projection import _identifier
from process.network_membership_validation import _json_object

COMPONENT = "compact_network_serving"
SLICE_HEAPS = frozenset({"network_slice_ref", "network_office_edge"})


class NetworkCompactReadinessError(ValueError):
    """A compact generation lacks exact native storage or retained dependencies."""


def is_compact_candidate(candidate):
    """Recognize the explicit compact physical contract in retained readiness."""
    report = _json_object(candidate.get("validation_json"))
    serving = report.get("serving_readiness") if type(report) is dict else None
    return type(serving) is dict and serving.get("component") == COMPONENT


def compact_serving_heaps(candidate):
    """Include a small direct-office heap only when its typed receipt requires it."""
    from process.network_compact_custom_preparation import compact_carrier_receipt, compact_custom_receipt

    receipt = compact_custom_receipt(candidate)
    carriers = compact_carrier_receipt(candidate)
    has_rows = (
        receipt is not None and receipt.custom_pair_count or carriers is not None and carriers.materialized_address_rows
    )
    return SLICE_HEAPS | ({"entity_address_unified"} if has_rows else set())


def _require(condition):
    if not condition:
        raise NetworkCompactReadinessError("Compact network readiness differs")


def _digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    ).hexdigest()


def compact_readiness(candidate, report, scope):
    """Validate the compact branch without accepting an old address-copy receipt."""
    canonical = report.get("candidate_readiness")
    serving = report.get("serving_readiness")
    _require(type(canonical) is dict and type(serving) is dict and candidate["index_ready"] is True)
    for receipt, component in ((canonical, "compact_office_slices"), (serving, COMPONENT)):
        _require(
            receipt.get("component") == component
            and type(receipt.get("readiness_revision")) is int
            and receipt["readiness_revision"] == 1
            and receipt.get("ready") is True
            and receipt.get("scope") == scope
        )
    _require(serving.get("index_profile") == "compact_membership")
    _require(
        type(serving.get("index_definition_sha256")) is str
        and re.fullmatch(r"[0-9a-f]{64}", serving["index_definition_sha256"]) is not None
        and type(serving.get("address_artifact")) is dict
        and type(serving.get("edge_artifacts")) is list
        and type(serving.get("slice_ref_sha256")) is str
        and re.fullmatch(r"[0-9a-f]{64}", serving["slice_ref_sha256"]) is not None
    )
    counts = ("membership_rows", "distinct_memberships", "projected_locations", "orphan_bindings")
    _require(
        all(type(canonical.get(field)) is int and canonical[field] == report[field] for field in counts)
        and type(canonical.get("ref_map_oid")) is int
        and 0 < canonical["ref_map_oid"] <= 4294967295
        and type(canonical.get("edge_heap_oid")) is int
        and 0 < canonical["edge_heap_oid"] <= 4294967295
        and canonical["ref_map_oid"] != canonical["edge_heap_oid"]
    )
    return canonical, serving


async def compact_index_digest(connection, schema_name, *, candidate=None):
    """Bind every native candidate index and require the two compact lookup paths."""
    _identifier(schema_name)
    indexes = await connection.fetch(
        """SELECT c.relname AS table_name,i.indexrelid::bigint AS index_oid,
        i.indrelid::bigint AS table_oid,i.indisvalid,i.indisready,i.indislive,
        i.indisprimary,i.indisunique,i.indnkeyatts,i.indnatts,am.amname,
        i.indpred IS NULL AS no_predicate,i.indexprs IS NULL AS no_expressions,
        ARRAY(SELECT a.attname FROM unnest(i.indkey::smallint[]) WITH ORDINALITY key(attnum,position)
          JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=key.attnum ORDER BY key.position) AS columns,
        pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid AND c.relkind='r'
        JOIN pg_index i ON i.indrelid=c.oid JOIN pg_class ic ON ic.oid=i.indexrelid
        JOIN pg_am am ON am.oid=ic.relam WHERE n.nspname=$1 ORDER BY i.indexrelid""",
        schema_name,
    )
    _require(
        indexes
        and all(
            all(index_record[key] is True for key in ("indisvalid", "indisready", "indislive"))
            for index_record in indexes
        )
    )
    _require_index_paths(indexes, candidate)
    return _digest([dict(index_record) for index_record in indexes])


def _require_index_paths(indexes, candidate):
    required_paths = [
        ("network_slice_ref", ["network_id", "office_bucket"], True, True),
        ("network_office_edge", ["slice_id", "location_key"], True, True),
        ("network_office_edge", ["location_key", "slice_id"], False, False),
    ]
    if candidate is not None:
        from process.registry_source_inventory_reference import source_inventory_heaps

        if source_inventory_heaps(candidate):
            required_paths.extend(_inventory_indexes())
    for table, columns, primary, unique in required_paths:
        _require(
            any(
                index_record["table_name"] == table
                and index_record["columns"] == columns
                and index_record["indnkeyatts"] == index_record["indnatts"] == len(columns)
                and index_record["amname"] == "btree"
                and index_record["no_predicate"] is True
                and index_record["no_expressions"] is True
                and (not primary or index_record["indisprimary"] is True)
                and (not unique or index_record["indisunique"] is True)
                for index_record in indexes
            )
        )


def _inventory_indexes():
    from process.network_provider_only_projection import TABLE_NAME
    from process.registry_source_inventory_store import _SCOPE_FIELDS, INVENTORY_TABLE

    return (
        (INVENTORY_TABLE, ["ordinal"], True, True),
        (INVENTORY_TABLE, ["recipe_sha256"], False, True),
        (INVENTORY_TABLE, [*_SCOPE_FIELDS, "fhir_scope_key", "fhir_scope_value"], False, False),
        (TABLE_NAME, ["network_id", "provider_system", "provider_id", "evidence_id"], True, True),
        (TABLE_NAME, ["provider_system", "provider_id", "network_id", "evidence_id"], False, False),
    )
