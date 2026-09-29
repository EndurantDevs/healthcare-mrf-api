# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observed CMS native layouts, separate from preventive heap/B-tree projections.

The owned native builder supplies declarations and final semantic index gates. This
check retains exact PostgreSQL expressions rather than normalizing SQL, and permits
index-free intermediate heaps. It does not estimate GIN/GiST growth or WAL.
"""

from __future__ import annotations

import importlib
import re
from dataclasses import dataclass

from process.entity_address_result_generation import ENTITY_ADDRESS_RESULT_MODELS


@dataclass(frozen=True)
class NativeRelationLayout:
    """Exact observed physical identity; never a Profile capacity projection."""

    relation_oid: int
    effective_tablespace_oids: tuple[int, ...]
    exact_fingerprint: str


def _native_model(name, targets):
    """Recognize only the signed full native recipe's owned stage names."""
    for model in ENTITY_ADDRESS_RESULT_MODELS:
        if model.__tablename__ in targets and re.fullmatch(
            re.escape(model.__tablename__) + r"_cms[0-9a-f]{20}(?:_raw|_evidence|_compact)?", name
        ):
            return model
    return None


def is_native_relation(name, targets):
    """Keep every unrelated FHIR relation on the existing closed Profile validator."""
    return _native_model(name, targets) is not None


def _declarations(name, model):
    """Reuse native index names and declarations, including bounded intermediate indexes."""
    native = importlib.import_module("process.entity_address_unified")

    if name.endswith("_raw"):
        return {
            native._stage_index_name(name, suffix): {
                "index_elements": (suffix,) if suffix == "checksum" else (None,) * count
            }
            for suffix, count in (
                ("checksum", 1),
                ("evidence_shard", 1),
                ("evidence_shard_group", 5),
                ("aggregate_shard_group", 5),
                ("group_key", 4),
                ("location_key", 1),
            )
        }
    if name.endswith("_evidence"):
        return {
            native._archived_identifier(name + "_idx_shard_location", ""): {
                "index_elements": ("evidence_shard", "location_key")
            }
        }
    declarations_by_name = {
        native._stage_index_name(name, declaration.get("name", "_".join(declaration["index_elements"]))): declaration
        for declaration in getattr(model, "__my_additional_indexes__", ())
    }
    if model.__tablename__ == "entity_address_unified":
        for suffix, columns in (
            ("identity", ("entity_subtype", "entity_id", "type", "checksum")),
            ("address", ("address_key", "entity_subtype")),
        ):
            declarations_by_name[native._stage_index_name(name, "facility_unresolved_" + suffix)] = {
                "index_elements": columns,
                "where": "native inference predicate",
            }
    return declarations_by_name


def _reject():
    """Fail closed without disclosing catalog contents in admission diagnostics."""
    raise RuntimeError("provider_directory_cms_native_storage_shape_unsupported")


def _assert_index_keys(index, declaration, attributes):
    """Check direct keys/includes exactly, retaining expressions in the exact fingerprint."""
    elements = declaration["index_elements"]
    includes = declaration.get("include", ())
    keys = [int(value) for value in index["indkey"].split()]
    if len(keys) != len(elements) + len(includes):
        _reject()
    for position, element in enumerate((*elements, *includes)):
        if element is None:
            continue
        # Direct column and explicit opclass declarations are unambiguous without SQL parsing.
        direct = re.fullmatch(r"([a-z_][a-z_0-9]*)(?: (?:public\.)?([a-z_][a-z_0-9]*))?", element)
        if direct:
            if attributes.get(keys[position]) != direct[1]:
                _reject()
            if direct[2] and index["opclass_names"][position] != direct[2]:
                _reject()
        elif keys[position] != 0:
            _reject()


def _assert_declared_index(index, declaration, attributes):
    """Accept only the builder's declared method, key count and ready nonunique shape."""
    keys = declaration["index_elements"]
    includes = declaration.get("include", ())
    if (
        index["index_am"] != declaration.get("using", "btree")
        or index["indnkeyatts"] != len(keys)
        or index["indnatts"] != len(keys) + len(includes)
        or index["indisunique"]
        or index["indisprimary"]
        or bool(index["index_predicate"]) != bool(declaration.get("where"))
    ):
        _reject()
    _assert_index_keys(index, declaration, attributes)


def _assert_indexes(indexes, attributes, relation, model, toast_oid):
    """Require exact native declarations; TOAST and primary keys remain native B-trees."""
    declarations_by_name = _declarations(relation.relation, model)
    attributes_by_number = {
        attribute["attnum"]: attribute["attname"]
        for attribute in attributes
        if attribute["relation_oid"] == relation.oid
    }
    for index in indexes:
        if (
            not all(index[field] is True for field in ("indisvalid", "indisready", "indislive", "indimmediate"))
            or index["indisexclusion"]
            or index["indisreplident"]
            or not index["relfilenode"]
            or index["index_persistence"] != relation.persistence
            or index["index_schema"] not in (relation.schema, "pg_toast")
        ):
            _reject()
        if index["relation_oid"] == toast_oid:
            if (
                index["index_am"] != "btree"
                or not index["indisunique"]
                or index["indkey"] != "1 2"
                or index["indnatts"] != 2
                or index["indnkeyatts"] != 2
                or index["index_predicate"]
                or index["index_expressions"]
            ):
                _reject()
        elif index["indisprimary"]:
            expected_keys = tuple(column.name for column in model.__table__.primary_key)
            actual_keys = tuple(attributes_by_number.get(int(key)) for key in index["indkey"].split())
            if (
                index["index_am"] != "btree"
                or not index["indisunique"]
                or actual_keys != expected_keys
                or index["indnkeyatts"] != len(expected_keys)
                or index["index_predicate"]
                or index["index_expressions"]
            ):
                _reject()
        else:
            declaration = declarations_by_name.get(index["index_name"])
            if declaration is None:
                _reject()
            _assert_declared_index(index, declaration, attributes_by_number)


async def _index_physical_details(fhir, indexes):
    """Add exact namespaces, persistence and operator-class identities to shared catalog rows."""
    if not indexes:
        return
    rows = await fhir.db.all(
        """
        SELECT relation.oid::bigint AS index_oid, namespace.nspname AS index_schema,
               relation.relpersistence::text AS index_persistence,
               ARRAY(SELECT operator.opcname::text FROM unnest(index.indclass::oid[]) WITH ORDINALITY AS item(oid,n)
                     JOIN pg_opclass operator ON operator.oid=item.oid ORDER BY item.n) AS opclass_names
          FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
          JOIN pg_index index ON index.indexrelid=relation.oid
         WHERE relation.oid=ANY(CAST(:oids AS oid[]))
    """,
        oids=[index["index_oid"] for index in indexes],
    )
    details = [dict(fhir._pagination_checkpoint_row_mapping(row)) for row in rows]
    details_by_oid = {row["index_oid"]: row for row in details}
    for index in indexes:
        if index["index_oid"] not in details_by_oid:
            _reject()
        index.update(details_by_oid[index["index_oid"]])


async def capture_native_layout(fhir, relation, native_targets):
    """Validate an owned heap and retain exact catalog identity without a B-tree growth claim."""
    model = _native_model(relation.relation, native_targets)
    if model is None or relation.persistence not in ("u", "p"):
        _reject()
    relation_map, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, relation.persistence, 0)
    if relation_map["schema_name"] != relation.schema or relation_map["relation_name"] != relation.relation:
        _reject()
    attributes, indexes, constraints, triggers = await fhir._profile_capacity_relation_catalog(
        [relation.oid] + ([toast_oid] if toast_oid else [])
    )
    if triggers or any(
        constraint["constraint_type"] not in ("p", "n")
        or constraint["condeferrable"]
        or constraint["condeferred"]
        or not constraint["convalidated"]
        for constraint in constraints
    ):
        _reject()
    await _index_physical_details(fhir, indexes)
    _assert_indexes(indexes, attributes, relation, model, toast_oid)
    catalog_by_field = {
        "contract": "cms-native-observed-layout.v1",
        "database_oid": await fhir.db.scalar("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"),
        "relation": relation_map,
        "attributes": attributes,
        "constraints": constraints,
        "triggers": triggers,
        "indexes": [
            {key: field_value for key, field_value in index.items() if key != "main_bytes"} for index in indexes
        ],
    }
    return NativeRelationLayout(
        relation.oid,
        fhir._profile_capacity_tablespaces(relation_map, indexes, toast_oid),
        fhir._identity_hash(catalog_by_field),
    )
