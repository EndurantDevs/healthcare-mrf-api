# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe only the signed retained-copy recipe's original native raw heaps."""

from __future__ import annotations

from uuid import UUID

from process.network_fhir_source_epoch import _LOOKUP_INDEXES, _PRIMARY_KEYS, _TABLES
from process.provider_directory_cms_capacity_contract import validated_registry_source_retention_policy
from process.provider_directory_cms_native_layout import NativeRelationLayout, _index_physical_details

_COLUMN_TYPES = {16, 20, 21, 23, 25, 114, 1015, 1043, 1114, 1184, 2950, 3802}
_KEY_CLASSES = {21: "int2_ops", 23: "int4_ops", 25: "text_ops", 26: "oid_ops", 1043: "text_ops", 2950: "uuid_ops"}
_UNCOPIED_FIELDS = {
    "atthasdef": False,
    "default_expression": "",
    "attgenerated": "",
    "attidentity": "",
    "attcompression": "",
}


def _reject():
    """Keep rejected catalog details out of public admission diagnostics."""
    raise RuntimeError("provider_directory_cms_raw_storage_shape_unsupported")


def _raw_profiles(name):
    """Preserve the creator's per-table PK and lookup-index order."""
    profiles = [
        (f"cms_epoch_pk_{ordinal}", keys, True) for ordinal, (table, keys) in enumerate(_PRIMARY_KEYS) if table == name
    ]
    profiles.extend((index, keys, False) for index, table, keys in _LOOKUP_INDEXES if table == name)
    return tuple(profiles)


def _require_raw_request(relation, raw_policy, expected_names):
    """Validate closed coordinates and a prefix without granting signed authority."""
    try:
        policy = validated_registry_source_retention_policy(raw_policy)
        schema = "registry_cms_epoch_" + UUID(policy["capture_id"]).hex
    except ValueError, TypeError, KeyError, AttributeError:
        _reject()
    profiles = _raw_profiles(relation.relation)
    if (
        relation.schema != schema
        or relation.persistence != "p"
        or type(relation.oid) is not int
        or not 0 < relation.oid < 2**32
        or relation.relation not in _TABLES
        or policy["raw_tables"] != list(_TABLES)
        or policy["source_pin"]["schema_name"] == schema
        or type(expected_names) is not tuple
        or expected_names != tuple(profile[0] for profile in profiles[: len(expected_names)])
    ):
        _reject()
    return policy, profiles[: len(expected_names)]


def _main_attributes(attributes, oid):
    return [attribute for attribute in attributes if attribute["relation_oid"] == oid]


async def _require_source_columns(fhir, relation, policy, attributes):
    """Compare the exact LIKE column shape, excluding options the creator omits."""
    source_ref = fhir._unscoped_qt(policy["source_pin"]["schema_name"], relation.relation)
    source_oid = await fhir.db.scalar(
        "SELECT oid::bigint FROM pg_class WHERE oid=to_regclass(:name) AND relkind='r' AND relpersistence='p'",
        name=source_ref,
    )
    if (
        type(source_oid) is not int
        or source_oid <= 0
        or (relation.relation == _TABLES[1] and source_oid != policy["source_pin"]["resource_table_oid"])
    ):
        _reject()
    source_attributes, _, _, _ = await fhir._profile_capacity_relation_catalog([source_oid])
    expected_attributes = [
        {**attribute, "relation_oid": relation.oid, "attnum": ordinal, **_UNCOPIED_FIELDS}
        for ordinal, attribute in enumerate(_main_attributes(source_attributes, source_oid), 1)
    ]
    actual = _main_attributes(attributes, relation.oid)
    if (
        not expected_attributes
        or actual != expected_attributes
        or any(attribute["atttypid"] not in _COLUMN_TYPES for attribute in actual)
    ):
        _reject()


def _require_toast_columns(attributes, toast_oid):
    """Keep the native TOAST heap at its fixed three-column ABI."""
    actual = _main_attributes(attributes, toast_oid)
    expected_attributes = [
        {
            "relation_oid": toast_oid,
            "attnum": ordinal,
            "attname": name,
            "atttypid": type_oid,
            "atttypmod": -1,
            "attcollation": 0,
            "attnotnull": False,
            "attstorage": "p",
            "typlen": length,
            **_UNCOPIED_FIELDS,
        }
        for ordinal, (name, type_oid, length) in enumerate(
            (("chunk_id", 26, 4), ("chunk_seq", 23, 4), ("chunk_data", 17, -1)), 1
        )
    ]
    if actual != expected_attributes:
        _reject()
    return actual


async def _default_opclasses(fhir):
    """Resolve built-in default B-tree classes without version-specific OIDs."""
    catalog = await fhir.db.all(
        "SELECT operator.opcname::text AS name,operator.oid::bigint AS oid FROM pg_opclass operator "
        "JOIN pg_am method ON method.oid=operator.opcmethod WHERE method.amname='btree' "
        "AND operator.opcnamespace='pg_catalog'::regnamespace AND operator.opcdefault",
    )
    return {entry["name"]: entry["oid"] for entry in map(fhir._pagination_checkpoint_row_mapping, catalog)}


def _require_index(index, key_attributes, primary, schema, tablespace, opclasses):
    """Require exactly the creator's ready native index and built-in key semantics."""
    key_count = len(key_attributes)
    flags_by_field = {
        "indisvalid": True,
        "indisready": True,
        "indislive": True,
        "indimmediate": True,
        "indisunique": primary,
        "indisprimary": primary,
        "indisexclusion": False,
        "indisreplident": False,
        "indnullsnotdistinct": False,
        "indisclustered": False,
        "indcheckxmin": False,
    }
    try:
        classes = " ".join(str(opclasses[_KEY_CLASSES[attribute["atttypid"]]]) for attribute in key_attributes)
    except KeyError:
        _reject()
    if (
        any(index[field] is not expected for field, expected in flags_by_field.items())
        or index["index_am"] != "btree"
        or index["index_schema"] != schema
        or index["index_persistence"] != "p"
        or index["indnatts"] != key_count
        or index["indnkeyatts"] != key_count
        or index["indkey"] != " ".join(str(attribute["attnum"]) for attribute in key_attributes)
        or index["indcollation"] != " ".join(str(attribute["attcollation"]) for attribute in key_attributes)
        or index["indclass"] != classes
        or index["indoption"] != " ".join("0" for _ in key_attributes)
        or index["index_expressions"]
        or index["index_predicate"]
        or index["reloptions"] != []
        or not index["relfilenode"]
        or index["effective_tablespace_oid"] != tablespace
    ):
        _reject()


def _require_raw_indexes(indexes, attributes, relation, toast_oid, profiles, opclasses, tablespace):
    """Reject missing, extra or substituted main/TOAST indexes in every observed prefix."""
    main_indexes = [index for index in indexes if index["relation_oid"] == relation.oid]
    toast_indexes = [index for index in indexes if toast_oid and index["relation_oid"] == toast_oid]
    if (
        len(main_indexes) + len(toast_indexes) != len(indexes)
        or len(main_indexes) != len(profiles)
        or {index["index_name"] for index in main_indexes} != {profile[0] for profile in profiles}
        or len(toast_indexes) != int(toast_oid is not None)
    ):
        _reject()
    attributes_by_name = {attribute["attname"]: attribute for attribute in _main_attributes(attributes, relation.oid)}
    profiles_by_name = {name: (keys, primary) for name, keys, primary in profiles}
    for index in main_indexes:
        keys, primary = profiles_by_name[index["index_name"]]
        try:
            key_attributes = [attributes_by_name[key] for key in keys]
        except KeyError:
            _reject()
        _require_index(index, key_attributes, primary, relation.schema, tablespace, opclasses)
    if toast_oid:
        toast_attributes = _require_toast_columns(attributes, toast_oid)
        _require_index(toast_indexes[0], toast_attributes[:2], True, "pg_toast", tablespace, opclasses)


def _require_constraints(constraints, relation, profiles):
    """Permit inherited NOT NULL and exactly the confirmed native primary key."""
    primary_constraints = [constraint for constraint in constraints if constraint["constraint_type"] == "p"]
    expected_keys = profiles[0][1] if profiles else ()
    if len(primary_constraints) != int(bool(profiles)) or any(
        constraint["relation_oid"] != relation.oid
        or constraint["constraint_type"] not in ("n", "p")
        or constraint["condeferrable"]
        or constraint["condeferred"]
        or not constraint["convalidated"]
        for constraint in constraints
    ):
        _reject()
    if primary_constraints and primary_constraints[0]["constraint_definition"] != (
        "PRIMARY KEY (" + ", ".join(expected_keys) + ")"
    ):
        _reject()


async def capture_retained_raw_layout(fhir, relation, raw_policy, expected_index_names):
    """Observe a signed recipe prefix without changing Profile projections or byte caps."""
    policy, profiles = _require_raw_request(relation, raw_policy, expected_index_names)
    relation_map, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, "p", 0)
    if (
        relation_map["relation_oid"] != relation.oid
        or relation_map["schema_name"] != relation.schema
        or relation_map["relation_name"] != relation.relation
        or relation_map["relreplident"] != "d"
        or not relation_map["relfilenode"]
        or relation_map["reloptions"] != []
        or relation_map["toast_reloptions"] != []
        or (toast_oid and not relation_map["toast_relfilenode"])
    ):
        _reject()
    attributes, indexes, constraints, triggers = await fhir._profile_capacity_relation_catalog(
        [relation.oid] + ([toast_oid] if toast_oid else [])
    )
    if triggers:
        _reject()
    await _require_source_columns(fhir, relation, policy, attributes)
    _require_constraints(constraints, relation, profiles)
    await _index_physical_details(fhir, indexes)
    opclasses = await _default_opclasses(fhir) if indexes else {}
    tablespaces = fhir._profile_capacity_tablespaces(relation_map, indexes, toast_oid)
    if tablespaces != (relation_map["effective_tablespace_oid"],):
        _reject()
    _require_raw_indexes(indexes, attributes, relation, toast_oid, profiles, opclasses, tablespaces[0])
    catalog_by_field = {
        "contract": "cms-retained-raw-observed-layout.v1",
        "database_oid": await fhir.db.scalar("SELECT oid::bigint FROM pg_database WHERE datname=current_database()"),
        "relation": relation_map,
        "attributes": attributes,
        "constraints": constraints,
        "indexes": [
            {key: field_value for key, field_value in index.items() if key != "main_bytes"} for index in indexes
        ],
    }
    return NativeRelationLayout(relation.oid, tablespaces, fhir._identity_hash(catalog_by_field))
