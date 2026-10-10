# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observed CMS native layouts, separate from preventive heap/B-tree projections.

The owned native builder supplies declarations and final semantic index gates. This
check retains exact PostgreSQL expressions rather than normalizing SQL, and permits
index-free intermediate heaps. It does not estimate GIN/GiST growth or WAL.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
from dataclasses import dataclass

from process.entity_address_result_generation import ENTITY_ADDRESS_RESULT_MODELS


@dataclass(frozen=True)
class NativeRelationLayout:
    """Exact observed physical identity; never a Profile capacity projection."""

    relation_oid: int
    effective_tablespace_oids: tuple[int, ...]
    exact_fingerprint: str


@dataclass(frozen=True)
class RetainedNativeSourceLayout:
    """Immutable catalog evidence from one already admitted, finalized native heap."""

    model_name: str
    schema: str
    relation: str
    oid: int
    database_oid: int
    catalog_json: str
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


def _scope_model(fhir, name):
    """Recognize one generated CMS scope and its exact native model hash."""
    if re.fullmatch(r"cms_directory_scope_[0-9a-f]{32}_[0-9a-f]{8}", name) is None:
        return None
    for model in (fhir.ProviderDirectorySource, *fhir.RESOURCE_MODELS):
        suffix = hashlib.sha256(model.__tablename__.encode("ascii")).hexdigest()[:8]
        if re.fullmatch(r"cms_directory_scope_[0-9a-f]{32}_" + suffix, name):
            return model
    return None


def is_scope_relation(fhir, name):
    """Keep unrelated scopes on the existing Profile B-tree validator."""
    return _scope_model(fhir, name) is not None


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


def _assert_indexes(indexes, attributes, relation, model, toast_oid, declarations_by_name=None):
    """Require exact native declarations; TOAST and primary keys remain native B-trees."""
    declarations_by_name = (
        _declarations(relation.relation, model) if declarations_by_name is None else declarations_by_name
    )
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
    return await _capture_model_layout(fhir, relation, model)


async def capture_scope_layout(fhir, relation):
    """Check native declared indexes on the original captured CMS scope heap."""
    model = _scope_model(fhir, relation.relation)
    if model is None or relation.persistence != "u":
        _reject()
    declarations_by_name = {
        declaration["name"]: dict(declaration) for declaration in getattr(model, "__my_additional_indexes__", ())
    }
    for declaration in declarations_by_name.values():
        elements = declaration["index_elements"]
        emitted = fhir._provider_directory_index_elements_sql(model, declaration)
        if emitted != ", ".join(elements):
            if len(elements) != 1 or emitted != f'("{elements[0]}"::jsonb)':
                _reject()
            declaration["expected_expression"] = elements[0] + "::jsonb"
            declaration["index_elements"] = (None,)
    bucket_name = None
    if model in (fhir.ProviderDirectoryPractitionerRole, fhir.ProviderDirectoryOrganizationAffiliation):
        bucket_name, _sql = fhir._provider_directory_profile_bucket_index_sql(relation.schema, relation.relation)
    captured = await _capture_model_layout(fhir, relation, model, declarations_by_name, bucket_name)
    if bucket_name is not None:
        bucket = await fhir._profile_bucket_index_row(
            relation.schema, relation.relation, bucket_name, model.__tablename__
        )
        fhir._validate_profile_bucket_index_shape(bucket, model.__tablename__)
    return captured


async def _capture_model_layout(fhir, relation, model, declarations_by_name=None, bucket_name=None):
    """Reuse strict heap/TOAST/catalog checks for an already recognized native model."""
    catalog_by_field, toast_oid = await _capture_model_catalog(fhir, relation, model, declarations_by_name, bucket_name)
    return _observed_layout(fhir, relation, catalog_by_field, toast_oid)


async def _capture_model_catalog(fhir, relation, model, declarations_by_name=None, bucket_name=None):
    """Keep the original native catalog validation shared with retained captures."""
    catalog_by_field, toast_oid = await _observe_catalog(fhir, relation)
    attributes, indexes = catalog_by_field["attributes"], catalog_by_field["indexes"]
    checked_indexes = [index for index in indexes if index["index_name"] != bucket_name]
    if declarations_by_name is not None:
        primary_indexes = [
            index for index in checked_indexes if index["relation_oid"] == relation.oid and index["indisprimary"]
        ]
        if (
            len(primary_indexes) != 1
            or primary_indexes[0]["index_name"] != fhir._artifact_scope_pk_names(relation.relation)[1]
        ):
            _reject()
        for index in checked_indexes:
            if index["relation_oid"] != relation.oid or index["indisprimary"]:
                continue
            declaration = declarations_by_name.get(index["index_name"])
            if declaration is None or index["index_expressions"] != declaration.get("expected_expression", ""):
                _reject()
            if declaration.get("expected_expression") and index["indkey"] != "0":
                _reject()
            expected_predicate = (
                "phone_number IS NOT NULL AND phone_number::text <> ''::text" if declaration.get("where") else ""
            )
            if index["index_predicate"] != expected_predicate:
                _reject()
    _assert_indexes(checked_indexes, attributes, relation, model, toast_oid, declarations_by_name)
    return catalog_by_field, toast_oid


async def _observe_catalog(fhir, relation):
    """Read only the existing strict native heap, TOAST and constraint catalog."""
    relation_map, toast_oid = await fhir._profile_capacity_relation_row(relation.oid, relation.persistence, 0)
    if (
        relation_map["relation_oid"] != relation.oid
        or relation_map["schema_name"] != relation.schema
        or relation_map["relation_name"] != relation.relation
    ):
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
    return catalog_by_field, toast_oid


def _observed_layout(fhir, relation, catalog_by_field, toast_oid):
    return NativeRelationLayout(
        relation.oid,
        fhir._profile_capacity_tablespaces(catalog_by_field["relation"], catalog_by_field["indexes"], toast_oid),
        fhir._identity_hash(catalog_by_field),
    )


def _canonical_catalog(catalog):
    return json.dumps(catalog, sort_keys=True, separators=(",", ":"))


def _retained_source_model(source, native_targets):
    if type(source) is not RetainedNativeSourceLayout or any(
        type(value) is not str
        for value in (source.model_name, source.schema, source.relation, source.catalog_json, source.exact_fingerprint)
    ):
        _reject()
    model = _native_model(source.relation, native_targets)
    if (
        model is None
        or source.model_name != model.__tablename__
        or re.fullmatch(re.escape(model.__tablename__) + r"_cms[0-9a-f]{20}", source.relation) is None
        or type(source.oid) is not int
        or not 0 < source.oid < 2**32
        or type(source.database_oid) is not int
        or not 0 < source.database_oid < 2**32
        or type(source.schema) is not str
        or re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]{0,62}", source.schema) is None
    ):
        _reject()
    return model


def _require_final_indexes(catalog, relation, model):
    indexes = [index for index in catalog["indexes"] if index["relation_oid"] == relation.oid]
    if len([index for index in indexes if index["indisprimary"]]) != 1:
        _reject()
    declarations = _declarations(relation.relation, model)
    required_names = {name for name in declarations if "facility_unresolved_" not in name}
    if not required_names.issubset({index["index_name"] for index in indexes}):
        _reject()


async def _capture_evidence_sequence(fhir, relation, catalog):
    """Permit only the native BIGSERIAL default bound to this heap's owned sequence."""
    sequence_rows = await fhir.db.all(
        """
        SELECT sequence.oid::bigint AS sequence_oid, namespace.nspname::text AS schema_name,
               sequence.relname::text AS sequence_name, sequence.relpersistence::text AS persistence,
               parameters.seqtypid::bigint AS type_oid, parameters.seqstart, parameters.seqincrement,
               parameters.seqmax, parameters.seqmin, parameters.seqcache, parameters.seqcycle,
               pg_get_expr(definition.adbin, definition.adrelid, true) AS default_expression,
               format('nextval(%L::regclass)', sequence.oid::regclass::text) AS expected_default
          FROM pg_attribute attribute
          JOIN pg_attrdef definition ON definition.adrelid=attribute.attrelid AND definition.adnum=attribute.attnum
          JOIN pg_depend dependency ON dependency.classid='pg_attrdef'::regclass AND dependency.objid=definition.oid
               AND dependency.objsubid=0 AND dependency.refclassid='pg_class'::regclass
               AND dependency.refobjsubid=0 AND dependency.deptype='n'
          JOIN pg_class sequence ON sequence.oid=dependency.refobjid AND sequence.relkind='S'
          JOIN pg_namespace namespace ON namespace.oid=sequence.relnamespace
          JOIN pg_sequence parameters ON parameters.seqrelid=sequence.oid
          JOIN pg_depend ownership ON ownership.classid='pg_class'::regclass AND ownership.objid=sequence.oid
               AND ownership.objsubid=0
               AND ownership.refclassid='pg_class'::regclass AND ownership.refobjid=attribute.attrelid
               AND ownership.refobjsubid=attribute.attnum AND ownership.deptype='a'
         WHERE attribute.attrelid=CAST(:relation_oid AS oid) AND attribute.attname='evidence_id'
        """,
        relation_oid=relation.oid,
    )
    if len(sequence_rows) != 1:
        _reject()
    sequence_by_field = dict(fhir._pagination_checkpoint_row_mapping(sequence_rows[0]))
    attributes = [
        attribute
        for attribute in catalog["attributes"]
        if attribute["relation_oid"] == relation.oid and attribute["attname"] == "evidence_id"
    ]
    if (
        len(attributes) != 1
        or sequence_by_field.get("schema_name") != relation.schema
        or sequence_by_field.get("sequence_name") != relation.relation + "_evidence_id_seq"
        or sequence_by_field.get("persistence") != relation.persistence
        or sequence_by_field.get("type_oid") != 20
        or sequence_by_field.get("default_expression") != sequence_by_field.get("expected_default")
        or attributes[0]["default_expression"] != sequence_by_field.get("expected_default")
    ):
        _reject()
    catalog["evidence_sequence"] = sequence_by_field


async def capture_retained_native_source(fhir, relation, native_targets):
    """Capture the strict final model catalog before cloning its already pinned heap."""
    model = _native_model(relation.relation, native_targets)
    if (
        model is None
        or relation.persistence != "p"
        or re.fullmatch(re.escape(model.__tablename__) + r"_cms[0-9a-f]{20}", relation.relation) is None
    ):
        _reject()
    catalog, _toast_oid = await _capture_model_catalog(fhir, relation, model)
    _require_final_indexes(catalog, relation, model)
    if model.__tablename__ == "entity_address_evidence":
        await _capture_evidence_sequence(fhir, relation, catalog)
    return RetainedNativeSourceLayout(
        model.__tablename__,
        relation.schema,
        relation.relation,
        relation.oid,
        catalog["database_oid"],
        _canonical_catalog(catalog),
        fhir._identity_hash(catalog),
    )


def _structural_catalog(catalog):
    """Exclude only clone-generated physical identities; SQL and operator identities stay exact."""
    relation_oid = catalog["relation"]["relation_oid"]
    attribute_positions = _live_attribute_positions(catalog["attributes"])
    excluded_fields = {
        "relation_oid",
        "index_oid",
        "index_name",
        "index_schema",
        "schema_name",
        "relation_name",
        "relfilenode",
        "toast_relfilenode",
        "toast_oid",
        "main_bytes",
    }
    structure_by_field = {
        "contract": catalog["contract"],
        "database_oid": catalog["database_oid"],
        "relation": {
            key: field_value for key, field_value in catalog["relation"].items() if key not in excluded_fields
        },
        "has_toast": bool(catalog["relation"]["toast_oid"]),
    }
    for field in ("attributes", "constraints", "indexes", "triggers"):
        entries = []
        for entry in catalog[field]:
            shape_by_field = {key: field_value for key, field_value in entry.items() if key not in excluded_fields}
            shape_by_field["relation_kind"] = "main" if entry["relation_oid"] == relation_oid else "toast"
            if field == "attributes":
                shape_by_field["attnum"] = attribute_positions[entry["relation_oid"]][entry["attnum"]]
            elif field == "indexes":
                shape_by_field["indkey"] = " ".join(
                    str(attribute_positions[entry["relation_oid"]][int(key)]) if int(key) else "0"
                    for key in entry["indkey"].split()
                )
            if (
                field == "attributes"
                and "evidence_sequence" in catalog
                and (shape_by_field["relation_kind"] == "main" and entry["attname"] == "evidence_id")
            ):
                shape_by_field.pop("default_expression")
            entries.append(shape_by_field)
        structure_by_field[field] = sorted(entries, key=_canonical_catalog)
    if "evidence_sequence" in catalog:
        structure_by_field["evidence_sequence"] = {
            key: field_value
            for key, field_value in catalog["evidence_sequence"].items()
            if key not in {"sequence_oid", "schema_name", "sequence_name", "default_expression", "expected_default"}
        }
    return structure_by_field


def _live_attribute_positions(attributes):
    """LIKE compacts dropped slots; preserve live column order and exact index-to-column identity."""
    return {
        relation_oid: {
            number: position
            for position, number in enumerate(
                sorted(attribute["attnum"] for attribute in attributes if attribute["relation_oid"] == relation_oid),
                1,
            )
        }
        for relation_oid in {attribute["relation_oid"] for attribute in attributes}
    }


async def capture_retained_native_layout(fhir, relation, source_layout, native_targets):
    """Require structural identity with the immutable original, despite generated clone index names."""
    model = _retained_source_model(source_layout, native_targets)
    if relation.relation != model.__tablename__ or relation.persistence != "p":
        _reject()
    try:
        original = json.loads(source_layout.catalog_json)
        if (
            _canonical_catalog(original) != source_layout.catalog_json
            or fhir._identity_hash(original) != source_layout.exact_fingerprint
            or original["contract"] != "cms-native-observed-layout.v1"
            or original["database_oid"] != source_layout.database_oid
            or original["relation"]["relation_oid"] != source_layout.oid
            or original["relation"]["schema_name"] != source_layout.schema
            or original["relation"]["relation_name"] != source_layout.relation
            or original["relation"]["relpersistence"] != "p"
        ):
            _reject()
        _require_final_indexes(original, source_layout, model)
        catalog, toast_oid = await _observe_catalog(fhir, relation)
        if model.__tablename__ == "entity_address_evidence":
            await _capture_evidence_sequence(fhir, relation, catalog)
        if _structural_catalog(catalog) != _structural_catalog(original):
            _reject()
        original_indexes = iter(
            sorted(
                original["indexes"],
                key=lambda index: _canonical_catalog(
                    _structural_catalog({**original, "indexes": [index]})["indexes"][0]
                ),
            )
        )
        renamed_indexes = []
        for index in sorted(
            catalog["indexes"],
            key=lambda index: _canonical_catalog(_structural_catalog({**catalog, "indexes": [index]})["indexes"][0]),
        ):
            renamed_indexes.append({**index, "index_name": next(original_indexes)["index_name"]})
        _assert_indexes(
            renamed_indexes,
            catalog["attributes"],
            relation,
            model,
            toast_oid,
            _declarations(source_layout.relation, model),
        )
    except KeyError, TypeError, ValueError, StopIteration:
        _reject()
    return _observed_layout(fhir, relation, catalog, toast_oid)
