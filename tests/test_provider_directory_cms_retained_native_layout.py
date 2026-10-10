# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Offline retained-native catalog identity and signed admission boundaries."""

import copy
import hashlib
import json
import re
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_layout as layout
from process.provider_directory_cms_preparation import (
    NonprofileAdmission,
    NonprofileAdmissionCheck,
    OwnedRelation,
    RetainedNativeRelation,
)
from tests.provider_directory_cms_capacity_test_support import signed_cms_plan
from tests.test_provider_directory_cms_capacity_contract import _retention_policy
from tests.test_provider_directory_cms_nonprofile_capacity import _producer


def _hash(value):
    return hashlib.sha256(layout._canonical_catalog(value).encode()).hexdigest()


def _index_fixture(name, keys, **options):
    relation_oid = options.get("relation_oid", 11)
    return {
        "relation_oid": relation_oid,
        "index_oid": options["index_oid"],
        "index_name": name,
        "relfilenode": 1000,
        "index_schema": "pg_toast" if relation_oid == 12 else "fixture",
        "index_persistence": "p",
        "index_am": options.get("method", "btree"),
        "indisvalid": True,
        "indisready": True,
        "indislive": True,
        "indimmediate": True,
        "indisexclusion": False,
        "indisreplident": False,
        "indisunique": options.get("primary", False) or relation_oid == 12,
        "indisprimary": options.get("primary", False),
        "indkey": " ".join(map(str, keys)),
        "indnatts": len(keys),
        "indnkeyatts": len(keys),
        "index_predicate": options.get("predicate", ""),
        "index_expressions": options.get("expression", ""),
        "opclass_names": list(options.get("opclasses", ())),
        "indclass": " ".join("42" for _key in keys),
        "indcollation": " ".join("0" for _key in keys),
        "indoption": " ".join("0" for _key in keys),
        "main_bytes": 8192,
    }


def _declared_index_fixture(source_relation, model, attribute_number_by_name):
    indexes = []
    primary_keys = [attribute_number_by_name[column.name] for column in model.__table__.primary_key]
    indexes.append(_index_fixture("original_primary", primary_keys, primary=True, index_oid=len(indexes) + 100))
    for name, declaration in layout._declarations(source_relation.relation, model).items():
        keys, expressions, opclasses = [], [], []
        for element in (*declaration["index_elements"], *declaration.get("include", ())):
            direct = re.fullmatch(r"([a-z_][a-z_0-9]*)(?: (?:public\.)?([a-z_][a-z_0-9]*))?", element)
            keys.append(attribute_number_by_name[direct[1]] if direct else 0)
            opclasses.append(direct[2] if direct and direct[2] else "synthetic_ops")
            if not direct:
                expressions.append(element)
        indexes.append(
            _index_fixture(
                name,
                keys,
                index_oid=len(indexes) + 100,
                method=declaration.get("using", "btree"),
                predicate=declaration.get("where", ""),
                expression=", ".join(expressions),
                opclasses=opclasses,
            )
        )
        indexes[-1]["indnkeyatts"] = len(declaration["index_elements"])
    indexes.append(_index_fixture("original_toast", [1, 2], relation_oid=12, index_oid=len(indexes) + 100))
    return indexes


def _clone_catalog_fixture(catalog_by_field, model_name):
    clone = copy.deepcopy(catalog_by_field)
    clone["relation"].update(
        relation_oid=21,
        toast_oid=22,
        schema_name="entity_address_archive_" + "0" * 31 + "1",
        relation_name=model_name,
        relfilenode=2000,
        toast_relfilenode=2001,
    )
    for field in ("attributes", "indexes", "constraints"):
        for entry in clone[field]:
            entry["relation_oid"] = 21 if entry["relation_oid"] == 11 else 22
            if field == "indexes":
                entry.update(
                    index_name="generated_" + str(entry["index_oid"]),
                    index_oid=entry["index_oid"] + 1000,
                    relfilenode=2000,
                    main_bytes=0,
                )
                if entry["index_schema"] != "pg_toast":
                    entry["index_schema"] = clone["relation"]["schema_name"]
    return clone


def _fixture(model_name="entity_address_unified", *, database_oid=3):
    """Use real declarations with synthetic PostgreSQL catalog identities."""
    model = next(model for model in layout.ENTITY_ADDRESS_RESULT_MODELS if model.__tablename__ == model_name)
    source_relation = OwnedRelation("fixture", model_name + "_cms" + "a" * 20, 11, 0, "p")
    attribute_number_by_name = {column.name: number for number, column in enumerate(model.__table__.columns, 1)}
    attributes = [
        {"relation_oid": 11, "attnum": number, "attname": name, "default_expression": ""}
        for name, number in attribute_number_by_name.items()
    ]
    attributes += [
        {"relation_oid": 12, "attnum": number, "attname": name, "default_expression": ""}
        for number, name in enumerate(("chunk_id", "chunk_seq", "chunk_data"), 1)
    ]

    indexes = _declared_index_fixture(source_relation, model, attribute_number_by_name)
    catalog_by_field = {
        "contract": "cms-native-observed-layout.v1",
        "database_oid": database_oid,
        "relation": {
            "relation_oid": 11,
            "schema_name": "fixture",
            "relation_name": source_relation.relation,
            "toast_oid": 12,
            "relpersistence": "p",
            "relfilenode": 1000,
            "toast_relfilenode": 1001,
        },
        "attributes": attributes,
        "indexes": indexes,
        "triggers": [],
        "constraints": [
            {
                "relation_oid": 11,
                "constraint_type": "p",
                "condeferrable": False,
                "condeferred": False,
                "convalidated": True,
                "constraint_definition": "PRIMARY KEY ("
                + ",".join(column.name for column in model.__table__.primary_key)
                + ")",
            }
        ],
    }
    clone = _clone_catalog_fixture(catalog_by_field, model_name)
    fhir = SimpleNamespace(
        _identity_hash=_hash,
        _pagination_checkpoint_row_mapping=lambda row: row,
        _profile_capacity_relation_row=AsyncMock(return_value=(catalog_by_field["relation"], 12)),
        _profile_capacity_relation_catalog=AsyncMock(
            return_value=(attributes, indexes, catalog_by_field["constraints"], [])
        ),
        _profile_capacity_tablespaces=lambda *_args: (42,),
        db=SimpleNamespace(scalar=AsyncMock(return_value=database_oid)),
    )
    return fhir, source_relation, catalog_by_field, clone


def _sequence(catalog):
    relation = catalog["relation"]
    sequence_name = relation["relation_name"] + "_evidence_id_seq"
    default = "nextval('" + relation["schema_name"] + "." + sequence_name + "'::regclass)"
    for attribute in catalog["attributes"]:
        if attribute["relation_oid"] == relation["relation_oid"] and attribute["attname"] == "evidence_id":
            attribute["default_expression"] = default
    return {
        "sequence_oid": relation["relation_oid"] + 100,
        "schema_name": relation["schema_name"],
        "sequence_name": sequence_name,
        "persistence": "p",
        "type_oid": 20,
        "seqstart": 1,
        "seqincrement": 1,
        "seqmax": 2**63 - 1,
        "seqmin": 1,
        "seqcache": 1,
        "seqcycle": False,
        "default_expression": default,
        "expected_default": default,
    }


async def _source_and_clone(monkeypatch, model_name="entity_address_unified"):
    fhir, source_relation, original, clone = _fixture(model_name)
    if model_name == "entity_address_evidence":
        original_sequence, clone_sequence = _sequence(original), _sequence(clone)
        fhir.db.all = AsyncMock(return_value=[original_sequence])
    monkeypatch.setattr(layout, "_index_physical_details", AsyncMock())
    source = await layout.capture_retained_native_source(fhir, source_relation, (model_name,))
    relation = OwnedRelation(clone["relation"]["schema_name"], model_name, 21, 0, "p")
    if model_name == "entity_address_evidence":
        fhir.db.all.return_value = [clone_sequence]
    fhir._profile_capacity_relation_row.return_value = (clone["relation"], 22)
    fhir._profile_capacity_relation_catalog.return_value = (
        clone["attributes"],
        clone["indexes"],
        clone["constraints"],
        clone["triggers"],
    )
    return fhir, source, relation, original, clone


@pytest.mark.asyncio
@pytest.mark.parametrize("model_name", [model.__tablename__ for model in layout.ENTITY_ADDRESS_RESULT_MODELS])
async def test_complete_declared_family_has_exact_clone_compatibility(monkeypatch, model_name):
    fhir, source, relation, _original, _clone = await _source_and_clone(monkeypatch, model_name)
    captured = await layout.capture_retained_native_layout(fhir, relation, source, (model_name,))
    assert captured.relation_oid == relation.oid


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "duplicate", "schema", "name", "type", "default", "parameters"])
async def test_evidence_sequence_requires_exact_native_dependency_and_default(monkeypatch, change):
    fhir, source, relation, _original, _clone = await _source_and_clone(monkeypatch, "entity_address_evidence")
    rows = fhir.db.all.return_value
    if change == "missing":
        rows.clear()
    elif change == "duplicate":
        rows.append(copy.deepcopy(rows[0]))
    else:
        field = {
            "schema": "schema_name",
            "name": "sequence_name",
            "type": "type_oid",
            "default": "default_expression",
            "parameters": "seqincrement",
        }[change]
        rows[0][field] = "changed"
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_retained_native_layout(fhir, relation, source, ("entity_address_evidence",))
    query = fhir.db.all.await_args.args[0]
    assert "pg_attrdef" in query and "pg_depend" in query and "pg_sequence" in query
    assert "ownership.refobjsubid=attribute.attnum" in query


@pytest.mark.asyncio
async def test_declared_native_clone_accepts_generated_names_and_physical_identities(monkeypatch):
    fhir, source, relation, original, _clone = await _source_and_clone(monkeypatch)
    assert {index["index_am"] for index in original["indexes"]} == {"btree", "gin", "gist"}
    captured = await layout.capture_retained_native_layout(fhir, relation, source, ("entity_address_unified",))
    assert captured.relation_oid == 21 and captured.effective_tablespace_oids == (42,)


def test_retained_column_coordinates_preserve_order_and_index_keys():
    _fhir, _relation, original, clone = _fixture()
    for entry in original["attributes"]:
        if entry["relation_oid"] == 11 and entry["attnum"] >= 27:
            entry["attnum"] += 1
    for index in original["indexes"]:
        if index["relation_oid"] == 11:
            index["indkey"] = " ".join(str(int(key) + (int(key) >= 27)) for key in index["indkey"].split())
    assert layout._structural_catalog(original) == layout._structural_catalog(clone)
    reordered = copy.deepcopy(clone)
    attributes = [entry for entry in reordered["attributes"] if entry["relation_oid"] == 21]
    attributes[0]["attnum"], attributes[1]["attnum"] = attributes[1]["attnum"], attributes[0]["attnum"]
    assert layout._structural_catalog(original) != layout._structural_catalog(reordered)
    changed = copy.deepcopy(clone)
    changed["indexes"][0]["indkey"] = "2"
    assert layout._structural_catalog(original) != layout._structural_catalog(changed)


def _mutate_clone_catalog(clone, mutation):
    index = next(entry for entry in clone["indexes"] if entry["index_am"] == "gin")
    field_by_mutation = {
        "method": "index_am",
        "predicate": "index_predicate",
        "expression": "index_expressions",
        "opclass": "indclass",
        "collation": "indcollation",
    }
    if mutation in field_by_mutation:
        index[field_by_mutation[mutation]] = "changed"
        return
    if mutation == "primary":
        clone["indexes"][0]["indkey"] = "0"
        return
    if mutation == "toast":
        clone["indexes"][-1]["indisunique"] = False
        return
    if mutation == "missing":
        clone["indexes"].pop()
        return
    if mutation == "extra":
        clone["indexes"].append(copy.deepcopy(index))
        return
    clone["attributes"][0]["default_expression"] = "unsafe_default()"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["method", "predicate", "expression", "opclass", "collation", "primary", "toast", "missing", "extra", "default"],
)
async def test_clone_rejects_any_nonphysical_catalog_difference(monkeypatch, mutation):
    fhir, source, relation, _original, clone = await _source_and_clone(monkeypatch)
    _mutate_clone_catalog(clone, mutation)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_retained_native_layout(fhir, relation, source, ("entity_address_unified",))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"model_name": "unknown"},
        {"relation": "entity_address_unified"},
        {"oid": True},
        {"database_oid": 0},
        {"catalog_json": "{}"},
        {"exact_fingerprint": "0" * 64},
    ],
)
async def test_source_evidence_refuses_wrong_model_or_malformed_identity(monkeypatch, change):
    fhir, source, relation, _original, _clone = await _source_and_clone(monkeypatch)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_retained_native_layout(
            fhir, relation, replace(source, **change), ("entity_address_unified",)
        )


@pytest.mark.asyncio
async def test_final_source_rejects_missing_primary_or_declared_indexes(monkeypatch):
    fhir, source, catalog, _clone = _fixture()
    monkeypatch.setattr(layout, "_index_physical_details", AsyncMock())
    catalog["indexes"].pop(0)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_retained_native_source(fhir, source, ("entity_address_unified",))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["unknown_index", "wrong_method", "wrong_opclass", "wrong_primary", "missing_declaration"]
)
async def test_original_source_must_pass_declared_native_checks(monkeypatch, change):
    fhir, source_relation, catalog_by_field, _clone = _fixture()
    monkeypatch.setattr(layout, "_index_physical_details", AsyncMock())
    if change == "unknown_index":
        catalog_by_field["indexes"][1]["index_name"] = "unknown"
    if change == "wrong_method":
        catalog_by_field["indexes"][1]["index_am"] = "gin"
    if change == "wrong_opclass":
        index = next(entry for entry in catalog_by_field["indexes"] if "gin__int_ops" in entry["opclass_names"])
        index["opclass_names"] = ["wrong_ops"] * len(index["opclass_names"])
    if change == "wrong_primary":
        catalog_by_field["indexes"][0]["indkey"] = "0"
    if change == "missing_declaration":
        catalog_by_field["indexes"].pop(1)
    with pytest.raises(RuntimeError, match="storage_shape_unsupported"):
        await layout.capture_retained_native_source(fhir, source_relation, ("entity_address_unified",))


def _annotated_producer():
    producer = _producer()
    policy = _retention_policy()
    producer.lease = signed_cms_plan(producer.plan, retention_policy=policy)
    _fhir, source_relation, original, clone = _fixture(database_oid=producer.lease.database_oid)
    source = layout.RetainedNativeSourceLayout(
        "entity_address_unified",
        source_relation.schema,
        source_relation.relation,
        source_relation.oid,
        producer.lease.database_oid,
        layout._canonical_catalog(original),
        _hash(original),
    )
    relation = OwnedRelation(clone["relation"]["schema_name"], "entity_address_unified", 21, 0, "p")
    annotation = RetainedNativeRelation(
        relation.schema,
        relation.relation,
        relation.oid,
        json.dumps(policy, sort_keys=True, separators=(",", ":")),
        source,
    )
    request = NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, (relation,), (), (), (annotation,))
    return producer, relation, annotation, request


@pytest.mark.asyncio
async def test_signed_clone_dispatch_preserves_profile_default(monkeypatch):
    producer, relation, annotation, request = _annotated_producer()
    capture = AsyncMock(return_value=layout.NativeRelationLayout(21, (42,), "a" * 64))
    monkeypatch.setattr(layout, "capture_retained_native_layout", capture)
    producer._current_wal_bytes = AsyncMock(return_value=0)
    await producer._assert_physical(request, {"data_tablespace_oid": 42})
    capture.assert_awaited_once_with(
        producer.fhir, relation, annotation.source_layout, producer.plan.native_address_targets
    )
    profile = AsyncMock(return_value=SimpleNamespace(relation_oid=21, effective_tablespace_oids=(42,)))
    producer.fhir._provider_directory_profile_relation_storage_fingerprint = profile
    await producer._assert_physical(replace(request, native_relations=()), {"data_tablespace_oid": 42})
    profile.assert_awaited_once_with(21, expected_persistence="p")


@pytest.mark.parametrize(
    "change", [{"schema": "wrong_capture"}, {"relation": "unknown"}, {"oid": 22}, {"oid": True}, {"policy_json": "{}"}]
)
def test_annotation_refuses_wrong_signed_capture_or_original_oid(change):
    producer, _relation, annotation, request = _annotated_producer()
    with pytest.raises(RuntimeError, match="native_identity_changed"):
        producer._native_relation_policies(replace(request, native_relations=(replace(annotation, **change),)))


def test_annotation_requires_signed_policy_and_refuses_duplicates():
    producer, _relation, annotation, request = _annotated_producer()
    with pytest.raises(RuntimeError, match="native_identity_changed"):
        producer._native_relation_policies(replace(request, native_relations=(annotation, annotation)))
    producer.lease = signed_cms_plan(producer.plan)
    with pytest.raises(RuntimeError, match="raw_policy_required"):
        producer._native_relation_policies(replace(request, lease=producer.lease))


@pytest.mark.asyncio
async def test_registration_rechecks_source_once_and_survives_source_rename(monkeypatch):
    producer, relation, annotation, _request = _annotated_producer()
    admission = NonprofileAdmission(producer.lease, producer.plan, AsyncMock())
    source_layout = annotation.source_layout
    admission._relations[(source_layout.schema, source_layout.relation)] = source_layout.oid
    admission._external_relations.add((source_layout.schema, source_layout.relation))
    admission._started = True
    source_relation = OwnedRelation(source_layout.schema, source_layout.relation, source_layout.oid, 0, "p")
    admission.measure = AsyncMock(return_value=(source_relation, relation))
    admission._check = AsyncMock()
    capture = AsyncMock(return_value=source_layout)
    monkeypatch.setattr(layout, "capture_retained_native_source", capture)
    await admission.register_external_relation(
        producer.fhir, relation.schema, relation.relation, relation.oid, native_relation=annotation
    )
    del admission._relations[(source_layout.schema, source_layout.relation)]
    await admission.assert_external_relation(
        producer.fhir, relation.schema, relation.relation, relation.oid, native_relation=annotation
    )
    capture.assert_awaited_once()
    with pytest.raises(RuntimeError, match="native_identity_invalid"):
        await admission.assert_external_relation(producer.fhir, relation.schema, relation.relation, relation.oid)
    with pytest.raises(RuntimeError, match="native_identity_invalid"):
        await admission.assert_external_relation(
            producer.fhir,
            relation.schema,
            relation.relation,
            relation.oid,
            native_relation=replace(annotation, policy_json="{}"),
        )
    with pytest.raises(RuntimeError, match="raw_identity_invalid"):
        await admission.rename_external_relation(
            producer.fhir, relation.schema, relation.relation, "renamed", relation.oid
        )
    producer.fhir._unscoped_qt = lambda schema, name: schema + "." + name
    producer.fhir.db.scalar.return_value = None
    await admission.retire_external_relation(producer.fhir, relation.schema, relation.relation, relation.oid)
    assert not admission._native_relations


def test_default_check_keeps_positional_compatibility():
    producer = _producer()
    assert NonprofileAdmissionCheck("readiness", producer.lease, producer.plan, ()).native_relations == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["unregistered", "source_oid", "source_evidence", "conflicting_clone"])
async def test_native_registration_rejects_unadmitted_or_conflicting_source(monkeypatch, change):
    producer, relation, annotation, _request = _annotated_producer()
    admission = NonprofileAdmission(producer.lease, producer.plan, AsyncMock())
    source_layout = annotation.source_layout
    source_relation = OwnedRelation(source_layout.schema, source_layout.relation, source_layout.oid, 0, "p")
    admission._started = True
    admission._relations[(source_layout.schema, source_layout.relation)] = source_layout.oid
    admission._external_relations.add((source_layout.schema, source_layout.relation))
    admission.measure = AsyncMock(return_value=(source_relation, relation))
    admission._check = AsyncMock()
    captured_source = source_layout
    if change == "unregistered":
        admission._external_relations.clear()
    if change == "source_oid":
        admission._relations[(source_layout.schema, source_layout.relation)] += 1
    if change == "source_evidence":
        captured_source = replace(source_layout, exact_fingerprint="0" * 64)
    if change == "conflicting_clone":
        admission._native_relations[(relation.schema, relation.relation)] = replace(annotation, oid=22)
    monkeypatch.setattr(layout, "capture_retained_native_source", AsyncMock(return_value=captured_source))
    with pytest.raises(RuntimeError, match="native_identity_invalid"):
        await admission.register_external_relation(
            producer.fhir, relation.schema, relation.relation, relation.oid, native_relation=annotation
        )
