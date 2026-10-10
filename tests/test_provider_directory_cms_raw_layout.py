# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed raw recipe prefixes and observed native heap/index rejection."""

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_raw_layout as layout
from process.provider_directory_cms_preparation import OwnedRelation
from tests.test_provider_directory_cms_capacity_contract import _retention_policy


def _attribute(oid, ordinal, name, type_oid=1043):
    return {
        "relation_oid": oid,
        "attnum": ordinal,
        "attname": name,
        "atttypid": type_oid,
        "atttypmod": 68 if type_oid == 1043 else -1,
        "attcollation": 100 if type_oid == 1043 else 0,
        "attnotnull": True,
        "attstorage": "x" if type_oid in (1043, 114) else "p",
        "typlen": -1 if type_oid in (1043, 114, 17) else 4,
        **layout._UNCOPIED_FIELDS,
    }


def _index(relation, name, keys, primary, attributes, index_oid):
    attributes_by_name = {attribute["attname"]: attribute for attribute in attributes}
    key_attributes = [attributes_by_name[key] for key in keys]
    return {
        "relation_oid": relation.oid,
        "index_oid": index_oid,
        "index_name": name,
        "index_am": "btree",
        "relfilenode": index_oid,
        "effective_tablespace_oid": 42,
        "tablespace_oid": 0,
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
        "indnatts": len(keys),
        "indnkeyatts": len(keys),
        "indoption": " ".join("0" for _ in keys),
        "indkey": " ".join(str(attribute["attnum"]) for attribute in key_attributes),
        "indcollation": " ".join(str(attribute["attcollation"]) for attribute in key_attributes),
        "indclass": " ".join(
            str(_classes()[layout._KEY_CLASSES[attribute["atttypid"]]]) for attribute in key_attributes
        ),
        "index_schema": relation.schema,
        "index_persistence": "p",
        "index_expressions": "",
        "index_predicate": "",
        "reloptions": [],
        "main_bytes": 8192,
    }


def _classes():
    return {name: ordinal for ordinal, name in enumerate(sorted(set(layout._KEY_CLASSES.values())), 501)}


def _fixture(prefix=0, toast=False):
    policy = _retention_policy()
    name = layout._TABLES[1] if toast else layout._TABLES[2]
    relation = OwnedRelation("registry_cms_epoch_" + "0" * 31 + "1", name, 11, 8192, "p")
    profiles = layout._raw_profiles(name)
    names = tuple(profile[0] for profile in profiles[:prefix])
    columns = tuple(dict.fromkeys(key for _, keys, _ in profiles for key in keys))
    attributes = [_attribute(relation.oid, ordinal, column) for ordinal, column in enumerate(columns, 1)]
    if toast:
        attributes.append(_attribute(relation.oid, len(attributes) + 1, "payload_json", 114))
    source_attributes = [{**attribute, "relation_oid": 123} for attribute in attributes]
    indexes = [
        _index(relation, index, keys, primary, attributes, ordinal)
        for ordinal, (index, keys, primary) in enumerate(profiles[:prefix], 101)
    ]
    constraints = (
        []
        if not prefix
        else [
            {
                "relation_oid": relation.oid,
                "constraint_type": "p",
                "condeferrable": False,
                "condeferred": False,
                "convalidated": True,
                "constraint_definition": "PRIMARY KEY (" + ", ".join(profiles[0][1]) + ")",
            }
        ]
    )
    relation_map = {
        "relation_oid": relation.oid,
        "schema_name": relation.schema,
        "relation_name": name,
        "relreplident": "d",
        "relfilenode": 201,
        "effective_tablespace_oid": 42,
        "reloptions": [],
        "toast_reloptions": [],
        "toast_relfilenode": 202 if toast else None,
        "toast_effective_tablespace_oid": 42 if toast else None,
    }
    if toast:
        _add_toast(attributes, indexes, relation)
    return SimpleNamespace(
        policy=policy,
        relation=relation,
        names=names,
        attributes=attributes,
        indexes=indexes,
        source_attributes=source_attributes,
        constraints=constraints,
        triggers=[],
        relation_map=relation_map,
        toast_oid=12 if toast else None,
    )


def _add_toast(attributes, indexes, relation):
    toast_columns = [
        {**_attribute(12, ordinal, name, type_oid), "attnotnull": False}
        for ordinal, (name, type_oid) in enumerate((("chunk_id", 26), ("chunk_seq", 23), ("chunk_data", 17)), 1)
    ]
    attributes.extend(toast_columns)
    toast_relation = replace(relation, schema="pg_toast", oid=12)
    index = _index(toast_relation, "pg_toast_11_index", ("chunk_id", "chunk_seq"), True, toast_columns, 301)
    indexes.append(index)


def _fhir(fixture):
    async def catalog(oids):
        if oids == [123]:
            return deepcopy(fixture.source_attributes), [], [], []
        return deepcopy(fixture.attributes), deepcopy(fixture.indexes), fixture.constraints, fixture.triggers

    async def scalar(query, **parameters):
        return 123 if "FROM pg_class" in query else 7

    async def all_indexes(query, **parameters):
        if "operator.opcdefault" in query:
            return [{"name": name, "oid": oid} for name, oid in _classes().items()]
        return [
            {
                "index_oid": index["index_oid"],
                "index_schema": index["index_schema"],
                "index_persistence": index["index_persistence"],
                "opclass_names": [],
            }
            for index in fixture.indexes
        ]

    def tablespaces(relation_map, indexes, toast_oid):
        spaces = {relation_map["effective_tablespace_oid"], *(index["effective_tablespace_oid"] for index in indexes)}
        if toast_oid:
            spaces.add(relation_map["toast_effective_tablespace_oid"])
        return tuple(sorted(spaces))

    return SimpleNamespace(
        db=SimpleNamespace(scalar=scalar, all=all_indexes),
        _unscoped_qt=lambda schema, name: f'"{schema}"."{name}"',
        _pagination_checkpoint_row_mapping=dict,
        _profile_capacity_relation_row=AsyncMock(return_value=(fixture.relation_map, fixture.toast_oid)),
        _profile_capacity_relation_catalog=catalog,
        _profile_capacity_tablespaces=tablespaces,
        _identity_hash=lambda catalog_by_field: "a" * 64,
    )


async def _capture(fixture):
    return await layout.capture_retained_raw_layout(_fhir(fixture), fixture.relation, fixture.policy, fixture.names)


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", [0, 1, 2, 3])
async def test_raw_heap_and_every_index_prefix(prefix):
    fixture = _fixture(prefix)
    captured = await _capture(fixture)
    assert captured == layout.NativeRelationLayout(11, (42,), "a" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix", [0, 1])
async def test_toast_heap_and_index_remain_measured(prefix):
    fixture = _fixture(prefix, toast=True)
    assert (await _capture(fixture)).effective_tablespace_oids == (42,)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["schema", "logical", "persistence", "oid", "policy", "prefix", "gap"])
async def test_raw_coordinates_and_prefix_rejected(mutation):
    fixture = _fixture(2)
    if mutation in {"schema", "logical", "persistence", "oid"}:
        field = "relation" if mutation == "logical" else mutation
        fixture.relation = replace(fixture.relation, **{field: 0 if field == "oid" else "unexpected"})
    elif mutation == "policy":
        fixture.policy["unexpected"] = True
    elif mutation == "prefix":
        fixture.names = list(fixture.names)
    else:
        fixture.names = (fixture.names[1],)
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,replacement",
    [
        ("atttypid", 23),
        ("atttypmod", 99),
        ("attcollation", 0),
        ("attstorage", "p"),
        ("attnotnull", False),
        ("attcompression", "l"),
        ("atthasdef", True),
        ("default_expression", "1"),
        ("attidentity", "a"),
        ("attgenerated", "s"),
    ],
)
async def test_copied_column_mutations_rejected(field, replacement):
    fixture = _fixture()
    fixture.attributes[0][field] = replacement
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
async def test_uncopied_source_options_are_not_inherited():
    fixture = _fixture()
    fixture.source_attributes[0].update(
        atthasdef=True, default_expression="'example'", attidentity="a", attgenerated="s", attcompression="l"
    )
    await _capture(fixture)


@pytest.mark.asyncio
async def test_unknown_source_type_rejected():
    fixture = _fixture()
    fixture.source_attributes[0]["atttypid"] = fixture.attributes[0]["atttypid"] = 900001
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
async def test_pinned_source_oid_rejected():
    fixture = _fixture(toast=True)
    fixture.policy["source_pin"]["resource_table_oid"] = 124
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,replacement",
    [
        ("index_name", "unknown"),
        ("index_am", "hash"),
        ("indkey", "2 1 3"),
        ("indclass", "999 999 999"),
        ("indoption", "1 0 0"),
        ("indcollation", "0 0 0"),
        ("index_expressions", "lower(source_id)"),
        ("index_predicate", "source_id IS NOT NULL"),
        ("indnatts", 4),
        ("indnkeyatts", 2),
        ("indisunique", False),
        ("indisprimary", False),
        ("indisvalid", False),
        ("indisready", False),
        ("indislive", False),
        ("indimmediate", False),
        ("index_persistence", "u"),
        ("index_schema", "unexpected"),
        ("effective_tablespace_oid", 43),
        ("reloptions", ["fillfactor=50"]),
    ],
)
async def test_raw_index_mutations_rejected(field, replacement):
    fixture = _fixture(1)
    fixture.indexes[0][field] = replacement
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["missing", "extra", "wrong_type", "wrong_key", "tablespace"])
async def test_toast_mutations_rejected(mutation):
    fixture = _fixture(toast=True)
    if mutation == "missing":
        fixture.indexes.clear()
    elif mutation == "extra":
        fixture.indexes.append(deepcopy(fixture.indexes[0]))
    elif mutation == "wrong_type":
        fixture.attributes[-1]["atttypid"] = 25
    elif mutation == "wrong_key":
        fixture.indexes[0]["indkey"] = "2 1"
    else:
        fixture.relation_map["toast_effective_tablespace_oid"] = 43
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
async def test_toast_primary_flag_rejected():
    fixture = _fixture(toast=True)
    toast_index = next(index for index in fixture.indexes if index["relation_oid"] == fixture.toast_oid)
    toast_index["indisprimary"] = False
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("ordinal", [0, 1, 2])
async def test_toast_notnull_flag_rejected(ordinal):
    fixture = _fixture(toast=True)
    toast_columns = [attribute for attribute in fixture.attributes if attribute["relation_oid"] == fixture.toast_oid]
    toast_columns[ordinal]["attnotnull"] = True
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["trigger", "constraint", "missing_pk", "extra_index", "moved_oid", "replica"])
async def test_unowned_catalog_changes_rejected(mutation):
    fixture = _fixture(1)
    if mutation in {"moved_oid", "replica"}:
        field, replacement = ("relation_oid", 99) if mutation == "moved_oid" else ("relreplident", "n")
        fixture.relation_map[field] = replacement
    elif mutation == "trigger":
        fixture.triggers.append({"trigger_name": "unexpected"})
    elif mutation == "constraint":
        fixture.constraints[0]["constraint_type"] = "c"
    elif mutation == "missing_pk":
        fixture.constraints.clear()
    else:
        fixture.indexes.append({**fixture.indexes[0], "index_name": "unexpected"})
    with pytest.raises(RuntimeError, match="raw_storage_shape_unsupported"):
        await _capture(fixture)
