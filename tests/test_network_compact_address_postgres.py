# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact offices across shared compact artifacts and manual overrides."""

import json
import os
from dataclasses import asdict, replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import MetaData, select, text
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.schema import CreateTable

from api.network_address_scope import (
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
    scoped_address_statement,
)
from api.network_compact_address import (
    compact_address_parameters,
    compact_address_relation_sql,
    compact_address_selectable,
)
from api.network_read_execution import network_read_relations
from db.models import EntityAddressUnified
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactEdgeArtifact,
    CompactNetworkLayout,
    verify_compact_layout_catalog,
)
from process.network_serving_read import PinnedNetworkServingManifest
from tests.test_network_membership_slices_postgres import _candidate, _pin, _prepare, slice_db

_VISIBLE_MEMBERSHIP = """EXISTS(SELECT 1 FROM unnest(canonical_network_ids) visible_network(network_id)
    WHERE visible_network.network_id=ANY(CAST(:visible_ids AS integer[])))"""


async def _address_heap(db, schema, records):
    model_table = EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema)
    await db.connection.execute(str(CreateTable(model_table).compile(dialect=dialect())))
    await db.connection.copy_records_to_table(
        "entity_address_unified",
        schema_name=schema,
        columns=("location_key", "entity_type", "entity_id", "npi", "entity_name", "row_origin", "checksum", "type"),
        records=records,
    )
    oid = await db.connection.fetchval("SELECT to_regclass($1)::oid", schema + ".entity_address_unified")
    return CompactAddressRelation(schema, "entity_address_unified", oid)


@pytest.fixture
async def compact_db(slice_db):
    db = slice_db
    manual_schema = "network_manual_test_" + uuid4().hex
    db.schemas.append(manual_schema)
    await db.connection.execute(f'CREATE SCHEMA "{manual_schema}"')
    base = await _address_heap(
        db,
        db.source_schema,
        [
            (db.keys[0], "npi", "provider-a", 1111111111, "base name", "base", 1, "practice"),
            (
                db.keys[1],
                "npi",
                "provider-a",
                1111111111,
                "unsupported same-provider office",
                "ptg_overlay",
                2,
                "practice",
            ),
        ],
    )
    custom = await _address_heap(
        db,
        manual_schema,
        [
            (db.keys[0], "npi", "provider-a", 1111111111, "manual override", "manual", 3, "practice"),
            (db.keys[2], "npi", "provider-b", 2222222222, "manual custom", "manual", 4, "practice"),
            (db.keys[3], "npi", "provider-c", 3333333333, "hidden manual", "manual", 5, "practice"),
        ],
    )
    first = await _candidate(db, [(42, 0), (42, 0), (7, 0), (88, 2)])
    async with db.connection.transaction():
        first_receipt = await _prepare(db, first)
    second = await _candidate(db, [(42, 0), (7, 0), (99, 2)])
    async with db.connection.transaction():
        second_receipt = await _prepare(db, second, previous=_pin(first, first_receipt))
    old_artifact = CompactEdgeArtifact(first.candidate_id, first.schema_name, first_receipt.edge_heap_oid)
    new_artifact = CompactEdgeArtifact(second.candidate_id, second.schema_name, second_receipt.edge_heap_oid)
    db.first_layout = CompactNetworkLayout(
        CompactAddressRelation(first.schema_name, "network_slice_ref", first_receipt.ref_map_oid),
        base,
        (old_artifact,),
        custom,
    )
    db.second_layout = CompactNetworkLayout(
        CompactAddressRelation(second.schema_name, "network_slice_ref", second_receipt.ref_map_oid),
        base,
        tuple(sorted((old_artifact, new_artifact), key=lambda artifact: artifact.candidate_id)),
        custom,
    )
    db.engine = create_async_engine(
        os.environ["NETWORK_REGISTRY_TEST_DSN"].replace("postgresql://", "postgresql+asyncpg://"), poolclass=NullPool
    )
    try:
        yield db
    finally:
        await db.engine.dispose()


async def _read(db, layout, *, network_ids=None, visible_ids=None, typed=False):
    visibility = _VISIBLE_MEMBERSHIP if visible_ids is not None else None
    options_by_name = {"network_ids": network_ids, "visible_membership_sql": visibility}
    if typed:
        relation = compact_address_selectable(layout, **options_by_name)
        statement = select(relation).order_by(relation.c.location_key)
    else:
        statement = text(
            f"SELECT * FROM {compact_address_relation_sql(layout, **options_by_name)} addresses ORDER BY location_key"
        )
    parameters = compact_address_parameters(layout)
    if network_ids is not None:
        parameters["_canonical_network_ids"] = list(network_ids)
    if visible_ids is not None:
        parameters["visible_ids"] = list(visible_ids)
    async with db.engine.connect() as connection:
        return [dict(row) for row in (await connection.execute(statement, parameters)).mappings()]


async def _inventory(db, layout):
    schemas = {layout.ref_map.schema_name, layout.base.schema_name}
    schemas.update(artifact.schema_name for artifact in layout.artifacts)
    if layout.custom is not None:
        schemas.add(layout.custom.schema_name)
    rows = await db.connection.fetch(
        """SELECT n.nspname,c.relname,c.oid FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=ANY($1::text[]) AND c.relkind='r'""",
        sorted(schemas),
    )
    return {(row["nspname"], row["relname"]): row["oid"] for row in rows}


@pytest.mark.asyncio
async def test_shared_generations_keep_exact_offices(compact_db):
    db = compact_db
    for typed in (False, True):
        first_rows = await _read(db, db.first_layout, network_ids=(42,), typed=typed)
        second_rows = await _read(db, db.second_layout, network_ids=(42,), typed=typed)
        assert first_rows == second_rows
        assert [(row["location_key"], row["canonical_network_ids"]) for row in second_rows] == [(db.keys[0], [7, 42])]
        assert second_rows[0]["entity_name"] == "manual override"
        assert await _read(db, db.first_layout, network_ids=(99,), typed=typed) == []
        assert await _read(db, db.second_layout, network_ids=(88,), typed=typed) == []
        custom_rows = await _read(db, db.second_layout, network_ids=(99,), typed=typed)
        assert [(row["location_key"], row["canonical_network_ids"]) for row in custom_rows] == [(db.keys[2], [99])]


@pytest.mark.asyncio
@pytest.mark.parametrize("component", ["base", "custom", "ref_map", "edge"])
async def test_compact_raw_and_typed_reads_exclude_attached_child_rows(compact_db, component):
    db, layout = compact_db, compact_db.second_layout
    assert verify_compact_layout_catalog(layout, await _inventory(db, layout)) is layout
    baseline = await _read(db, layout, visible_ids=(7, 42, 99))
    if component == "edge":
        candidate_id = await db.connection.fetchval(
            f'SELECT artifact_candidate_id::text FROM "{layout.ref_map.schema_name}".network_slice_ref '
            "WHERE network_id=42 LIMIT 1"
        )
        artifact = next(artifact for artifact in layout.artifacts if artifact.candidate_id == candidate_id)
        parent = f'"{artifact.schema_name}".network_office_edge'
        changes, predicate, argument = {"location_key": db.keys[1]}, "location_key=$2", db.keys[0]
    else:
        relation = getattr(layout, component)
        parent = f'"{relation.schema_name}"."{relation.table_name}"'
        if component == "ref_map":
            changes, predicate, argument = {"network_id": 777}, "network_id=$2", 42
        else:
            changes, predicate = {}, "location_key=$2"
            argument = db.keys[1] if component == "base" else db.keys[0]
    child = f'"{layout.custom.schema_name}".raw_read_child'
    await db.connection.execute(f"CREATE TABLE {child} (LIKE {parent} INCLUDING CONSTRAINTS)")
    try:
        await db.connection.execute(
            f"INSERT INTO {child} SELECT (jsonb_populate_record(NULL::{parent},to_jsonb(row)||$1::jsonb)).* "
            f"FROM ONLY {parent} row WHERE {predicate} LIMIT 1",
            json.dumps(changes),
            argument,
        )
        assert await db.connection.fetchval(f"SELECT count(*) FROM {child}") == 1
        await db.connection.execute(f"ALTER TABLE {child} INHERIT {parent}")
        for typed in (False, True):
            assert await _read(db, layout, visible_ids=(7, 42, 99), typed=typed) == baseline
    finally:
        await db.connection.execute(f"DROP TABLE {child}")


@pytest.mark.asyncio
async def test_default_visibility_and_exact_manual_override(compact_db):
    db = compact_db
    for typed in (False, True):
        without_custom = await _read(db, replace(db.second_layout, custom=None), typed=typed)
        assert [row["location_key"] for row in without_custom] == [db.keys[0], db.keys[1]]
        assert without_custom[0]["entity_name"] == "base name"
        base_only = await _read(db, db.second_layout, typed=typed)
        assert [row["location_key"] for row in base_only] == [db.keys[1]]
        assert base_only[0]["row_origin"] == "ptg_overlay" and base_only[0]["canonical_network_ids"] == []
        visible = await _read(db, db.second_layout, visible_ids=(42,), typed=typed)
        assert [row["location_key"] for row in visible] == [db.keys[0], db.keys[1]]
        assert visible[0]["entity_name"] == "manual override" and visible[0]["canonical_network_ids"] == [7, 42]
        all_visible = await _read(db, db.second_layout, visible_ids=(42, 99), typed=typed)
        assert [row["location_key"] for row in all_visible] == [db.keys[0], db.keys[1], db.keys[2]]
    assert await _read(db, db.second_layout, visible_ids=(42, 99)) == await _read(
        db, db.second_layout, visible_ids=(42, 99), typed=True
    )


@pytest.mark.asyncio
async def test_columns_parameters_and_physical_fanout(compact_db):
    layout = compact_db.second_layout
    raw = compact_address_relation_sql(layout, network_ids=(42,))
    selectable = compact_address_selectable(layout, network_ids=(42,))
    assert raw.startswith("(") and raw.endswith(")")
    assert tuple(selectable.c.keys()) == tuple(EntityAddressUnified.__table__.columns.keys())
    assert "ARRAY[42]" not in raw and "=42" not in raw and "IN (42" not in raw
    assert ":_canonical_network_ids" in raw
    for index, artifact in enumerate(layout.artifacts):
        assert artifact.candidate_id not in raw
        assert f":_compact_artifact_{index}" in raw
        assert raw.count(f'"{artifact.schema_name}".network_office_edge') == 2
    assert set(compact_address_parameters(layout).values()) == {artifact.candidate_id for artifact in layout.artifacts}
    async with compact_db.engine.connect() as connection:
        await connection.execute(text("SET LOCAL enable_seqscan=off"))
        plan = await connection.scalar(
            text(f"EXPLAIN (FORMAT JSON) SELECT location_key FROM {raw} addresses"),
            {**compact_address_parameters(layout), "_canonical_network_ids": [42]},
        )
        assert "Index" in str(plan) and "network_office_edge" in str(plan)


@pytest.mark.asyncio
async def test_empty_artifacts_are_valid_and_select_nothing(compact_db):
    db = compact_db
    empty = await _candidate(db, [])
    async with db.connection.transaction():
        receipt = await _prepare(db, empty)
    layout = replace(
        db.second_layout,
        ref_map=CompactAddressRelation(empty.schema_name, "network_slice_ref", receipt.ref_map_oid),
        artifacts=(),
    )
    for typed in (False, True):
        assert await _read(db, layout, network_ids=(42,), typed=typed) == []
        rows = await _read(db, layout, typed=typed)
        assert [(row["location_key"], row["canonical_network_ids"]) for row in rows] == [(db.keys[1], [])]


@pytest.mark.asyncio
async def test_legitimate_retained_name_requires_fresh_resolution(compact_db):
    db = compact_db
    layout = db.second_layout
    assert verify_compact_layout_catalog(layout, await _inventory(db, layout)) is layout
    retained_name = "entity_address_unified_retained_" + format(layout.base.table_oid, "x")
    await db.connection.execute(
        f'ALTER TABLE "{layout.base.schema_name}"."{layout.base.table_name}" RENAME TO "{retained_name}"'
    )
    with pytest.raises(ValueError, match="physical relation"):
        verify_compact_layout_catalog(layout, await _inventory(db, layout))
    resolved = replace(layout, base=replace(layout.base, table_name=retained_name))
    assert verify_compact_layout_catalog(resolved, await _inventory(db, resolved)) is resolved
    assert [row["location_key"] for row in await _read(db, resolved, network_ids=(42,))] == [db.keys[0]]


@pytest.mark.asyncio
@pytest.mark.parametrize("role", ["ref_map", "base", "custom", "artifact"])
async def test_physical_swaps_fail_fresh_inventory_verifier(compact_db, role):
    db = compact_db
    layout = db.second_layout
    relation = layout.artifacts[0] if role == "artifact" else getattr(layout, role)
    table_name = "network_office_edge" if role == "artifact" else relation.table_name
    namespace = '"' + relation.schema_name + '".'
    await db.connection.execute(f'ALTER TABLE {namespace}"{table_name}" RENAME TO swapped_original')
    await db.connection.execute(
        f'CREATE TABLE {namespace}"{table_name}"(LIKE {namespace}swapped_original INCLUDING ALL)'
    )
    assert layout.base.table_oid > 0
    with pytest.raises(ValueError, match="physical relation"):
        verify_compact_layout_catalog(layout, await _inventory(db, layout))


@pytest.mark.parametrize("network_ids", [(), (0,), (2147483648,), (True,), (42, 42), (99, 42), [42]])
def test_invalid_selectors_are_rejected(network_ids):
    layout = CompactNetworkLayout(
        CompactAddressRelation("generation_a", "network_slice_ref", 1),
        CompactAddressRelation("base_a", "entity_address_unified", 2),
        (),
    )
    with pytest.raises(ValueError, match="selector"):
        compact_address_relation_sql(layout, network_ids=network_ids)


def _manifest(layout):
    candidate_hex = layout.ref_map.schema_name.removeprefix("network_candidate_")
    candidate_id = str(UUID(candidate_hex))
    manifest = PinnedNetworkServingManifest(
        2,
        candidate_id,
        layout.ref_map.schema_name,
        2,
        {"unified_address": "address-edition-a"},
        7,
        "a" * 64,
        layout.base.table_oid,
    )
    object.__setattr__(manifest, "_compact_layout", layout)
    return manifest


async def _scoped_rows(db):
    parameters = scoped_address_parameters({})
    raw_relation = scoped_address_relation_sql("mrf.entity_address_unified")
    raw_statement = text(f"SELECT * FROM {raw_relation} addresses ORDER BY location_key")
    original = EntityAddressUnified.__table__
    typed_statement = scoped_address_statement(select(original).order_by(original.c.location_key))
    async with db.engine.connect() as connection:
        raw_rows = [dict(row) for row in (await connection.execute(raw_statement, parameters)).mappings()]
        typed_rows = [dict(row) for row in (await connection.execute(typed_statement, parameters)).mappings()]
    assert raw_rows == typed_rows
    return raw_rows


@pytest.mark.asyncio
async def test_actual_scope_reads_and_context_reset(compact_db, monkeypatch):
    db = compact_db
    manifest = _manifest(db.second_layout)
    monkeypatch.setenv("HLTHPRT_NETWORK_REGISTRY_SCHEMA", db.source_schema)
    await db.connection.execute(f"""CREATE TABLE "{db.source_schema}".registry_approved_record(
        approved_revision bigint,record_kind text,record_key text,record_json jsonb)
    """)
    for network_id in (42, 99):
        await db.connection.execute(
            f"INSERT INTO \"{db.source_schema}\".registry_approved_record VALUES(7,'network',$1,"
            "jsonb_build_object('archived',false,'network_id',$2::integer))",
            str(network_id),
            network_id,
        )
    assert current_network_address_scope() is None
    with network_address_read_scope(manifest, (42,)) as scope:
        assert current_network_address_scope() is scope
        assert scope.manifest.compact_layout.ref_map.schema_name == manifest.schema_name
        assert scope.manifest.compact_layout.base.table_oid == manifest.address_table_oid
        offices = await _scoped_rows(db)
        assert [(office["location_key"], office["canonical_network_ids"]) for office in offices] == [
            (db.keys[0], [7, 42])
        ]
    assert current_network_address_scope() is None
    with network_address_read_scope(manifest, None, access_scope_sha256="b" * 64, excluded_network_ids=(99,)):
        offices = await _scoped_rows(db)
        assert [office["location_key"] for office in offices] == [db.keys[0], db.keys[1]]
    assert current_network_address_scope() is None
    with pytest.raises(RuntimeError, match="synthetic failure"):
        with network_address_read_scope(manifest, (42,)):
            raise RuntimeError("synthetic failure")
    assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_scope_rejects_reserved_parameter_mixtures(compact_db):
    manifest = _manifest(compact_db.second_layout)
    physical_parameters = compact_address_parameters(manifest.compact_layout)
    name = next(iter(physical_parameters))
    with network_address_read_scope(manifest, (42,)):
        expected_parameters_by_name = {**physical_parameters, "_canonical_network_ids": [42]}
        assert scoped_address_parameters(physical_parameters) == expected_parameters_by_name
        for provided in (
            {name: str(uuid4())},
            {name: UUID(physical_parameters[name])},
            {"_compact_unknown": "value"},
            {**physical_parameters, "_compact_artifact_9999": str(uuid4())},
            {"_canonical_network_ids": [99]},
            {"_network_excluded_ids": []},
        ):
            with pytest.raises(ValueError, match="reserved_parameter"):
                scoped_address_parameters(provided)
    with network_address_read_scope(manifest, None, access_scope_sha256="b" * 64, excluded_network_ids=()):
        for provided in ({"_canonical_network_ids": [42]}, {"_compact_unknown": "value"}):
            with pytest.raises(ValueError, match="reserved_parameter"):
                scoped_address_parameters(provided)
    assert current_network_address_scope() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["ref_schema", "base_oid"])
async def test_scope_rejects_mismatched_layout_coordinates(compact_db, damage):
    layout = compact_db.second_layout
    manifest = _manifest(layout)
    if damage == "ref_schema":
        layout = replace(layout, ref_map=replace(layout.ref_map, schema_name="different_generation"))
    else:
        layout = replace(layout, base=replace(layout.base, table_oid=4294967295))
    object.__setattr__(manifest, "_compact_layout", layout)
    with pytest.raises(ValueError, match="scope_invalid"):
        with network_address_read_scope(manifest, (42,)):
            pytest.fail("Mismatched physical coordinates must not enter a scope")
    assert current_network_address_scope() is None


@pytest.mark.asyncio
async def test_legacy_scope_and_manifest_wire_stay_unchanged(compact_db):
    manifest = _manifest(compact_db.second_layout)
    legacy = replace(manifest)
    assert legacy.compact_layout is None
    assert asdict(manifest) == asdict(legacy)
    assert set(asdict(manifest)) == {
        "generation_id",
        "candidate_id",
        "schema_name",
        "schema_revision",
        "source_generations",
        "approved_custom_revision",
        "manifest_sha256",
        "address_table_oid",
    }
    with network_address_read_scope(legacy, (42,)) as scope:
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        relations = (
            CompactAddressRelation(manifest.schema_name, "entity_address_unified", manifest.address_table_oid),
        )
        assert network_read_relations(relation) == relations
        marker = scope.read_statements.marker(relations)
        assert relation == (
            f'({marker} SELECT * FROM ONLY "{manifest.schema_name}".entity_address_unified '
            "WHERE canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[]))"
        )
        assert scoped_address_parameters({}) == {"_canonical_network_ids": [42]}
    assert scoped_address_relation_sql("mrf.entity_address_unified") == "mrf.entity_address_unified"
    statement = select(EntityAddressUnified.__table__)
    assert scoped_address_statement(statement) is statement
