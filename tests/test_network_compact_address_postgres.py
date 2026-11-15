# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact offices across shared compact artifacts and manual overrides."""

import json
from dataclasses import asdict, replace
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.asyncio import AsyncSession

from api.network_address_scope import (
    current_network_address_scope,
    network_address_read_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
    scoped_address_statement,
)
from api.network_compact_address import (
    _membership_arms,
    compact_address_native_sql,
    compact_address_parameters,
    compact_address_read_relations,
    compact_address_relation_sql,
    compact_address_selectable,
)
from api.network_read_execution import qualified_relation_sql
from db.models import EntityAddressUnified
from process.network_compact_layout import (
    CompactAddressRelation,
    CompactNetworkLayout,
)
from tests import test_custom_import_provider_geo_sql as geo
from tests.network_compact_address_postgres_support import (
    _lookup_manifest,
    _manifest,
)
from tests.network_compact_address_postgres_support import (
    compact_db as compact_db,
)
from tests.test_network_membership_slices_postgres import _candidate, _prepare
from tests.test_network_membership_slices_postgres import slice_db as slice_db

_VISIBLE_MEMBERSHIP = """EXISTS(SELECT 1 FROM unnest(canonical_network_ids) visible_network(network_id)
    WHERE visible_network.network_id=ANY(CAST(:visible_ids AS integer[])))"""


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
        """SELECT n.nspname,c.relname FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=ANY($1::text[]) AND c.relkind='r'""",
        sorted(schemas),
    )
    return {(row["nspname"], row["relname"]) for row in rows}


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
    assert {
        (relation.schema_name, relation.table_name) for relation in compact_address_read_relations(layout)
    } <= await _inventory(db, layout)
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
    selected = _membership_arms(layout, selected=True)
    assert "office." not in selected and "SELECT edge.location_key" in selected
    assert "WHERE office.location_key IN (" + selected + ")" in raw
    assert "edge.location_key=office.location_key" in _membership_arms(layout, selected=False)
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
async def test_selected_semijoin_preserves_correlated_reference_rows(compact_db):
    db = compact_db
    for layout in (db.first_layout, db.second_layout):
        for network_ids in ((7, 42), (42, 99), (88,), (99,), (2147483647,)):
            current = compact_address_relation_sql(layout, network_ids=network_ids)
            selected = _membership_arms(layout, selected=True)
            original = selected.replace("SELECT edge.location_key", "SELECT ref.network_id").replace(
                "AND ref.network_id=ANY", "AND edge.location_key=office.location_key AND ref.network_id=ANY"
            )
            reference = current.replace(
                "WHERE office.location_key IN (" + selected + ")",
                "WHERE EXISTS(SELECT 1 FROM (" + original + ") selected_membership)",
            )
            assert reference != current
            parameters_by_name = {
                **compact_address_parameters(layout),
                "_canonical_network_ids": list(network_ids),
            }
            async with db.engine.connect() as connection:
                same = await connection.scalar(
                    text(
                        "SELECT NOT EXISTS((SELECT * FROM "
                        + current
                        + " actual EXCEPT ALL SELECT * FROM "
                        + reference
                        + " expected) UNION ALL (SELECT * FROM "
                        + reference
                        + " expected EXCEPT ALL SELECT * FROM "
                        + current
                        + " actual))"
                    ),
                    parameters_by_name,
                )
                assert same is True
            assert await _read(db, layout, network_ids=network_ids) == await _read(
                db, layout, network_ids=network_ids, typed=True
            )


@pytest.mark.asyncio
async def test_empty_artifacts_are_valid_and_select_nothing(compact_db):
    db = compact_db
    empty = await _candidate(db, [])
    async with db.connection.transaction():
        receipt = await _prepare(db, empty)
    assert (receipt.membership_rows, receipt.total_slices, receipt.copied_edge_rows) == (0, 0, 0)
    layout = replace(
        db.second_layout,
        ref_map=CompactAddressRelation(empty.schema_name, "network_slice_ref"),
        artifacts=(),
    )
    for typed in (False, True):
        assert await _read(db, layout, network_ids=(42,), typed=typed) == []
        rows = await _read(db, layout, typed=typed)
        assert [(row["location_key"], row["canonical_network_ids"]) for row in rows] == [(db.keys[1], [])]


@pytest.mark.asyncio
async def test_explicit_retained_name_keeps_complete_rows_and_missing_name_fails(compact_db):
    db = compact_db
    layout = db.second_layout
    expected = await _read(db, layout, network_ids=(42,))
    retained_name = "entity_address_unified_archived"
    await db.connection.execute(
        f'ALTER TABLE "{layout.base.schema_name}"."{layout.base.table_name}" RENAME TO "{retained_name}"'
    )
    with pytest.raises(ProgrammingError):
        await _read(db, layout, network_ids=(42,))
    resolved = replace(layout, base=replace(layout.base, table_name=retained_name))
    assert await _read(db, resolved, network_ids=(42,)) == expected
    assert await _read(db, resolved, network_ids=(42,), typed=True) == expected


@pytest.mark.parametrize("network_ids", [(), (0,), (2147483648,), (True,), (42, 42), (99, 42), [42]])
def test_invalid_selectors_are_rejected(network_ids):
    layout = CompactNetworkLayout(
        CompactAddressRelation("generation_a", "network_slice_ref"),
        CompactAddressRelation("base_a", "entity_address_unified"),
        (),
    )
    with pytest.raises(ValueError, match="selector"):
        compact_address_relation_sql(layout, network_ids=network_ids)


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
        assert scope.manifest.compact_layout.base == manifest.address_relation
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
@pytest.mark.parametrize("damage", ["ref_schema", "base_name"])
async def test_scope_rejects_mismatched_layout_coordinates(compact_db, damage):
    layout = compact_db.second_layout
    manifest = _manifest(layout)
    if damage == "ref_schema":
        layout = replace(layout, ref_map=replace(layout.ref_map, schema_name="different_generation"))
    else:
        layout = replace(layout, base=replace(layout.base, table_name="other_addresses"))
    object.__setattr__(manifest, "_compact_layout", layout)
    with pytest.raises(ValueError, match="scope_invalid"):
        with network_address_read_scope(manifest, (42,)):
            pytest.fail("Mismatched named layout must not enter a scope")
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
        "address_relation",
    }
    with network_address_read_scope(legacy, (42,)) as scope:
        relation = scoped_address_relation_sql("mrf.entity_address_unified")
        assert manifest.address_relation == scope.manifest.address_relation
        assert (
            qualified_relation_sql(manifest.schema_name, "entity_address_unified")
            == '"' + manifest.schema_name + '"."entity_address_unified"'
        )
        assert relation == (
            f'(SELECT * FROM ONLY "{manifest.schema_name}".entity_address_unified '
            "WHERE canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[]))"
        )
        assert scoped_address_parameters({}) == {"_canonical_network_ids": [42]}
    assert scoped_address_relation_sql("mrf.entity_address_unified") == "mrf.entity_address_unified"
    statement = select(EntityAddressUnified.__table__)
    assert scoped_address_statement(statement) is statement


async def _lookup_rows(db, layout, network_ids):
    options_by_name = {"network_ids": network_ids, "location_lookup": True}
    raw = compact_address_relation_sql(layout, **options_by_name)
    typed = compact_address_selectable(layout, **options_by_name)
    native, arguments = compact_address_native_sql(layout, first_parameter=2, **options_by_name)
    parameters_by_name = {**compact_address_parameters(layout), "_canonical_network_ids": list(network_ids)}
    async with db.engine.connect() as connection:
        raw_rows = [
            dict(entry)
            for entry in (
                await connection.execute(text(f"SELECT * FROM {raw} office ORDER BY location_key"), parameters_by_name)
            ).mappings()
        ]
        typed_rows = [
            dict(entry)
            for entry in (
                await connection.execute(select(typed).order_by(typed.c.location_key), parameters_by_name)
            ).mappings()
        ]
    native_rows = [
        dict(entry)
        for entry in await db.connection.fetch(
            f"SELECT * FROM {native} office WHERE office.location_key<>$1::text ORDER BY location_key", "", *arguments
        )
    ]
    assert raw_rows == typed_rows == native_rows
    return raw_rows


@pytest.mark.asyncio
async def test_lookup_matches_complete_compact_rows(compact_db):
    db = compact_db
    for layout in (db.first_layout, db.second_layout, replace(db.second_layout, custom=None)):
        for network_ids in ((42,), (7, 42), (42, 99), (88,), (99,), (2147483647,)):
            assert await _lookup_rows(db, layout, network_ids) == await _read(db, layout, network_ids=network_ids)
    overlap = await _lookup_rows(db, db.second_layout, (7, 42))
    assert len(overlap) == 1 and overlap[0]["entity_name"] == "manual override"
    assert overlap[0]["canonical_network_ids"] == [7, 42]
    assert await _lookup_rows(db, replace(db.second_layout, artifacts=()), (42,)) == []


@pytest.mark.asyncio
async def test_lookup_keeps_custom_geo_precedence(compact_db):
    db, layout = compact_db, compact_db.second_layout
    for relation in (layout.base, layout.custom):
        await db.connection.execute(f'UPDATE "{relation.schema_name}"."{relation.table_name}" SET lat=0,long=0')
    await db.connection.execute(
        f'UPDATE "{layout.custom.schema_name}"."{layout.custom.table_name}" SET lat=10,long=10 WHERE location_key=$1',
        db.keys[0],
    )
    predicate = "ST_DWithin(geography(ST_MakePoint(long::float8,lat::float8)),geography(ST_MakePoint(0,0)),1609.34)"
    for network_ids, expected in (((42,), []), ((99,), [db.keys[2]]), ((42, 99), [db.keys[2]])):
        for lookup in (False, True):
            relation, arguments = compact_address_native_sql(
                layout, first_parameter=1, network_ids=network_ids, location_lookup=lookup
            )
            keys = await db.connection.fetch(
                f"SELECT location_key FROM {relation} office WHERE {predicate} ORDER BY location_key", *arguments
            )
            assert [entry["location_key"] for entry in keys] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("direction", ["asc", "desc"])
@pytest.mark.parametrize("require_match", [False, True])
async def test_compact_imported_geo_page_parity(compact_db, monkeypatch, direction, require_match):
    db, layout = compact_db, compact_db.second_layout
    monkeypatch.setattr(geo.provider_list_sql, "ConnectionProxy", geo._PostgresGeoProxy)
    monkeypatch.setattr(geo.npi_module, "_plan_release_npi_scope", geo._empty_plan_scope)
    page_pairs = []
    async with AsyncSession(db.engine) as session:
        await geo._seed_geo_tables(session)
        for relation in (layout.base, layout.custom):
            await session.execute(
                text(f'''UPDATE "{relation.schema_name}"."{relation.table_name}"
                  SET lat=0,long=0,type='primary',address_precision='street',
                    npi=CASE WHEN location_key=:selected THEN 1000000003 ELSE 1000000002 END,
                    address_key=CASE WHEN location_key=:selected THEN CAST(:ranked AS uuid) ELSE CAST(:unranked AS uuid) END'''),
                {"selected": db.keys[0], "ranked": geo._THIRD_ADDRESS_KEY, "unranked": geo._SECOND_ADDRESS_KEY},
            )
        for lookup in (False, True):

            def address_relation(reference, **options):
                assert options["location_lookup"] is True
                relation = scoped_address_relation_sql(reference, location_lookup=lookup)
                assert ("bucket_members OFFSET 0" in relation) is lookup
                assert ("ref.office_bucket=(get_byte(sha256(" in relation) is lookup
                return relation

            monkeypatch.setattr(geo.npi_module, "scoped_address_relation_sql", address_relation)
            with network_address_read_scope(_lookup_manifest(layout), (7, 42, 99)):
                first = await geo._read_postgres_geo_page(session, direction, require_match=require_match)
                anchor = geo._provider_identities(first)[0]
                second = await geo._read_postgres_geo_page(
                    session, direction, (str(anchor[0]), anchor[1]), require_match=require_match
                )
            page_pairs.append((first, second))
    assert page_pairs[0] == page_pairs[1]
    first, second = page_pairs[1]
    assert first["total_count"] == second["total_count"] == 2
    assert geo._provider_identities(first) == [
        (1000000003, geo._THIRD_ADDRESS_KEY),
        (1000000002, geo._SECOND_ADDRESS_KEY),
    ]
    assert geo._provider_identities(second) == [(1000000002, geo._SECOND_ADDRESS_KEY)]
