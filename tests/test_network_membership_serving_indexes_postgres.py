# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native complete serving profile on the model's synthetic unified address source."""

import asyncio
import json
from dataclasses import replace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateColumn

from db.models import EntityAddressUnified
from process.entity_address_unified import _post_publish_index_plan
from process.network_membership_candidate_indexes import prepare_network_candidate_indexes
from process.network_membership_serving_indexes import NetworkServingIndexesError, prepare_network_serving_indexes
from tests.test_network_address_projection_postgres import projection_db
from tests.test_network_membership_candidate_indexes_postgres import _project_validated
from tests.test_network_membership_validation_postgres import validation_db
from tests.test_network_serving_schema_postgres import serving_schema


@pytest.fixture
async def serving_indexes_db(validation_db, request):
    fixture = validation_db
    await _full_source(fixture)
    if getattr(request, "param", 0):
        await _more_source_rows(fixture, request.param)
    await _project_validated(fixture)
    async with fixture.connection.transaction():
        await prepare_network_candidate_indexes(
            fixture.connection, fixture.copy_target, fixture.address_source, control_schema=fixture.control_schema
        )
    return fixture


async def _more_source_rows(fixture, row_count):
    source_table = f'"{fixture.control_schema}".retained_addresses'
    source_columns = await fixture.connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        source_table,
    )
    expressions = [
        "lpad(to_hex(sequence_id),64,'0')" if column["attname"] == "location_key" else f'source."{column["attname"]}"'
        for column in source_columns
    ]
    await fixture.connection.execute(
        f"INSERT INTO {source_table} SELECT "
        + ",".join(expressions)
        + f" FROM {source_table} source CROSS JOIN generate_series(1,$1::integer) sequence_id WHERE source.location_key=$2",
        row_count,
        "d" * 64,
    )


async def _full_source(fixture):
    source_table = f'"{fixture.control_schema}".retained_addresses'
    existing_columns = await fixture.connection.fetch(
        "SELECT attname FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped",
        source_table,
    )
    existing_names = {column["attname"] for column in existing_columns}
    additions = []
    constraints = []
    for model_column in EntityAddressUnified.__table__.columns:
        if model_column.name in existing_names:
            continue
        fixture_column = model_column._copy()
        fixture_column.nullable = True
        additions.append("ADD COLUMN " + str(CreateColumn(fixture_column).compile(dialect=postgresql.dialect())))
        if not model_column.nullable:
            constraints.append(f'ALTER COLUMN "{model_column.name}" SET NOT NULL')
    await fixture.connection.execute(f"ALTER TABLE {source_table} " + ",".join(additions))
    await fixture.connection.execute(
        f"""UPDATE {source_table} SET checksum=42,type='primary',
        npi=CASE entity_id WHEN 'provider-a' THEN 1111111111 WHEN 'provider-b' THEN 2222222222 ELSE 3333333333 END,
        lat=40.75,long=-73.98,taxonomy_array='{{17}}',procedures_array='{{123}}',medications_array='{{456}}',
        address_sources='{{synthetic}}',postal_code='10001',zip5='10001',state_name='NY',city_name='Synthetic city',
        telephone_number='212-555-0100',phone_number='2125550100',address_precision='street',address_key=$1,premise_key=$2
    """,
        uuid4(),
        uuid4(),
    )
    await fixture.connection.execute(f"ALTER TABLE {source_table} " + ",".join(constraints))


async def _prepare(fixture, **changes):
    return await prepare_network_serving_indexes(
        fixture.connection,
        changes.get("copy_target", fixture.copy_target),
        changes.get("address_source", fixture.address_source),
        control_schema=fixture.control_schema,
    )


async def _state(fixture):
    return await fixture.connection.fetchrow(
        f'SELECT state,index_ready,validation_json FROM "{fixture.control_schema}".network_membership_candidate'
    )


async def _indexes(fixture):
    return await fixture.connection.fetch(
        "SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=$1 AND tablename='entity_address_unified' ORDER BY indexname",
        fixture.copy_target.schema_name,
    )


@pytest.mark.asyncio
async def test_complete_profile_and_native_replay(serving_indexes_db):
    fixture = serving_indexes_db
    original_state = await _state(fixture)
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        readiness = await _prepare(fixture)
        await asyncio.sleep(0)
        print({"serving_build_roundtrips": len(logged_queries)})
        build_count = len(logged_queries)
        assert build_count == 41
    indexes = await _indexes(fixture)
    plan, _ = _post_publish_index_plan(f'"{fixture.copy_target.schema_name}"', "serving", build_concurrently=False)
    assert len(plan) == 24 and len(indexes) == 25
    assert all(
        any(index["indexname"] == "entity_address_unified_idx_" + name for index in indexes)
        for name, _ in plan
        if name != "canonical_network_ids"
    )
    assert not any(index["indexname"] == "entity_address_unified_idx_canonical_network_ids" for index in indexes)
    state = await _state(fixture)
    assert state["state"] == original_state["state"] == "ready" and state["index_ready"] is True
    assert json.loads(state["validation_json"]) == json.loads(original_state["validation_json"]) | {
        "serving_readiness": readiness
    }
    assert readiness["component"] == "unified_address_serving" and len(readiness["index_definition_sha256"]) == 64
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        assert await _prepare(fixture) == readiness
        await asyncio.sleep(0)
        print({"serving_replay_roundtrips": len(logged_queries), "build_roundtrips": build_count})
        assert len(logged_queries) == 13
        assert not any("CREATE INDEX" in query.query or "ALTER TABLE" in query.query for query in logged_queries)
    assert await _state(fixture) == state and await _indexes(fixture) == indexes
    from tests.test_network_membership_publication_postgres import _candidate_writer_roles, _publish

    async with _candidate_writer_roles(fixture):
        async with fixture.connection.transaction():
            manifest = await _publish(fixture)
        assert manifest["eligible"] is True


@pytest.mark.asyncio
async def test_source_defaults_and_not_null_restored(serving_indexes_db):
    fixture = serving_indexes_db
    async with fixture.connection.transaction():
        await _prepare(fixture)
    columns = await fixture.connection.fetch(
        """
        SELECT column_record.attname,column_record.attnotnull,pg_get_expr(default_record.adbin,default_record.adrelid) AS default_sql
        FROM pg_attribute column_record LEFT JOIN pg_attrdef default_record
          ON default_record.adrelid=column_record.attrelid AND default_record.adnum=column_record.attnum
        WHERE column_record.attrelid=to_regclass($1) AND column_record.attname=ANY($2::text[])
    """,
        fixture.copy_target.schema_name + ".entity_address_unified",
        ["row_origin", "checksum", "address_sources", "canonical_network_ids"],
    )
    columns_by_name = {column["attname"]: column for column in columns}
    assert all(column["attnotnull"] is True for column in columns)
    assert columns_by_name["row_origin"]["default_sql"] == "'base'::character varying"
    assert columns_by_name["address_sources"]["default_sql"] == "'{}'::character varying[]"
    assert columns_by_name["canonical_network_ids"]["default_sql"] == "'{}'::integer[]"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    ["missing_column", "wrong_type", "array", "source_payload", "unknown_network", "accounting", "source_generation"],
)
async def test_inputs_fail_without_receipt_or_indexes(serving_indexes_db, damage):
    fixture = serving_indexes_db
    await _damage_input(fixture, damage)
    original_state, indexes = await _state(fixture), await _indexes(fixture)
    async with fixture.connection.transaction():
        with pytest.raises((NetworkServingIndexesError,)):
            await _prepare(fixture)
    assert await _state(fixture) == original_state and await _indexes(fixture) == indexes


async def _damage_input(fixture, damage):
    candidate_table = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    statements_by_damage = {
        "missing_column": f"ALTER TABLE {candidate_table} DROP COLUMN phone_number",
        "wrong_type": f"ALTER TABLE {candidate_table} ALTER COLUMN npi TYPE numeric USING npi::numeric",
        "array": f"UPDATE {candidate_table} SET canonical_network_ids=ARRAY[42,7] WHERE entity_id='provider-a'",
        "source_payload": f"UPDATE \"{fixture.control_schema}\".retained_addresses SET city_name='changed synthetic city'",
        "unknown_network": f'DELETE FROM "{fixture.control_schema}".network_registry_identity WHERE network_id=88',
        "accounting": f'DELETE FROM "{fixture.control_schema}".network_membership_batch',
        "source_generation": f'UPDATE "{fixture.control_schema}".network_membership_candidate SET source_generations=\'{{"unified_address":"changed"}}\'',
    }
    await fixture.connection.execute(statements_by_damage[damage])


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", ["wrong_index", "unrelated_table"])
async def test_existing_planned_names_are_rejected(serving_indexes_db, collision):
    fixture = serving_indexes_db
    namespace = f'"{fixture.copy_target.schema_name}"'
    index_name = "entity_address_unified_idx_npi"
    statement = f"CREATE INDEX {index_name} ON {namespace}.entity_address_unified(entity_id)"
    if collision == "unrelated_table":
        statement = f"CREATE TABLE {namespace}.{index_name}(synthetic_value integer)"
    await fixture.connection.execute(statement)
    original_state, indexes = await _state(fixture), await _indexes(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingIndexesError, match="name already exists"):
            await _prepare(fixture)
    assert await _state(fixture) == original_state and await _indexes(fixture) == indexes


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["drop_index", "wrong_definition", "changed_default", "nullable", "receipt", "source_default"]
)
async def test_replay_checks_native_definitions_and_semantics(serving_indexes_db, damage):
    fixture = serving_indexes_db
    async with fixture.connection.transaction():
        await _prepare(fixture)
    await _damage_replay(fixture, damage)
    original_state, indexes = await _state(fixture), await _indexes(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingIndexesError):
            await _prepare(fixture)
    assert await _state(fixture) == original_state and await _indexes(fixture) == indexes


async def _damage_replay(fixture, damage):
    namespace = f'"{fixture.copy_target.schema_name}"'
    if damage in {"drop_index", "wrong_definition"}:
        await fixture.connection.execute(f"DROP INDEX {namespace}.entity_address_unified_idx_npi")
        if damage == "wrong_definition":
            await fixture.connection.execute(
                f"CREATE INDEX entity_address_unified_idx_npi ON {namespace}.entity_address_unified(entity_id)"
            )
        return
    if damage == "receipt":
        report = json.loads((await _state(fixture))["validation_json"])
        report["serving_readiness"]["index_definition_sha256"] = "a" * 64
        await fixture.connection.execute(
            f'UPDATE "{fixture.control_schema}".network_membership_candidate SET validation_json=$1::jsonb',
            json.dumps(report),
        )
        return
    statements_by_damage = {
        "changed_default": f"ALTER TABLE {namespace}.entity_address_unified ALTER COLUMN row_origin SET DEFAULT 'changed'",
        "nullable": f"ALTER TABLE {namespace}.entity_address_unified ALTER COLUMN row_origin DROP NOT NULL",
        "source_default": f"ALTER TABLE \"{fixture.control_schema}\".retained_addresses ALTER COLUMN row_origin SET DEFAULT 'changed'",
    }
    await fixture.connection.execute(statements_by_damage[damage])


@pytest.mark.asyncio
async def test_outer_rollback_and_wrong_scope(serving_indexes_db):
    fixture = serving_indexes_db
    original_state, indexes = await _state(fixture), await _indexes(fixture)
    with pytest.raises(NetworkServingIndexesError, match="caller-owned"):
        await _prepare(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingIndexesError, match="ownership scope"):
            await _prepare(fixture, copy_target=replace(fixture.copy_target, producer_id=str(uuid4())))
    outer_transaction = fixture.connection.transaction()
    await outer_transaction.start()
    try:
        await _prepare(fixture)
    finally:
        await outer_transaction.rollback()
    assert await _state(fixture) == original_state and await _indexes(fixture) == indexes


@pytest.mark.asyncio
async def test_source_reader_and_native_query_structure(serving_indexes_db):
    fixture = serving_indexes_db
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        source_rows = await fixture.observer.fetch(
            f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
        )
        async with fixture.connection.transaction():
            await _prepare(fixture)
        assert (
            await fixture.observer.fetch(
                f'SELECT * FROM "{fixture.control_schema}".retained_addresses ORDER BY location_key'
            )
            == source_rows
        )
    await fixture.connection.execute("SET enable_seqscan=off")
    try:
        await _assert_query_indexes(fixture)
    finally:
        await fixture.connection.execute("SET enable_seqscan=on")


async def _assert_query_indexes(fixture):
    candidate_table = f'"{fixture.copy_target.schema_name}".entity_address_unified'
    predicates_by_query = {
        "npi": "npi=1111111111",
        "canonical": "canonical_network_ids && ARRAY[42]",
        "taxonomy": "type='primary' AND taxonomy_array && ARRAY[17] AND plans_network_array && ARRAY[42]",
        "geo": "type IN ('primary','secondary','practice','site') AND COALESCE(address_precision,'')<>'city_zip' AND lat IS NOT NULL AND long IS NOT NULL AND ST_DWithin(Geography(ST_MakePoint(long::double precision,lat::double precision)),Geography(ST_MakePoint(-73.98,40.75)),1000)",
    }
    expected_keys_by_query = {
        "npi": ["a" * 64, "b" * 64],
        "canonical": ["a" * 64],
        "taxonomy": ["a" * 64, "c" * 64],
        "geo": [character * 64 for character in "abcd"],
    }
    for query_name, predicate in predicates_by_query.items():
        plan = await fixture.connection.fetchval(
            f"EXPLAIN (FORMAT JSON) SELECT location_key FROM {candidate_table} WHERE {predicate}"
        )
        plan_text = json.dumps(json.loads(plan))
        assert "Index" in plan_text, query_name
        matching_keys = await fixture.connection.fetch(
            f"SELECT location_key FROM {candidate_table} WHERE {predicate} ORDER BY location_key"
        )
        assert [record["location_key"] for record in matching_keys] == expected_keys_by_query[query_name]


@pytest.mark.asyncio
@pytest.mark.parametrize("serving_indexes_db", [20000], indirect=True)
async def test_large_candidate_uses_same_statement_count(serving_indexes_db):
    fixture = serving_indexes_db
    logged_queries = []
    fixture.connection.add_query_logger(logged_queries.append)
    async with fixture.connection.transaction():
        await asyncio.sleep(0)
        logged_queries.clear()
        await _prepare(fixture)
        await asyncio.sleep(0)
        assert len(logged_queries) == 41
    assert (
        await fixture.connection.fetchval(
            f'SELECT count(*) FROM "{fixture.copy_target.schema_name}".entity_address_unified'
        )
        == 20004
    )


async def _column_state(fixture):
    return await fixture.connection.fetch(
        """
        SELECT attribute.attname,attribute.atttypid,attribute.atttypmod,attribute.attndims,attribute.attnotnull,
            pg_get_expr(default_record.adbin,default_record.adrelid) AS default_sql
        FROM pg_attribute attribute LEFT JOIN pg_attrdef default_record
            ON default_record.adrelid=attribute.attrelid AND default_record.adnum=attribute.attnum
        WHERE attribute.attrelid=to_regclass($1) AND attribute.attnum>0 AND NOT attribute.attisdropped ORDER BY attribute.attnum
    """,
        fixture.copy_target.schema_name + ".entity_address_unified",
    )


@pytest.mark.asyncio
async def test_native_index_error_rolls_back_partial_ddl(serving_indexes_db):
    fixture = serving_indexes_db
    for schema_name, table_name in [
        (fixture.control_schema, "retained_addresses"),
        (fixture.copy_target.schema_name, "entity_address_unified"),
    ]:
        await fixture.connection.execute(
            f'UPDATE "{schema_name}".{table_name} SET taxonomy_array=ARRAY[NULL]::integer[]'
        )
    original_state, indexes, column_state = await _state(fixture), await _indexes(fixture), await _column_state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(asyncpg.DataError, match="null"):
            await _prepare(fixture)
        assert await _state(fixture) == original_state and await _indexes(fixture) == indexes
        assert await _column_state(fixture) == column_state
        assert await fixture.connection.fetchval("SELECT 1") == 1


@pytest.mark.asyncio
async def test_invalid_native_index_replay_rejected(serving_indexes_db):
    fixture = serving_indexes_db
    async with fixture.connection.transaction():
        await _prepare(fixture)
    namespace = f'"{fixture.copy_target.schema_name}"'
    await fixture.connection.execute(f"DROP INDEX {namespace}.entity_address_unified_idx_npi")
    async with fixture.observer.transaction(isolation="repeatable_read", readonly=True):
        await fixture.observer.fetchval(f"SELECT count(*) FROM {namespace}.entity_address_unified")
        await fixture.connection.execute("SET statement_timeout='500ms'")
        try:
            with pytest.raises(asyncpg.QueryCanceledError):
                await fixture.connection.execute(
                    f"CREATE INDEX CONCURRENTLY entity_address_unified_idx_npi ON {namespace}.entity_address_unified(npi)"
                )
        finally:
            await fixture.connection.execute("SET statement_timeout=0")
    index_valid = await fixture.connection.fetchval(
        "SELECT indisvalid FROM pg_index WHERE indexrelid=to_regclass($1)",
        fixture.copy_target.schema_name + ".entity_address_unified_idx_npi",
    )
    assert index_valid is False
    original_state = await _state(fixture)
    async with fixture.connection.transaction():
        with pytest.raises(NetworkServingIndexesError, match="missing or invalid"):
            await _prepare(fixture)
    assert await _state(fixture) == original_state


@pytest.mark.asyncio
async def test_search_path_is_restored_and_replay_digest_stable(serving_indexes_db):
    fixture = serving_indexes_db
    original_search_path = await fixture.connection.fetchval("SHOW search_path")
    try:
        await fixture.connection.execute(
            "SELECT set_config('search_path',$1,false)", '"' + fixture.control_schema + '",public'
        )
        caller_search_path = await fixture.connection.fetchval("SHOW search_path")
        async with fixture.connection.transaction():
            readiness = await _prepare(fixture)
            assert await fixture.connection.fetchval("SHOW search_path") == caller_search_path
        await fixture.connection.execute(
            "SELECT set_config('search_path',$1,false)", '"' + fixture.copy_target.schema_name + '",pg_catalog,public'
        )
        alternate_search_path = await fixture.connection.fetchval("SHOW search_path")
        async with fixture.connection.transaction():
            assert await _prepare(fixture) == readiness
            assert await fixture.connection.fetchval("SHOW search_path") == alternate_search_path
            with pytest.raises(NetworkServingIndexesError, match="ownership scope"):
                await _prepare(fixture, copy_target=replace(fixture.copy_target, producer_id=str(uuid4())))
            assert await fixture.connection.fetchval("SHOW search_path") == alternate_search_path
    finally:
        await fixture.connection.execute("SELECT set_config('search_path',$1,false)", original_search_path)
