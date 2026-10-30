# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native retained COPY preserves JSON encoding, nullable scopes and exact row parity."""

import asyncio
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_ordinary_row_retention as retention
from process.reference_family_archive import ReferenceFamilyArchiveError
from tests.test_provider_directory_profile_selection_db import _profile_test_database


@asynccontextmanager
async def _native_owner(monkeypatch):
    database = await _profile_test_database()
    schema = "row_retention_test_" + uuid.uuid4().hex[:16]
    try:
        await database.status(f"CREATE SCHEMA {schema}")
        actor_by_field = await database.first(
            "SELECT current_user::regrole::oid::bigint AS owner_oid,(SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid"
        )
        actor_by_field = dict(getattr(actor_by_field, "_mapping", actor_by_field))
        capture = SimpleNamespace(scope_payload={"owner_oid": actor_by_field["owner_oid"]})
        admission = SimpleNamespace(
            plan=object(),
            captured_inputs=capture,
            lease=SimpleNamespace(database_oid=actor_by_field["database_oid"]),
            assert_ready=AsyncMock(),
        )
        prepared = retention.preparation.PreparedServingArtifacts(
            SimpleNamespace(db=database), None, None, admission, None, None, {}, {}, None
        )
        monkeypatch.setattr(retention, "require_capture", lambda *_args: capture)
        monkeypatch.setattr(retention.capacity, "check_ordinary_retained_growth", AsyncMock(), raising=False)
        monkeypatch.setattr(retention.preparation, "remaining_build_seconds", AsyncMock(return_value=30))
        yield prepared, database, schema
    finally:
        try:
            await database.status(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


async def _target(database, schema, source, retained_relation):
    await database.status(
        f'CREATE UNLOGGED TABLE "{schema}"."{retained_relation}" (LIKE "{schema}"."{source}" INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING INDEXES)'
    )
    oid = await database.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{retained_relation}")
    return retention.preparation.OwnedRelation(schema, retained_relation, oid, 0, "u")


@pytest.mark.asyncio
@pytest.mark.parametrize("payload_type", ["json", "jsonb"])
async def test_native_bounded_copy_keeps_deployed_json_type_and_encoding(monkeypatch, payload_type):
    async with _native_owner(monkeypatch) as (prepared, database, schema):
        await database.status(
            f"CREATE TABLE {schema}.source_rows (npi bigint PRIMARY KEY,payload {payload_type} NOT NULL,"
            "source_id varchar(64) NOT NULL,CHECK(npi>0))"
        )
        await database.status(
            f"INSERT INTO {schema}.source_rows SELECT n,CAST(:payload AS {payload_type}),"
            "CASE WHEN n%2=0 THEN 'source_one' ELSE 'source_other' END FROM generate_series(1,2059) n",
            payload='{ "z": 1, "a": [2, 3], "same": {"x": true} }',
        )
        async with database.transaction():
            origin = await retention._origin(prepared)
            retained_relation = await _target(database, schema, "source_rows", "before_rows")
            predicate, arguments = "live.source_id=ANY($1::varchar[])", (["source_one"],)
            copied_bytes = await retention._copy_rows(
                prepared,
                origin,
                f'"{schema}"."source_rows"',
                retained_relation,
                ("npi", "payload", "source_id"),
                ("npi",),
                predicate,
                arguments,
            )
            await retention._assert_parity(
                origin[1], f'"{schema}"."source_rows"', f'"{schema}"."before_rows"', predicate, arguments
            )
            assert copied_bytes > 0 and retention.capacity.check_ordinary_retained_growth.await_count == 3
            assert await database.scalar(f"SELECT count(*) FROM {schema}.before_rows") == 1029
            assert (
                await database.scalar(
                    f"SELECT bool_and(old.payload::text=saved.payload::text) FROM {schema}.source_rows old "
                    f"JOIN {schema}.before_rows saved USING(npi)"
                )
                is True
            )
            await database.status(f"ALTER TABLE {schema}.before_rows SET LOGGED")
            assert (
                await database.scalar(
                    "SELECT relpersistence::text FROM pg_class WHERE oid=to_regclass(:relation)",
                    relation=f"{schema}.before_rows",
                )
                == "p"
            )


@pytest.mark.asyncio
async def test_native_composite_pk_copy_keeps_all_tuple_keys(monkeypatch):
    async with _native_owner(monkeypatch) as (prepared, database, schema):
        await database.status(
            f"CREATE TABLE {schema}.bridge_rows (location_key varchar(64),entity_type varchar(64),"
            "entity_id varchar(64),plan_id varchar(64),payload json NOT NULL,PRIMARY KEY(location_key,entity_type,entity_id,plan_id))"
        )
        await database.status(
            f"INSERT INTO {schema}.bridge_rows SELECT 'location_'||(n/3)::text,'npi',(n%3)::text,'plan_'||(n%7)::text,"
            "CAST(:payload AS json) FROM generate_series(1,1025)n",
            payload='{"value":true}',
        )
        async with database.transaction():
            origin = await retention._origin(prepared)
            retained_relation = await _target(database, schema, "bridge_rows", "before_bridge")
            columns = ("location_key", "entity_type", "entity_id", "plan_id", "payload")
            await retention._copy_rows(
                prepared, origin, f'"{schema}"."bridge_rows"', retained_relation, columns, columns[:-1], "TRUE", ()
            )
            await retention._assert_parity(
                origin[1], f'"{schema}"."bridge_rows"', f'"{schema}"."before_bridge"', "TRUE", ()
            )
            assert await database.scalar(f"SELECT count(*) FROM {schema}.before_bridge") == 1025


@pytest.mark.asyncio
async def test_native_frozen_ctid_copy_keeps_nullable_group_multiset(monkeypatch):
    async with _native_owner(monkeypatch) as (prepared, database, schema):
        await database.status(
            f"CREATE TABLE {schema}.group_rows (entity_type varchar,entity_id varchar,entity_npi bigint,address_key uuid,"
            "street_key varchar,city_key varchar,state_key varchar,zip_key varchar,country_key varchar)"
        )
        await database.status(
            f"CREATE INDEX group_rows_native_idx ON {schema}.group_rows(entity_npi,entity_type,entity_id,address_key,street_key,city_key,state_key,zip_key,country_key)"
        )
        await database.status(
            f"INSERT INTO {schema}.group_rows SELECT 'npi',n::text,CASE WHEN n%2=0 THEN n ELSE NULL END,NULL,"
            "CASE WHEN n%3=0 THEN NULL ELSE '' END,'city','ST',NULL,NULL FROM generate_series(1,1025)n"
        )
        await database.status(f"INSERT INTO {schema}.group_rows SELECT * FROM {schema}.group_rows WHERE entity_id='1'")
        async with database.transaction():
            origin = await retention._origin(prepared)
            await database.status(f"LOCK TABLE {schema}.group_rows IN SHARE MODE NOWAIT")
            retained_relation = await _target(database, schema, "group_rows", "before_groups")
            columns = (
                "entity_type",
                "entity_id",
                "entity_npi",
                "address_key",
                "street_key",
                "city_key",
                "state_key",
                "zip_key",
                "country_key",
            )
            await retention._copy_rows(
                prepared, origin, f'"{schema}"."group_rows"', retained_relation, columns, ("ctid",), "TRUE", ()
            )
            await retention._assert_parity(
                origin[1], f'"{schema}"."group_rows"', f'"{schema}"."before_groups"', "TRUE", ()
            )
            assert await database.scalar(f"SELECT count(*) FROM {schema}.before_groups") == 1026
            assert (
                await database.scalar(
                    "SELECT count(*) FROM pg_index WHERE indrelid=to_regclass(:relation) AND indisprimary",
                    relation=f"{schema}.before_groups",
                )
                == 0
            )


@pytest.mark.asyncio
async def test_native_oversized_copy_refuses_before_any_target_rows(monkeypatch):
    async with _native_owner(monkeypatch) as (prepared, database, schema):
        await database.status(f"CREATE TABLE {schema}.large_rows(npi bigint PRIMARY KEY,payload json NOT NULL)")
        await database.status(
            f"INSERT INTO {schema}.large_rows VALUES(1,json_build_object('synthetic',repeat('x',17*1024*1024)))"
        )
        async with database.transaction():
            origin = await retention._origin(prepared)
            retained_relation = await _target(database, schema, "large_rows", "before_large")
            with pytest.raises(ReferenceFamilyArchiveError, match="byte cap exceeded"):
                await retention._copy_rows(
                    prepared,
                    origin,
                    f'"{schema}"."large_rows"',
                    retained_relation,
                    ("npi", "payload"),
                    ("npi",),
                    "TRUE",
                    (),
                )
            assert await database.scalar(f"SELECT count(*) FROM {schema}.before_large") == 0
