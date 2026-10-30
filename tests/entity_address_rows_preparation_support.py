# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable inputs for connected raw and direct preparation checks."""

import importlib
import json
import os
from contextlib import asynccontextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import entity_address_preparation_admission as admitted
from process import entity_address_rows_native as rows
from tests.test_address_format_db import FOUNDATION_MIGRATION_PATH, _load_migration
from tests.test_provider_directory_cms_preparation import _admission, _inputs, _signed_lease


def source_records():
    source_rows = []
    for ordinal in range(1, 5):
        values_by_field = dict.fromkeys(rows.TEXT_COLUMNS)
        values_by_field.update(
            entity_type="organization",
            entity_id="office-one" if ordinal < 3 else f"office-{ordinal}",
            entity_name=" Éxample ",
            type=" ",
            first_line=" 12 Main St. ",
            city_name=" New York ",
            state_name=" ny ",
            postal_code="10001-1234",
            country_code="US",
            base_address_version=" version-one ",
            telephone_number=None if ordinal == 1 else "3125550102 x007",
            fax_number=" None ",
            npi=None,
            inferred_npi=None,
            inference_confidence=None,
            inference_method=None,
            taxonomy_array=None if ordinal == 1 else [7, 11],
            plans_network_array=[3],
            procedures_array=None,
            medications_array=[9],
            aca_plan_array=["plan-example"],
            aca_network_array=[],
            ptg_plan_array=[],
            ptg_source_array=["source-example"],
            group_plan_array=[],
            lat=None,
            long=None,
            date_added="2026-01-01",
            updated_at="2026-01-02T00:00:00",
            address_source="nppes" if ordinal == 1 else "provider_directory_fhir",
            source_record_id=f"provider_directory_fhir:practitioner_role:source-example:role-{ordinal}",
            address_key="00000000-0000-4000-8000-000000000001" if ordinal < 3 else None,
        )
        if ordinal == 3:
            values_by_field.update(first_line=" K Street ", city_name="İstanbul", telephone_number="3125550102\u2003x7")
        source_rows.append(values_by_field)
    return source_rows


def source_columns_sql():
    return ", ".join(
        [f"{field} varchar" for field in rows.TEXT_COLUMNS]
        + [
            "entity_type varchar",
            "entity_id varchar",
            "npi bigint",
            "inferred_npi bigint",
            "inference_confidence float8",
            "inference_method varchar",
        ]
        + [
            f"{field} int[]"
            for field in ("taxonomy_array", "plans_network_array", "procedures_array", "medications_array")
        ]
        + [
            f"{field} varchar[]"
            for field in (
                "aca_plan_array",
                "aca_network_array",
                "ptg_plan_array",
                "ptg_source_array",
                "group_plan_array",
            )
        ]
        + [
            "lat numeric",
            "long numeric",
            "date_added date",
            "updated_at timestamp",
            "address_source varchar",
            "source_record_id varchar",
            "address_key uuid",
        ]
    )


def source_select_sql(schema, *, count_reads=False):
    columns = [field.split()[0] for field in source_columns_sql().split(", ")]
    projections = [f"s.{field}" for field in columns if field != "address_key"]
    projections.append("COALESCE(a.target_key,s.address_key)::uuid AS address_key")
    if count_reads:
        projections.append(f"nextval('{schema}.source_reads') AS source_read")
    return (
        f"SELECT {', '.join(projections)} FROM {schema}.source_rows s "
        f"LEFT JOIN {schema}.source_alias a ON a.source_key=s.address_key"
    )


async def _create_source_fixture(database, schema):
    await database.status(
        f"CREATE TABLE {schema}.source_rows AS SELECT * FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS s({source_columns_sql()})",
        payload=json.dumps(source_records()),
    )
    await database.status(f"CREATE TABLE {schema}.source_alias(source_key uuid PRIMARY KEY,target_key uuid NOT NULL)")
    await database.status(
        f"INSERT INTO {schema}.source_alias VALUES ('00000000-0000-4000-8000-000000000001','00000000-0000-4000-8000-000000000002')"
    )
    await database.status(f"CREATE SEQUENCE {schema}.source_reads")


async def _create_output_fixture(database, schema, sql):
    await database.status(sql._prepare_raw_stage_sql(schema, "raw_expected", unlogged=False))
    await database.status(sql._prepare_raw_stage_sql(schema, "raw_actual", unlogged=False))
    async with database.engine.begin() as connection:
        for name in ("direct_expected", "direct_actual"):
            model = sql.EntityAddressUnified.__table__.to_metadata(MetaData(), schema=schema, name=name)
            await connection.run_sync(model.create)
        foundation = _load_migration(FOUNDATION_MIGRATION_PATH, "address_rows_foundation")
        await connection.run_sync(
            lambda sync: foundation._exec_sql_batch(sync, foundation._create_functions_sql(schema))
        )
    formatter = _load_migration()
    await database.status(formatter._humanize_component_function_sql(schema))
    await database.status(formatter._formatted_address_function_sql(schema))


@asynccontextmanager
async def preparation_database(monkeypatch):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select the test database")
    sql = importlib.import_module("process.entity_address_unified")
    engine = create_async_engine(dsn.replace("postgresql://", "postgresql+asyncpg://"))
    database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
    schema = "address_rows_test_" + uuid4().hex[:12]
    await database.status(f"CREATE SCHEMA {schema}")
    monkeypatch.setattr(sql, "db", database)
    monkeypatch.setattr(sql, "_entity_address_sql_settings", lambda: [])
    try:
        await _create_source_fixture(database, schema)
        await _create_output_fixture(database, schema, sql)
        yield sql, database, schema
    finally:
        await database.status(f"DROP SCHEMA {schema} CASCADE")
        await database.disconnect()


async def assert_no_row_scratch(database, schema):
    assert not await database.scalar(
        "SELECT EXISTS(SELECT FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=:schema AND (c.relname LIKE 'address_rows_input_%' OR c.relname LIKE 'address_rows_prepared_%'))",
        schema=schema,
    )


async def apply_source_locale(database, schema, locale):
    if locale == "C":
        return
    collation = await database.scalar(
        "SELECT quote_ident(n.nspname)||'.'||quote_ident(c.collname) FROM pg_collation c "
        "JOIN pg_namespace n ON n.oid=c.collnamespace WHERE c.collname LIKE 'tr-%' "
        "AND c.collprovider='i' ORDER BY c.collname LIMIT 1"
    )
    if collation is None:
        pytest.skip("Turkish ICU collation is unavailable")
    for name in rows.TEXT_COLUMNS:
        await database.status(f"ALTER TABLE {schema}.source_rows ALTER COLUMN {name} TYPE varchar COLLATE {collation}")
    await database.status(
        f"UPDATE {schema}.source_rows SET first_line='I ROAD',city_name='I',state_name='i',"
        "country_code='unitedstatesofamerica',telephone_number='3125550102 EXTENSION 07' WHERE entity_id='office-4'"
    )


@asynccontextmanager
async def preparation_admission(sql, database, schema, monkeypatch, inventories, *, workers=1):
    authority = _admission()
    authority.plan = replace(
        authority.plan,
        worker_count=workers,
        temp_file_limit_bytes_per_backend=32 * 1024,
        reservation_bytes=(("data", 1_000_000), ("temp", 100_000), ("wal", 100_000)),
    )
    authority.lease = _signed_lease(authority.plan)
    approve = authority.check_phase

    async def observe(request):
        """Retain actual measured ownership before the standard signed check replies."""
        inventories.append(tuple((item.relation, item.total_bytes) for item in request.relations))
        return await approve(request)

    authority.check_phase = observe
    execution, fence, projection = _inputs()
    await authority.before_scratch(
        execution, fence, projection, set(authority.plan.publish_targets), frozenset(authority.plan.resource_types)
    )
    fhir = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(fhir, "db", database)
    monkeypatch.setattr(sql, "_entity_address_sql_settings", lambda: admitted.sql_settings([]))
    scope = admitted._admitted_preparation(authority, authority.plan.native_address_input_hash)
    token = admitted._ADMISSION.set(scope)
    try:
        yield authority, scope
    finally:
        for name in tuple(scope.owned_oids):
            await admitted.drop_stage(schema, name)
        admitted._ADMISSION.reset(token)
