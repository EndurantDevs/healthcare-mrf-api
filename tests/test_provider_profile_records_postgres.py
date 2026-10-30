# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native conversion parity and independence from renamed live composite types."""

import json
from contextlib import asynccontextmanager

import asyncpg
import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg
from sqlalchemy.schema import CreateTable

from api import provider_profile_scoped_sql as scoped
from process import provider_directory_profile as profile
from process.provider_directory_profile_records import PROFILE_RESOURCE_MODELS, profile_resource_record_columns
from tests.test_network_address_read_race_postgres import address_race_db as address_race_db
from tests.test_provider_profile_scoped_sql import _arguments

pytestmark = pytest.mark.asyncio


@asynccontextmanager
async def _legacy_tables(resources):
    tables = [
        model.__table__.to_metadata(MetaData(), schema=resources.schema) for model in PROFILE_RESOURCE_MODELS.values()
    ]
    assert not any(table.foreign_keys for table in tables)
    created_names = []
    try:
        for table in tables:
            await resources.admin.execute(str(CreateTable(table).compile(dialect=PGDialect_asyncpg())))
            created_names.append(table.name)
        yield
    finally:
        for name in reversed(created_names):
            await resources.admin.execute(f'DROP TABLE "{resources.schema}"."{name}"')


async def _converted(connection, sql, document):
    try:
        async with connection.transaction(readonly=True):
            return {"document": json.loads(await connection.fetchval(sql, json.dumps(document)))}
    except asyncpg.PostgresError as error:
        return {"refused": error.sqlstate}


def _documents(role):
    base_by_field = {"source_id": "example-source", "resource_id": "resource", "unknown": {"ignored": True}}
    yield {}
    yield base_by_field
    yield {**base_by_field, "fhir_meta": {"versionId": "one"}, "observed_at": "2026-02-03T04:05:06+02:00"}
    yield {**base_by_field, "fhir_meta": None, "resource_id": None}
    yield {**base_by_field, "resource_id": "x" * 257}
    yield {**base_by_field, "observed_at": "invalid timestamp"}
    if role != "endpoint":
        yield {**base_by_field, "npi": "1000000491", "active": False, "identifiers": [{"value": "retained"}]}
        yield {**base_by_field, "npi": 2**64}
        yield {**base_by_field, "active": "invalid boolean"}
    if role == "practitioner":
        yield {**base_by_field, "age_years": 2**31}


@pytest.mark.parametrize("address_race_db", [True], indirect=True)
async def test_all_resource_conversions_preserve_values_and_refusals(address_race_db, record_property):
    resources = address_race_db
    outcomes = []
    async with _legacy_tables(resources):
        for role, model in PROFILE_RESOURCE_MODELS.items():
            legacy = f'SELECT to_jsonb(jsonb_populate_record(NULL::"{resources.schema}"."{model.__tablename__}",$1::jsonb))::text'
            static = f"SELECT to_jsonb(projected)::text FROM jsonb_to_record($1::jsonb) AS projected({profile_resource_record_columns(role)})"
            for document in _documents(role):
                before = await _converted(resources.reader, legacy, document)
                after = await _converted(resources.reader, static, document)
                assert before == after, (role, document, before, after)
                outcomes.append({"role": role, "refused": before.get("refused")})
    assert len(outcomes) == 52 and any(entry["refused"] for entry in outcomes)
    record_property("static_record_parity", json.dumps(outcomes, sort_keys=True))


async def _profile_document(connection, query, parameters):
    compiled = text("SELECT to_jsonb(result)::text FROM (" + query + ") result").compile(dialect=PGDialect_asyncpg())
    return json.loads(await connection.fetchval(str(compiled), *(parameters[key] for key in compiled.positiontup)))


@pytest.mark.parametrize("address_race_db", [True], indirect=True)
async def test_complete_profile_matches_legacy_and_needs_no_live_resource_types(address_race_db, monkeypatch):
    resources = address_race_db
    arguments = _arguments()
    arguments["row_type_refs"] = {
        kind: (resources.schema, PROFILE_RESOURCE_MODELS[role].__tablename__)
        for kind, role in scoped._RESOURCE_TYPES.items()
    }
    current_sql, parameters = scoped.build_scoped_profile_sql(**arguments)
    assert "jsonb_populate_record" not in current_sql and resources.schema not in current_sql
    # New query works before the reference row types exist.
    expected = await _profile_document(resources.reader, current_sql, parameters)
    assert expected["profile_json"] is not None and expected["evidence_json"] is not None
    async with _legacy_tables(resources):
        legacy_sql = _legacy_profile_sql(arguments, monkeypatch)
        assert await _profile_document(resources.reader, legacy_sql, parameters) == expected
        cached = await resources.reader.prepare(
            str(
                text("SELECT to_jsonb(result)::text FROM (" + current_sql + ") result").compile(
                    dialect=PGDialect_asyncpg()
                )
            )
        )
        compiled = text(current_sql).compile(dialect=PGDialect_asyncpg())
        bound_values = tuple(parameters[key] for key in compiled.positiontup)
        assert json.loads(await cached.fetchval(*bound_values)) == expected
    # Cached static query still works after every former composite dependency is dropped.
    assert json.loads(await cached.fetchval(*bound_values)) == expected


def _legacy_profile_sql(arguments, monkeypatch):
    original = profile.profile_evidence_select_sql

    def evidence(**options):
        options["static_resource_rows"] = False
        return original(**options)

    def typed_ctes():
        return ",\n".join(
            f"scoped_{role} AS MATERIALIZED (SELECT (jsonb_populate_record(NULL::{profile.qualified_table(*arguments['row_type_refs'][kind])},payload_json)).* FROM scoped_resources WHERE resource_type='{kind}')"
            for kind, role in scoped._RESOURCE_TYPES.items()
        )

    with monkeypatch.context() as patch:
        patch.setattr(profile, "profile_evidence_select_sql", evidence)
        patch.setattr(scoped, "_static_resource_ctes", typed_ctes)
        return scoped.build_scoped_profile_sql(**arguments)[0]
