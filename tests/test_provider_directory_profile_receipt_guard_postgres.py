# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Strict recognition of the installed common-receipt guards in Profile capacity fingerprints."""

import importlib
from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text

from db.connection import Database
from process.provider_directory_cms_receipt_guard import profile_transition_body
from tests.test_provider_directory_cms_serving_receipt_postgres import _database as _historical_database
from tests.test_provider_directory_cms_serving_receipt_postgres import _migration

fhir = importlib.import_module("process.provider_directory_fhir")


@asynccontextmanager
async def _database(monkeypatch, *, install_receipt=True):
    """Install the current named Profile guard without changing historical fixtures."""
    async with _historical_database(monkeypatch, install_receipt=install_receipt) as (engine, schema):
        if install_receipt:
            revision = _migration("20261116090000")
            body = revision.named_profile_transition_body('"' + schema + '"')
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f'ALTER TABLE "{schema}".provider_directory_profile_serving_generation ADD COLUMN targets_json jsonb'
                    )
                )
                await connection.execute(
                    text(
                        f'CREATE OR REPLACE FUNCTION "{schema}".cms_serving_profile_transition() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ {body} $$'
                    )
                )
        yield engine, schema


async def _fingerprint(database, schema):
    oid = await database.scalar(
        "SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.provider_directory_profile_serving_generation"
    )
    return await fhir._provider_directory_profile_capacity_relation_layout(oid, expected_persistence="p")


@pytest.mark.asyncio
@pytest.mark.parametrize("installed", [False, True])
async def test_exact_serving_receipt_guards_and_legacy_catalog_are_supported(monkeypatch, installed):
    async with _database(monkeypatch, install_receipt=installed) as (_engine, schema):
        database = Database()
        await database.connect()
        monkeypatch.setattr(fhir, "db", database)
        try:
            layout = await _fingerprint(database, schema)
            assert (layout.schema_name, layout.relation_name) == (
                schema,
                "provider_directory_profile_serving_generation",
            )
            assert layout.main_index_pages
        finally:
            await database.disconnect()


async def _tamper_guard(connection, schema, mutation):
    table = f"{schema}.provider_directory_profile_serving_generation"
    function = "cms_serving_profile_transition"
    if mutation == "missing":
        await connection.execute(text(f"DROP TRIGGER {function} ON {table}"))
        await connection.execute(text(f"DROP TRIGGER cms_serving_no_truncate ON {table}"))
        return
    if mutation == "body":
        await connection.execute(
            text(
                f"CREATE OR REPLACE FUNCTION {schema}.{function}() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ BEGIN RETURN NULL; END $$"
            )
        )
        return
    if mutation == "disabled":
        await connection.execute(text(f"ALTER TABLE {table} DISABLE TRIGGER {function}"))
        return
    if mutation == "extra":
        await connection.execute(
            text(
                f"CREATE TRIGGER synthetic_extra_guard BEFORE UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION {schema}.{function}()"
            )
        )
        return
    await connection.execute(text(f"DROP TRIGGER {function} ON {table}"))
    if mutation == "identity":
        function = "synthetic_other_guard"
        await connection.execute(
            text(
                f"CREATE FUNCTION {schema}.{function}() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$ {profile_transition_body(chr(34) + schema + chr(34))} $$"
            )
        )
    events = "UPDATE" if mutation == "events" else "INSERT OR UPDATE OR DELETE"
    deferred = "IMMEDIATE" if mutation == "deferral" else "DEFERRED"
    argument = "'synthetic'" if mutation == "argument" else ""
    await connection.execute(
        text(f"""CREATE CONSTRAINT TRIGGER cms_serving_profile_transition AFTER {events}
        ON {table} DEFERRABLE INITIALLY {deferred} FOR EACH ROW
        EXECUTE FUNCTION {schema}.{function}({argument})""")
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation", ["body", "disabled", "missing", "extra", "identity", "events", "deferral", "argument"]
)
async def test_serving_receipt_guard_drift_is_rejected(monkeypatch, mutation):
    async with _database(monkeypatch) as (engine, schema):
        database = Database()
        await database.connect()
        monkeypatch.setattr(fhir, "db", database)
        try:
            async with engine.begin() as connection:
                await _tamper_guard(connection, schema, mutation)
            with pytest.raises(
                (RuntimeError, fhir.ProviderDirectoryArtifactBuildStale),
                match="receipt_guard_shape_changed|storage_shape_unsupported",
            ):
                await _fingerprint(database, schema)
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_fingerprint_rejects_unsupported_body_in_captured_catalog(monkeypatch):
    async with _database(monkeypatch) as (_engine, schema):
        database = Database()
        await database.connect()
        monkeypatch.setattr(fhir, "db", database)
        catalog = fhir._profile_capacity_relation_catalog

        async def stale_catalog(relation_oids, **kwargs):
            attributes, indexes, constraints, triggers = await catalog(relation_oids, **kwargs)
            triggers[0]["trigger_function_source"] = "BEGIN RETURN NULL; END"
            return attributes, indexes, constraints, triggers

        monkeypatch.setattr(fhir, "_profile_capacity_relation_catalog", stale_catalog)
        try:
            with pytest.raises(RuntimeError, match="receipt_guard_shape_changed"):
                await _fingerprint(database, schema)
        finally:
            await database.disconnect()
