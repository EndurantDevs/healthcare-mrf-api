# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Catalog recognition against the complete migration-installed ImportRun guards."""

from __future__ import annotations

import importlib.util
import os
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from db import migration_provider_directory_terminal_root_retirement_guards as terminal

_MIGRATIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"
_FILES = (
    "20260808150000_ptg_import_wave_admission_rollback.py",
    "20260808180000_ptg_import_wave_materialized_preclaim.py",
    "20260808230000_npi_canonical_publication_receipt.py",
    "20260809040000_ptg_import_wave_ordinary_cutover.py",
    "20260810110000_ptg_wave_receipt_authority.py",
    "202608170001_ptg_v13_post_ready_failure_guard.py",
)


def _module(filename):
    spec = importlib.util.spec_from_file_location("import_guard_" + filename, _MIGRATIONS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _migration_statements(schema):
    """Capture SQL constructors offline; no recorded migration command is executed here."""
    statements = []

    class Recorder:
        def get_context(self):
            return SimpleNamespace(as_sql=True)

        def execute(self, sql):
            statements.append(str(sql))

    with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema, "DB_SCHEMA": schema}):
        for filename in _FILES:
            module = _module(filename)
            module.op = Recorder()
            module.upgrade()
        module = _module("20260810090000_provider_directory_terminal_root_retirement.py")
        module.op = Recorder()
        module._create_import_run_triggers(schema)
    statements.extend((terminal.run_retired_function_sql(schema), terminal.import_run_guard_function_sql(schema)))
    quoted = '"' + schema.replace('"', '""') + '"'
    triggers = [
        sql
        for sql in statements
        if re.match(r"\s*CREATE (?:CONSTRAINT )?TRIGGER ", sql)
        and f'ON {quoted}."import_run" ' in " ".join(sql.split())
    ]
    functions_by_name = {
        re.search(r'FUNCTION "[^"]+"\."([^"]+)"', sql)[1]: sql
        for sql in statements
        if re.match(r"\s*CREATE (?:OR REPLACE )?FUNCTION ", sql)
    }
    needed_names = {re.search(r'EXECUTE FUNCTION "[^"]+"\."([^"]+)"', sql)[1] for sql in triggers}
    needed_names.update((terminal.RUN_RETIRED_FUNCTION, "ptg_import_wave_materialized_write_isolation_guard"))
    return {name: functions_by_name[name] for name in sorted(needed_names)}, triggers


def test_frozen_contract_matches_local_migration_sources():
    from process import provider_directory_import_run_guards as guards

    functions, triggers = _migration_statements("guard_schema")
    assert len(functions) == 13 and len(triggers) == 12
    assert set(functions) == set(guards._FUNCTIONS)
    for name, sql in functions.items():
        match = re.search(r"\bAS\s+(\$(?:[a-zA-Z_][a-zA-Z_0-9]*)?\$)", sql)
        body = sql[match.end() :].rsplit(match[1], 1)[0]
        assert guards._source_hash(body, "guard_schema") == guards._FUNCTIONS[name]["source_sha256"]


def _support_tables(schema):
    statements = []

    class Recorder:
        def execute(self, sql):
            statements.append(str(sql))

        def get_context(self):
            return SimpleNamespace(as_sql=True)

    with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema, "DB_SCHEMA": schema}):
        module = _module("20260808220000_public_evidence_nppes_registry_admission.py")
        module.op = Recorder()
        module._create_chain_tables(schema)
        module = _module("20260808230000_npi_canonical_publication_receipt.py")
        module.op = Recorder()
        module.upgrade()
    from db import migration_ptg2_legacy_v3_metadata_reconcile as events

    events._install_event_table(Recorder(), schema)
    names = {
        "public_evidence_nppes_registry_chain_admission",
        "public_evidence_nppes_registry_chain_admission_seal",
        "npi_canonical_publication_receipt",
        "npi_canonical_publication_receipt_seal",
        "ptg_source_attempt_event",
    }
    return [
        sql
        for sql in statements
        if re.match(r"\s*CREATE TABLE ", sql) and re.search(r'CREATE TABLE "[^"]+"\."([^"]+)"', sql)[1] in names
    ]


def _install(connection, schema="import_guards", *, create_schema=True):
    import sqlalchemy as sa

    from db.models import Base

    if create_schema:
        connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
    functions, triggers = _migration_statements(schema)
    needed = set().union(
        *(set(re.findall(r'"' + re.escape(schema) + r'"\."([^"]+)"', sql)) for sql in functions.values())
    )
    models_by_name = {table.name: table for table in Base.metadata.tables.values()}
    needed = (needed - set(functions)) & set(models_by_name)
    while True:
        expanded = needed | {key.column.table.name for name in needed for key in models_by_name[name].foreign_keys}
        if expanded == needed:
            break
        needed = expanded
    metadata = sa.MetaData()
    for name in sorted(needed):
        models_by_name[name].to_metadata(metadata, schema=schema, referred_schema_fn=lambda *_: schema)
    metadata.create_all(connection)
    for sql in _support_tables(schema):
        connection.execute(sa.text(sql))
    for sql in functions.values():
        connection.execute(sa.text(sql))
    for sql in triggers:
        connection.execute(sa.text(sql))
        trigger = re.search(r'CREATE TRIGGER "?([^"\s]+)"?', sql)[1]
        connection.execute(sa.text(f'ALTER TABLE "{schema}".import_run ENABLE ALWAYS TRIGGER "{trigger}"'))
    connection.execute(
        metadata.tables[f"{schema}.import_run"]
        .insert()
        .values(run_id="run_" + "e" * 32, importer="provider-directory-fhir", status="running", params={}, metrics={})
    )


async def _measure_ordinary_update(connection):
    """Observe one guarded update under finite deadlines, without asserting a universal bound."""
    import json

    import sqlalchemy as sa

    await connection.execute(sa.text("SET LOCAL statement_timeout='5s'"))
    await connection.execute(sa.text("SET LOCAL lock_timeout='500ms'"))
    before = await connection.scalar(sa.text("SELECT pg_current_wal_insert_lsn()::text"))
    plan = await connection.scalar(
        sa.text("""EXPLAIN (ANALYZE,BUFFERS,WAL,FORMAT JSON)
            UPDATE import_guards.import_run SET phase_detail='capacity_preflight' WHERE run_id=:run_id"""),
        {"run_id": "run_" + "e" * 32},
    )
    wal_delta = await connection.scalar(
        sa.text("SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),CAST(CAST(:before AS text) AS pg_lsn))::bigint"),
        {"before": before},
    )
    assert wal_delta >= 0
    assert len(plan[0]["Triggers"]) == 8
    assert all(trigger_entry["Calls"] == 1 for trigger_entry in plan[0]["Triggers"])
    assert (
        await connection.scalar(
            sa.text("SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory'")
        )
        == 0
    )
    locks = (
        (
            await connection.execute(
                sa.text("""SELECT DISTINCT mode FROM pg_locks
            WHERE pid=pg_backend_pid() AND relation IS NOT NULL ORDER BY mode""")
            )
        )
        .scalars()
        .all()
    )
    measurement_by_field = {
        "scope": "one ordinary update against empty guard metadata; no universal bound",
        "guard_count": 12,
        "function_count": 13,
        "wal_insert_delta_bytes": wal_delta,
        "explain": plan,
        "relation_lock_modes": locks,
        "advisory_lock_count": 0,
        "statement_timeout_ms": 5000,
        "lock_timeout_ms": 500,
    }
    if os.getenv("HLTHPRT_IMPORT_GUARD_TEST_RECEIPT"):
        Path(os.environ["HLTHPRT_IMPORT_GUARD_TEST_RECEIPT"]).write_text(
            json.dumps(measurement_by_field, indent=2) + "\n"
        )


async def _assert_captured_guards(database, relation_by_field, dependencies):
    """Accept native production-captured rows and reject each captured identity mismatch."""
    import pytest

    fhir = importlib.import_module("process.provider_directory_fhir")

    oid = relation_by_field["relation_oid"]
    relation = (await database.all(fhir._PROFILE_CAPACITY_RELATION_SQL, relation_oid=oid))[0]
    relation_map = dict(relation._mapping)
    relation_oids = [oid] + ([relation_map["toast_oid"]] if relation_map["toast_oid"] else [])
    with pytest.MonkeyPatch.context() as settings:
        settings.setattr(fhir, "db", database)
        attributes, indexes, constraints, captured = await fhir._profile_capacity_relation_catalog(relation_oids)
        assert len(captured) == 12
        assert (
            await fhir.profile_capacity_projection.assert_relation_guards(
                fhir, relation_map, captured, oid, attributes, indexes, constraints, (0, None, False)
            )
            == dependencies
        )
        for name, changed_value in (
            ("tgtype", captured[0]["tgtype"] ^ 1),
            ("trigger_enabled", "O"),
            ("trigger_function_oid", captured[0]["trigger_function_oid"] + 1),
            ("trigger_function_source", "BEGIN RETURN NEW; END;"),
        ):
            changed_triggers = [dict(catalog_row) for catalog_row in captured]
            changed_triggers[0][name] = changed_value
            with pytest.raises(RuntimeError, match="import_run_guard_shape_changed"):
                await fhir.profile_capacity_projection.assert_relation_guards(
                    fhir, relation_map, changed_triggers, oid, attributes, indexes, constraints, (0, None, False)
                )
        assert (
            await fhir.profile_capacity_projection.assert_relation_guards(
                fhir, relation_map, captured, oid, attributes, indexes, constraints, (0, None, False)
            )
            == dependencies
        )


async def _native_proof(engine):
    """Install real migration guards and check native updates plus catalog drift refusals."""
    import pytest
    import sqlalchemy as sa

    from process import provider_directory_import_run_guards as guards
    from tests.test_provider_directory_profile_initial_migration import _CatalogDatabase

    async with engine.begin() as connection:
        await connection.run_sync(_install)
    async with engine.begin() as connection:
        oid = await connection.scalar(sa.text("SELECT 'import_guards.import_run'::regclass::oid::bigint"))
        relation_by_field = {"schema_name": "import_guards", "relation_oid": oid}
        database = _CatalogDatabase(connection)
        for catalog_result in await database.all(
            guards._FUNCTION_CATALOG, schema="import_guards", names=list(guards._FUNCTIONS)
        ):
            actual_by_field = dict(catalog_result._mapping)
            actual_by_field["source_sha256"] = guards._source_hash(actual_by_field["source"], "import_guards")
            expected = guards._FUNCTIONS[actual_by_field["function_name"]] | guards._FUNCTION_DEFAULTS
            assert {name: actual_by_field[name] for name in expected} == expected, actual_by_field["function_name"]
        dependencies = await guards.assert_import_run_guards(database, relation_by_field)
        assert len(dependencies) == 25
        await _assert_captured_guards(database, relation_by_field, dependencies)
        await _measure_ordinary_update(connection)
    mutations = (
        "DROP TRIGGER ptg_wave_retired_run_guard ON import_guards.import_run",
        "ALTER TABLE import_guards.import_run DISABLE TRIGGER ptg_wave_retired_run_guard",
        "ALTER TABLE import_guards.import_run ENABLE TRIGGER ptg_wave_retired_run_guard",
        """CREATE TRIGGER unexpected BEFORE UPDATE ON import_guards.import_run FOR EACH ROW
           EXECUTE FUNCTION import_guards.ptg_import_wave_retired_run_guard()""",
        """CREATE OR REPLACE FUNCTION import_guards.ptg_import_wave_retired_run_guard()
           RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$""",
        """CREATE OR REPLACE FUNCTION import_guards.ptg_import_wave_materialized_write_isolation_guard(
           is_v5_retirement boolean,candidate_run_ids text[],candidate_wave_ids text[],candidate_wave_digests text[])
           RETURNS void LANGUAGE plpgsql AS $$ BEGIN RETURN; END $$""",
        """CREATE OR REPLACE FUNCTION import_guards.provider_directory_terminal_root_run_retired(candidate_run_id text)
           RETURNS boolean LANGUAGE sql STABLE SECURITY DEFINER SET search_path=pg_catalog AS $$ SELECT false $$""",
        "ALTER FUNCTION import_guards.ptg_import_wave_retired_run_guard() SECURITY DEFINER",
        "ALTER FUNCTION import_guards.ptg_import_wave_retired_run_guard() SET search_path=pg_catalog",
        "ALTER FUNCTION import_guards.ptg_import_wave_retired_run_guard() SET SCHEMA public",
        """CREATE FUNCTION import_guards.ptg_import_wave_retired_run_guard(integer)
           RETURNS boolean LANGUAGE sql AS $$ SELECT false $$""",
    )
    for sql in mutations:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                await connection.execute(sa.text(sql))
                with pytest.raises(RuntimeError, match="import_run_guard_shape_changed"):
                    await guards.assert_import_run_guards(_CatalogDatabase(connection), relation_by_field)
            finally:
                await transaction.rollback()
    async with engine.connect() as connection:
        assert await guards.assert_import_run_guards(_CatalogDatabase(connection), relation_by_field) == dependencies


def test_native_complete_guard_catalog_and_ordinary_update():
    import asyncio

    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine

    from tests.cms_npd_admission_postgres_support import _database_url

    async def exercise():
        engine = create_async_engine(_database_url())
        try:
            await _native_proof(engine)
        finally:
            try:
                async with engine.begin() as connection:
                    await connection.execute(sa.text('DROP SCHEMA IF EXISTS "import_guards" CASCADE'))
            finally:
                await engine.dispose()

    asyncio.run(exercise())
