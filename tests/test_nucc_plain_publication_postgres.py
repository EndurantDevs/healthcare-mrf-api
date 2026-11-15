# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual migration/bootstrap, ordinary publication, replay and late rollback."""

import asyncio
import datetime
import importlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import insert, text

from db.models import ImportRun
from process import provider_directory_cms_native_registration as registration
from process import reference_family_result_generation as generation
from tests.test_cms_archive_transaction_migration import _load
from tests.test_cms_native_input_named_revision_migration import _migration
from tests.test_nucc_protected_flow_postgres import _native_candidates, _records
from tests.test_reference_family_result_generation_postgres import (
    _CENSUS_MIGRATION_PATH,
    _GEO_MIGRATION_PATH,
    _PHARMACY_MIGRATION_PATH,
    _TERMINOLOGY_MIGRATION_PATH,
    _run_migration,
    _upgrade_reference_generation_chain,
)

nucc = importlib.import_module("process.nucc")

_VERSIONS = Path(__file__).parents[1] / "alembic" / "versions"


async def _install_existing_counter(resources):
    """The genuine empty historical ledger needs no duplicate-history preparation."""
    connection = resources.admin
    statements = []
    historical = _load("20260930120000")
    historical._schema = lambda: '"mrf"'
    historical.op = SimpleNamespace(execute=statements.append)
    historical.upgrade()
    for statement in statements:
        await connection.execute(statement)
    await connection.execute(_load("20261019130000")._counter_sql("mrf"))
    named = _migration()
    await connection.execute(
        named._function_sql("mrf", names=("cms_native_input_advance", "cms_native_input_revision_guard"))
    )
    for statement in named._key_sql("mrf"):
        await connection.execute(statement)
    owner = resources.roles["owner"]
    publisher = resources.roles["publisher"]
    reader = resources.roles["writer"]
    await connection.execute(f'ALTER FUNCTION mrf.cms_native_input_advance() OWNER TO "{owner}"')
    await connection.execute(f'GRANT EXECUTE ON FUNCTION mrf.cms_native_input_advance() TO "{publisher}"')
    await connection.execute(f'GRANT SELECT ON mrf.cms_native_input_revision TO "{owner}","{publisher}","{reader}"')
    await connection.execute(f'GRANT INSERT(schema_name,table_name) ON mrf.cms_native_input_revision TO "{publisher}"')
    await connection.execute(f'GRANT UPDATE(revision,mutation_xid) ON mrf.cms_native_input_revision TO "{owner}"')


async def _bootstrap_plain(resources):
    async with resources.publisher.transaction() as session:
        connection = await session.connection()
        await _upgrade_reference_generation_chain(connection)
        for path in (
            _GEO_MIGRATION_PATH,
            _PHARMACY_MIGRATION_PATH,
            _TERMINOLOGY_MIGRATION_PATH,
            _CENSUS_MIGRATION_PATH,
        ):
            await _run_migration(connection, path, "upgrade")
        await _run_migration(connection, _VERSIONS / "20260929040000_reference_source_generation_guard.py", "upgrade")
        await _run_migration(connection, _VERSIONS / "20261006010000_nucc_reference_result_generation.py", "upgrade")
        spec = nucc.native.reference_family_spec("nucc")
        await nucc.native._create_model_heaps(session, spec, "mrf", create_indexes=False, ordinary_heaps=True)
        await nucc.native.native_copy_record_batch(
            session,
            spec.model_types[0],
            schema_name="mrf",
            table_name="nucc_taxonomy",
            columns=tuple(spec.model_types[0].__table__.columns.keys()),
            records=_records(2),
        )
        await nucc.native._create_model_indexes(session, spec, "mrf", create_constraints=True)
        await _run_migration(connection, _VERSIONS / "20261114180000_reference_family_named_generation.py", "upgrade")
        await connection.run_sync(lambda sync: ImportRun.__table__.create(sync))
        driver = await nucc.native._native_model_copy_driver(
            session, ("execute", "fetch", "fetchval", "fetchrow", "transaction", "is_in_transaction")
        )
        await registration.register_native_input_candidates(
            driver,
            ledger_schema="mrf",
            candidates=(registration.NativeInputCandidate("mrf", "nucc_taxonomy", "mrf", "nucc_taxonomy"),),
        )
        seed = await generation.read_reference_family_result_generation_authority(
            session, importer_id="nucc", schema_name="mrf", lock=True
        )
        assert seed.local_generation == 0 and seed.serving_generation is None
        await nucc._freeze_plain_publication(session, "mrf")
        first = await generation.publish_immutable_nucc_generation(
            session, schema_name="mrf", expected_authority=seed.as_dict()
        )
        assert first.local_generation == 1 and first.relations == (("mrf", "nucc_taxonomy"),)


async def _new_attempt(resources, row_count):
    run = "taxonomy_" + uuid4().hex
    attempt = run + ":" + uuid4().hex
    started = datetime.datetime.now(datetime.UTC).isoformat()
    context_by_field = {
        "_nucc_database": resources.publisher,
        "context": {
            "control_run_id": run,
            "_control_attempt_id": attempt,
            "_control_attempt_started_at": started,
            "run": 1,
            "rows": row_count,
            "start": datetime.datetime.now(),
        },
    }
    async with resources.publisher.transaction() as session:
        await session.execute(
            insert(ImportRun).values(
                run_id=run,
                engine="healthcare-mrf-api",
                node_id="taxonomy-test",
                importer="nucc",
                status="running",
                params={},
                progress={"attempt_id": attempt, "attempt_started_at": started},
                metrics={},
            )
        )
    await nucc._prepare_plain_stage(context_by_field)
    columns = tuple(nucc.NUCCTaxonomy.__table__.columns.keys())
    await nucc._write_nucc_batch(
        context_by_field,
        nucc.NUCCTaxonomy,
        [dict(zip(columns, record_values, strict=True)) for record_values in _records(row_count)],
    )
    return context_by_field


async def _published_state(resources):
    async with resources.publisher.transaction() as session:
        publication = await nucc._plain_predecessor(session, "mrf")
        data = await session.scalar(
            text("SELECT jsonb_agg(to_jsonb(row_value) ORDER BY code) FROM mrf.nucc_taxonomy row_value")
        )
        retained = None
        if await session.scalar(text("SELECT to_regclass('mrf.nucc_taxonomy_old') IS NOT NULL")):
            retained = await session.scalar(
                text("SELECT jsonb_agg(to_jsonb(row_value) ORDER BY code) FROM mrf.nucc_taxonomy_old row_value")
            )
        indexes = (
            (
                await session.execute(
                    text(
                        "SELECT pg_get_indexdef(indexrelid) FROM pg_index WHERE indrelid='mrf.nucc_taxonomy'::regclass ORDER BY pg_get_indexdef(indexrelid)"
                    )
                )
            )
            .scalars()
            .all()
        )
    return publication, data, indexes, retained


async def _assert_published_write_denial(resources):
    for role in resources.roles.values():
        async with resources.admin.transaction():
            await resources.admin.execute(f'SET LOCAL ROLE "{role}"')
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with resources.admin.transaction():
                    await resources.admin.execute("UPDATE mrf.nucc_taxonomy SET display_name=display_name WHERE FALSE")
            await resources.admin.execute("RESET ROLE")


@pytest.mark.asyncio
async def test_native_plain_publication_replay_and_late_failures_preserve_named_generations(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    async with _native_candidates(monkeypatch) as resources:
        # This fresh ordinary installation has no retained catalogue or claims.
        assert not await resources.admin.fetchval(
            "SELECT EXISTS(SELECT FROM pg_class WHERE relnamespace='hp_snapshot_retention'::regnamespace)"
        )
        await resources.admin.execute("DROP SCHEMA hp_snapshot_retention")
        await resources.admin.execute(f'CREATE SCHEMA mrf AUTHORIZATION "{resources.roles["owner"]}"')
        await resources.admin.execute(
            f'GRANT USAGE ON SCHEMA mrf TO "{resources.roles["publisher"]}","{resources.roles["writer"]}"'
        )
        await _install_existing_counter(resources)
        await _bootstrap_plain(resources)
        baseline = await _published_state(resources)
        assert baseline[0]["native_input"]["revision"] == 0
        context = await _new_attempt(resources, 3)
        stage = context["context"]["nucc_plain_stage"]
        result_by_field = await nucc._publish_plain_generation(context)
        published = await _published_state(resources)
        assert published[0]["family_generation"]["local_generation"] == 2
        assert published[0]["native_input"]["revision"] == 1
        assert len(published[1]) == 3 and result_by_field["publication"] == published[0]
        replay = await nucc._read_plain_commit(context, stage, result_by_field)
        assert replay["finished_at"] == context["context"]["_control_committed_finished_at"]
        assert await nucc.publish_nucc_generation(context) is None
        assert await _published_state(resources) == published
        await _assert_published_write_denial(resources)
        original_freeze = nucc._freeze_plain_publication
        for cancelled in (False, True):
            rejected = await _new_attempt(resources, 4)
            candidate = rejected["context"]["nucc_plain_stage"]

            async def interrupt(session, schema):
                await original_freeze(session, schema)
                raise asyncio.CancelledError() if cancelled else RuntimeError("late publication interrupted")

            with monkeypatch.context() as failure_patch:
                failure_patch.setattr(nucc, "_freeze_plain_publication", interrupt)
                with pytest.raises(asyncio.CancelledError if cancelled else RuntimeError):
                    await nucc._publish_plain_generation(rejected)
            assert await _published_state(resources) == published
            assert await resources.admin.fetchval("SELECT to_regclass($1) IS NULL", f'"mrf"."{candidate["name"]}"')
            run = await resources.admin.fetchrow(
                "SELECT status,finished_at,metrics::text FROM mrf.import_run WHERE run_id=$1",
                rejected["context"]["control_run_id"],
            )
            assert run["status"] == "running" and run["finished_at"] is None
            assert "nucc_named_publication" not in json.loads(run["metrics"])
            await _assert_published_write_denial(resources)
