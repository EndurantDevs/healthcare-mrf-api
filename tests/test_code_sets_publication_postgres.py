# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The three code-set sources must become visible in one transaction."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
import re
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import MetaData

from api import control_imports
from db.connection import Database
from db.models import CodeCatalog, ImportRun
from process import code_sets_mutable_publication as mutable
from process import code_sets_result_archive as archive
from process import control_lifecycle
from process import reference_family_archive as native
from process import scoped_catalog_handoff as handoff
from process.scoped_catalog_retention import cleanup_retained_catalog
from tests.scoped_catalog_native_fixture import catalog_actors, seal_catalog

code_sets = importlib.import_module("process.code_sets")

POS_HTML = "<table><tr><td>23</td><td>Emergency</td><td>Emergency care</td></tr></table>"
RC_HTML = "<table><tr><td>0450</td><td>Emergency revenue</td></tr></table>"


def _dsn() -> str:
    raw = os.getenv("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN", "")
    if not raw:
        pytest.skip("HLTHPRT_CODE_SETS_PUBLICATION_TEST_DSN is not set")
    url = make_url(raw)
    if (
        url.drivername != "postgresql"
        or url.username != "postgres"
        or url.host not in {"127.0.0.1", "localhost"}
        or url.port not in {5432, 5440}
        or re.fullmatch(r"hc_scoped_reference_[0-9a-f]{32}", url.database or "") is None
    ):
        pytest.fail("code-set publication test requires its dedicated local database")
    return url.set(drivername="postgresql+asyncpg").render_as_string(hide_password=False)


async def _seed_catalog(engine):
    metadata = MetaData(schema="mrf")
    CodeCatalog.__table__.to_metadata(metadata, schema="mrf")
    ImportRun.__table__.to_metadata(metadata, schema="mrf")
    async with engine.begin() as connection:
        assert await connection.scalar(text("SELECT to_regnamespace('mrf')")) is None
        await connection.execute(text("CREATE SCHEMA mrf"))
        await connection.run_sync(metadata.create_all)
        await connection.execute(
            text(
                "CREATE TABLE mrf.code_sets_result_generation (id smallint primary key, "
                "local_lineage_id uuid not null, local_generation bigint not null, "
                "origin_lineage_id uuid, origin_generation bigint, published_at timestamptz, "
                "code_catalog_oid bigint, row_count bigint, row_sha256 text)"
            )
        )
        await connection.execute(
            text("INSERT INTO mrf.code_sets_result_generation VALUES (1,:lineage,0,NULL,NULL,NULL,NULL,NULL,NULL)"),
            {"lineage": str(uuid4())},
        )
        await connection.execute(
            text(
                "INSERT INTO mrf.code_catalog (code_system, code, source) VALUES ('OTHER', '1', 'synthetic-unrelated')"
            )
        )
        await connection.execute(
            text("INSERT INTO mrf.code_catalog (code_system, code, source) VALUES ('POS', '99', :source)"),
            {"source": code_sets.SOURCE_POS},
        )


def _stub_feeds(monkeypatch):
    monkeypatch.setattr(code_sets, "ensure_database", AsyncMock())
    monkeypatch.setattr(
        code_sets,
        "modifier_code_rows",
        lambda: [code_sets.CodeSetRow("MODIFIER", "26", "Professional", source=code_sets.SOURCE_MODIFIER)],
    )
    monkeypatch.setattr(
        code_sets,
        "_download_text",
        lambda url: POS_HTML if url == code_sets.DEFAULT_POS_URL else RC_HTML,
    )


def _copy():
    return native.ReferenceFamilySourceCopy(native.native_copy_projection, 1024**2, 30)


async def _ordinary_preparation(actors):
    context_by_field = {
        "control_run_id": uuid4().hex,
        "_control_attempt_id": uuid4().hex,
        "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
    }
    async with actors.builder.begin() as session:
        await session.execute(
            text(
                "INSERT INTO mrf.import_run (run_id,engine,importer,node_id,status,progress,params) "
                "VALUES (:run_id,'healthcare-mrf-api','code-sets','synthetic-node','running',CAST(:progress AS json),'{}')"
            ),
            {
                "run_id": context_by_field["control_run_id"],
                "progress": json.dumps(
                    {
                        "attempt_id": context_by_field["_control_attempt_id"],
                        "attempt_started_at": context_by_field["_control_attempt_started_at"],
                    }
                ),
            },
        )
    result = await code_sets.import_code_sets(_control_context={"context": context_by_field})
    assert result["status"] == "finalizing"
    assert context_by_field["control_run_handoff_committed"] is True
    return result


async def _cancel_preparation(actors, candidate):
    async with actors.builder.begin() as session:
        current = (
            (
                await session.execute(
                    text("SELECT * FROM mrf.import_run WHERE run_id=:run_id"), {"run_id": candidate["run_id"]}
                )
            )
            .mappings()
            .one()
        )
        await handoff.request_catalog_cancel(session, current)
    async with actors.publisher.begin() as session:
        receipt = await handoff.cancel_catalog_handoff(session, candidate)
        assert receipt == {"handoff": candidate, "status": "canceled"}
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) == receipt


async def _assert_late_failure_rolls_back(actors):
    result = await _ordinary_preparation(actors)
    candidate = result[handoff.METRIC]
    with pytest.raises(RuntimeError, match="synthetic late failure"):
        async with actors.publisher.begin() as session:
            await handoff.publish_catalog_handoff(session, candidate, source_copy=_copy())
            raise RuntimeError("synthetic late failure")
    async with actors.builder.begin() as session:
        catalog_sources = (
            (await session.execute(text("SELECT source FROM mrf.code_catalog ORDER BY source"))).scalars().all()
        )
        assert catalog_sources == [code_sets.SOURCE_POS, "synthetic-unrelated"]
        assert await session.scalar(text("SELECT local_generation FROM mrf.code_sets_result_generation")) == 0
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) is None
    await _cancel_preparation(actors, candidate)


async def _assert_foreign_conflict_rolls_back(actors):
    async with actors.publisher.begin() as session:
        await session.execute(
            text(
                "INSERT INTO mrf.code_catalog (code_system, code, display_name, source) "
                "VALUES ('RC', '0450', 'Foreign revenue', 'synthetic-foreign')"
            )
        )
    candidate = (await _ordinary_preparation(actors))[handoff.METRIC]
    with pytest.raises(RuntimeError, match="foreign key ownership"):
        async with actors.publisher.begin() as session:
            await handoff.publish_catalog_handoff(session, candidate, source_copy=_copy())
    async with actors.publisher.begin() as session:
        catalog_entries = (
            await session.execute(
                text("SELECT code_system, code, display_name, source FROM mrf.code_catalog ORDER BY code_system")
            )
        ).all()
        assert [tuple(entry) for entry in catalog_entries] == [
            ("OTHER", "1", None, "synthetic-unrelated"),
            ("POS", "99", None, code_sets.SOURCE_POS),
            ("RC", "0450", "Foreign revenue", "synthetic-foreign"),
        ]
        assert await session.scalar(text("SELECT local_generation FROM mrf.code_sets_result_generation")) == 0
        await session.execute(text("DELETE FROM mrf.code_catalog WHERE source='synthetic-foreign'"))
    await _cancel_preparation(actors, candidate)


async def _assert_partial_feed_retains_prior_code(actors):
    import_counts = await _ordinary_preparation(actors)
    assert (import_counts["pos_rows"], import_counts["rc_rows"], import_counts["modifier_rows"]) == (1, 1, 1)
    candidate = import_counts[handoff.METRIC]
    async with actors.builder.begin() as reader:
        await reader.execute(text("LOCK TABLE mrf.code_catalog IN ACCESS SHARE MODE"))
        before_oid = await reader.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid"))
        with pytest.raises(DBAPIError, match="lock"):
            async with actors.publisher.begin() as publisher:
                await handoff.publish_catalog_handoff(publisher, candidate, source_copy=_copy())
        assert await reader.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid")) == before_oid
        assert await reader.scalar(text("SELECT count(*) FROM mrf.code_catalog")) == 2
    async with actors.publisher.begin() as session:
        receipt = await handoff.publish_catalog_handoff(session, candidate, source_copy=_copy())
    async with actors.publisher.begin() as session:
        assert await handoff.read_catalog_handoff_outcome(session, candidate) == receipt
    async with actors.builder.begin() as session:
        catalog_sources = (
            (await session.execute(text("SELECT source FROM mrf.code_catalog ORDER BY source"))).scalars().all()
        )
        assert catalog_sources == [
            code_sets.SOURCE_RC,
            code_sets.SOURCE_MODIFIER,
            code_sets.SOURCE_POS,
            code_sets.SOURCE_POS,
            "synthetic-unrelated",
        ]
        # A partial but nonempty POS response must not delete a previously valid code.
        assert await session.scalar(text("SELECT count(*) FROM mrf.code_catalog WHERE code='99'")) == 1
        generation = (
            await session.execute(
                text(
                    "SELECT local_generation,origin_generation,row_count,row_sha256 FROM mrf.code_sets_result_generation"
                )
            )
        ).one()
        assert generation[0:3] == (1, 1, 4)
        assert len(generation[3]) == 64
        assert await session.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid")) != before_oid
    async with actors.publisher.begin() as session:
        await cleanup_retained_catalog(session, receipt["retained"])


@pytest.mark.asyncio
async def test_code_sets_rolls_back_late_failure_and_foreign_key_conflict(monkeypatch):
    """A failed import leaves no partial rows; a partial feed retains prior codes."""
    dsn = _dsn()
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", make_url(dsn).database)
    engine = create_async_engine(dsn)
    try:
        async with catalog_actors(engine, ["mrf"]) as actors:
            await _seed_catalog(engine)
            await seal_catalog(actors, "mrf", (CodeCatalog,), "code_sets_result_generation")
            async with engine.begin() as connection:
                await connection.execute(text(f'ALTER TABLE mrf.import_run OWNER TO "{actors.roles["builder"]}"'))
            monkeypatch.setattr(
                code_sets, "db", Database(engine=actors.builder.kw["bind"], session_factory=actors.builder)
            )
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            _stub_feeds(monkeypatch)
            await _assert_late_failure_rolls_back(actors)
            await _assert_foreign_conflict_rolls_back(actors)
            await _assert_partial_feed_retains_prior_code(actors)
    finally:
        await engine.dispose()


@asynccontextmanager
async def _mutable_catalog(engine):
    async with catalog_actors(engine, ["mrf"]) as actors:
        namespace_owner = "catalog_namespace_" + uuid4().hex
        is_namespace_created = False
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        f'CREATE ROLE "{namespace_owner}" NOLOGIN NOSUPERUSER NOCREATEROLE NOCREATEDB NOREPLICATION NOBYPASSRLS'
                    )
                )
            is_namespace_created = True
            await _seed_catalog(engine)
            async with engine.begin() as connection:
                await connection.execute(text(f'ALTER DATABASE "{engine.url.database}" OWNER TO "{namespace_owner}"'))
                await connection.execute(text(f'ALTER SCHEMA mrf OWNER TO "{namespace_owner}"'))
                await connection.execute(text(f'GRANT USAGE,CREATE ON SCHEMA mrf TO "{actors.roles["builder"]}"'))
                await connection.execute(
                    text(
                        "ALTER TABLE mrf.code_catalog ALTER COLUMN display_name TYPE TEXT, ALTER COLUMN short_description TYPE TEXT"
                    )
                )
                await connection.execute(
                    text(
                        "CREATE SEQUENCE mrf.code_sets_result_generation_id_seq OWNED BY mrf.code_sets_result_generation.id"
                    )
                )
                await connection.execute(
                    text(
                        "ALTER TABLE mrf.code_sets_result_generation ALTER COLUMN id SET DEFAULT nextval('mrf.code_sets_result_generation_id_seq')"
                    )
                )
                for name in ("code_catalog", "code_sets_result_generation", "import_run"):
                    await connection.execute(text(f'ALTER TABLE mrf.{name} OWNER TO "{actors.roles["builder"]}"'))
            yield actors
        finally:
            async with engine.begin() as connection:
                await connection.execute(text(f'ALTER DATABASE "{engine.url.database}" OWNER TO postgres'))
                if await connection.scalar(text("SELECT to_regnamespace('mrf')")) is not None:
                    await connection.execute(text("ALTER SCHEMA mrf OWNER TO postgres"))
                if is_namespace_created:
                    await connection.execute(text(f'DROP ROLE "{namespace_owner}"'))


async def _mutable_context(actors, *, status="running"):
    context_by_field = {
        "control_run_id": uuid4().hex,
        "_control_attempt_id": uuid4().hex,
        "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
    }
    async with actors.builder.begin() as session:
        await session.execute(
            text(
                "INSERT INTO mrf.import_run (run_id,engine,importer,node_id,status,progress,params) "
                "VALUES (:run_id,'healthcare-mrf-api','code-sets','synthetic-node',:status,CAST(:progress AS json),'{}')"
            ),
            {
                "run_id": context_by_field["control_run_id"],
                "status": status,
                "progress": json.dumps(
                    {
                        "attempt_id": context_by_field["_control_attempt_id"],
                        "attempt_started_at": context_by_field["_control_attempt_started_at"],
                    }
                ),
            },
        )
    return {"context": context_by_field}


async def _mutable_state(actors):
    async with actors.builder.begin() as session:
        rows = (
            (await session.execute(text("SELECT * FROM mrf.code_catalog ORDER BY code_system,code"))).mappings().all()
        )
        generation = (await session.execute(text("SELECT * FROM mrf.code_sets_result_generation"))).one()
        oid = await session.scalar(text("SELECT 'mrf.code_catalog'::regclass::oid"))
        indexes = (
            await session.execute(
                text("SELECT indexrelid FROM pg_index WHERE indrelid='mrf.code_catalog'::regclass ORDER BY indexrelid")
            )
        ).all()
        sequence = (
            await session.execute(text("SELECT last_value,is_called FROM mrf.code_sets_result_generation_id_seq"))
        ).one()
        default = await session.scalar(
            text(
                "SELECT pg_get_expr(adbin,adrelid) FROM pg_attrdef WHERE adrelid='mrf.code_sets_result_generation'::regclass"
            )
        )
        return rows, generation, oid, indexes, sequence, default


def _mutable_dependencies(monkeypatch, actors):
    database = Database(engine=actors.builder.kw["bind"], session_factory=actors.builder)
    monkeypatch.setattr(code_sets, "db", database)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", "legacy-mutable")
    _stub_feeds(monkeypatch)
    return database


async def _claim_mutable_context(monkeypatch, actors, database, ctx=None):
    if ctx is None:
        ctx = await _mutable_context(actors, status="queued")
    database._database_name = actors.engine.url.database
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", actors.engine.url.database)
    monkeypatch.setattr(control_lifecycle, "db", database)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", Mock())
    context = ctx["context"]
    assert await control_lifecycle.mark_control_run(
        context["control_run_id"],
        status="running",
        phase_detail="main running",
        progress_message="running",
        attempt_id=context["_control_attempt_id"],
        attempt_started_at=context["_control_attempt_started_at"],
    )
    async with actors.builder.begin() as session:
        assert await session.scalar(text("SELECT error::text FROM mrf.import_run")) == "null"
    return ctx


async def _assert_mutable_export(actors, result_by_field, published_state):
    stage = None
    try:
        async with actors.builder.begin() as session:
            async with native._nucc_catalog_search_path(session):
                stage = await archive.prepare_source(session, "mrf", uuid4(), source_copy=_copy())
                assert (stage.row_count, stage.row_sha256) == (
                    result_by_field["generation"]["row_count"],
                    result_by_field["generation"]["row_sha256"],
                )
                assert stage.catalog_oid != published_state[2]
                assert await archive.scope_receipt(session, stage.schema_name, stage=True) == (
                    stage.row_count,
                    stage.row_sha256,
                    stage.catalog_oid,
                )
                assert stage.source_generation.schema_sha256 == archive._schema_digest(
                    await archive._column_signature(session, published_state[2])
                )
    finally:
        if stage is not None:
            async with actors.builder.begin() as session:
                await archive.cleanup_stage(session, stage)
                assert (
                    await session.scalar(
                        text("SELECT pg_catalog.to_regnamespace(:schema)"), {"schema": stage.schema_name}
                    )
                    is None
                )
    assert await _mutable_state(actors) == published_state


@pytest.mark.asyncio
@pytest.mark.parametrize("session_timezone", ["UTC", "Pacific/Honolulu"])
async def test_mutable_code_sets_preserve_heap_indexes_partial_feed_and_generation_default(
    monkeypatch, session_timezone
):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            transaction = database.transaction

            @asynccontextmanager
            async def publication_transaction():
                async with transaction() as session:
                    await session.execute(
                        text("SELECT pg_catalog.set_config('TimeZone',:timezone,true)"), {"timezone": session_timezone}
                    )
                    yield session

            monkeypatch.setattr(database, "transaction", publication_transaction)
            ctx = await _claim_mutable_context(monkeypatch, actors, database)
            async with actors.builder.begin() as session:
                await session.execute(
                    text("UPDATE mrf.import_run SET metrics=CAST(:metrics AS json)"),
                    {"metrics": json.dumps({"unrelated": 17})},
                )
                await session.execute(
                    text(
                        "CREATE TRIGGER synthetic_control_guard BEFORE UPDATE ON mrf.import_run "
                        "FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()"
                    )
                )
            before = await _mutable_state(actors)
            result_by_field = await code_sets.import_code_sets(_control_context=ctx)
            after = await _mutable_state(actors)
            assert after[2:] == before[2:]
            assert len(after[0]) == 5 and after[1][2] == 1
            assert result_by_field["generation"]["row_count"] == 4
            assert {catalog_row["source"] for catalog_row in after[0]} == {
                code_sets.SOURCE_POS,
                code_sets.SOURCE_RC,
                code_sets.SOURCE_MODIFIER,
                "synthetic-unrelated",
            }
            assert ctx["context"]["control_run_terminal_committed"] is True
            assert ctx["context"].get("control_run_handoff_committed") is not True
            async with actors.builder.begin() as session:
                stored = await session.scalar(text("SELECT metrics FROM mrf.import_run"))
                assert stored["unrelated"] == 17
                assert (stored["pos_rows"], stored["rc_rows"], stored["modifier_rows"]) == (1, 1, 1)
                await _assert_utc_completion(session, ctx)
            download = Mock(side_effect=AssertionError("completed attempts must not reacquire feeds"))
            monkeypatch.setattr(code_sets, "_download_text", download)
            assert await code_sets.import_code_sets(_control_context=ctx) == result_by_field
            download.assert_not_called()
            assert await _mutable_state(actors) == after
            await _assert_mutable_export(actors, result_by_field, after)
    finally:
        await engine.dispose()


async def _assert_utc_completion(session, ctx):
    row = (
        (
            await session.execute(
                text(
                    "SELECT heartbeat_at,finished_at,pg_catalog.timezone('UTC',pg_catalog.clock_timestamp()) AS utc_now "
                    "FROM mrf.import_run"
                )
            )
        )
        .mappings()
        .one()
    )
    for field in ("heartbeat_at", "finished_at"):
        assert row[field].tzinfo is None and 0 <= (row["utc_now"] - row[field]).total_seconds() < 30
        assert ctx["context"]["_control_committed_" + field] == row[field].replace(tzinfo=UTC).isoformat(
            timespec="microseconds"
        )
    result = ctx["context"]["_control_committed_result"]
    published_at = datetime.fromisoformat(result["generation"]["published_at"])
    assert published_at.tzinfo is not None
    assert 0 <= (row["utc_now"].replace(tzinfo=UTC) - published_at).total_seconds() < 30


def _patch_mutable_failure(monkeypatch, failure):
    if failure == "rollback":
        original = native.cleanup_model_family_stage

        async def fail_cleanup(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("synthetic late failure")

        monkeypatch.setattr(native, "cleanup_model_family_stage", fail_cleanup)
    if failure == "postimage":
        original_confirmation = mutable._confirmed

        def fail_confirmation(row_by_field, attempt, storage, expected=None):
            if expected is not None:
                row_by_field = {**row_by_field, "phase_detail": "synthetic changed terminal postimage"}
            return original_confirmation(row_by_field, attempt, storage, expected)

        monkeypatch.setattr(mutable, "_confirmed", fail_confirmation)
    if failure == "generation":
        original_copy = mutable.publication.copy_catalog_records

        async def change_generation(session, *args, **kwargs):
            result = await original_copy(session, *args, **kwargs)
            await session.execute(text("UPDATE mrf.code_sets_result_generation SET local_generation=4"))
            return result

        monkeypatch.setattr(mutable.publication, "copy_catalog_records", change_generation)
    drift_at = {"storage-lock": 4, "storage-write": 5}.get(failure)
    if drift_at is not None:
        original_storage = mutable._mutable_storage
        observations = []

        async def changed_storage(*args):
            storage = await original_storage(*args)
            observations.append(storage)
            if len(observations) == drift_at:
                return {**storage, "database_oid": storage["database_oid"] + 1}
            return storage

        monkeypatch.setattr(mutable, "_mutable_storage", changed_storage)
    if failure == "closed-binding":
        monkeypatch.setattr(mutable.binding, "pin_catalog_source", AsyncMock(return_value=True))
    if failure == "completion-cas":
        original_apply = mutable._apply_rows

        async def changed_attempt(session, *args):
            generation = await original_apply(session, *args)
            await session.execute(
                text("UPDATE mrf.import_run SET progress=jsonb_set(progress::jsonb,'{attempt_id}','\"changed\"')::json")
            )
            return generation

        monkeypatch.setattr(mutable, "_apply_rows", changed_attempt)


async def _configure_mutable_refusal(monkeypatch, actors, ctx, failure):
    if failure == "foreign":
        async with actors.builder.begin() as session:
            await session.execute(
                text("INSERT INTO mrf.code_catalog (code_system,code,source) VALUES ('RC','0450','synthetic-foreign')")
            )
    if failure == "error":
        async with actors.builder.begin() as session:
            await session.execute(
                text("UPDATE mrf.import_run SET error=CAST(:error AS json)"),
                {"error": json.dumps({"code": "synthetic_failure"})},
            )
    if failure == "stale":
        ctx["context"]["_control_attempt_id"] = uuid4().hex
    if failure == "protected":
        monkeypatch.setattr(
            code_sets, "db", Database(engine=actors.publisher.kw["bind"], session_factory=actors.publisher)
        )
    if failure == "mixed":
        async with actors.engine.begin() as connection:
            await connection.execute(text(f'ALTER TABLE mrf.code_catalog OWNER TO "{actors.roles["owner"]}"'))
            await connection.execute(text(f'GRANT SELECT ON mrf.code_catalog TO "{actors.roles["builder"]}"'))
    if failure == "payload-hook":
        async with actors.builder.begin() as session:
            await session.execute(
                text(
                    "CREATE TRIGGER synthetic_payload_hook BEFORE UPDATE ON mrf.code_catalog "
                    "FOR EACH ROW EXECUTE FUNCTION pg_catalog.suppress_redundant_updates_trigger()"
                )
            )
    if failure == "missing-retention":
        async with actors.engine.begin() as connection:
            await connection.execute(text("DROP SCHEMA hp_snapshot_retention"))
    statements_by_failure = {
        "generation-model": "ALTER TABLE mrf.code_sets_result_generation ALTER COLUMN row_count TYPE numeric",
        "generation-singleton": "UPDATE mrf.code_sets_result_generation SET id=2",
        "not-running": "UPDATE mrf.import_run SET status='failed'",
        "already-finished": "UPDATE mrf.import_run SET finished_at=TIMESTAMP '2026-01-01'",
    }
    if failure in statements_by_failure:
        async with actors.builder.begin() as session:
            await session.execute(text(statements_by_failure[failure]))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "foreign",
        "rollback",
        "postimage",
        "generation",
        "stale",
        "cancel",
        "protected",
        "missing",
        "mixed",
        "missing-retention",
        "payload-hook",
        "error",
        "generation-model",
        "generation-singleton",
        "not-running",
        "already-finished",
        "storage-lock",
        "storage-write",
        "closed-binding",
        "completion-cas",
    ],
)
async def test_mutable_code_sets_refuse_conflicts_and_roll_back_the_whole_attempt(monkeypatch, failure):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            _mutable_dependencies(monkeypatch, actors)
            ctx = await _mutable_context(actors, status="canceling" if failure == "cancel" else "running")
            _patch_mutable_failure(monkeypatch, failure)
            await _configure_mutable_refusal(monkeypatch, actors, ctx, failure)
            if failure == "missing":
                async with actors.builder.begin() as session:
                    await session.execute(text("DROP TABLE mrf.code_catalog"))
                with pytest.raises(RuntimeError, match="relation inventory"):
                    await code_sets.import_code_sets(_control_context=ctx)
                async with actors.builder.begin() as session:
                    assert await session.scalar(text("SELECT to_regclass('mrf.code_catalog')")) is None
                assert ctx["context"].get("control_run_terminal_committed") is not True
                return
            before = await _mutable_state(actors)
            with pytest.raises((RuntimeError, mutable.ImportCancelledError)):
                await code_sets.import_code_sets(_control_context=ctx)
            assert await _mutable_state(actors) == before
            assert ctx["context"].get("control_run_terminal_committed") is not True
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mutable_code_sets_concurrent_attempts_publish_once(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            _mutable_dependencies(monkeypatch, actors)
            ctx = await _mutable_context(actors)
            entered, resume = asyncio.Event(), asyncio.Event()
            original = publication_copy = mutable.publication.copy_catalog_records

            async def pause_copy(*args, **kwargs):
                result = await original(*args, **kwargs)
                entered.set()
                await resume.wait()
                return result

            monkeypatch.setattr(mutable.publication, "copy_catalog_records", pause_copy)
            first = asyncio.create_task(code_sets.import_code_sets(_control_context=ctx))
            try:
                await asyncio.wait_for(entered.wait(), 10)
                with pytest.raises(DBAPIError, match="lock"):
                    await code_sets.import_code_sets(_control_context={"context": dict(ctx["context"])})
            finally:
                resume.set()
                await first
            monkeypatch.setattr(mutable.publication, "copy_catalog_records", publication_copy)
            assert (await _mutable_state(actors))[1][2] == 1
    finally:
        await engine.dispose()


async def _release_reconciliation(task, outcome, reconciling, resume):
    try:
        await asyncio.wait_for(reconciling.wait(), 10)
        if outcome == "repeated-cancel":
            for _ in range(2):
                task.cancel()
                await asyncio.sleep(0)
            assert not task.done()
    finally:
        resume.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["lost-response", "repeated-cancel", "unknown"])
async def test_mutable_code_sets_reconcile_a_lost_commit_response(monkeypatch, outcome):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            ctx = await _mutable_context(actors)
            transaction = database.transaction
            commit_seen = asyncio.Event()
            reconciling, resume = asyncio.Event(), asyncio.Event()
            original_reconcile = mutable._reconcile

            async def paused_reconcile(*args):
                reconciling.set()
                await resume.wait()
                if outcome == "unknown":
                    raise RuntimeError("synthetic unavailable commit reconciliation")
                return await original_reconcile(*args)

            monkeypatch.setattr(mutable, "_reconcile", paused_reconcile)

            @asynccontextmanager
            async def lost_commit():
                async with transaction() as session:
                    yield session
                    is_published = await session.scalar(text("SELECT status='succeeded' FROM mrf.import_run"))
                if is_published and not commit_seen.is_set():
                    commit_seen.set()
                    raise asyncio.CancelledError()

            monkeypatch.setattr(database, "transaction", lost_commit)
            task = asyncio.create_task(code_sets.import_code_sets(_control_context=ctx))
            await _release_reconciliation(task, outcome, reconciling, resume)
            if outcome == "unknown":
                with pytest.raises(RuntimeError, match="unavailable commit reconciliation"):
                    await task
                assert ctx["context"]["scoped_catalog_commit_unknown"] is True
                assert ctx["context"].get("control_run_terminal_committed") is not True
                assert (await _mutable_state(actors))[1][2] == 1
                return
            result_by_field = await task
            assert result_by_field["status"] == "succeeded"
            assert ctx["context"]["control_run_terminal_committed"] is True
            assert ctx["context"].get("scoped_catalog_commit_unknown") is not True
            assert (await _mutable_state(actors))[1][2] == 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "canceling", "canceled", "failed"])
async def test_mutable_code_sets_reconcile_a_rejected_commit_without_claiming_success(monkeypatch, status):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            ctx = await _mutable_context(actors)
            before = await _mutable_state(actors)
            transaction = database.transaction
            commit_seen = asyncio.Event()

            @asynccontextmanager
            async def reject_commit():
                try:
                    async with transaction() as session:
                        yield session
                        if not commit_seen.is_set() and await session.scalar(
                            text("SELECT status='succeeded' FROM mrf.import_run")
                        ):
                            commit_seen.set()
                            raise RuntimeError("synthetic rejected commit")
                except RuntimeError:
                    async with transaction() as session:
                        await session.execute(text("UPDATE mrf.import_run SET status=:status"), {"status": status})
                    raise

            monkeypatch.setattr(database, "transaction", reject_commit)
            with pytest.raises((RuntimeError, mutable.ImportCancelledError)) as raised:
                await code_sets.import_code_sets(_control_context=ctx)
            expected_by_status = {
                "running": "synthetic rejected commit",
                "canceling": "code-set managed attempt was canceled",
                "canceled": "code-set managed attempt was canceled",
                "failed": "mutable code-set commit outcome differs",
            }
            assert str(raised.value) == expected_by_status[status]
            assert await _mutable_state(actors) == before
            assert ctx["context"].get("control_run_terminal_committed") is not True
            assert ctx["context"].get("scoped_catalog_commit_unknown", False) is (status == "failed")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mutable_code_sets_reject_older_feed_after_another_publish(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            _mutable_dependencies(monkeypatch, actors)
            older = await _mutable_context(actors)
            newer = await _mutable_context(actors)
            fetched, resume = asyncio.Event(), asyncio.Event()
            publish = code_sets._publish_code_sets

            async def pause_older(mode, ctx, *args):
                if ctx is older:
                    fetched.set()
                    await resume.wait()
                return await publish(mode, ctx, *args)

            monkeypatch.setattr(code_sets, "_publish_code_sets", pause_older)
            task = asyncio.create_task(code_sets.import_code_sets(_control_context=older))
            try:
                await asyncio.wait_for(fetched.wait(), 10)
                monkeypatch.setattr(
                    code_sets,
                    "_download_text",
                    lambda url: (POS_HTML if url == code_sets.DEFAULT_POS_URL else RC_HTML).replace(
                        "Emergency", "Newest"
                    ),
                )
                result_by_field = await code_sets.import_code_sets(_control_context=newer)
                winner = await _mutable_state(actors)
                assert result_by_field["generation"]["local_generation"] == 1
                assert any(code_row["display_name"] == "Newest" for code_row in winner[0])
                resume.set()
                with pytest.raises(RuntimeError, match="source changed during acquisition"):
                    await task
                assert await _mutable_state(actors) == winner
                assert older["context"].get("control_run_terminal_committed") is not True
                latest = await _mutable_context(actors)
                assert (await code_sets.import_code_sets(_control_context=latest))["generation"][
                    "local_generation"
                ] == 2
                advanced = await _mutable_state(actors)
                download = Mock(side_effect=AssertionError("completed attempts must not reacquire feeds"))
                monkeypatch.setattr(code_sets, "_download_text", download)
                assert await code_sets.import_code_sets(_control_context=newer) == result_by_field
                download.assert_not_called()
                assert await _mutable_state(actors) == advanced
            finally:
                resume.set()
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
    finally:
        await engine.dispose()


def _mutable_api_dependencies(monkeypatch, database):
    database._database_name = database.engine.url.database
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", database.engine.url.database)
    monkeypatch.setattr(control_imports, "db", database)
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", Mock())
    monkeypatch.setattr(control_imports, "enqueue_status_event", Mock())
    monkeypatch.setattr(control_imports, "_sync_terminal_worker_failure", AsyncMock(side_effect=lambda run: run))
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-node")


async def _late_queued_cancel(run_id, current):
    return await control_imports._persist_cancel_request(
        run_id,
        current_run=current,
        requested_at=control_imports.utc_now(),
        cancel_state_by_name={
            "canceled_now": True,
            "status": "canceled",
            "phase_detail": "canceled before start",
            "progress": {"message": "canceled"},
        },
        run_metrics_by_name={"cancel_signal": {}},
        publish=False,
    )


@pytest.mark.asyncio
async def test_mutable_normal_api_ack_cancel_status_and_idempotency_preserve_completion(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            original_rows = []

            async def publish_before_ack(run_by_field):
                async with actors.builder.begin() as session:
                    assert (
                        await session.scalar(
                            text("SELECT count(*) FROM mrf.import_run WHERE run_id=:run_id"),
                            {"run_id": run_by_field["run_id"]},
                        )
                        == 1
                    )
                original_rows.append(dict(run_by_field))
                assert run_by_field["status"] == "queued" and run_by_field["phase_detail"] == "created"
                context_by_field = {
                    "context": {
                        "control_run_id": run_by_field["run_id"],
                        "_control_attempt_id": uuid4().hex,
                        "_control_attempt_started_at": "2026-01-01T00:00:00+00:00",
                    }
                }
                await _claim_mutable_context(monkeypatch, actors, database, context_by_field)
                await code_sets.import_code_sets(_control_context=context_by_field)
                return control_imports._successful_enqueue_result(
                    params={}, adapter=control_imports._SINGLE_JOB_ADAPTERS["code-sets"], job_id="synthetic-job"
                )

            enqueue = AsyncMock(side_effect=publish_before_ack)
            monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
            request_by_field = {"importer": "code-sets", "run_id": uuid4().hex, "idempotency_key": uuid4().hex}
            completed, created = await control_imports.create_import_run(request_by_field)
            assert created and completed["status"] == "succeeded"
            assert completed["metrics"][mutable.METRIC]["generation"]["local_generation"] == 1
            before = await _mutable_state(actors)
            assert (await _late_queued_cancel(completed["run_id"], original_rows[0]))["status"] == "succeeded"
            for status in ("succeeded", "failed", "canceled"):
                assert not await control_lifecycle.mark_control_run(
                    completed["run_id"],
                    status=status,
                    phase_detail="stale callback",
                    progress_message="stale",
                    metrics={},
                )
            replay, created = await control_imports.create_import_run({**request_by_field, "run_id": uuid4().hex})
            assert not created and replay == completed
            enqueue.assert_awaited_once()
            assert await _mutable_state(actors) == before
            assert await control_imports.get_import_run(completed["run_id"]) == completed
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mutable_queued_cancel_cannot_replace_a_new_worker_claim(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            ctx = await _mutable_context(actors, status="queued")
            run_id = ctx["context"]["control_run_id"]
            queued = await control_imports.get_import_run(run_id)
            await _claim_mutable_context(monkeypatch, actors, database, ctx)
            current = await _late_queued_cancel(run_id, queued)
            assert current["status"] == "running"
            assert current["progress"]["attempt_id"] == ctx["context"]["_control_attempt_id"]
            assert (await code_sets.import_code_sets(_control_context=ctx))["status"] == "succeeded"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy-mutable", "native"])
async def test_mutable_api_admission_requires_commit_boundary_and_preserves_native_callers(monkeypatch, mode):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", mode)
            enqueue = AsyncMock(
                return_value=control_imports._successful_enqueue_result(
                    params={}, adapter=control_imports._SINGLE_JOB_ADAPTERS["code-sets"], job_id="synthetic-job"
                )
            )
            monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
            expected_error = (
                pytest.raises(RuntimeError, match="own commit boundary") if mode == "legacy-mutable" else nullcontext()
            )
            async with database.transaction():
                with expected_error:
                    queued_run, created = await control_imports.create_import_run(
                        {"importer": "code-sets", "run_id": uuid4().hex}
                    )
                    assert created and queued_run["status"] == "queued"
            async with actors.builder.begin() as session:
                assert await session.scalar(text("SELECT count(*) FROM mrf.import_run")) == int(mode == "native")
            assert enqueue.await_count == int(mode == "native")
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mutable_code_sets_cancel_before_completion_rolls_back(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            _mutable_dependencies(monkeypatch, actors)
            ctx = await _mutable_context(actors)
            before = await _mutable_state(actors)
            copied = asyncio.Event()
            original = mutable.publication.copy_catalog_records

            async def paused_copy(*args, **kwargs):
                result = await original(*args, **kwargs)
                copied.set()
                await asyncio.Event().wait()
                return result

            monkeypatch.setattr(mutable.publication, "copy_catalog_records", paused_copy)
            task = asyncio.create_task(code_sets.import_code_sets(_control_context=ctx))
            try:
                await asyncio.wait_for(copied.wait(), 10)
            finally:
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            assert await _mutable_state(actors) == before
            assert ctx["context"].get("control_run_terminal_committed") is not True
    finally:
        await engine.dispose()


def _key_migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261111010000_code_sets_publication_idempotency.py"
    spec = importlib.util.spec_from_file_location("code_sets_key_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _migrate_publication_key(engine, *, downgrade=False):
    migration = _key_migration()

    def migrate(connection):
        migration.op = Operations(MigrationContext.configure(connection))
        (migration.downgrade if downgrade else migration.upgrade)()

    async with engine.begin() as connection:
        await connection.run_sync(migrate)


async def _assert_reserved_key(session, key):
    with pytest.raises(IntegrityError):
        async with session.begin_nested():
            await _insert_key_run(session, key)


async def _insert_key_run(session, key, *, status="queued", metrics=None, importer="code-sets"):
    run_id = uuid4().hex
    await session.execute(
        text(
            "INSERT INTO mrf.import_run (run_id,engine,importer,node_id,status,idempotency_key,metrics,progress,params) "
            "VALUES (:run_id,'healthcare-mrf-api',:importer,'synthetic-node',:status,:key,CAST(:metrics AS json),'{}','{}')"
        ),
        {"run_id": run_id, "importer": importer, "status": status, "key": key, "metrics": json.dumps(metrics or {})},
    )
    return run_id


@pytest.mark.asyncio
async def test_mutable_key_migration_preserves_populated_native_history_and_exact_index(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            async with actors.builder.begin() as session:
                for status in ("queued", "succeeded", "failed", "canceled"):
                    await _insert_key_run(session, "native-history", status=status)
                await _insert_key_run(
                    session, "published", status="succeeded", metrics={mutable.METRIC: {"kept": True}}
                )
                await _insert_key_run(session, "published", status="failed", importer="other")
                for _ in range(2):
                    await _insert_key_run(session, None)
                before = (await session.execute(text("SELECT * FROM mrf.import_run ORDER BY run_id"))).all()
            await _migrate_publication_key(engine)
            async with actors.builder.begin() as session:
                index_oid = await session.scalar(
                    text("SELECT 'mrf.import_run_code_sets_publication_idempotency_idx'::regclass::oid")
                )
                assert (await session.execute(text("SELECT * FROM mrf.import_run ORDER BY run_id"))).all() == before
                await _insert_key_run(session, "native-history", status="succeeded")
                for key in ("native-history", "published"):
                    await _assert_reserved_key(session, key)
            await _migrate_publication_key(engine)
            async with actors.builder.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT 'mrf.import_run_code_sets_publication_idempotency_idx'::regclass::oid")
                    )
                    == index_oid
                )
                assert await session.scalar(text("SELECT count(*) FROM mrf.import_run")) == len(before) + 1
            await _migrate_publication_key(engine, downgrade=True)
            async with actors.builder.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT to_regclass('mrf.import_run_code_sets_publication_idempotency_idx')")
                    )
                    is None
                )
                assert await session.scalar(text("SELECT count(*) FROM mrf.import_run")) == len(before) + 1
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("statuses", [("queued", "running"), ("queued", "succeeded"), ("succeeded", "succeeded")])
async def test_mutable_key_migration_refuses_conflicts_without_changing_rows(monkeypatch, statuses):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            async with actors.builder.begin() as session:
                for status in statuses:
                    metrics = {mutable.METRIC: {"kept": True}} if status == "succeeded" else {}
                    await _insert_key_run(session, "same-key", status=status, metrics=metrics)
                before = (await session.execute(text("SELECT * FROM mrf.import_run ORDER BY run_id"))).all()
            with pytest.raises(IntegrityError):
                await _migrate_publication_key(engine)
            async with actors.builder.begin() as session:
                assert (await session.execute(text("SELECT * FROM mrf.import_run ORDER BY run_id"))).all() == before
                assert (
                    await session.scalar(
                        text("SELECT to_regclass('mrf.import_run_code_sets_publication_idempotency_idx')")
                    )
                    is None
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mutable_key_migration_refuses_a_different_existing_index(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
            async with actors.builder.begin() as session:
                await session.execute(
                    text("CREATE INDEX import_run_code_sets_publication_idempotency_idx ON mrf.import_run (run_id)")
                )
                before = await session.scalar(
                    text("SELECT 'mrf.import_run_code_sets_publication_idempotency_idx'::regclass::oid")
                )
            with pytest.raises(RuntimeError, match="existing_schema_index_mismatch"):
                await _migrate_publication_key(engine)
            async with actors.builder.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT 'mrf.import_run_code_sets_publication_idempotency_idx'::regclass::oid")
                    )
                    == before
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_mutable_request_cancel_losing_to_claim_never_signals_worker(monkeypatch):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            context = await _mutable_context(actors, status="queued")
            run_id = context["context"]["control_run_id"]
            async with actors.builder.begin() as session:
                await session.execute(
                    text("UPDATE mrf.import_run SET progress='{}' WHERE run_id=:run_id"), {"run_id": run_id}
                )
            original = control_imports.get_import_run
            claimed_runs = []

            async def claim_after_read(selected):
                current = await original(selected)
                if not claimed_runs:
                    claimed_runs.append(selected)
                    assert current["status"] == "queued" and not current["progress"].get("attempt_id")
                    await _claim_mutable_context(monkeypatch, actors, database, context)
                return current

            monkeypatch.setattr(control_imports, "get_import_run", claim_after_read)
            signals = [AsyncMock() for _ in range(3)]
            for name, signal in zip(
                ("_set_cancel_flag", "_remove_queued_job", "_delete_active_worker_jobs"), signals, strict=True
            ):
                monkeypatch.setattr(control_imports, name, signal)
            current = await control_imports.request_cancel(run_id)
            assert current["status"] == "running"
            assert current["progress"]["attempt_id"] == context["context"]["_control_attempt_id"]
            for signal in signals:
                signal.assert_not_awaited()
            assert (await code_sets.import_code_sets(_control_context=context))["status"] == "succeeded"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", ["pending", "arq_single_job"])
@pytest.mark.parametrize("signal_error", [False, True])
async def test_mutable_queued_cancel_commits_before_signals_and_recovers(monkeypatch, adapter, signal_error):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            context = await _mutable_context(actors, status="queued")
            run_id = context["context"]["control_run_id"]
            async with actors.builder.begin() as session:
                await session.execute(
                    text("UPDATE mrf.import_run SET progress='{}',metrics=CAST(:metrics AS json) WHERE run_id=:run_id"),
                    {"run_id": run_id, "metrics": json.dumps({"enqueue_adapter": adapter})},
                )
            signal_adapters = []

            async def signal_after_commit(_selected, **_options):
                async with actors.builder.begin() as session:
                    assert (
                        await session.scalar(
                            text("SELECT status FROM mrf.import_run WHERE run_id=:run_id"), {"run_id": run_id}
                        )
                        == "canceling"
                    )
                assert not await control_lifecycle.mark_control_run(
                    run_id,
                    status="running",
                    phase_detail="synthetic claim",
                    progress_message="running",
                    attempt_id=context["context"]["_control_attempt_id"],
                    attempt_started_at=context["context"]["_control_attempt_started_at"],
                )
                signal_adapters.append(adapter)
                if signal_error and len(signal_adapters) == 1:
                    raise RuntimeError("synthetic signal interruption")
                return {"redis": True, "removed": True}

            monkeypatch.setattr(control_lifecycle, "db", database)
            monkeypatch.setattr(control_imports, "_cancel_signal_for_run", signal_after_commit)
            if signal_error:
                with pytest.raises(RuntimeError, match="synthetic signal interruption"):
                    await control_imports.request_cancel(run_id)
                assert (await control_imports.get_import_run(run_id))["status"] == "canceling"
            canceled = await control_imports.request_cancel(run_id)
            assert canceled["status"] == "canceled" and canceled["finished_at"] is not None
            assert canceled["progress"]["done"] == 1 and canceled["progress"].get("attempt_id") is None
            assert await control_imports.request_cancel(run_id) == canceled
            assert len(signal_adapters) == (2 if signal_error else 1)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict_recovery", [False, True])
async def test_mutable_api_key_owner_survives_completion_during_admission_or_conflict(monkeypatch, conflict_recovery):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            await _migrate_publication_key(engine)
            context = await _mutable_context(actors)
            run_id = context["context"]["control_run_id"]
            key = uuid4().hex
            async with actors.builder.begin() as session:
                await session.execute(
                    text("UPDATE mrf.import_run SET idempotency_key=:key WHERE run_id=:run_id"),
                    {"key": key, "run_id": run_id},
                )
            lookup = control_imports._idempotent_import_run
            lookup_calls = []
            owner_queries = []
            completed_runs = []
            execute = database.execute

            async def select_owner(importer, selected_key, **options):
                lookup_calls.append(selected_key)
                if conflict_recovery and len(lookup_calls) == 1:
                    return None
                return await lookup(importer, selected_key, **options)

            async def publish_after_owner_read(statement, **options):
                result = await execute(statement, **options)
                if statement.is_select:
                    owner_queries.append(statement)
                    if not completed_runs:
                        completed_runs.append(run_id)
                        await code_sets.import_code_sets(_control_context=context)
                return result

            monkeypatch.setattr(control_imports, "_idempotent_import_run", select_owner)
            monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
            monkeypatch.setattr(database, "execute", publish_after_owner_read)
            enqueue = AsyncMock(side_effect=AssertionError("the owner's key remains reserved"))
            monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
            replay, created = await control_imports.create_import_run({"importer": "code-sets", "idempotency_key": key})
            assert not created and replay["run_id"] == run_id
            assert len(lookup_calls) == (2 if conflict_recovery else 1)
            assert len(owner_queries) == 1 and completed_runs == [run_id]
            enqueue.assert_not_awaited()
            assert (await control_imports.get_import_run(run_id))["status"] == "succeeded"
            async with actors.builder.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM mrf.import_run WHERE idempotency_key=:key"), {"key": key}
                    )
                    == 1
                )
    finally:
        await engine.dispose()


async def _wait_for_key_waiter(engine, blocker_pid):
    async with asyncio.timeout(10):
        while True:
            async with engine.begin() as session:
                blocked = await session.scalar(
                    text("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))"),
                    {"pid": blocker_pid},
                )
            if blocked:
                return
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_mutable_api_key_waiter_cannot_insert_after_publication_leaves_active_index(monkeypatch):
    engine = create_async_engine(_dsn())
    task = None
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            await _migrate_publication_key(engine)
            async with actors.builder.begin() as session:
                await session.execute(
                    text(
                        "CREATE UNIQUE INDEX import_run_importer_active_idempotency_idx ON mrf.import_run (importer,idempotency_key) "
                        "WHERE status IN ('queued','starting','running','finalizing','canceling')"
                    )
                )
            key = uuid4().hex
            enqueue = AsyncMock(side_effect=AssertionError("a waiting duplicate must replay"))
            monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
            async with actors.builder.begin() as owner:
                run_id = await _insert_key_run(owner, key)
                blocker_pid = await owner.scalar(text("SELECT pg_backend_pid()"))
                task = asyncio.create_task(
                    control_imports.create_import_run({"importer": "code-sets", "idempotency_key": key})
                )
                await _wait_for_key_waiter(engine, blocker_pid)
                await owner.execute(
                    text(
                        "UPDATE mrf.import_run SET status='succeeded',metrics=CAST(:metrics AS json),finished_at=timezone('UTC',clock_timestamp()) "
                        "WHERE run_id=:run_id"
                    ),
                    {"run_id": run_id, "metrics": json.dumps({mutable.METRIC: {"fixture": "published"}})},
                )
            replay, created = await asyncio.wait_for(task, 10)
            assert not created and replay["run_id"] == run_id and replay["status"] == "succeeded"
            enqueue.assert_not_awaited()
            async with actors.builder.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM mrf.import_run WHERE idempotency_key=:key"), {"key": key}
                    )
                    == 1
                )
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["succeeded", "failed", "canceled"])
async def test_native_completed_code_sets_keys_keep_their_existing_api_reuse(monkeypatch, status):
    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            await _migrate_publication_key(engine)
            key = uuid4().hex
            async with actors.builder.begin() as session:
                original_run = await _insert_key_run(session, key, status=status)
            monkeypatch.setenv("HLTHPRT_CODE_SETS_PUBLICATION_MODE", "native")
            enqueue = AsyncMock(
                return_value=control_imports._successful_enqueue_result(
                    params={}, adapter=control_imports._SINGLE_JOB_ADAPTERS["code-sets"], job_id="synthetic-job"
                )
            )
            monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
            queued, created = await control_imports.create_import_run({"importer": "code-sets", "idempotency_key": key})
            assert created and queued["status"] == "queued" and queued["run_id"] != original_run
            assert (await control_imports.get_import_run(original_run))["status"] == status
            enqueue.assert_awaited_once()
            async with actors.builder.begin() as session:
                assert (
                    await session.scalar(
                        text("SELECT count(*) FROM mrf.import_run WHERE idempotency_key=:key"), {"key": key}
                    )
                    == 2
                )
    finally:
        await engine.dispose()


def _code_sets_cancel_redis(monkeypatch):
    from tests.test_control_imports_api import _install_arq_cleanup_redis

    redis, pipeline = _install_arq_cleanup_redis(monkeypatch, None)
    redis.set = AsyncMock()
    pipeline.zscore = AsyncMock(return_value=None)
    pipeline.exists = AsyncMock(return_value=0)
    monkeypatch.setattr(
        control_imports,
        "_delete_active_worker_jobs",
        AsyncMock(
            return_value={
                "enabled": True,
                "deleted": 0,
                "items": [],
            }
        ),
    )
    monkeypatch.setattr(control_imports, "exact_worker_presence", Mock(return_value={"enabled": True, "stopped": True}))
    return redis, pipeline


@pytest.mark.asyncio
@pytest.mark.parametrize("queued_before_cancel", [False, True])
async def test_mutable_api_cancel_before_enqueue_ack_keeps_exact_job_identity(monkeypatch, queued_before_cancel):
    """A durable queue identity survives cancellation and fences a late worker claim."""
    from types import SimpleNamespace

    from tests.test_control_imports_api import _serialized_arq_job

    engine = create_async_engine(_dsn())
    try:
        async with _mutable_catalog(engine) as actors:
            database = _mutable_dependencies(monkeypatch, actors)
            _mutable_api_dependencies(monkeypatch, database)
            monkeypatch.setattr(control_lifecycle, "db", database)
            run_id = uuid4().hex
            job_id = f"code_sets_start_{run_id}"
            redis, pipeline = _code_sets_cancel_redis(monkeypatch)
            payloads = []
            before = await _mutable_state(actors)

            async def enqueue_with_delayed_ack(function, payload, **options):
                admitted = await control_imports.get_import_run(run_id)
                assert admitted["status"] == "queued" and admitted["phase_detail"] == "created"
                assert admitted["metrics"]["job_id"] == options["_job_id"] == job_id
                assert admitted["metrics"]["queue"] == options["_queue_name"] == "arq:CodeSets"
                payloads.append(payload)
                pipeline.get.return_value = _serialized_arq_job(function, payload) if queued_before_cancel else None
                pipeline.execute.return_value = [1, 4] if queued_before_cancel else [True]
                canceled = await control_imports.request_cancel(run_id)
                assert canceled["status"] == "canceled"
                assert canceled["metrics"]["job_id"] == canceled["metrics"]["cancel_signal"]["job_id"] == job_id
                pipeline.get.assert_awaited_once_with(f"arq:job:{job_id}")
                return SimpleNamespace(job_id=job_id)

            redis.enqueue_job = AsyncMock(side_effect=enqueue_with_delayed_ack)
            canceled_run_by_name, created = await control_imports.create_import_run(
                {"importer": "code-sets", "run_id": run_id}
            )
            assert (
                created
                and canceled_run_by_name["status"] == "canceled"
                and canceled_run_by_name["metrics"]["job_id"] == job_id
            )
            cleanup_calls = pipeline.execute.await_count
            assert await control_imports.request_cancel(run_id) == canceled_run_by_name
            assert pipeline.execute.await_count == cleanup_calls
            redis.set.assert_awaited_once()
            target_loader = Mock(side_effect=AssertionError("a canceled run must not load its importer"))
            monkeypatch.setattr(control_lifecycle, "import_module", target_loader)
            outcome = await control_lifecycle.control_single_job_start({}, payloads[0])
            assert outcome["status"] == "skipped" and outcome["reason"] == "newer_attempt_active"
            target_loader.assert_not_called()
            assert await _mutable_state(actors) == before
            assert await control_imports.get_import_run(run_id) == canceled_run_by_name
    finally:
        await engine.dispose()
