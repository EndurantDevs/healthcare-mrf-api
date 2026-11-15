"""Native concurrent publication and request-owned read lock proof."""

import asyncio
import json
import os
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import orjson
import pytest
import pytest_asyncio
from multidict import CIMultiDict
from sanic import Sanic, response
from sanic.request.parameters import RequestParameters
from sqlalchemy import text

from api import plan_release_read_boundary as boundary
from db.connection import db
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_validation as validation
from tests.test_plan_release_read_admission import DIGEST, RELEASE, REVISION
from tests.test_ptg2_local_physical_read_view import _view_fixture


async def _native_admission_owner():
    """Require an explicitly named native test database before opening its owner."""
    database = os.environ.get("HLTHPRT_DB_DATABASE", "")
    if re.search(r"(?:^|[_-])test(?:[_-]|$)", database, re.I) is None:
        pytest.skip("explicit test database required")
    return await asyncpg.connect(
        host=os.environ["HLTHPRT_DB_HOST"],
        port=int(os.environ["HLTHPRT_DB_PORT"]),
        user=os.environ["HLTHPRT_DB_USER"],
        database=database,
        password=os.environ.get("HLTHPRT_DB_PASSWORD") or None,
    )


@pytest_asyncio.fixture
async def native_admission(monkeypatch):
    """Own a native release fixture and authenticate its separate Reader login."""
    owner = await _native_admission_owner()
    schema = "read_admission_" + uuid4().hex
    reader = "read_admission_reader_" + uuid4().hex
    reader_password = uuid4().hex
    await db.disconnect()
    try:
        await owner.execute(f'CREATE ROLE "{reader}" LOGIN PASSWORD \'{reader_password}\'; CREATE SCHEMA "{schema}"')
        await owner.execute(f"""
            CREATE TABLE {schema}.plan_release_serving_revision
                (serving_revision_id text, published_at timestamptz, plan_release_id text,
                 healthporta_plan_id text, plan_version_id text, release_month text,
                 release_status text, expected_binding_count integer, binding_set_digest text,
                 serving_status text, is_current boolean, source_manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb);
            CREATE TABLE {schema}.plan_release_snapshot_binding
                (serving_revision_id text, binding_ordinal integer, snapshot_id text,
                 source_key text, plan_id text, plan_market_type text, role text, required boolean);
            CREATE TABLE {schema}.plan_release_snapshot_pin (serving_revision_id text,snapshot_id text);
            CREATE TABLE {schema}.ptg2_snapshot (snapshot_id text, status text, manifest jsonb DEFAULT '{{"serving_index":{{"source_key":"synthetic"}}}}');
            CREATE TABLE {schema}.ptg2_current_snapshot (slot text, snapshot_id text, previous_snapshot_id text);
            CREATE TABLE {schema}.ptg2_current_source_snapshot (source_key text, snapshot_id text, previous_snapshot_id text);
            CREATE TABLE {schema}.ptg2_current_plan_source (source_key text, snapshot_id text, previous_snapshot_id text);
            CREATE TABLE {schema}.ptg2_snapshot_pin (owner_type text, owner_id text, snapshot_id text);
            CREATE TABLE {schema}.ptg2_v3_snapshot_plan_scope
                (snapshot_id text, plan_id text, plan_market_type text);
            CREATE TABLE {schema}.shared_payload (snapshot_id text, payload text);
            INSERT INTO {schema}.plan_release_serving_revision VALUES
                ('{REVISION}', now(), '{RELEASE}', 'plan', NULL, '2026-09',
                 'published', 1, '{DIGEST}', 'published', true);
            INSERT INTO {schema}.plan_release_snapshot_binding VALUES
                ('{REVISION}', 0, 'snapshot-old', 'synthetic', 'logical', 'group', 'in_network', true);
            INSERT INTO {schema}.ptg2_snapshot (snapshot_id,status) VALUES ('snapshot-old', 'published');
            INSERT INTO {schema}.ptg2_snapshot_pin VALUES
                ('plan_release_serving_revision', '{REVISION}', 'snapshot-old');
            INSERT INTO {schema}.ptg2_v3_snapshot_plan_scope VALUES ('snapshot-old', 'logical', 'group');
            INSERT INTO {schema}.shared_payload VALUES ('snapshot-old', 'old');
            GRANT USAGE ON SCHEMA {schema} TO "{reader}";
            GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{reader}";
        """)
        from api import plan_release_serving_resolution as resolution
        from api.plan_release_pricing_projection import plan_release_serving_sql

        monkeypatch.setattr(resolution, "PTG2_SCHEMA", schema)
        monkeypatch.setattr(
            resolution, "_PLAN_RELEASE_GUARD_SQL", plan_release_serving_sql(schema, include_pricing_projection=False)
        )
        monkeypatch.setenv("HLTHPRT_DB_READER_USER", reader)
        monkeypatch.setenv("HLTHPRT_DB_READER_PASSWORD", reader_password)
        yield owner, schema, reader
    finally:
        await db.disconnect()
        await owner.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE; DROP ROLE IF EXISTS "{reader}"')
        await owner.close()


def _request(session, revision=REVISION, digest=DIGEST):
    args = RequestParameters(
        {
            "plan_release_id": [RELEASE],
            boundary.PLAN_RELEASE_REVISION_SELECTOR: [revision],
            boundary.PLAN_RELEASE_DIGEST_SELECTOR: [digest],
        }
    )
    return SimpleNamespace(
        get_args=lambda **_kwargs: args,
        headers=CIMultiDict({boundary.PLAN_RELEASE_READ_BOUNDARY_HEADER: "internal"}),
        ctx=SimpleNamespace(sa_session=session),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("middleware_enabled", [False, True])
async def test_http_admission_and_serialization_share_the_real_request_reader(
    native_admission, monkeypatch, middleware_enabled
):
    owner, schema, reader = native_admission
    from api.plan_release_serving_resolution import resolve_plan_release_guard_selection

    monkeypatch.setenv("HLTHPRT_API_READER_ENABLED", str(middleware_enabled).lower())
    app = Sanic("read-admission-" + uuid4().hex)
    db.init_app(app)
    app.register_middleware(boundary.require_internal_plan_release_read, "request")

    handler_calls = []

    @app.get("/api/v1/pricing/synthetic-admission")
    async def serve(request):
        handler_calls.append(request.path)
        session = request.ctx.sa_session
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        admitted = await resolve_plan_release_guard_selection(session, RELEASE)
        await owner.execute(f"UPDATE {schema}.plan_release_serving_revision SET is_current=false")
        retained = await resolve_plan_release_guard_selection(session, RELEASE)
        assert retained.serving_revision_id == admitted.serving_revision_id == REVISION
        return response.json({"serving_revision_id": admitted.serving_revision_id})

    query = f"plan_release_id={RELEASE}&{boundary.PLAN_RELEASE_REVISION_SELECTOR}={REVISION}&{boundary.PLAN_RELEASE_DIGEST_SELECTOR}={DIGEST}"
    _, stale = await app.asgi_client.get(
        "/api/v1/pricing/synthetic-admission?" + query.replace(REVISION, "hpserve_" + "2" * 26),
        headers={boundary.PLAN_RELEASE_READ_BOUNDARY_HEADER: "internal"},
    )
    assert stale.status == 409
    assert handler_calls == []
    _, http_response = await app.asgi_client.get(
        "/api/v1/pricing/synthetic-admission?" + query,
        headers={boundary.PLAN_RELEASE_READ_BOUNDARY_HEADER: "internal"},
    )
    assert http_response.status == 200, http_response.text
    assert http_response.json == {"serving_revision_id": REVISION}
    assert len(handler_calls) == 1
    assert not db.has_reader_session()
    assert (
        await owner.fetchval(
            "SELECT count(*) FROM pg_stat_activity WHERE usename=$1 AND xact_start IS NOT NULL", reader
        )
        == 0
    )


@pytest.mark.asyncio
async def test_current_cut_keeps_admitted_tuple_and_rejects_new_stale_admission(native_admission):
    owner, schema, _reader = native_admission
    next_revision = "hpserve_" + "2" * 26
    async with db.reader_session() as session:
        assert await boundary.require_internal_plan_release_read(_request(session)) is None
        await owner.execute(f"""
            UPDATE {schema}.plan_release_serving_revision SET is_current=false;
            INSERT INTO {schema}.plan_release_serving_revision VALUES
                ('{next_revision}', now(), '{RELEASE}', 'plan', NULL, '2026-09',
                 'published', 1, '{"b" * 64}', 'published', true);
            INSERT INTO {schema}.plan_release_snapshot_binding VALUES
                ('{next_revision}', 0, 'snapshot-new', 'synthetic', 'logical', 'group', 'in_network', true);
            INSERT INTO {schema}.ptg2_snapshot (snapshot_id,status) VALUES ('snapshot-new', 'published');
            INSERT INTO {schema}.ptg2_snapshot_pin VALUES
                ('plan_release_serving_revision', '{next_revision}', 'snapshot-new');
            DELETE FROM {schema}.ptg2_snapshot_pin WHERE owner_id='{REVISION}';
        """)
        from process.ptg_parts.ptg2_source_snapshot_gc import _CURRENT_SNAPSHOT_IDS_SQL
        from process.ptg_parts.snapshot_cleanup import _CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL

        retained = await owner.fetch(_CURRENT_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", schema))
        assert {reference_record["snapshot_id"] for reference_record in retained} == {"snapshot-old", "snapshot-new"}
        targeted = await owner.fetch(
            _CURRENT_SOURCE_POINTER_SNAPSHOT_IDS_SQL.replace("__SCHEMA__", schema).replace(":source_key", "$1"),
            "synthetic",
        )
        assert {reference_record["snapshot_id"] for reference_record in targeted} == {"snapshot-old", "snapshot-new"}
        assert await boundary.require_internal_plan_release_read(_request(session)) is None
        assert await session.scalar(text(f"SELECT payload FROM {schema}.shared_payload")) == "old"
        async with db.reader_session(independent=True) as child:
            assert (
                await child.scalar(
                    text(f"SELECT payload FROM {schema}.shared_payload WHERE snapshot_id='snapshot-old'")
                )
                == "old"
            )
        encoded = orjson.dumps({"revision": REVISION, "payload": "old"})
        assert json.loads(encoded)["revision"] == REVISION
    async with db.reader_session() as session:
        assert (await boundary.require_internal_plan_release_read(_request(session))).status == 409
        assert await boundary.require_internal_plan_release_read(_request(session, next_revision, "b" * 64)) is None


async def _prepare_local_lock_binding(owner, metadata_schema, reader, monkeypatch):
    """Exercise real payload locks while keeping unrelated authority fixtures bounded."""
    binding, authority_by_field = _view_fixture()
    authority_by_field["native_publication"] = {}
    schema = binding.schema_name
    await owner.execute(f'CREATE SCHEMA "{schema}"')
    relation_names = [name for _schema, name in binding.relations]
    for name in relation_names:
        await owner.execute(f'CREATE TABLE "{schema}"."{name}" (payload text)')
    await owner.execute(
        f'GRANT USAGE ON SCHEMA "{schema}" TO "{reader}"; GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{reader}"'
    )
    monkeypatch.setattr(validation, "resolve_ptg2_schema", lambda: metadata_schema)
    monkeypatch.setattr(validation, "_local_read_authority", AsyncMock(return_value=authority_by_field))
    monkeypatch.setattr(
        validation, "_local_read_authority_binding", lambda *_args, **_kwargs: (binding, authority_by_field)
    )
    monkeypatch.setattr(
        native, "_local_candidate_control", AsyncMock(return_value={"snapshot_id": binding.snapshot_id})
    )
    monkeypatch.setattr(native, "_require_local_published_postimage", lambda *_args: None)
    return binding, relation_names


async def _assert_local_drop_refused(owner, binding, relation_names):
    """Every captured relation must still reject DROP after child reads and encoding."""
    for name in relation_names:
        with pytest.raises(asyncpg.LockNotAvailableError):
            async with owner.transaction():
                await owner.execute("SET LOCAL lock_timeout='50ms'")
                await owner.execute(f"DROP TABLE {binding.relation(name)}")


async def _serve_local_response(binding, relation_names, ready, finish, ending):
    """Keep the genuine Reader open through independent children and serialized output."""
    async with db.reader_session() as session:
        await validation.local_data_physical_read_state(session, binding.snapshot_id, is_prepared=False)
        async with db.reader_session(independent=True) as child:
            await child.execute(text(f"SELECT * FROM {binding.relation(relation_names[-1])}"))
        orjson.dumps({"snapshot_id": binding.snapshot_id, "items": []})
        ready.set()
        await finish.wait()
        if ending == "error":
            raise RuntimeError("synthetic response failure")


async def _finish_local_response(task, finish, ending):
    """Request completion, exception and cancellation must all release the transaction."""
    if ending == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        finish.set()
        if ending == "error":
            with pytest.raises(RuntimeError, match="synthetic response failure"):
                await task
        else:
            await task


@pytest.mark.asyncio
@pytest.mark.parametrize("ending", ("complete", "error", "cancel"))
async def test_local_family_locks_cover_children_encoding_and_request_cleanup(native_admission, monkeypatch, ending):
    """Concurrent DDL fails while the response owner lives and succeeds after every exit."""
    owner, metadata_schema, reader = native_admission
    binding, relation_names = await _prepare_local_lock_binding(owner, metadata_schema, reader, monkeypatch)
    ready, finish = asyncio.Event(), asyncio.Event()
    task = asyncio.create_task(_serve_local_response(binding, relation_names, ready, finish, ending))
    ready_task = asyncio.create_task(ready.wait())
    try:
        done, _pending = await asyncio.wait((ready_task, task), timeout=10, return_when=asyncio.FIRST_COMPLETED)
        if task in done:
            await task
        assert ready.is_set()
        await _assert_local_drop_refused(owner, binding, relation_names)
        await _finish_local_response(task, finish, ending)
        await owner.execute(f"DROP TABLE {binding.relation(relation_names[-1])}")
    finally:
        for owned_task in (task, ready_task):
            if not owned_task.done():
                owned_task.cancel()
        await asyncio.gather(task, ready_task, return_exceptions=True)
        await owner.execute(f'DROP SCHEMA "{binding.schema_name}" CASCADE')


@pytest.mark.asyncio
async def test_copied_partitioned_binding_root_with_new_identity_still_serves(native_admission):
    owner, schema, reader = native_admission
    from api.plan_release_serving_resolution import resolve_plan_release_guard_selection

    binding = f"{schema}.plan_release_snapshot_binding"
    original_oid = await owner.fetchval("SELECT to_regclass($1)::oid", binding)
    await owner.execute(f"""
        ALTER TABLE {binding} RENAME TO original_binding;
        CREATE TABLE {binding} (LIKE {schema}.original_binding) PARTITION BY LIST (serving_revision_id);
        CREATE TABLE {schema}.copied_cohort PARTITION OF {binding} FOR VALUES IN ('{REVISION}');
        INSERT INTO {binding} SELECT * FROM {schema}.original_binding;
        DROP TABLE {schema}.original_binding;
        GRANT SELECT ON {binding} TO "{reader}";
    """)
    assert await owner.fetchval("SELECT to_regclass($1)::oid", binding) != original_oid
    async with db.reader_session() as session:
        selection = await resolve_plan_release_guard_selection(session, RELEASE)
    assert selection.serving_revision_id == REVISION and selection.bindings[0].snapshot_id == "snapshot-old"


@pytest.mark.asyncio
@pytest.mark.parametrize("table", ["plan_release_serving_revision", "plan_release_snapshot_binding"])
async def test_row_security_committed_after_reader_snapshot_is_refused(native_admission, table):
    owner, schema, _reader = native_admission
    from api.plan_release_serving_resolution import resolve_plan_release_guard_selection

    relation = f"{schema}.{table}"
    flags_sql = text("SELECT relrowsecurity FROM pg_catalog.pg_class WHERE oid=CAST(:relation AS regclass)")
    async with db.reader_session() as session:
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert await session.scalar(flags_sql, {"relation": relation}) is False
        async with owner.transaction():
            await owner.execute("SET LOCAL lock_timeout='5s'")
            await owner.execute(f"ALTER TABLE {relation} ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY")
        # The repeatable-read snapshot predates the change, so pg_class still shows row security off.
        assert await session.scalar(flags_sql, {"relation": relation}) is False
        with pytest.raises(ValueError, match="^plan_release_storage_read_table_refused$"):
            await resolve_plan_release_guard_selection(session, RELEASE)
