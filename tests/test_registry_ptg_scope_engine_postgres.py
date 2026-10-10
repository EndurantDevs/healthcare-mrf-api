# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in native source reconstruction and protected operator-review persistence.

The source fixture is synthetic; registry and scope storage use actual migrations.
These checks establish neither a complete source migration nor cohort admission.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import threading
import time
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import asyncpg
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import registry_company_approval_fence as fence
from process import registry_ptg_scope_engine as scope_engine
from process.ptg_parts.frozen_rate_binding import frozen_rate_binding_sha256
from process.registry_ptg_producer_scope import (
    RegistryPTGProducerScopeError,
    RegistryPTGProducerScopeStore,
    _digest,
)
from process.registry_ptg_scope_runtime import build_registry_ptg_scope_runtime
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordCommand,
    apply_registry_record_command,
)
from tests import test_registry_ptg_graph_reader_postgres as graph_fixture
from tests.test_registry_approval_store_postgres import _approve, _command
from tests.test_result_archive_published_authority_postgres import _database


def _migration(filename="20261009010000_registry_ptg_producer_scope.py"):
    path = Path(__file__).resolve().parents[1] / "alembic/versions" / filename
    specification = importlib.util.spec_from_file_location("native_scope_migration", path)
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _preview_document(source_snapshot):
    descriptor = source_snapshot.source_fixture.records[0].descriptor
    binding = source_snapshot.source_fixture.binding
    client = "synthetic_client"
    return {
        "actor": {"kind": "platform_admin", "user_id": str(uuid4()), "client_id": "system"},
        "session_token_sha256": "a" * 64,
        "command": {
            "scope_id": str(uuid4()),
            "statement_id": str(uuid4()),
            "client_id": client,
            "legal_company_id": str(uuid4()),
            "approved_revision": 3,
            "source_file_import_id": binding["source_file_import_id"],
            "file_versions": [
                {
                    "source_file_version_id": descriptor["engine_source_file_version_id"],
                    "source_identity_sha256": descriptor["engine_source_identity_hash"],
                    "raw_sha256": descriptor["raw_sha256"],
                }
            ],
            "company_key": "synthetic_company",
            "cohort_id": "synthetic_cohort",
            "reason": "Reviewed synthetic frozen source",
            "idempotency_key": "synthetic_review",
        },
        "ownership": {
            "source_file_import_id": binding["source_file_import_id"],
            "client_id": client,
            "source_file_id": "synthetic_file",
            "content_version": "opaque-version",
            "import_month": "opaque-period",
            "assigned_node_id": "synthetic_node",
            "status": "succeeded",
            "engine_run_id": graph_fixture._RUN,
            "snapshot_id": graph_fixture._SNAPSHOT,
            "source_key": binding["source_key"],
            "engine_source_identity_hash": descriptor["engine_source_identity_hash"],
            "engine_source_file_version_id": descriptor["engine_source_file_version_id"],
        },
    }


def _app_authority(monkeypatch):
    """Replace only the upstream HTTP decision; retain bounded client validation."""
    state = SimpleNamespace(
        authorized=True,
        envelopes=[],
        actor_override=None,
        timeouts=[],
        delay_seconds=0,
        started=threading.Event(),
        finished=threading.Event(),
    )

    class Response:
        status = 200

        def __init__(self, receipt):
            self.encoded = json.dumps(receipt, ensure_ascii=False).encode()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, count):
            assert count == 4097
            return self.encoded[:count]

    class Upstream:
        def open(self, request, *, timeout):
            assert request.full_url == "https://authority.example/internal/v1/registry/ptg-source-scopes/authorize"
            assert request.get_header("Authorization") == "Bearer synthetic-authority"
            assert request.method == "POST" and 0 < timeout <= 10
            envelope = json.loads(request.data)
            assert request.data == scope_engine._canonical(envelope).encode("utf-8")
            state.envelopes.append(envelope)
            state.timeouts.append(timeout)
            state.started.set()
            time.sleep(state.delay_seconds)
            state.finished.set()
            return Response(
                {
                    "authorized": state.authorized,
                    "actor": state.actor_override or envelope["actor"],
                    "command_sha256": _digest(envelope["command"]),
                    "policy_revision": 7,
                }
            )

    def opener(*handlers):
        assert handlers[0].proxies == {}
        assert type(handlers[1]) is scope_engine._NoRedirects
        return Upstream()

    monkeypatch.setattr(scope_engine, "build_opener", opener)
    return state


async def _bind_frozen_source(connection, source_snapshot):
    """Add actual source binding coordinates omitted by the graph-only fixture."""
    schema = source_snapshot.schema
    binding = source_snapshot.source_fixture.binding
    await connection.exec_driver_sql(
        f"ALTER TABLE {schema}.ptg2_frozen_source_file_binding "
        "ADD COLUMN source_file_import_id text, ADD COLUMN source_key text, ADD COLUMN binding_sha256 text"
    )
    await connection.execute(
        text(
            f"UPDATE {schema}.ptg2_frozen_source_file_binding "
            "SET source_file_import_id=:import_id,source_key=:source_key,binding_sha256=:digest"
        ),
        {
            "import_id": binding["source_file_import_id"],
            "source_key": binding["source_key"],
            "digest": frozen_rate_binding_sha256(binding),
        },
    )


async def _apply_registry_migrations(connection, control):
    """Apply the actual additive registry migrations to the registered schema."""
    filenames = sorted(
        path
        for path in Path(__file__).resolve().parents[1].joinpath("alembic/versions").glob("20261007*.py")
        if "20261007010000" <= path.name[:14] <= "20261007140000"
    )
    assert len(filenames) == 14
    prior_by_key = {key: os.environ.get(key) for key in ("HLTHPRT_DB_SCHEMA", "HLTHPRT_NETWORK_REGISTRY_SCHEMA")}
    try:
        for key in prior_by_key:
            os.environ[key] = control
        for path in filenames:
            specification = importlib.util.spec_from_file_location(path.stem, path)
            migration = importlib.util.module_from_spec(specification)
            specification.loader.exec_module(migration)

            def upgrade(sync_connection):
                with Operations.context(MigrationContext.configure(sync_connection)):
                    migration.upgrade()

            await connection.run_sync(upgrade)
    finally:
        for key, value in prior_by_key.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


async def _create_control_store(connection, control, roles, document):
    """Apply actual registry/scope DDL and native restricted-role grants."""
    owner, approver, reader = roles
    namespace = f'"{control}"'
    table = namespace + '."registry_ptg_producer_scope"'
    assert await connection.scalar(text("SELECT to_regnamespace(:name)"), {"name": control}) is None
    await connection.exec_driver_sql(f'CREATE SCHEMA {namespace} AUTHORIZATION "{owner}"')
    await _apply_registry_migrations(connection, control)
    for filename in (
        "20261009010000_registry_ptg_producer_scope.py",
        "20261009020000_company_registry_assertions.py",
        "20261009030000_network_catalog_evidence.py",
    ):
        for statement in _migration(filename)._ddl(control):
            await connection.exec_driver_sql(statement)
    await connection.exec_driver_sql(f'ALTER TABLE {table} OWNER TO "{owner}"')
    await connection.exec_driver_sql(f"REVOKE ALL ON SCHEMA {namespace} FROM PUBLIC")
    await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA {namespace} TO "{approver}","{reader}"')
    await connection.exec_driver_sql(f'GRANT SELECT,INSERT ON {table} TO "{approver}"')
    await connection.exec_driver_sql(f'GRANT SELECT ON {table} TO "{reader}"')
    await connection.exec_driver_sql(
        f'GRANT SELECT ON {namespace}.registry_approved_record,{namespace}.registry_revision_control TO "{approver}"'
    )


@asynccontextmanager
async def _native_connection(fixture):
    connection = await asyncpg.connect(
        fixture.engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    )
    try:
        yield connection
    finally:
        await connection.close()


async def _draft_company(engine, control, document, operation="create"):
    actor = RegistryActor("platform_admin", UUID(document["actor"]["user_id"]), "system")
    command = RegistryRecordCommand(
        "company",
        UUID(document["command"]["legal_company_id"]),
        operation,
        0 if operation == "create" else 1,
        {}
        if operation == "archive"
        else {
            "display_name": "Corrected example" if operation == "correct" else "Reviewed example",
            "roles": ["employer"],
            "aliases": [],
        },
        "Reviewed company",
        uuid4().hex,
    )
    async with async_sessionmaker(engine)() as session, session.begin():
        record = await apply_registry_record_command(session, command, actor, schema=control)
    return record, actor


async def _seed_approved_company(engine, control, document):
    record, actor = await _draft_company(engine, control, document)
    async with _native_connection(SimpleNamespace(engine=engine)) as connection:
        receipt = await _approve(connection, control, await _command(connection, control, record), actor)
    document["command"]["approved_revision"] = receipt["approved_revision"]


async def _grant_source_locks(connection, source_schema, approver, reader):
    await connection.exec_driver_sql(f'GRANT USAGE ON SCHEMA {source_schema} TO "{approver}","{reader}"')
    await connection.exec_driver_sql(f'GRANT SELECT ON ALL TABLES IN SCHEMA {source_schema} TO "{approver}","{reader}"')
    # Native custody takes FOR KEY SHARE; scope readers still have no scope INSERT.
    await connection.exec_driver_sql(f'GRANT UPDATE ON {source_schema}.ptg2_snapshot TO "{approver}","{reader}"')


async def _cleanup_scope(engine, role_engines, control, is_control_created, source_name, created_roles):
    """Remove only the exact registered roles and schema, then verify cleanup."""
    for role_engine in role_engines:
        await role_engine.dispose()
    async with engine.begin() as connection:
        if is_control_created:
            await connection.exec_driver_sql(f'DROP SCHEMA "{control}" CASCADE')
        assert await connection.scalar(text("SELECT to_regnamespace(:name)"), {"name": control}) is None
        for role in reversed(created_roles):
            await connection.exec_driver_sql(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{source_name}" FROM "{role}"')
            await connection.exec_driver_sql(f'REVOKE ALL ON SCHEMA "{source_name}" FROM "{role}"')
            await connection.exec_driver_sql(f'DROP ROLE "{role}"')
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )


async def _create_roles(engine, roles, created_roles, journal, login_password):
    """Record each successful role creation so partial setup remains cleanable."""
    for role in roles:
        async with engine.begin() as connection:
            assert not await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_roles WHERE rolname=:role)"), {"role": role}
            )
            options = "NOLOGIN" if role == roles[0] else f"LOGIN PASSWORD '{login_password}'"
            await connection.exec_driver_sql(f'CREATE ROLE "{role}" ' + options)
        created_roles.append(role)
        journal.write_text(json.dumps({"roles": roles, "created_roles": created_roles, "phase": "roles_created"}))


def _runtime_configuration(engine, source_name, control, roles, login_password):
    owner, approver, reader = roles
    return {
        "ptg_schema": source_name,
        "control_schema": control,
        "owner_role": owner,
        "approval_role": approver,
        "reader_dsn": engine.url.set(username=reader, password=login_password).render_as_string(hide_password=False),
        "approver_dsn": engine.url.set(username=approver, password=login_password).render_as_string(
            hide_password=False
        ),
        "app_origin": "https://authority.example",
        "authority_token": "synthetic-authority",
    }


@asynccontextmanager
async def _native_scope(tmp_path):
    """Own synthetic source evidence, actual scope DDL, roles and exact cleanup."""
    # Exact cleanup inventory exists before the first schema/role mutation.
    roles = ["scope_native_" + kind + "_" + uuid4().hex for kind in ("owner", "approver", "reader")]
    owner, approver, reader = roles
    login_password = uuid4().hex
    control = "scope_native_" + uuid4().hex
    journal = tmp_path / "scope-cleanup-registration.json"
    created_roles, role_engines = [], []
    journal.write_text(json.dumps({"roles": roles, "control_schema": control, "phase": "registered"}))
    async with _database() as (engine, source_name):
        is_control_created = False
        runtime = None
        try:
            await _create_roles(engine, roles, created_roles, journal, login_password)
            source_snapshot = await graph_fixture._seed(engine, source_name, tmp_path)
            document = _preview_document(source_snapshot)
            async with engine.begin() as connection:
                await _bind_frozen_source(connection, source_snapshot)
                await _create_control_store(connection, control, roles, document)
                await _grant_source_locks(connection, source_snapshot.schema, approver, reader)
            is_control_created = True
            await _seed_approved_company(engine, control, document)
            runtime = await build_registry_ptg_scope_runtime(
                _runtime_configuration(engine, source_name, control, roles, login_password)
            )
            service = runtime.service
            ambient = create_async_engine(engine.url, pool_size=1, max_overflow=0)
            role_engines.append(ambient)
            table = f'"{control}"."registry_ptg_producer_scope"'
            yield SimpleNamespace(
                engine=engine,
                service=service,
                source=source_snapshot,
                document=document,
                table=table,
                reader=service.reader_sessions,
                approver=service.approval_sessions,
                ambient=async_sessionmaker(ambient),
                control=control,
                roles=roles,
            )
        finally:
            if runtime is not None:
                await runtime.close()
            await _cleanup_scope(engine, role_engines, control, is_control_created, source_name, created_roles)
            journal.write_text(
                json.dumps(
                    {
                        "roles": roles,
                        "control_schema": control,
                        "source_schema": source_name,
                        "phase": "cleanup_verified",
                    }
                )
            )


async def _rows(fixture):
    async with fixture.engine.connect() as connection:
        return (await connection.execute(text(f"SELECT approval_json FROM {fixture.table}"))).scalars().all()


async def _approval(fixture):
    preview = await fixture.service.preview(fixture.document)
    assert set(preview) == {"command"}
    return {name: fixture.document[name] for name in ("actor", "session_token_sha256")} | preview


def test_scope_wire_preserves_actual_compact_engine_identity(tmp_path):
    source_snapshot = SimpleNamespace(source_fixture=graph_fixture._source_fixture(tmp_path))
    document = _preview_document(source_snapshot)
    assert len(document["ownership"]["engine_source_identity_hash"]) == 16
    assert scope_engine.validated_registry_ptg_scope_envelope(document, "preview") == document


@pytest.mark.asyncio
async def test_native_engine_reconstructs_approves_and_retains_exact_replay(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        assert await _rows(fixture) == []
        first = await fixture.service.approve(envelope)
        assert await fixture.service.approve(envelope) == first
        rows = await _rows(fixture)
        assert len(rows) == 1 and len(app.envelopes) == 2
        assert rows[0]["evidence"]["selected_dense_source_keys"] == [0]
        assert rows[0]["operator_review"]["source_ownership"] == fixture.document["ownership"]
        assert first["command_sha256"] == _digest(envelope["command"])
        assert first["approval_sha256"] == _digest(rows[0])
        assert app.envelopes == [envelope, envelope]
        conflict = deepcopy(envelope)
        conflict["command"]["reason"] = "Different reviewed statement"
        with pytest.raises(RegistryPTGProducerScopeError, match="idempotency_conflict"):
            await fixture.service.approve(conflict)
        assert await _rows(fixture) == rows


@pytest.mark.asyncio
async def test_native_revoked_full_command_authority_keeps_store_empty(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        app.authorized = False
        with pytest.raises(PermissionError, match="authority_changed"):
            await fixture.service.approve(envelope)
        assert app.envelopes == [envelope]
        assert await _rows(fixture) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("session_kind", ["reader", "ambient"])
async def test_native_read_role_and_ambient_service_cannot_append(tmp_path, monkeypatch, session_kind):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        sessions = fixture.reader if session_kind == "reader" else fixture.ambient
        forbidden = replace(fixture.service, approval_sessions=sessions)
        with pytest.raises(RegistryPTGProducerScopeError, match="store_unprotected"):
            await forbidden.approve(envelope)
        assert app.envelopes == [] and await _rows(fixture) == []
        if session_kind == "reader":
            async with fixture.reader() as session, session.begin():
                with pytest.raises(DBAPIError) as denied:
                    await session.execute(
                        text(f"INSERT INTO {fixture.table}(scope_id) VALUES(CAST(:id AS uuid))"), {"id": str(uuid4())}
                    )
                assert denied.value.orig.sqlstate == "42501"


@pytest.mark.asyncio
@pytest.mark.parametrize("substitution", ["command", "manifest", "root", "file", "frozen"])
async def test_native_full_command_or_source_change_refuses_without_append(tmp_path, monkeypatch, substitution):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        schema = fixture.source.schema
        if substitution == "command":
            envelope["command"]["coordinates"]["edition_id"] = "f" * 64
        else:
            query_by_substitution = {
                "manifest": f"UPDATE {schema}.ptg2_snapshot SET manifest=manifest||'{{\"changed\":true}}'::jsonb",
                "root": f"UPDATE {schema}.ptg2_v4_snapshot_map_root SET map_digest=decode(repeat('f',64),'hex')",
                "file": f"UPDATE {schema}.ptg2_source_file_version SET raw_sha256=repeat('f',64)",
                "frozen": f"UPDATE {schema}.ptg2_frozen_source_file_binding SET binding_sha256=repeat('f',64)",
            }
            async with fixture.engine.begin() as connection:
                await connection.exec_driver_sql(query_by_substitution[substitution])
        with pytest.raises((ValueError, RuntimeError)):
            await fixture.service.approve(envelope)
        assert app.envelopes == [] and await _rows(fixture) == []


async def _pending_company(fixture, operation):
    record, actor = await _draft_company(fixture.engine, fixture.control, fixture.document, operation)
    async with _native_connection(fixture) as connection:
        command = await _command(connection, fixture.control, record)
    return command, actor


async def _advance_company(fixture, operation):
    command, actor = await _pending_company(fixture, operation)
    async with _native_connection(fixture) as connection:
        return await _approve(connection, fixture.control, command, actor)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["archive", "correct"])
async def test_native_new_scope_requires_latest_active_company(tmp_path, monkeypatch, operation):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        stale = await _approval(fixture)
        current = await _advance_company(fixture, operation)
        with pytest.raises(RegistryPTGProducerScopeError, match="company_unapproved"):
            await fixture.service.approve(stale)
        assert await _rows(fixture) == [] and not app.envelopes
        active = deepcopy(stale)
        active["command"]["approved_revision"] = current["approved_revision"]
        if operation == "archive":
            with pytest.raises(RegistryPTGProducerScopeError, match="company_unapproved"):
                await fixture.service.approve(active)
            assert await _rows(fixture) == [] and not app.envelopes
        else:
            receipt = await fixture.service.approve(active)
            assert receipt["approval_sha256"] == _digest((await _rows(fixture))[0])


@pytest.mark.asyncio
async def test_native_historical_replay_preserves_document_and_fresh_authority(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        receipt = await fixture.service.approve(envelope)
        before = await _rows(fixture)
        await _advance_company(fixture, "archive")
        assert await fixture.service.approve(envelope) == receipt
        assert await _rows(fixture) == before
        app.authorized = False
        with pytest.raises(PermissionError, match="authority_changed"):
            await fixture.service.approve(envelope)
        assert await _rows(fixture) == before and app.envelopes == [envelope] * 3


async def _wait_native_writer(fixture, pid):
    async with fixture.engine.connect() as connection:
        async with asyncio.timeout(3):
            while not await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_locks WHERE pid=:pid AND locktype='advisory' AND NOT granted)"),
                {"pid": pid},
            ):
                await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_native_pre_snapshot_fence_blocks_actual_pointer_writer(tmp_path, monkeypatch):
    _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture, _native_connection(fixture) as writer:
        command, actor = await _pending_company(fixture, "correct")
        writer_pid = await writer.fetchval("SELECT pg_backend_pid()")
        task = None
        try:
            async with fence.registry_company_approval_transaction(
                fixture.approver, control_schema=fixture.control
            ) as session:
                await fence.require_registry_company_approval_fence(session, fixture.control)
                assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
                before = await session.scalar(
                    text(f'SELECT approved_revision FROM "{fixture.control}".registry_revision_control')
                )
                task = asyncio.create_task(_approve(writer, fixture.control, command, actor))
                await _wait_native_writer(fixture, writer_pid)
                assert not task.done()
                assert (
                    await session.scalar(
                        text(f'SELECT approved_revision FROM "{fixture.control}".registry_revision_control')
                    )
                    == before
                )
            receipt = await asyncio.wait_for(task, 3)
            assert receipt["approved_revision"] > before
            async with fence.registry_company_approval_transaction(
                fixture.approver, control_schema=fixture.control
            ) as session:
                assert (
                    await session.scalar(
                        text(f'SELECT approved_revision FROM "{fixture.control}".registry_revision_control')
                    )
                    == receipt["approved_revision"]
                )
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_writer_first_refuses_busy_then_reads_fresh_revision(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture, _native_connection(fixture) as writer:
        stale = await _approval(fixture)
        command, actor = await _pending_company(fixture, "archive")
        async with writer.transaction():
            receipt = await _approve(writer, fixture.control, command, actor)
            with pytest.raises(ValueError, match="company_busy"):
                await fixture.service.approve(stale)
            assert not app.envelopes and await _rows(fixture) == []
        async with fence.registry_company_approval_transaction(
            fixture.approver, control_schema=fixture.control
        ) as session:
            assert (
                await session.scalar(
                    text(f'SELECT approved_revision FROM "{fixture.control}".registry_revision_control')
                )
                == receipt["approved_revision"]
            )
        with pytest.raises(RegistryPTGProducerScopeError, match="company_unapproved"):
            await fixture.service.approve(stale)
        assert not app.envelopes and await _rows(fixture) == []


async def _assert_backend_released(fixture, pid):
    async with fixture.engine.connect() as connection:
        async with asyncio.timeout(3):
            while await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid=:pid)"), {"pid": pid}
            ):
                await asyncio.sleep(0.01)
        assert not await connection.scalar(text("SELECT EXISTS(SELECT FROM pg_locks WHERE pid=:pid)"), {"pid": pid})
    async with fixture.approver() as session:
        assert await session.scalar(text("SELECT pg_backend_pid()")) != pid


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["cancel", "database"])
async def test_native_failed_scope_connection_is_retired_without_session_lock(tmp_path, monkeypatch, failure):
    _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        ready = asyncio.Event()
        backend_ids = []

        async def scope_operation():
            async with fence.registry_company_approval_transaction(
                fixture.approver, control_schema=fixture.control
            ) as session:
                backend_ids.append(await session.scalar(text("SELECT pg_backend_pid()")))
                await fence.require_registry_company_approval_fence(session, fixture.control)
                ready.set()
                await session.execute(text("SELECT pg_sleep(20)" if failure == "cancel" else "SELECT 1/0"))

        task = asyncio.create_task(scope_operation())
        try:
            await asyncio.wait_for(ready.wait(), 3)
            if failure == "cancel":
                task.cancel()
            with pytest.raises(asyncio.CancelledError if failure == "cancel" else DBAPIError):
                await asyncio.wait_for(task, 3)
            await _assert_backend_released(fixture, backend_ids[0])
            assert await _rows(fixture) == []
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_scope_roles_cannot_update_pointer_or_reader_insert(tmp_path, monkeypatch):
    _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        approver, reader = fixture.roles[1:]
        pointer = f'"{fixture.control}".registry_revision_control'
        async with fixture.engine.connect() as connection:
            assert await connection.scalar(
                text("SELECT has_table_privilege(:role,:table,'SELECT')"), {"role": approver, "table": pointer}
            )
            for role in (approver, reader):
                assert not await connection.scalar(
                    text("SELECT has_table_privilege(:role,:table,'UPDATE')"), {"role": role, "table": pointer}
                )
            assert not await connection.scalar(
                text("SELECT has_table_privilege(:role,:table,'INSERT')"), {"role": reader, "table": fixture.table}
            )
        async with fixture.approver() as session, session.begin():
            with pytest.raises(DBAPIError) as denied:
                await session.execute(text(f"UPDATE {pointer} SET approved_revision=approved_revision"))
            assert denied.value.orig.sqlstate == "42501"


@pytest.mark.asyncio
async def test_native_rr_lock_select_retains_snapshot_taken_before_wait(tmp_path, monkeypatch):
    """Demonstrate why an xact lock SELECT inside RR cannot establish freshness."""
    _app_authority(monkeypatch)
    async with (
        _native_scope(tmp_path) as fixture,
        _native_connection(fixture) as writer,
        _native_connection(fixture) as reader,
    ):
        command, actor = await _pending_company(fixture, "correct")
        before = fixture.document["command"]["approved_revision"]
        reader_pid = reader.get_server_pid()
        transaction = reader.transaction(isolation="repeatable_read")
        task = None
        await transaction.start()
        try:
            async with writer.transaction():
                receipt = await _approve(writer, fixture.control, command, actor)
                task = asyncio.create_task(
                    reader.execute("SELECT pg_advisory_xact_lock_shared($1)", fence._fence_key(fixture.control))
                )
                await _wait_native_writer(fixture, reader_pid)
                assert not task.done()
            await asyncio.wait_for(task, 3)
            # BEGIN itself and get_server_pid issue no snapshot-taking SELECT.
            actual = await reader.fetchval(
                f'SELECT approved_revision FROM "{fixture.control}".registry_revision_control'
            )
            assert actual == before and actual < receipt["approved_revision"]
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await transaction.rollback()


@pytest.mark.asyncio
async def test_native_success_unlocks_and_readonly_transaction_refuses_write(tmp_path, monkeypatch):
    _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        async with fence.registry_company_approval_transaction(
            fixture.approver, control_schema=fixture.control
        ) as session:
            pid = await session.scalar(text("SELECT pg_backend_pid()"))
            await fence.require_registry_company_approval_fence(session, fixture.control)
        async with fixture.approver() as session:
            assert await session.scalar(text("SELECT pg_backend_pid()")) == pid
            assert not await session.scalar(
                text("SELECT EXISTS(SELECT FROM pg_locks WHERE pid=pg_backend_pid() AND locktype='advisory')")
            )
        async with fence.registry_company_approval_transaction(
            fixture.approver, control_schema=fixture.control
        ) as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            with pytest.raises(DBAPIError) as denied:
                await session.execute(
                    text(f"INSERT INTO {fixture.table}(scope_id) VALUES(CAST(:id AS uuid))"), {"id": str(uuid4())}
                )
            assert denied.value.orig.sqlstate == "25006"
        assert await _rows(fixture) == []


@pytest.mark.asyncio
async def test_native_fresh_authority_actor_mismatch_refuses_append(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        app.actor_override = {**envelope["actor"], "user_id": str(uuid4())}
        with pytest.raises(PermissionError, match="authority_changed"):
            await fixture.service.approve(envelope)
        assert app.envelopes == [envelope] and await _rows(fixture) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["preview", "approve"])
async def test_native_expired_deadline_never_checks_out_source_session(tmp_path, monkeypatch, operation):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = fixture.document if operation == "preview" else await _approval(fixture)
        checkouts = []
        engines = [factory.kw["bind"].sync_engine for factory in (fixture.reader, fixture.approver)]

        def checkout(_connection, _record, _proxy):
            checkouts.append(True)

        for engine in engines:
            event.listen(engine, "checkout", checkout)
        try:
            with pytest.raises(scope_engine.RegistryPTGScopeDeadlineExpired) as expired:
                await getattr(fixture.service, operation)(envelope, deadline=asyncio.get_running_loop().time() - 1)
            assert expired.value.outcome_unknown is False and checkouts == []
        finally:
            for engine in engines:
                event.remove(engine, "checkout", checkout)
        assert app.envelopes == [] and await _rows(fixture) == []


async def _wait_blocked_source_query(fixture, blocker_pid, journal):
    """Observe only the registered approver's actual snapshot custody query."""
    async with fixture.engine.connect() as connection:
        await connection.execution_options(isolation_level="AUTOCOMMIT")
        async with asyncio.timeout(3):
            while True:
                observation = (
                    (
                        await connection.execute(
                            text(
                                "SELECT pid,wait_event_type,wait_event,length(query) AS query_length, "
                                "query LIKE '%FOR KEY SHARE%' AS has_lock_clause,pg_blocking_pids(pid) AS blockers "
                                "FROM pg_stat_activity WHERE usename=:role AND wait_event_type='Lock' "
                                "AND query LIKE :query"
                            ),
                            # The native activity query may truncate the statement's trailing lock clause.
                            {"role": fixture.roles[1], "query": f"%FROM {fixture.source.schema}.ptg2_snapshot%"},
                        )
                    )
                    .mappings()
                    .one_or_none()
                )
                if observation is not None:
                    assert blocker_pid in observation["blockers"]
                    journal.write_text(json.dumps(dict(observation), sort_keys=True))
                    return observation["pid"]
                await asyncio.sleep(0.01)


async def _assert_source_query_failure(task, failure):
    if failure == "cancel":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        return
    with pytest.raises(scope_engine.RegistryPTGScopeDeadlineExpired) as expired:
        await asyncio.wait_for(task, 3)
    assert expired.value.outcome_unknown is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["deadline", "cancel"])
async def test_native_source_query_expiry_retires_physical_fence_without_append(tmp_path, monkeypatch, failure):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        task = None
        try:
            async with fixture.engine.begin() as blocker:
                blocker_pid = await blocker.scalar(text("SELECT pg_backend_pid()"))
                await blocker.execute(text(f"SELECT snapshot_id FROM {fixture.source.schema}.ptg2_snapshot FOR UPDATE"))
                task = asyncio.create_task(
                    # Expire before the unchanged native lifecycle lock timeout can fire.
                    fixture.service.approve(envelope, deadline=asyncio.get_running_loop().time() + 0.25)
                )
                pid = await _wait_blocked_source_query(fixture, blocker_pid, tmp_path / "source-query-observer.json")
                assert not task.done()
                await _assert_source_query_failure(task, failure)
                await _assert_backend_released(fixture, pid)
                assert app.envelopes == [] and await _rows(fixture) == []
        finally:
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_late_fresh_authority_cannot_resume_retained_insert(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        async with fixture.approver() as session:
            pid = await session.scalar(text("SELECT pg_backend_pid()"))
        app.delay_seconds = 3
        try:
            with pytest.raises(scope_engine.RegistryPTGScopeDeadlineExpired) as expired:
                await fixture.service.approve(envelope, deadline=asyncio.get_running_loop().time() + 2)
            assert expired.value.outcome_unknown is False
            assert app.started.is_set() and app.envelopes == [envelope]
            assert len(app.timeouts) == 1 and 0 < app.timeouts[0] <= 2
            assert await _rows(fixture) == []
            await _assert_backend_released(fixture, pid)
        finally:
            # The read-only HTTP thread can finish after local cancellation.
            assert await asyncio.to_thread(app.finished.wait, 4)
        assert await _rows(fixture) == []


@pytest.mark.asyncio
async def test_native_expiry_after_real_uncommitted_insert_rolls_back(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    actual_retain = scope_engine._retain_approval
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        inserted = asyncio.Event()
        backend_ids = []

        async def delayed_native_return(session, table, document):
            retained = await actual_retain(session, table, document)
            assert await session.scalar(text(f"SELECT count(*) FROM {table}")) == 1
            backend_ids.append(await session.scalar(text("SELECT pg_backend_pid()")))
            inserted.set()
            # Delay only after the unchanged retention helper has executed its real INSERT and SELECT.
            await session.execute(text("SELECT pg_sleep(6)"))
            return retained

        monkeypatch.setattr(scope_engine, "_retain_approval", delayed_native_return)
        task = asyncio.create_task(fixture.service.approve(envelope, deadline=asyncio.get_running_loop().time() + 4))
        try:
            await asyncio.wait_for(inserted.wait(), 3)
            assert await _rows(fixture) == []
            with pytest.raises(scope_engine.RegistryPTGScopeDeadlineExpired) as expired:
                await asyncio.wait_for(task, 5)
            assert expired.value.outcome_unknown is False
            assert app.envelopes == [envelope] and await _rows(fixture) == []
            await _assert_backend_released(fixture, backend_ids[0])
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_native_lost_committed_reply_recovers_exactly_with_fresh_authority(tmp_path, monkeypatch):
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        envelope = await _approval(fixture)
        committed_receipts = []

        async def lose_reply():
            committed_receipts.append(await fixture.service.approve(envelope))
            raise TimeoutError("synthetic_reply_lost_after_commit")

        with pytest.raises(TimeoutError, match="synthetic_reply_lost_after_commit"):
            await lose_reply()
        retained = await _rows(fixture)
        assert len(retained) == 1
        recovered = await fixture.service.approve(deepcopy(envelope))
        assert recovered == committed_receipts[0] and await _rows(fixture) == retained
        assert app.envelopes == [envelope, envelope]
        app.authorized = False
        with pytest.raises(PermissionError, match="authority_changed"):
            await fixture.service.approve(envelope)
        assert app.envelopes == [envelope] * 3 and await _rows(fixture) == retained


def test_restricted_runtime_urls_keep_exact_login_credentials():
    """Check fixture URL wiring offline without claiming native authentication."""
    from sqlalchemy.engine import URL, make_url

    roles = ("synthetic_owner", "synthetic_approver", "synthetic_reader")
    login_password, ambient_password = uuid4().hex, uuid4().hex
    engine = SimpleNamespace(
        url=URL.create(
            "postgresql+asyncpg",
            username="synthetic_admin",
            password=ambient_password,
            host="localhost",
            port=5432,
            database="synthetic_fixture",
        )
    )
    configuration = _runtime_configuration(engine, "source", "control", roles, login_password)
    for key, role in (("reader_dsn", roles[2]), ("approver_dsn", roles[1])):
        url = make_url(configuration[key])
        assert url.username == role
        assert url.host == engine.url.host and url.database == engine.url.database
        if url.password != login_password or url.password == ambient_password:
            pytest.fail("restricted role credential missing or mismatched", pytrace=False)
    assert configuration["owner_role"] == roles[0]


async def test_restricted_role_fixture_assigns_matching_login_credentials(tmp_path):
    """Inspect emitted role DDL and journal only; this is not a database login test."""
    roles = ("synthetic_owner", "synthetic_approver", "synthetic_reader")
    login_password, statements, created = uuid4().hex, [], []

    class RecordedStatements:
        """Capture application-generated fixture SQL without database execution."""

        scalar = AsyncMock(return_value=False)

        async def exec_driver_sql(self, statement):
            statements.append(statement)

        @asynccontextmanager
        async def begin(self):
            yield self

    journal = tmp_path / "role-cleanup.json"
    await _create_roles(RecordedStatements(), roles, created, journal, login_password)
    assert created == list(roles)
    assert statements[0] == 'CREATE ROLE "synthetic_owner" NOLOGIN'
    for statement, role in zip(statements[1:], roles[1:], strict=True):
        if statement != f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{login_password}'":
            pytest.fail("restricted LOGIN role credential missing or mismatched", pytrace=False)
    if login_password in journal.read_text():
        pytest.fail("fixture credential leaked into cleanup journal", pytrace=False)


async def _published_source_fixture(fixture):
    from tests.test_result_archive_published_authority_postgres import _seed as seed_published

    source_name = fixture.service.schema_name
    snapshot = await seed_published(fixture.engine, source_name, suffix="review", key=719)
    schema = f'"{source_name}"'
    async with fixture.engine.begin() as connection:
        await connection.execute(
            text(f"INSERT INTO {schema}.ptg2_source_trace_set VALUES(:hash,CAST(:traces AS text[]))"),
            {"hash": "ab" * 32, "traces": ["cd" * 32]},
        )
        await connection.execute(
            text(f"INSERT INTO {schema}.ptg2_source_trace VALUES(:hash,:version)"),
            {"hash": "cd" * 32, "version": "published-version"},
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_source_identity VALUES(:hash,'in_network','https://source.example/published')"
            ),
            {"hash": "e" * 16},
        )
        await connection.execute(
            text(f"""INSERT INTO {schema}.ptg2_source_file_version
            (source_file_version_id,source_identity_hash,raw_sha256,logical_sha256,content_length,etag,last_modified,verification_mode,payload)
            VALUES('published-version',:identity,:raw,NULL,1,NULL,NULL,'downloaded','{{}}'::jsonb)"""),
            {"identity": "e" * 16, "raw": (b"r" * 32).hex()},
        )
    return fixture.document["ownership"] | {
        "snapshot_id": snapshot,
        "source_key": "source_review",
        "engine_run_id": "run-review",
        "engine_source_file_version_id": "published-version",
        "engine_source_identity_hash": "e" * 16,
    }


@asynccontextmanager
async def _published_lock_provision(fixture, tmp_path):
    """Register narrow grants/constant columns before changing task-owned fixtures."""
    from process.registry_ptg_published_provisioning import (
        HEADER_TABLES,
        LOCK_COLUMN,
        PIN_INSERT_COLUMNS,
        published_lock_provisioning_statements,
    )

    schema_name = fixture.service.schema_name
    owner, approver, reader = fixture.roles
    statements = published_lock_provisioning_statements(schema_name, reader, approver)
    journal = tmp_path / "published-lock-capability-registration.json"
    journal.write_text(
        json.dumps(
            {
                "phase": "registered",
                "source_schema": schema_name,
                "statements": statements,
                "cleanup_columns": HEADER_TABLES,
                "cleanup_pin_insert_columns": PIN_INSERT_COLUMNS,
            }
        )
    )
    try:
        async with fixture.engine.begin() as connection:
            # Existing broad lock grants belong only to this fixture, not a live role.
            await connection.exec_driver_sql(
                f'REVOKE UPDATE ON "{schema_name}".ptg2_snapshot FROM "{reader}","{approver}"'
            )
            for statement in statements:
                await connection.exec_driver_sql(statement)
        yield
    finally:
        async with fixture.engine.begin() as connection:
            for table in HEADER_TABLES:
                await connection.exec_driver_sql(
                    f'REVOKE UPDATE({LOCK_COLUMN}) ON "{schema_name}"."{table}" FROM "{reader}","{approver}"'
                )
            await connection.exec_driver_sql(
                f'REVOKE INSERT({",".join(PIN_INSERT_COLUMNS)}) ON "{schema_name}".ptg2_snapshot_pin FROM "{approver}"'
            )
        journal.write_text(json.dumps({"phase": "revoked", "source_schema": schema_name, "statements": statements}))


@pytest.mark.asyncio
async def test_native_published_plan_inventory_uses_complete_real_source_set(tmp_path):
    from process.ptg_parts.result_archive_published_identity import load_published_result_identity
    from process.registry_ptg_published_plan_source import published_plan_inventory

    async with _native_scope(tmp_path) as fixture:
        ownership = await _published_source_fixture(fixture)
        async with async_sessionmaker(fixture.engine)() as session, session.begin():
            identity_before_by_field = await load_published_result_identity(
                session, schema_name=fixture.service.schema_name, snapshot_id=ownership["snapshot_id"]
            )
            identity_before_by_field = {
                field_name: field_value.hex() if isinstance(field_value, bytes) else field_value
                for field_name, field_value in identity_before_by_field.items()
            }
        async with _published_lock_provision(fixture, tmp_path):
            async with fixture.reader() as session, session.begin():
                inventory = await published_plan_inventory(
                    session, fixture.service.schema_name, ownership, operation_id="native_published_plan_inventory"
                )
                assert inventory["identity"] == identity_before_by_field
                assert inventory["authority"]["contract"] == "ptg_published_result_source_authority.v1"
                assert inventory["plan_scopes"] == [{"plan_id": "plan-review", "plan_market_type": "group"}]
                assert inventory["source_keys"] == [1]
                assert inventory["selection_mode"] == "complete_snapshot_source_set"
                assert inventory["file_versions"] == [
                    {
                        "source_file_version_id": "published-version",
                        "source_identity_sha256": "e" * 16,
                        "raw_sha256": (b"r" * 32).hex(),
                    }
                ]
                assert not await session.scalar(
                    text(
                        f'SELECT EXISTS(SELECT FROM "{fixture.service.schema_name}".ptg2_frozen_source_file_binding WHERE internal_run_id=:run)'
                    ),
                    {"run": ownership["engine_run_id"]},
                )


async def _deny_published_header_mutations(session, schema_name):
    from process.registry_ptg_published_provisioning import HEADER_TABLES

    protected_columns_by_table = {
        "ptg2_snapshot": "manifest",
        "ptg2_import_run": "options",
        "ptg2_v3_snapshot_binding": "snapshot_key",
        "ptg2_v3_snapshot_scope": "plan_id",
        "ptg2_v3_snapshot_layout": "mapping_digest",
        "ptg2_snapshot_pin": "reason",
    }
    for table in HEADER_TABLES:
        relation = f'"{schema_name}"."{table}"'
        column = protected_columns_by_table[table]
        with pytest.raises(DBAPIError) as denied:
            async with session.begin_nested():
                await session.execute(text(f'UPDATE {relation} SET "{column}"="{column}"'))
        assert denied.value.orig.sqlstate == "42501"
        with pytest.raises(DBAPIError) as constant:
            async with session.begin_nested():
                await session.execute(text(f"UPDATE {relation} SET registry_ptg_read_lock=1"))
        assert constant.value.orig.sqlstate == "23514"


async def _published_role_probe(fixture, ownership, sessions, pin_writer):
    from process.ptg_parts.result_archive_source_authority import commit_ptg_result_archive_source_authority
    from process.registry_ptg_published_plan_source import published_plan_inventory
    from process.registry_ptg_published_provisioning import require_published_lock_privileges

    async with sessions() as session, session.begin():
        await require_published_lock_privileges(session, fixture.service.schema_name, pin_writer=pin_writer)
        before = await published_plan_inventory(
            session,
            fixture.service.schema_name,
            ownership,
            operation_id="native_immutable_slot_probe",
        )
        if pin_writer:
            await commit_ptg_result_archive_source_authority(
                session, schema_name=fixture.service.schema_name, authority=before["authority"]
            )
            await commit_ptg_result_archive_source_authority(
                session, schema_name=fixture.service.schema_name, authority=before["authority"]
            )
        await _deny_published_header_mutations(session, fixture.service.schema_name)
        after = await published_plan_inventory(
            session,
            fixture.service.schema_name,
            ownership,
            operation_id="native_immutable_slot_probe",
        )
        assert before == after


@pytest.mark.asyncio
async def test_native_published_lock_slots_deny_identity_mutation_and_preserve_hashes(tmp_path):
    async with _native_scope(tmp_path) as fixture:
        ownership = await _published_source_fixture(fixture)
        async with _published_lock_provision(fixture, tmp_path):
            await _published_role_probe(fixture, ownership, fixture.approver, True)
            await _published_role_probe(fixture, ownership, fixture.reader, False)


async def _approve_published_association(fixture):
    """Use real manual writers/approval for the current legal-company/network link."""
    actor = RegistryActor("platform_admin", UUID(fixture.document["actor"]["user_id"]), "system")
    async with async_sessionmaker(fixture.engine)() as session, session.begin():
        network = await apply_registry_record_command(
            session,
            RegistryRecordCommand(
                "network",
                None,
                "create",
                0,
                {"display_name": "Published review network", "aliases": []},
                "Review",
                uuid4().hex,
                uuid4(),
            ),
            actor,
            schema=fixture.control,
        )
        network_id = int(network["record_id"])
        links = await apply_registry_record_command(
            session,
            RegistryRecordCommand(
                "company_links",
                UUID(fixture.document["command"]["legal_company_id"]),
                "create",
                0,
                {"network_ids": [network_id], "group_id": None},
                "Reviewed actual association",
                uuid4().hex,
            ),
            actor,
            schema=fixture.control,
        )
    async with _native_connection(fixture) as connection:
        receipt = await _approve(
            connection, fixture.control, await _command(connection, fixture.control, network, links), actor
        )
    return network_id, receipt["approved_revision"]


async def _published_review_setup(fixture, tmp_path, ownership, network_id, approved_revision):
    table = f'"{fixture.control}".registry_ptg_published_plan_scope'
    registration = tmp_path / "published-scope-table-registration.json"
    registration.write_text(
        json.dumps(
            {
                "phase": "registered",
                "table": table,
                "owner": fixture.roles[0],
                "reader": fixture.roles[2],
                "approver": fixture.roles[1],
                "cleanup": "parent task-owned control schema",
            }
        )
    )
    async with fixture.engine.begin() as connection:
        for statement in _migration("20261009040000_registry_ptg_published_plan_scope.py")._ddl(fixture.control):
            await connection.exec_driver_sql(statement)
        await connection.exec_driver_sql(f'ALTER TABLE {table} OWNER TO "{fixture.roles[0]}"')
        await connection.exec_driver_sql(f'GRANT SELECT ON {table} TO "{fixture.roles[2]}"')
        await connection.exec_driver_sql(f'GRANT SELECT,INSERT ON {table} TO "{fixture.roles[1]}"')
    old_intent_by_field = fixture.document["command"]
    intent_by_field = {
        name: field_value
        for name, field_value in old_intent_by_field.items()
        if name not in {"company_key", "cohort_id"}
    }
    intent_by_field |= {
        "review_type": "published_complete_snapshot_plan",
        "selection_mode": "complete_snapshot_source_set",
        "network_id": network_id,
        "approved_revision": approved_revision,
        "plan_id": "plan-review",
        "plan_market_type": "group",
        "file_versions": [
            {
                "source_file_version_id": "published-version",
                "source_identity_sha256": "e" * 16,
                "raw_sha256": (b"r" * 32).hex(),
            }
        ],
    }
    preview_envelope_by_field = {**fixture.document, "command": intent_by_field, "ownership": ownership}
    return table, registration, intent_by_field, preview_envelope_by_field


async def _published_reader_probe(fixture, intent_by_field, first, preview, table, network_id):
    from process.registry_ptg_published_plan_scope import read_registry_ptg_published_plan_scope

    async with fixture.reader() as session, session.begin():
        receipt = await read_registry_ptg_published_plan_scope(
            session,
            scope_id=UUID(intent_by_field["scope_id"]),
            client_id=intent_by_field["client_id"],
            approval_sha256=first["approval_sha256"],
            store=fixture.service.store,
        )
        assert receipt["command"] == preview["command"]
        assert receipt["evidence"]["selected_source_keys"] == [1]
        assert receipt["command"]["network_id"] == network_id
        assert "company_key" not in receipt["command"] and "cohort_id" not in receipt["command"]
        with pytest.raises(DBAPIError):
            async with session.begin_nested():
                await session.execute(text(f"DELETE FROM {table}"))


@pytest.mark.asyncio
async def test_native_published_operator_review_replay_and_protected_consumer(tmp_path, monkeypatch):
    """Only upstream HTTP authority is synthetic; roles/source/store/receipt are real."""
    app = _app_authority(monkeypatch)
    async with _native_scope(tmp_path) as fixture:
        ownership = await _published_source_fixture(fixture)
        network_id, approved_revision = await _approve_published_association(fixture)
        table, registration, intent_by_field, preview_envelope_by_field = await _published_review_setup(
            fixture, tmp_path, ownership, network_id, approved_revision
        )
        async with _published_lock_provision(fixture, tmp_path):
            preview = await fixture.service.preview(preview_envelope_by_field)
            approval_envelope_by_field = {
                name: fixture.document[name] for name in ("actor", "session_token_sha256")
            } | preview
            first = await fixture.service.approve(approval_envelope_by_field)
            assert await fixture.service.approve(approval_envelope_by_field) == first
            await _published_reader_probe(fixture, intent_by_field, first, preview, table, network_id)
            app.authorized = False
            with pytest.raises(PermissionError):
                await fixture.service.approve(approval_envelope_by_field)
            async with fixture.engine.connect() as connection:
                assert await connection.scalar(text(f"SELECT count(*) FROM {table}")) == 1
        registration.write_text(json.dumps({"phase": "retained_until_parent_cleanup", "table": table}))
