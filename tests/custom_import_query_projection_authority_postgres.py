# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native signed projection authority, ordinary reader grants and page rollback."""

import asyncio
import datetime as dt
import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sanic.compat import Header
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api import control_query_projection_batch as service
from db.models.custom_import import CustomImportBuildAttempt, CustomImportGeneration, CustomImportLease
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_output as output
from process.custom_import import execution as lifecycle
from process.custom_import import query_projection_build as projection
from process.custom_import.build_source import SourceBuildRequest, _page_session
from process.custom_import.capture_pending import (
    PendingCaptureRequest,
    begin_pending_parquet_bundle,
    seal_pending_parquet_bundle,
)
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.query_projection_authorization import PROJECTION_PATH, PROJECTION_PERMIT_CONTRACT
from process.custom_import.query_projection_evidence import read_projection_tables
from process.custom_import.query_projection_storage import QUERY_PROJECTION_TABLE_NAMES, query_projection_tables
from process.custom_import.runner_registry import load_registry
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from process.custom_import.snowflake_source_binding import SnowflakeSourceBinding, register_snowflake_source_binding
from process.custom_import.storage_layout import snapshot_schema
from tests import test_custom_import_admission_authorization as envelope
from tests.custom_import_postgres_support import isolated_publication_case
from tests.custom_import_query_projection_build_postgres import _definition, _records
from tests.test_custom_import_build_output_postgres import _build
from tests.test_custom_import_build_source_postgres import _sealed_parts
from tests.test_custom_import_bulk_snapshot_writers_postgres import _drop_fixture_role
from tests.test_custom_import_capture_pending_postgres import _receipt, _retain
from tests.test_custom_import_segmented_capture_postgres import _POLICY
from tests.test_custom_import_writer_cutover_postgres import _assert_denied, _record_role

pytestmark = pytest.mark.asyncio
_TOKEN = b"synthetic-projection-owner"
_DOMAIN = b"CUSTOM_IMPORT_QUERY_PROJECTION_PERMIT_V1\x00"


def _source_definition():
    definition = _definition()
    document = json.loads(definition.canonical)
    document["aliases"] = {
        stream.stream_id: {
            field.field_id.upper(): field.field_id
            for field in definition.fields
            if field.collection == stream.child_collection
        }
        for stream in definition.source_streams
    }
    return CustomImportDefinition.from_mapping(document)


def _binding(definition, policy):
    return SnowflakeSourceBinding.from_mapping(
        dict(
            contract="custom-import/source-binding/v2",
            connector="snowflake_bundle",
            definition_sha256=definition.digest,
            schema_sha256=definition.schema_digest,
            source_object={"fingerprint_sha256": "1" * 64, "version": "synthetic-projection-source"},
            role="synthetic_reader",
            warehouse="synthetic_load",
            processing_policy=policy.to_mapping(),
            streams=[
                dict(
                    stream_id=stream.stream_id,
                    relation=["synthetic", "public", stream.stream_id],
                    source_snapshot_token_relation=["synthetic", "public", f"{stream.stream_id}_snapshots"],
                    semantic_token_metadata_key=stream.snapshot_token,
                    source_snapshot_token_column_identifier="snapshot_token",
                    columns=[
                        dict(field_id=field.field_id, column_identifier=field.field_id)
                        for field in definition.fields
                        if field.collection == stream.child_collection
                    ],
                )
                for stream in definition.source_streams
            ],
        )
    )


async def _pending_capture(case, definition, policy):
    async with case.sessions() as session, session.begin():
        binding = await register_snowflake_source_binding(
            session,
            dataset_key="synthetic_projection_authority",
            definition=definition,
            binding=_binding(definition, policy),
        )
    identity = hashlib.sha256(b"synthetic-projection-request").digest()
    idempotency_key = uuid.uuid4().hex
    async with case.sessions() as session, session.begin():
        execution = await lifecycle.reserve_execution(
            session,
            dataset_id=binding.dataset_id,
            definition_revision_id=binding.definition_revision_id,
            schema_revision_id=binding.schema_revision_id,
            idempotency_key=idempotency_key,
            mechanism="local",
            request_identity_sha256=identity,
            source_binding_revision_id=binding.source_binding_revision_id,
        )
        lease = await lifecycle.claim_execution(session, execution_id=execution.execution_id, token=_TOKEN)
    pending = PendingCaptureRequest(
        binding.dataset_id,
        binding.definition_revision_id,
        binding.schema_revision_id,
        execution.execution_id,
        lease.fence,
        _TOKEN,
        identity,
        hashlib.sha256(b"synthetic-source-request").digest(),
        hashlib.sha256(b"synthetic-source-statement").digest(),
        "synthetic-projection-snapshot",
        policy.capture,
        binding.source_binding_revision_id,
        binding.source_binding_sha256,
    )
    async with case.sessions() as session, session.begin():
        registration = await begin_pending_parquet_bundle(session, request=pending)
    return pending, registration, binding, idempotency_key


def _bound_receipt(pending, stream, parts):
    receipt = _receipt(pending, stream, parts)
    document = json.loads(receipt.canonical_manifest) | {"source_binding_sha256": pending.source_binding_sha256.hex()}
    canonical = canonical_json(document)
    return replace(
        receipt, canonical_manifest=canonical, manifest_sha256=hashlib.sha256(canonical.encode()).hexdigest()
    )


async def _winning_bound_build(case):
    definition = _source_definition()
    policy = ProcessingPolicy(
        SegmentedCapturePolicy.from_mapping(_POLICY), 17, BuildPolicy(64, 65_536, 10_000, 120, 300)
    )
    pending, registration, binding, idempotency_key = await _pending_capture(case, definition, policy)
    parts = _sealed_parts(pending, definition, _records())
    await _retain(case, pending, registration, parts)
    async with case.sessions() as session, session.begin():
        await seal_pending_parquet_bundle(
            session,
            request=pending,
            capture_bundle_id=registration.capture_bundle_id,
            receipts=tuple(_bound_receipt(pending, stream, captures) for stream, captures in parts.items()),
        )
        now = await session.scalar(select(func.clock_timestamp()))
    request = SourceBuildRequest(
        pending.dataset_id,
        pending.definition_revision_id,
        pending.schema_revision_id,
        pending.execution_id,
        pending.token,
        pending.fence,
        definition,
        expected_base_generation_id=None,
        expected_pointer_version=0,
        complete_scope=definition.refresh_mode == "snapshot",
        page_row_limit=policy.build.page_row_limit,
        page_byte_limit=policy.build.page_byte_limit,
        statement_timeout_ms=policy.build.statement_timeout_ms,
        build_deadline_at=now + dt.timedelta(seconds=policy.build.build_deadline_seconds),
        lease_seconds=policy.build.lease_seconds,
    )
    build_id, generation_id = await _build(case, request)
    async with _page_session(case.sessions, request, build_id) as (session, _build_row):
        registry = await load_registry(session, request)
        generation = await session.get(CustomImportGeneration, generation_id)
        session.expunge(generation)
    await output._attach_families(case.sessions, request, build_id)
    await output._write_winners(case.sessions, request, registry, build_id, generation)
    async with case.sessions() as session:
        family_id = await session.scalar(
            select(CustomImportSnapshotFamily.family_id).where(
                CustomImportSnapshotFamily.generation_id == generation_id
            )
        )
    return SimpleNamespace(
        case=case,
        request=request,
        build_id=build_id,
        family_id=family_id,
        binding=binding,
        idempotency_key=idempotency_key,
    )


def _wire(state, *, context_changes=None, pins_changes=None, expires_at=None, domain=_DOMAIN):
    now = dt.datetime.now(dt.UTC)
    context = envelope.permit(
        contract=PROJECTION_PERMIT_CONTRACT,
        path=PROJECTION_PATH,
        dataset_id=state.request.dataset_id,
        definition_revision_id=state.request.definition_revision_id,
        schema_revision_id=state.request.schema_revision_id,
        source_binding_revision_id=state.binding.source_binding_revision_id,
        source_binding_sha256=state.binding.source_binding_sha256.hex(),
        idempotency_key=state.idempotency_key,
        issued_at=(now - dt.timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        expires_at=(expires_at or now + dt.timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    ) | (context_changes or {})
    headers = envelope.signed_headers(envelope.canonical(context), token=_TOKEN, domain=domain)
    pins = dict(
        build_id=state.build_id,
        execution_id=state.request.execution_id,
        fence=state.request.fence,
        expected_phase="root",
        expected_after_entity_binding_id=0,
        expected_after_selected_value=None,
        expected_after_child_key_sha256=None,
        expected_index_count=0,
        expected_proof_count=0,
    ) | (pins_changes or {})
    return SimpleNamespace(
        headers=Header([(name.replace("-Admission-", "-Projection-"), header_value) for name, header_value in headers]),
        body=envelope.canonical(pins),
        method="POST",
        path=PROJECTION_PATH,
        query_string="",
    )


async def _serve(state, wire, *, factory=None):
    return await service.serve_projection_batch(
        wire,
        factory or state.case.sessions,
        keyring=envelope.keyring(),
        expected_origin=envelope.ORIGIN,
        trusted_now=dt.datetime.now(dt.UTC),
    )


async def _native_state(state):
    async with state.case.sessions() as session:
        manifest_rows = (await session.execute(select(CustomImportQueryProjection.__table__))).all()
        relations = (
            await session.execute(
                text(
                    "SELECT c.oid,c.relname,c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relname=ANY(CAST(:names AS text[])) ORDER BY c.relname"
                ),
                {"schema": snapshot_schema(state.family_id), "names": list(QUERY_PROJECTION_TABLE_NAMES)},
            )
        ).all()
        counts = []
        for table in query_projection_tables(state.family_id, state.request.definition).values():
            if any(relation.relname == table.name for relation in relations):
                counts.append(await session.scalar(select(func.count()).select_from(table)))
        lease = await session.get(CustomImportLease, state.request.execution_id)
        build = await session.get(CustomImportBuildAttempt, state.build_id)
        return manifest_rows, relations, counts, lease.expires_at, build.phase


async def test_signed_scope_rejections_leave_native_projection_state_untouched():
    async with isolated_publication_case() as case:
        state = await _winning_bound_build(case)
        before = await _native_state(state)
        rejected_requests = [
            (_wire(state, domain=b"CUSTOM_IMPORT_ADMISSION_PERMIT_V1\x00"), 403),
            (_wire(state, context_changes={"contract": "custom-import-admission-permit/v1"}), 403),
            (_wire(state, expires_at=dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)), 403),
            (_wire(state, context_changes={"dataset_id": state.request.dataset_id + 1}), 503),
            (_wire(state, context_changes={"source_binding_sha256": "f" * 64}), 503),
            (_wire(state, context_changes={"idempotency_key": "other-execution"}), 503),
            (_wire(state, pins_changes={"execution_id": state.request.execution_id + 1}), 503),
            (_wire(state, pins_changes={"fence": state.request.fence + 1}), 503),
        ]
        for wire, expected_status in rejected_requests:
            assert (await _serve(state, wire)).status == expected_status
            assert await _native_state(state) == before
        accepted = await _serve(state, _wire(state))
        assert accepted.status == 200
        assert json.loads(accepted.body)["after"]["root_count"] == 3


@asynccontextmanager
async def _reader_roles(case):
    names = tuple("projection_" + uuid.uuid4().hex[:16] for _ in range(2))
    created_names = []
    try:
        for name in names:
            _record_role(name, case.schema_name, "planned")
            async with case.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{name}" NOLOGIN NOSUPERUSER NOBYPASSRLS'))
            created_names.append(name)
            async with case.engine.begin() as connection:
                await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{name}"'))
                await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{case.schema_name}" TO "{name}"'))
            _record_role(name, case.schema_name, "created")
        yield names
    finally:
        for name in reversed(created_names):
            async with case.engine.begin() as connection:
                await _drop_fixture_role(connection, name)
                assert not await connection.scalar(
                    text("SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=:name)"), {"name": name}
                )
            _record_role(name, case.schema_name, "removed")


@asynccontextmanager
async def _role_session(case, role):
    async with case.engine.connect() as connection:
        await connection.execute(text(f'SET SESSION AUTHORIZATION "{role}"'))
        await connection.commit()
        try:
            async with AsyncSession(connection, expire_on_commit=False, autoflush=False) as session:
                yield session
        finally:
            await connection.rollback()
            await connection.execute(text("RESET SESSION AUTHORIZATION"))
            await connection.commit()


async def _worker_cannot_mutate(state, worker):
    namespace = snapshot_schema(state.family_id)
    root = f'"{namespace}".custom_import_derived_root'
    manifest = f'"{state.case.schema_name}".custom_import_query_projection'
    registry = f'"{state.case.schema_name}".custom_import_snapshot_relation'
    async with _role_session(state.case, worker) as session, session.begin():
        assert await session.scalar(text("SELECT current_user = session_user"))
        assert await session.scalar(text(f"SELECT count(*) FROM {root}")) == 3
        for statement in (
            f"INSERT INTO {root} DEFAULT VALUES",
            f"UPDATE {root} SET field_1=0",
            f"DELETE FROM {root}",
            f"TRUNCATE {root}",
            f"ALTER TABLE {root} ADD COLUMN unexpected bigint",
            f'CREATE TABLE "{namespace}".unregistered_projection (entity_binding_id bigint)',
            f"UPDATE {manifest} SET root_table_oid=child_table_oid",
            f"INSERT INTO {manifest} SELECT * FROM {manifest}",
            f"DELETE FROM {manifest}",
            f"UPDATE {registry} SET table_oid=1",
        ):
            await _assert_denied(session, statement)


async def _sealed_reader(state, reader):
    sealed = await output.build_output(state.case.sessions, state.request, state.build_id)
    target = SimpleNamespace(
        generation_id=sealed.generation_id,
        **{
            name: getattr(state.request, name)
            for name in ("dataset_id", "definition_revision_id", "schema_revision_id")
        },
    )
    async with _role_session(state.case, reader) as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        tables = await read_projection_tables(
            session, target, state.request.definition, snapshot_family_id=state.family_id
        )
        assert tuple(tables) == QUERY_PROJECTION_TABLE_NAMES
        assert [await session.scalar(select(func.count()).select_from(table)) for table in tables.values()] == [3, 261]
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"


async def test_native_worker_writes_close_while_registered_snapshot_readers_remain_allowed():
    async with isolated_publication_case() as case, _reader_roles(case) as (worker, reader):
        state = await _winning_bound_build(case)
        async with case.engine.begin() as connection:
            for role in (worker, reader):
                await connection.execute(
                    text(f'GRANT USAGE ON SCHEMA "{snapshot_schema(state.family_id)}" TO "{role}"')
                )
            await connection.execute(text(f'ALTER DEFAULT PRIVILEGES GRANT ALL ON TABLES TO "{worker}"'))
        accepted = await _serve(state, _wire(state))
        assert accepted.status == 200
        before = await _native_state(state)
        await _worker_cannot_mutate(state, worker)
        progress = json.loads(accepted.body)["after"]
        pins_by_name = {
            f"expected_{name}": value for name, value in progress.items() if name not in {"root_count", "child_count"}
        }
        rejected = await _serve(
            state, _wire(state, pins_changes=pins_by_name), factory=lambda: _role_session(case, worker)
        )
        assert rejected.status == 503 and await _native_state(state) == before
        await _sealed_reader(state, reader)


@pytest.mark.parametrize("interruption", ["cancel", "permit_expiry"])
async def test_interrupted_native_step_rolls_back_and_a_fresh_permit_retries(monkeypatch, interruption):
    async with isolated_publication_case() as case:
        state = await _winning_bound_build(case)
        before = await _native_state(state)
        written, release = asyncio.Event(), asyncio.Event()
        original = projection._advance_projection
        expires_at = (dt.datetime.now(dt.UTC) + dt.timedelta(seconds=3)).replace(microsecond=0)
        failed_sessions = []

        async def interrupt_after_write(session, request, build, manifest):
            await original(session, request, build, manifest)
            await session.flush()
            failed_sessions.append(session)
            assert request.authorization_expires_at == expires_at
            assert manifest.root_count == 3
            written.set()
            await release.wait()

        monkeypatch.setattr(projection, "_advance_projection", interrupt_after_write)
        operation = asyncio.create_task(_serve(state, _wire(state, expires_at=expires_at)))
        try:
            await asyncio.wait_for(written.wait(), timeout=10)
            assert await _native_state(state) == before
            if interruption == "cancel":
                operation.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await operation
            else:
                await asyncio.sleep(max(0, (expires_at - dt.datetime.now(dt.UTC)).total_seconds()) + 0.05)
                release.set()
                assert (await asyncio.wait_for(operation, timeout=10)).status == 409
        finally:
            release.set()
            if not operation.done():
                operation.cancel()
                await asyncio.gather(operation, return_exceptions=True)
        assert len(failed_sessions) == 1 and not failed_sessions[0].in_transaction()
        assert await _native_state(state) == before
        monkeypatch.setattr(projection, "_advance_projection", original)
        accepted = await _serve(state, _wire(state))
        assert accepted.status == 200 and json.loads(accepted.body)["after"]["root_count"] == 3
        assert (await _native_state(state))[2] == [3, 0]
