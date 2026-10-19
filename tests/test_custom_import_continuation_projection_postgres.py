# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A limited continued worker uses the owner projection API without gaining DDL."""

import hashlib
import json
from dataclasses import asdict
from types import SimpleNamespace

import httpx
import pytest
from sanic.compat import Header
from sqlalchemy import func, select, text, update

from db.models.custom_import import CustomImportBuildAttempt, CustomImportGeneration, CustomImportLease
from db.models.custom_import_query_projection import CustomImportQueryProjection
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import continuation_projection as service
from process.custom_import import execution as lifecycle
from process.custom_import import query_projection_build as projection
from process.custom_import import query_projection_worker as worker
from process.custom_import.build_continuation import authorize_build_continuation, start_build_continuation
from process.custom_import.build_source import _page_session
from process.custom_import.query_projection_evidence import read_projection_tables
from process.custom_import.query_projection_values import configured_fields
from process.custom_import.runner_registry import load_registry
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_schema
from tests.admission_transport_test_support import writer_tls_context
from tests.custom_import_postgres_support import isolated_publication_case
from tests.custom_import_query_projection_authority_postgres import _reader_roles, _role_session, _winning_bound_build
from tests.test_custom_import_admission_worker import _Stream
from tests.test_custom_import_continuation_postgres import _authorization
from tests.test_custom_import_writer_cutover_postgres import _assert_denied


async def _admitted_graph(case, monkeypatch):
    retained_dict = {}

    async def before_output(_sessions, request, build_id, _capture_bundle_id):
        retained_dict.update(request=request, build_id=build_id)
        raise ConnectionError("synthetic stopped before output")

    with monkeypatch.context() as paused_graph:
        paused_graph.setattr(graph, "_open_output", before_output)
        with pytest.raises(ConnectionError, match="stopped before output"):
            await _winning_bound_build(case)
    return SimpleNamespace(case=case, **retained_dict)


async def _limited_grants(state, role):
    schema = f'"{state.case.schema_name}"'
    async with state.case.engine.begin() as connection:
        for table, columns in (
            ("custom_import_dataset", "dataset_id"),
            ("custom_import_execution", "execution_id"),
            ("custom_import_lease", "execution_id,heartbeat_at,expires_at,updated_at,continuation_started_at"),
            ("custom_import_build_attempt", "build_id"),
        ):
            await connection.execute(text(f'GRANT UPDATE({columns}) ON {schema}.{table} TO "{role}"'))
        family_id = await connection.scalar(
            text(f"SELECT family_id FROM {schema}.custom_import_snapshot_family WHERE execution_id=:execution_id"),
            {"execution_id": state.request.execution_id},
        )
        namespace = f'"{snapshot_schema(family_id)}"'
        await connection.execute(text(f'GRANT USAGE ON SCHEMA {namespace} TO "{role}"'))
        await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA {namespace} TO "{role}"'))
    return family_id


async def _start_limited(state, role):
    nonce = hashlib.sha256(b"synthetic-projection-continuation-nonce").digest()
    async with state.case.sessions() as session, session.begin():
        await session.execute(
            update(CustomImportLease)
            .where(CustomImportLease.execution_id == state.request.execution_id)
            .values(expires_at=func.clock_timestamp() - text("interval '1 second'"))
        )
    document, pins = await _authorization(state.case, state.request, state.build_id, nonce)
    async with state.case.sessions() as session, session.begin():
        await authorize_build_continuation(session, document, **pins)
    async with _role_session(state.case, role) as session, session.begin():
        request = await start_build_continuation(session, document, nonce, lease_seconds=120)
        assert not await session.scalar(
            text(
                "SELECT has_table_privilege(current_user,:snapshot,'UPDATE') OR "
                "has_table_privilege(current_user,:ledger,'UPDATE')"
            ),
            {
                "snapshot": f"{state.case.schema_name}.custom_import_snapshot_family",
                "ledger": f"{state.case.schema_name}.custom_import_build_continuation",
            },
        )
    return request, document


async def _winners(state):
    generation_id = await graph.build_graph(state.case.sessions, state.request, state.build_id)
    async with _page_session(state.case.sessions, state.request, state.build_id) as (session, _build):
        registry = await load_registry(session, state.request)
        generation = await session.get(CustomImportGeneration, generation_id)
        session.expunge(generation)
    await output._attach_families(state.case.sessions, state.request, state.build_id)
    await output._write_winners(state.case.sessions, state.request, registry, state.build_id, generation)
    return generation_id


def _wire(document, nonce, progress):
    return SimpleNamespace(
        method="POST",
        path=service.PROJECTION_PATH,
        query_string="",
        headers=Header({"Content-Type": "application/json", "Authorization": service.wire.encode_lease_bearer(nonce)}),
        body=json.dumps(
            {
                "authorization_id": document["authorization_id"],
                "continuation_id": progress.continuation_id,
                "build_id": document["build_id"],
                "expected_progress": asdict(progress.value),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode(),
    )


async def _transport_projections(state, document, role):
    calls = []

    async def handler(outbound):
        calls.append(outbound)
        request = SimpleNamespace(
            method="POST",
            path=outbound.url.path,
            query_string="",
            headers=Header(list(outbound.headers.multi_items())),
            body=outbound.content,
        )
        reply = await service.serve_continuation_projection(request, state.case.sessions)
        assert reply.status == 200, reply.body
        if reply.status == 200 and len(calls) == 1:
            raise httpx.ReadError("synthetic lost committed projection acknowledgement")
        return httpx.Response(
            reply.status,
            headers=[("Content-Type", reply.content_type), *reply.headers.items()],
            stream=_Stream([reply.body]),
        )

    def sessions():
        return _role_session(state.case, role)

    async with service.ContinuationProjectionTransport(
        expected_origin="https://engine.example.invalid",
        authorization_id=document["authorization_id"],
        continuation_id=state.request.continuation.continuation_id,
        build_id=state.build_id,
        nonce=state.request.lease_token,
        tls_context=writer_tls_context(),
        transport=httpx.MockTransport(handler),
    ) as transport:
        with pytest.raises(worker.ProjectionReconciliationRequired) as reconciled:
            await worker.projection_next_batch(sessions, state.request, state.build_id, transport)
        assert reconciled.value.progress is not None and len(calls) == 1
        await projection.stage_query_projections(sessions, state.request, state.build_id, transport=transport)
    assert len(calls) > 1
    positions = [json.loads(call.content)["expected_progress"] for call in calls]
    assert all(previous != current for previous, current in zip(positions, positions[1:]))
    return calls


async def test_limited_worker_starts_and_uses_current_nonce_owner_projection(monkeypatch):
    async with isolated_publication_case() as case, _reader_roles(case) as (role, _reader):
        state = await _admitted_graph(case, monkeypatch)
        state.family_id = await _limited_grants(state, role)
        state.request, document = await _start_limited(state, role)
        assert configured_fields(state.request.definition)
        generation_id = await _winners(state)

        def sessions():
            return _role_session(case, role)

        with pytest.raises(CandidateRunnerError, match="existing snapshot owner"):
            await projection.projection_step(sessions, state.request, state.build_id)
        observed = await worker._retained_progress(sessions, state.request, state.build_id)
        expected = SimpleNamespace(continuation_id=state.request.continuation.continuation_id, value=observed.progress)
        async with case.sessions() as session:
            before = (await session.get(CustomImportLease, state.request.execution_id)).expires_at
        for changed_document, nonce in (
            (document, hashlib.sha256(b"synthetic-obsolete-nonce").digest()),
            (document | {"authorization_id": "f" * 64}, state.request.lease_token),
        ):
            reply = await service.serve_continuation_projection(_wire(changed_document, nonce, expected), case.sessions)
            assert reply.status == 403
        async with case.sessions() as session:
            assert (await session.get(CustomImportLease, state.request.execution_id)).expires_at == before
            assert await session.get(CustomImportQueryProjection, generation_id) is None
        await _transport_projections(state, document, role)
        async with _role_session(case, role) as session, session.begin():
            for statement in (
                f'UPDATE "{case.schema_name}".custom_import_query_projection SET root_count=0',
                f'CREATE TABLE "{snapshot_schema(state.family_id)}".unexpected (value bigint)',
                f'UPDATE "{snapshot_schema(state.family_id)}".custom_import_derived_root SET field_1=0',
            ):
                await _assert_denied(session, statement)
        sealed = await output.build_output(case.sessions, state.request, state.build_id)
        async with _role_session(case, role) as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            generation = await session.get(CustomImportGeneration, sealed.generation_id)
            tables = await read_projection_tables(
                session, generation, state.request.definition, snapshot_family_id=state.family_id
            )
            assert [await session.scalar(select(func.count()).select_from(table)) for table in tables.values()] == [
                3,
                261,
            ]
