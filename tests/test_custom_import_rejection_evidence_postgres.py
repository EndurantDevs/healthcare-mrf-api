# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native retained evidence reads use existing storage locks and original pins."""

from __future__ import annotations

import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from api import control_rejection_evidence as http
from db.models.custom_import import CustomImportRejection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import build_source, execution
from process.custom_import import rejection_evidence as reader
from process.custom_import.storage_layout import snapshot_models, snapshot_schema
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_build_source_postgres import _retained_request, _root


@asynccontextmanager
async def _retained_case():
    async with isolated_publication_case() as case:
        request = await _retained_request(
            case,
            records_by_stream={
                "root_source": [[_root(score=None), _root("1234567893", score=None)]],
                "detail_source": [[]],
            },
        )
        build = await build_source.stage_segmented_source(case.sessions, request)
        async with case.sessions() as session, session.begin():
            family_id = await session.scalar(
                select(CustomImportSnapshotFamily.family_id).where(
                    CustomImportSnapshotFamily.execution_id == request.execution_id
                )
            )
        yield SimpleNamespace(case=case, request=request, build=build, family_id=family_id)


async def _finish(retained):
    async with retained.case.sessions() as session, session.begin():
        transition = await execution.finish_execution(
            session,
            execution_id=retained.request.execution_id,
            fence=retained.request.fence,
            token=retained.request.lease_token,
            terminal_state="failed",
            terminal_reason="candidate_rejected",
        )
        assert transition.changed and transition.state == "failed"


def _pins(retained):
    return dict(
        dataset_id=retained.request.dataset_id,
        execution_id=retained.request.execution_id,
        build_id=retained.build.build_id,
    )


@asynccontextmanager
async def _reader_role(retained):
    role = "rejection_reader_" + uuid.uuid4().hex[:16]
    is_created = False
    try:
        async with retained.case.engine.begin() as connection:
            await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN'))
            control = retained.case.schema_name
            snapshot = snapshot_schema(retained.family_id)
            await connection.execute(text(f'GRANT USAGE ON SCHEMA "{control}","{snapshot}" TO "{role}"'))
            for model in (
                reader.Execution,
                reader.Build,
                reader.Snapshot,
                reader.Definition,
                reader.Schema,
                reader.Binding,
                reader.Capture,
            ):
                await connection.execute(text(f'GRANT SELECT ON "{control}"."{model.__tablename__}" TO "{role}"'))
            await connection.execute(text(f'GRANT SELECT ON "{control}".custom_import_child_collection TO "{role}"'))
            await connection.execute(text(f'GRANT SELECT ON "{control}".custom_import_snapshot_relation TO "{role}"'))
            await connection.execute(text(f'GRANT SELECT ON "{snapshot}".custom_import_rejection TO "{role}"'))
            await connection.execute(
                text(
                    f'GRANT EXECUTE ON FUNCTION "{control}".lock_custom_import_materialization_storage(bigint,boolean) TO "{role}"'
                )
            )
        is_created = True
        yield role
    finally:
        if is_created:
            async with retained.case.engine.begin() as connection:
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


@asynccontextmanager
async def _read_session(case, role):
    async with case.engine.connect() as connection:
        try:
            await connection.execute(text(f'SET ROLE "{role}"'))
            await connection.commit()
            async with AsyncSession(bind=connection, expire_on_commit=False, autoflush=False) as session:
                yield session
        finally:
            await connection.rollback()
            await connection.execute(text("RESET ROLE"))
            await connection.commit()


async def test_terminal_pages_work_with_select_only_retained_access():
    async with _retained_case() as retained:
        async with retained.case.sessions() as session:
            with pytest.raises(reader.RejectionEvidenceNotFinal):
                await reader.read_rejection_page(session, **_pins(retained))
        await _finish(retained)
        async with _reader_role(retained) as role, _read_session(retained.case, role) as session:
            first = await reader.read_rejection_page(session, **_pins(retained), limit=1)
            assert first["scope"]["generation_id"] is None and not first["complete"]
            second = await reader.read_rejection_page(
                session,
                **_pins(retained),
                limit=1,
                after_ordinal=int(first["next_after_ordinal"]),
                scope_sha256=first["scope_sha256"],
            )
            assert second["complete"] and second["next_after_ordinal"] is None
            assert first["scope_sha256"] == second["scope_sha256"]
            assert first["events"][0]["logical_key"] == {"npi": {"type": "string", "value": "1003000126"}}
            assert second["events"][0]["logical_key"] == {"npi": {"type": "string", "value": "1234567893"}}
            assert all(
                event["offending_value_status"] == "not_retained" for event in first["events"] + second["events"]
            )
            empty = await reader.read_rejection_page(
                session,
                **_pins(retained),
                after_ordinal=int(second["events"][-1]["ordinal"]),
                scope_sha256=second["scope_sha256"],
            )
            assert empty["complete"] and empty["events"] == [] and empty["next_after_ordinal"] is None
            async with session.begin():
                writable = await session.scalar(
                    text("SELECT has_table_privilege(current_user,:relation,'INSERT')"),
                    {"relation": f"{snapshot_schema(retained.family_id)}.custom_import_rejection"},
                )
                assert writable is False


async def test_retained_read_pin_blocks_leaf_replacement():
    async with _retained_case() as retained:
        await _finish(retained)
        async with retained.case.sessions() as session, session.begin():
            await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
            scope = await reader._resolve_scope(session, **_pins(retained))
            assert scope.family_id == retained.family_id
            with pytest.raises(DBAPIError, match="lock timeout"):
                async with retained.case.engine.begin() as writer:
                    await writer.execute(text("SET LOCAL lock_timeout = '100ms'"))
                    await writer.execute(
                        text(
                            f'ALTER TABLE "{snapshot_schema(retained.family_id)}".custom_import_rejection RENAME TO other'
                        )
                    )
            rejections, exhausted = await reader._page_rows(session, scope, -1, 100)
            assert len(rejections) == 2 and exhausted


async def test_wrong_owner_or_build_never_reads_a_retained_leaf():
    async with _retained_case() as retained:
        await _finish(retained)
        async with retained.case.sessions() as session:
            for overrides in (
                {"dataset_id": retained.request.dataset_id + 100},
                {"build_id": retained.build.build_id + 100},
            ):
                with pytest.raises(reader.RejectionEvidenceUnavailable):
                    await reader.read_rejection_page(session, **(_pins(retained) | overrides))
            page = await reader.read_rejection_page(session, **_pins(retained))
            assert page["complete"] and page["returned_rejection_events"] == 2
        async with retained.case.sessions() as session:
            model = snapshot_models(retained.family_id)[CustomImportRejection]
            ordinals = list(
                (await session.scalars(select(model.rejection_ordinal).order_by(model.rejection_ordinal))).all()
            )
            assert [int(event["ordinal"]) for event in page["events"]] == ordinals


async def _replacement_leaf(retained):
    namespace = snapshot_schema(retained.family_id)
    async with retained.case.engine.begin() as writer:
        await writer.execute(
            text(f'CREATE TABLE "{namespace}".replacement (LIKE "{namespace}".custom_import_rejection INCLUDING ALL)')
        )
        await writer.execute(
            text(f'INSERT INTO "{namespace}".replacement SELECT * FROM "{namespace}".custom_import_rejection')
        )
        await writer.execute(text(f"UPDATE \"{namespace}\".replacement SET code='replacement_marker'"))
        return await writer.scalar(
            text("SELECT to_regclass(:name)::oid::bigint"), {"name": f"{namespace}.custom_import_rejection"}
        )


async def _swap_leaf(retained):
    namespace = snapshot_schema(retained.family_id)
    async with retained.case.engine.begin() as writer:
        await writer.execute(text("SET LOCAL lock_timeout = '1000ms'"))
        await writer.execute(text(f'ALTER TABLE "{namespace}".custom_import_rejection RENAME TO original_rejection'))
        await writer.execute(text(f'ALTER TABLE "{namespace}".replacement RENAME TO custom_import_rejection'))


async def test_pre_acquisition_replacement_is_not_retained_evidence(monkeypatch):
    async with _retained_case() as retained:
        await _finish(retained)
        original_oid = await _replacement_leaf(retained)
        lock_snapshot = reader._lock_snapshot
        swaps = []

        async def replace_before_lock(session, family_id):
            await _swap_leaf(retained)
            # Explicit catalog reads retain the earlier transaction snapshot.
            catalog_name = await session.scalar(
                text("SELECT relname FROM pg_class WHERE oid=:oid"), {"oid": original_oid}
            )
            assert catalog_name == "custom_import_rejection"
            swaps.append(True)
            return await lock_snapshot(session, family_id)

        monkeypatch.setattr(reader, "_lock_snapshot", replace_before_lock)
        async with retained.case.sessions() as session:
            with pytest.raises(reader.RejectionEvidenceUnavailable):
                await reader.read_rejection_page(session, **_pins(retained))
        assert swaps == [True]


@asynccontextmanager
async def _replacement_namespace(retained, *, empty=False):
    namespace = snapshot_schema(retained.family_id)
    alternate = "replacement_" + uuid.uuid4().hex[:16]
    original = "original_" + uuid.uuid4().hex[:16]
    is_created = False
    try:
        async with retained.case.engine.begin() as writer:
            await writer.execute(text(f'CREATE SCHEMA "{alternate}"'))
            await writer.execute(
                text(
                    f'CREATE TABLE "{alternate}".custom_import_rejection '
                    f'(LIKE "{namespace}".custom_import_rejection INCLUDING ALL)'
                )
            )
            if not empty:
                await writer.execute(
                    text(
                        f'INSERT INTO "{alternate}".custom_import_rejection '
                        f'SELECT * FROM "{namespace}".custom_import_rejection'
                    )
                )
                await writer.execute(
                    text(f"UPDATE \"{alternate}\".custom_import_rejection SET code='replacement_marker'")
                )
        is_created = True
        yield alternate, original
    finally:
        if is_created:
            async with retained.case.engine.begin() as writer:
                await writer.execute(text(f'DROP SCHEMA IF EXISTS "{original}" CASCADE'))
                await writer.execute(text(f'DROP SCHEMA IF EXISTS "{alternate}" CASCADE'))


async def _swap_namespace(retained, names):
    alternate, original = names
    namespace = snapshot_schema(retained.family_id)
    async with retained.case.engine.begin() as writer:
        await writer.execute(text("SET LOCAL lock_timeout = '1000ms'"))
        await writer.execute(text(f'ALTER SCHEMA "{namespace}" RENAME TO "{original}"'))
        await writer.execute(text(f'ALTER SCHEMA "{alternate}" RENAME TO "{namespace}"'))


@pytest.mark.parametrize("empty", [False, True])
async def test_post_verification_namespace_swap_is_rejected(monkeypatch, empty):
    async with _retained_case() as retained:
        await _finish(retained)
        async with _replacement_namespace(retained, empty=empty) as names:
            verified = reader._verify_relation_locks
            swaps = []

            async def swap_after_verification(session, quoted_schema, family_id):
                result = await verified(session, quoted_schema, family_id)
                await _swap_namespace(retained, names)
                swaps.append(True)
                return result

            monkeypatch.setattr(reader, "_verify_relation_locks", swap_after_verification)
            async with retained.case.sessions() as session:
                with pytest.raises(reader.RejectionEvidenceUnavailable):
                    await reader.read_rejection_page(session, **_pins(retained))
            assert swaps == [True]


@pytest.mark.parametrize("empty", [False, True])
async def test_namespace_swap_between_headers_and_hydration_is_rejected(monkeypatch, empty):
    async with _retained_case() as retained:
        await _finish(retained)
        async with _replacement_namespace(retained, empty=empty) as names:
            page_headers = reader._page_headers
            swaps = []

            async def swap_after_headers(session, scope, leaf, after, limit):
                headers = await page_headers(session, scope, leaf, after, limit)
                assert len(headers) == 2
                await _swap_namespace(retained, names)
                swaps.append(True)
                return headers

            monkeypatch.setattr(reader, "_page_headers", swap_after_headers)
            async with retained.case.sessions() as session:
                with pytest.raises(reader.RejectionEvidenceUnavailable):
                    await reader.read_rejection_page(session, **_pins(retained))
            assert swaps == [True]


async def _http_request(retained):
    async with retained.case.sessions() as session:
        key = await session.scalar(
            select(reader.Execution.idempotency_key).where(
                reader.Execution.execution_id == retained.request.execution_id
            )
        )
    request_dict = {
        "dataset_id": retained.request.dataset_id,
        "definition_revision_id": retained.request.definition_revision_id,
        "idempotency_key": key,
        "build_id": None,
        "after_ordinal": None,
        "scope_sha256": None,
        "limit": 1,
    }
    request = SimpleNamespace(
        headers={"Authorization": "Bearer synthetic-rejection-http", "Content-Type": "application/json"},
        body=json.dumps(request_dict).encode(),
        query_string="",
    )
    return request, request_dict


def _http_token(monkeypatch):
    monkeypatch.setenv(http._TOKEN_ENV, "synthetic-rejection-http")
    for name in http._OTHER_TOKEN_ENVS:
        monkeypatch.delenv(name, raising=False)


async def test_http_pages_resolve_the_terminal_build_with_select_only_access(monkeypatch):
    _http_token(monkeypatch)
    async with _retained_case() as retained:
        request, document = await _http_request(retained)
        async with retained.case.sessions() as session:
            pending = await http.serve_rejection_evidence(request, session)
            assert pending.status == 409 and json.loads(pending.body) == {"error": "evidence_not_final"}
        await _finish(retained)
        async with _reader_role(retained) as role, _read_session(retained.case, role) as session:
            first = await http.serve_rejection_evidence(request, session)
            assert first.status == 200 and first.headers["Cache-Control"] == "no-store"
            page = json.loads(first.body)
            assert page["scope"]["build_id"] == str(retained.build.build_id)
            assert page["scope"]["execution_id"] == str(retained.request.execution_id)
            assert page["scope"]["generation_id"] is None and not page["complete"]
            assert not session.in_transaction() and len(first.body) <= http._MAX_PAGE_BYTES
            request.body = json.dumps(
                document
                | {
                    "build_id": retained.build.build_id,
                    "after_ordinal": int(page["next_after_ordinal"]),
                    "scope_sha256": page["scope_sha256"],
                }
            ).encode()
            last = await http.serve_rejection_evidence(request, session)
            final = json.loads(last.body)
            assert last.status == 200 and final["complete"] and final["next_after_ordinal"] is None
            assert final["scope"] == page["scope"] and final["scope_sha256"] == page["scope_sha256"]
            assert [event["logical_key"]["npi"]["value"] for event in page["events"] + final["events"]] == [
                "1003000126",
                "1234567893",
            ]
            request.body = json.dumps(document | {"dataset_id": retained.request.dataset_id + 100}).encode()
            denied = await http.serve_rejection_evidence(request, session)
            assert denied.status == 404 and json.loads(denied.body) == {"error": "not_found"}
            assert not session.in_transaction()


async def test_http_does_not_replace_missing_read_lock_privilege(monkeypatch):
    _http_token(monkeypatch)
    async with _retained_case() as retained:
        await _finish(retained)
        request, _document = await _http_request(retained)
        async with _reader_role(retained) as role:
            async with retained.case.engine.begin() as connection:
                await connection.execute(
                    text(
                        f'REVOKE EXECUTE ON FUNCTION "{retained.case.schema_name}".'
                        f'lock_custom_import_materialization_storage(bigint,boolean) FROM "{role}"'
                    )
                )
            async with _read_session(retained.case, role) as session:
                unavailable = await http.serve_rejection_evidence(request, session)
                assert unavailable.status == 503 and json.loads(unavailable.body) == {"error": "evidence_unavailable"}
                assert unavailable.headers["Cache-Control"] == "no-store" and not session.in_transaction()
