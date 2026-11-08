# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native checkpoint rollback, frozen authority, accounting and ordinary sealing."""

import hashlib
from dataclasses import replace

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportBuildAttempt, CustomImportBuildVerification, CustomImportGenerationSeal
from process.custom_import import build_output as output
from process.custom_import.build_continuation import authorize_build_continuation, start_build_continuation
from process.custom_import.continuation_runtime import continuation_epoch_binding_statement
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_output_postgres import _build, _records, _request_for
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_continuation_postgres import _authorization, _partial_graph
from tests.test_custom_import_staged_verification import _migration


def _install(connection, schema):
    migration = _migration()
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


async def _frozen(case, monkeypatch, *, continuation=False):
    request = await _request_for(case, _records(2, 3))
    if continuation:
        from process.custom_import.build_graph import build_graph

        build_id = await _partial_graph(case, request, monkeypatch)
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(f"""
                    UPDATE "{case.schema_name}".custom_import_lease
                    SET expires_at=clock_timestamp()-interval '1 second' WHERE execution_id=:execution
                """),
                {"execution": request.execution_id},
            )
        nonce = hashlib.sha256(b"synthetic-staged-verification-nonce").digest()
        document, pins = await _authorization(case, request, build_id, nonce)
        async with case.sessions() as session, session.begin():
            await authorize_build_continuation(session, document, **pins)
        async with case.sessions() as session, session.begin():
            request = await start_build_continuation(session, document, nonce, lease_seconds=120)
        generation_id = await build_graph(case.sessions, request, build_id)
    else:
        build_id, generation_id = await _build(case, request)

    def interrupted(*_):
        raise ConnectionError("synthetic pause before verification")

    with monkeypatch.context() as patch:
        patch.setattr(output, "_verify_page", interrupted)
        with pytest.raises(ConnectionError, match="before verification"):
            await output.build_output(case.sessions, request, build_id)
    async with case.sessions() as session:
        assert await session.get(CustomImportBuildVerification, build_id) is None
        assert (await session.get(CustomImportBuildAttempt, build_id)).phase == "verifying"
    return request, build_id, generation_id


async def _state(case, build_id):
    async with case.sessions() as session:
        build = await session.get(CustomImportBuildAttempt, build_id)
        proof = await session.get(CustomImportBuildVerification, build_id)
        return build.phase, None if proof is None else (
            proof.verification_state,
            proof.scan_stage,
            proof.page_sequence,
            proof.verified_at,
        )


async def _step(case, request, build_id):
    async with case.sessions() as session:
        return await session.run_sync(lambda sync: output._verify_page(sync, request, build_id))


async def _native_step(session, case, build_id):
    await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
    return (
        await session.execute(
            text(f'SELECT * FROM "{case.schema_name}".verify_custom_import_build_structure(:build)'),
            {"build": build_id},
        )
    ).one()


async def _insert_proof(session, case, build_id, *, frozen_offset="0 seconds", cursor="NULL"):
    await session.execute(
        text(f"""
        INSERT INTO "{case.schema_name}".custom_import_build_verification
            (build_id,generation_id,source_frozen_at,graph_frozen_at,output_frozen_at,after_capture_stream_slot)
        SELECT build_id,generation_id,source_frozen_at,graph_frozen_at,
            output_frozen_at + interval '{frozen_offset}',{cursor}
        FROM "{case.schema_name}".custom_import_build_attempt WHERE build_id=:build
    """),
        {"build": build_id},
    )


async def test_stages_commit_resume_after_rollback_and_seal_only_at_completion(monkeypatch):
    async with _source_case() as case:
        request, build_id, generation_id = await _frozen(case, monkeypatch)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        assert await _step(case, request, build_id) == "scanning"
        first = await _state(case, build_id)
        assert first == ("verifying", ("scanning", "profiles", 1, None))

        def interrupted(_):
            raise ConnectionError("synthetic uncommitted group")

        with monkeypatch.context() as patch:
            patch.setattr(output, "_require_budget", interrupted)
            with pytest.raises(ConnectionError, match="uncommitted group"):
                await _step(case, request, build_id)
        assert await _state(case, build_id) == first
        with pytest.raises(CandidateRunnerError, match="complete frozen structural proof"):
            await output._terminal_seal(case.sessions, request, build_id, None)
        async with case.sessions() as session:
            assert await session.get(CustomImportGenerationSeal, generation_id) is None

        # A repeated migration preserves new partial progress and existing metadata.
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        for sequence, stage in enumerate(("families", "root_scalars", "child_scalars", "winners"), start=2):
            assert await _step(case, request, build_id) == "scanning"
            assert await _state(case, build_id) == ("verifying", ("scanning", stage, sequence, None))

        async with case.sessions() as session, session.begin():
            with pytest.raises(DBAPIError, match="structure_mismatch"):
                async with session.begin_nested():
                    await session.execute(
                        text(f"""
                        UPDATE "{case.schema_name}".custom_import_build_attempt
                        SET winner_count=winner_count+1 WHERE build_id=:build
                    """),
                        {"build": build_id},
                    )
                    await _native_step(session, case, build_id)
        assert await _state(case, build_id) == ("verifying", ("scanning", "winners", 5, None))
        assert await _step(case, request, build_id) == "complete"
        complete = await _state(case, build_id)
        assert complete[0] == "verified" and complete[1][:3] == ("complete", "complete", 6)
        assert complete[1][3] is not None
        assert await _step(case, request, build_id) == "complete"
        assert await _state(case, build_id) == complete
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id and sealed.no_change is None


async def _reject_proof(case, build_id, offset, cursor, message):
    async with case.sessions() as session, session.begin():
        with pytest.raises(DBAPIError, match=message):
            async with session.begin_nested():
                await _insert_proof(session, case, build_id, frozen_offset=offset, cursor=cursor)
                await _native_step(session, case, build_id)


async def test_old_progress_freeze_and_authority_rejected(monkeypatch):
    async with _source_case() as case:
        request, build_id, _ = await _frozen(case, monkeypatch)
        async with case.engine.begin() as connection:
            with pytest.raises(RuntimeError, match="incompatible_progress"):
                async with connection.begin_nested():
                    await connection.execute(
                        text(f"""
                        INSERT INTO "{case.schema_name}".custom_import_build_verification
                            (build_id,generation_id,source_frozen_at,graph_frozen_at,output_frozen_at,
                             scan_stage,page_sequence)
                        SELECT build_id,generation_id,source_frozen_at,graph_frozen_at,output_frozen_at,'profiles',1
                        FROM "{case.schema_name}".custom_import_build_attempt WHERE build_id=:build
                    """),
                        {"build": build_id},
                    )
                    await connection.run_sync(_install, case.schema_name)
            await connection.run_sync(_install, case.schema_name)

        for offset, cursor, message in (
            ("1 second", "NULL", "structure_mismatch"),
            ("0 seconds", "1", "progress_mismatch"),
        ):
            await _reject_proof(case, build_id, offset, cursor, message)
        with pytest.raises(CandidateRunnerError, match="identity or bounds"):
            await _step(case, replace(request, fence=request.fence + 1), build_id)
        async with case.sessions() as session, session.begin():
            with pytest.raises(DBAPIError, match="lease_lost"):
                async with session.begin_nested():
                    await session.execute(
                        text(f"""
                        UPDATE "{case.schema_name}".custom_import_lease
                        SET expires_at=clock_timestamp()-interval '1 second' WHERE execution_id=:execution
                    """),
                        {"execution": request.execution_id},
                    )
                    await _native_step(session, case, build_id)
        assert await _state(case, build_id) == ("verifying", None)
        assert await _step(case, request, build_id) == "scanning"


async def _reject_epoch(case, request, build_id, nonce):
    async with case.sessions() as session, session.begin():
        with pytest.raises(DBAPIError, match="continuation_lost"):
            async with session.begin_nested():
                if nonce is not None:
                    await session.execute(
                        continuation_epoch_binding_statement(request.continuation.continuation_id, nonce)
                    )
                await _native_step(session, case, build_id)


async def test_staged_continuation_requires_current_epoch_at_each_checkpoint(monkeypatch):
    async with _source_case() as case:
        request, build_id, generation_id = await _frozen(case, monkeypatch, continuation=True)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        assert await _step(case, request, build_id) == "scanning"
        first = await _state(case, build_id)
        for nonce in (None, hashlib.sha256(b"synthetic-obsolete-nonce").digest()):
            await _reject_epoch(case, request, build_id, nonce)
            assert await _state(case, build_id) == first
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id
        state = await _state(case, build_id)
        assert state[0] == "verified" and state[1][:3] == ("complete", "complete", 6)
