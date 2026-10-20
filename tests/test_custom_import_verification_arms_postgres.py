# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native arm checkpoints preserve rollback, authority and terminal sealing."""

import hashlib
import re

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_output as output
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_staged_verification_postgres import (
    _frozen,
    _insert_proof,
    _native_step,
    _reject_epoch,
    _reject_proof,
    _state,
    _step,
)
from tests.test_custom_import_staged_verification_postgres import _install as _install_staged
from tests.test_custom_import_verification_arms import _migration


def _install(connection, schema):
    migration = _migration()
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


def _install_intermediate(connection, schema, body_kind):
    migration = _migration()
    migration._schema = lambda: schema
    migration._body = getattr(migration, f"_{body_kind}_body")
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


async def _metadata(connection, schema):
    return await connection.scalar(
        text("SELECT to_jsonb(p)-'prosrc' FROM pg_proc p WHERE oid=to_regprocedure(:identity)"),
        {"identity": f'"{schema}".verify_custom_import_build_structure(bigint)'},
    )


async def test_each_arm_commits_and_rollback_resumes_without_early_seal(monkeypatch):
    async with _source_case() as case:
        request, build_id, generation_id = await _frozen(case, monkeypatch)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
            metadata = await _metadata(connection, case.schema_name)
            await connection.run_sync(_install, case.schema_name)
            assert await _metadata(connection, case.schema_name) == metadata
        assert await _step(case, request, build_id) == "scanning"
        first = await _state(case, build_id)
        assert first == ("verifying", ("scanning", "capture", 1, None))

        def interrupted(_):
            raise ConnectionError("synthetic uncommitted arm")

        with monkeypatch.context() as patch:
            patch.setattr(output, "_require_budget", interrupted)
            with pytest.raises(ConnectionError, match="uncommitted arm"):
                await _step(case, request, build_id)
        assert await _state(case, build_id) == first
        with pytest.raises(CandidateRunnerError, match="complete frozen structural proof"):
            await output._terminal_seal(case.sessions, request, build_id, None)

        # A later ordinary upgrade recognizes the exact body and keeps its progress.
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
            assert await _metadata(connection, case.schema_name) == metadata
        assert await _state(case, build_id) == first
        stages = _migration()._STAGES
        for sequence in range(2, len(stages)):
            assert await _step(case, request, build_id) == "scanning"
            assert await _state(case, build_id) == ("verifying", ("scanning", stages[sequence], sequence, None))
        await _reject_counts(case, build_id)
        assert await _state(case, build_id) == ("verifying", ("scanning", "winners", len(stages) - 1, None))
        assert await _step(case, request, build_id) == "complete"
        complete = await _state(case, build_id)
        assert complete[0] == "verified" and complete[1][:3] == ("complete", "complete", len(stages))
        assert complete[1][3] is not None
        assert await _step(case, request, build_id) == "complete"
        assert await _state(case, build_id) == complete
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        assert await _state(case, build_id) == complete
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id and sealed.no_change is None


async def _reject_counts(case, build_id):
    async with case.sessions() as session, session.begin():
        with pytest.raises(DBAPIError, match="structure_mismatch"):
            async with session.begin_nested():
                await session.execute(
                    text(
                        f'UPDATE "{case.schema_name}".custom_import_build_attempt '
                        "SET winner_count=winner_count+1 WHERE build_id=:build"
                    ),
                    {"build": build_id},
                )
                await _native_step(session, case, build_id)


@pytest.mark.parametrize("body_kind,completed", (("checkpoint", 39), ("plan", 39), ("resource", 40)))
async def test_intermediate_partial_progress_survives_normal_upgrade(monkeypatch, body_kind, completed):
    async with _source_case() as case:
        request, build_id, generation_id = await _frozen(case, monkeypatch)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
            await connection.run_sync(_install_intermediate, case.schema_name, body_kind)
            metadata = await _metadata(connection, case.schema_name)
        for _ in range(completed):
            assert await _step(case, request, build_id) == "scanning"
        before = await _state(case, build_id)
        assert before == ("verifying", ("scanning", "profiles", completed, None))
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
            assert await _metadata(connection, case.schema_name) == metadata
        assert await _state(case, build_id) == before
        assert await _step(case, request, build_id) == "scanning"
        assert await _state(case, build_id) == ("verifying", ("scanning", "profiles", completed + 1, None))
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id
        state = await _state(case, build_id)
        assert state[0] == "verified" and state[1][:3] == ("complete", "complete", len(_migration()._STAGES))


async def test_old_noninitial_progress_is_refused_but_initial_progress_is_preserved(monkeypatch):
    async with _source_case() as case:
        request, build_id, _ = await _frozen(case, monkeypatch)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
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
        async with case.sessions() as session, session.begin():
            await _insert_proof(session, case, build_id)
        initial = await _state(case, build_id)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install, case.schema_name)
        assert await _state(case, build_id) == initial
        assert await _step(case, request, build_id) == "scanning"


async def test_arm_progress_rejects_wrong_stage_frozen_identity_and_old_cursors(monkeypatch):
    async with _source_case() as case:
        request, build_id, _ = await _frozen(case, monkeypatch)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
            await connection.run_sync(_install, case.schema_name)
        for offset, cursor, message in (
            ("1 second", "NULL", "structure_mismatch"),
            ("0 seconds", "1", "progress_mismatch"),
        ):
            await _reject_proof(case, build_id, offset, cursor, message)
        async with case.sessions() as session, session.begin():
            with pytest.raises(DBAPIError, match="progress_mismatch"):
                async with session.begin_nested():
                    await _insert_proof(session, case, build_id)
                    await session.execute(
                        text(
                            f'UPDATE "{case.schema_name}".custom_import_build_verification '
                            "SET scan_stage='profiles',page_sequence=1 WHERE build_id=:build"
                        ),
                        {"build": build_id},
                    )
                    await _native_step(session, case, build_id)
        assert await _state(case, build_id) == ("verifying", None)
        assert await _step(case, request, build_id) == "scanning"


async def test_later_arm_exposes_named_failure_columns(monkeypatch):
    async with _source_case() as case:
        request, _build_id, generation_id = await _frozen(case, monkeypatch)
        migration = _migration()
        staged = migration._staged()
        bulk = staged._finality()._bulk()
        storage = bulk._storage()
        async with case.sessions() as session:
            family_id = await session.scalar(
                text(
                    f'SELECT family_id FROM "{case.schema_name}".custom_import_snapshot_family '
                    "WHERE generation_id=:generation"
                ),
                {"generation": generation_id},
            )
            query = bulk._control_sql(storage, case.schema_name, migration._queries(staged)[5])
            query = query.replace("__CANDIDATE__", storage._quote(f"ci_snapshot_{family_id}"))
            query = query.replace("__BASE__", storage._quote(case.schema_name))
            query = re.sub(r"\$([1-9])", r":p\1", query)
            failure_row = (
                await session.execute(
                    text(query),
                    {
                        "p2": request.dataset_id + 1,
                        "p5": request.execution_id,
                        "p7": request.fence,
                    },
                )
            ).one()
            assert failure_row._mapping["failure_code"] == "root_dictionary_owner"
            assert type(failure_row._mapping["row_identity"]) is int


async def test_continuation_current_epoch_is_required_after_arm_checkpoint(monkeypatch):
    async with _source_case() as case:
        request, build_id, generation_id = await _frozen(case, monkeypatch, continuation=True)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
            await connection.run_sync(_install, case.schema_name)
        assert await _step(case, request, build_id) == "scanning"
        first = await _state(case, build_id)
        for nonce in (None, hashlib.sha256(b"synthetic-obsolete-arm-nonce").digest()):
            await _reject_epoch(case, request, build_id, nonce)
            assert await _state(case, build_id) == first
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id
        state = await _state(case, build_id)
        assert state[0] == "verified" and state[1][:3] == ("complete", "complete", len(_migration()._STAGES))
