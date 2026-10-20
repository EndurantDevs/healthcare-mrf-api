# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""A recorded operator decision resumes unchanged accounting, materialization and sealing."""

import json

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process.custom_import import build_output as output
from process.custom_import.continuation_runtime import continuation_epoch_binding_statement
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_manual_acceptance import _migration
from tests.test_custom_import_staged_verification_postgres import _frozen as _frozen_continuation
from tests.test_custom_import_staged_verification_postgres import _install as _install_staged
from tests.test_custom_import_staged_verification_postgres import _state, _step
from tests.test_custom_import_verification_arms_postgres import _install as _install_arms
from tests.test_custom_import_verification_root_ranges_postgres import _frozen, _install_parent
from tests.test_custom_import_verification_root_ranges_postgres import _install as _install_ranges


def _install(connection, schema):
    migration = _migration()
    migration._schema = lambda: schema
    migration.op = Operations(MigrationContext.configure(connection))
    migration.upgrade()


async def _ready(case, monkeypatch, *, continuation=False):
    if continuation:
        request, build_id, generation_id = await _frozen_continuation(case, monkeypatch, continuation=True)
        async with case.engine.begin() as connection:
            await connection.run_sync(_install_staged, case.schema_name)
            await connection.run_sync(_install_arms, case.schema_name)
        for _ in range(40):
            assert await _step(case, request, build_id) == "scanning"
    else:
        request, build_id, generation_id = await _frozen(case, monkeypatch)
    async with case.engine.begin() as connection:
        await connection.run_sync(_install_ranges, case.schema_name, 16_384)
        before = await connection.scalar(
            text("SELECT to_jsonb(p)-'prosrc' FROM pg_proc p WHERE oid=to_regprocedure(:identity)"),
            {"identity": f'"{case.schema_name}".guard_custom_import_build_owned()'},
        )
        await connection.run_sync(_install, case.schema_name)
        after = await connection.scalar(
            text("SELECT to_jsonb(p)-'prosrc' FROM pg_proc p WHERE oid=to_regprocedure(:identity)"),
            {"identity": f'"{case.schema_name}".guard_custom_import_build_owned()'},
        )
        assert before == after
    return request, build_id, generation_id


def _acceptance_update(schema):
    cursors = _migration()._ranges()._arms()._staged()._CURSORS
    assignments = ",".join(name + "=NULL" for name in cursors)
    return (
        f'UPDATE "{schema}".custom_import_build_verification SET '
        "verification_contract='custom-import/manual-acceptance/v1',scan_stage='winners',page_sequence=61,"
        f"manual_acceptance=CAST(:approval AS jsonb),{assignments} WHERE build_id=:build"
    )


async def _accept(session, case, build_id, *, statement=None, approval=None, request=None):
    await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
    if request is not None and request.continuation is not None:
        await session.execute(
            continuation_epoch_binding_statement(
                request.continuation.continuation_id, lease_token_sha256(request.lease_token)
            )
        )
    await session.execute(
        text(statement or _acceptance_update(case.schema_name)),
        {
            "build": build_id,
            "approval": json.dumps(approval or {"request_ref": "synthetic-approval", "reason": "accepted for review"}),
        },
    )


async def _proof(case, build_id):
    async with case.sessions() as session:
        return await session.scalar(
            text(
                f'SELECT to_jsonb(v) FROM "{case.schema_name}".custom_import_build_verification v WHERE build_id=:build'
            ),
            {"build": build_id},
        )


@pytest.mark.parametrize("continuation", (False, True))
async def test_manual_acceptance_resumes_unmodified_accounting_materialization_and_seal(monkeypatch, continuation):
    async with _source_case() as case:
        request, build_id, generation_id = await _ready(case, monkeypatch, continuation=continuation)
        assert await _step(case, request, build_id) == "scanning"
        original = await _proof(case, build_id)
        assert original["after_root_record_id"] is not None
        async with case.sessions() as session, session.begin():
            await _accept(session, case, build_id, request=request)
        accepted = await _proof(case, build_id)
        evidence = accepted["manual_acceptance"]
        assert evidence["previous_proof"] == original
        assert evidence["waived_ordinals"] == list(range(41, 62))
        assert evidence["frozen_build"]["build_id"] == build_id
        assert evidence["accepted_by"] and evidence["accepted_at"]
        assert (await _state(case, build_id)) == ("verifying", ("scanning", "winners", 61, None))
        assert accepted["root_count"] == accepted["winner_count"] == 0
        with pytest.raises(CandidateRunnerError, match="complete frozen structural proof"):
            await output._terminal_seal(case.sessions, request, build_id, None)
        assert await _step(case, request, build_id) == "complete"
        complete = await _proof(case, build_id)
        assert complete["verification_contract"] == _migration()._MANUAL
        assert complete["page_sequence"] == 62 and complete["manual_acceptance"] == evidence
        assert (complete["root_count"], complete["family_child_count"]) == ((2, 4) if continuation else (17, 18))
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id and sealed.no_change is None
        async with case.sessions() as session, session.begin():
            with pytest.raises(DBAPIError, match="manual_acceptance_immutable"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f"UPDATE \"{case.schema_name}\".custom_import_build_verification SET manual_acceptance='{{}}'::jsonb WHERE build_id=:build"
                        ),
                        {"build": build_id},
                    )
        assert (await _proof(case, build_id))["manual_acceptance"] == evidence


async def _reject_decision(session, case, build_id, statement, approval):
    with pytest.raises(DBAPIError, match="manual_acceptance_invalid"):
        async with session.begin_nested():
            await _accept(session, case, build_id, statement=statement, approval=approval)


async def test_forged_decisions_rollback_and_accounting_failure_preserve_checkpoint(monkeypatch):
    async with _source_case() as case:
        request, build_id, _ = await _ready(case, monkeypatch)
        original = await _proof(case, build_id)
        update = _acceptance_update(case.schema_name)
        async with case.sessions() as session, session.begin():
            for statement, approval in (
                (update.replace("page_sequence=61", "page_sequence=60"), None),
                (update.replace("page_sequence=61", "page_sequence=61,winner_count=1"), None),
                (update, {"request_ref": "synthetic", "reason": "review", "accepted_by": "forged"}),
            ):
                await _reject_decision(session, case, build_id, statement, approval)
            with pytest.raises(RuntimeError, match="synthetic rollback"):
                async with session.begin_nested():
                    await _accept(session, case, build_id)
                    raise RuntimeError("synthetic rollback")
        assert await _proof(case, build_id) == original
        async with case.sessions() as session, session.begin():
            await _accept(session, case, build_id)
            with pytest.raises(DBAPIError, match="manual_acceptance_immutable"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'UPDATE "{case.schema_name}".custom_import_build_verification SET manual_acceptance=NULL WHERE build_id=:build'
                        ),
                        {"build": build_id},
                    )
            with pytest.raises(DBAPIError, match="structure_mismatch"):
                async with session.begin_nested():
                    await session.execute(
                        text(
                            f'UPDATE "{case.schema_name}".custom_import_build_attempt SET winner_count=winner_count+1 WHERE build_id=:build'
                        ),
                        {"build": build_id},
                    )
                    await session.execute(
                        text(f'SELECT * FROM "{case.schema_name}".verify_custom_import_build_structure(:build)'),
                        {"build": build_id},
                    )
        assert (await _state(case, build_id))[1][:3] == ("scanning", "winners", 61)
        assert await _step(case, request, build_id) == "complete"


async def test_unprivileged_session_cannot_borrow_definer_owner_to_accept(monkeypatch):
    async with _source_case() as case:
        _request, build_id, _ = await _ready(case, monkeypatch)
        original = await _proof(case, build_id)
        statement = (
            _acceptance_update(case.schema_name)
            .replace(":build", str(build_id))
            .replace("CAST(:approval AS jsonb)", '\'{"request_ref":"synthetic","reason":"review"}\'::jsonb')
        )
        async with case.sessions() as session, session.begin():
            await session.execute(
                text(
                    f'CREATE FUNCTION "{case.schema_name}".synthetic_accept() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN {statement}; END $$'
                )
            )
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO pg_signal_backend'))
            await session.execute(
                text(f'GRANT EXECUTE ON FUNCTION "{case.schema_name}".synthetic_accept() TO pg_signal_backend')
            )
            with pytest.raises(DBAPIError, match="manual_acceptance_invalid"):
                async with session.begin_nested():
                    await session.execute(text("SET LOCAL SESSION AUTHORIZATION pg_signal_backend"))
                    await session.execute(text(f'SELECT "{case.schema_name}".synthetic_accept()'))
        assert await _proof(case, build_id) == original


async def test_ordinary_automated_path_remains_complete_without_approval(monkeypatch):
    async with _source_case() as case:
        request, build_id, generation_id = await _ready(case, monkeypatch)
        sealed = await output.build_output(case.sessions, request, build_id)
        proof = await _proof(case, build_id)
        assert sealed.generation_id == generation_id
        assert proof["verification_contract"] == _migration()._NORMAL
        assert proof["verification_state"] == "complete" and proof["page_sequence"] == 62
        assert proof["manual_acceptance"] is None


async def _reject_verifier_metadata_drift(connection, schema):
    identity = f'"{schema}".verify_custom_import_build_structure(bigint)'
    catalog_query = text("SELECT to_jsonb(p) FROM pg_proc p WHERE oid=to_regprocedure(:identity)")
    original = await connection.scalar(catalog_query, {"identity": identity})
    for installer, arguments in ((_install_parent, (16_384,)), (_install_ranges, (16_384,)), (_install, ())):
        for statement in (
            f"ALTER FUNCTION {identity} SECURITY INVOKER",
            f"ALTER FUNCTION {identity} SET search_path=public",
            f"GRANT EXECUTE ON FUNCTION {identity} TO PUBLIC",
        ):
            with pytest.raises(RuntimeError, match="custom_import_continuation_function_identity_mismatch"):
                async with connection.begin_nested():
                    await connection.execute(text(statement))
                    await connection.run_sync(installer, schema, *arguments)
            assert await connection.scalar(catalog_query, {"identity": identity}) == original


async def test_normal_forward_upgrade_preserves_temporary_manual_checkpoint(monkeypatch):
    async with _source_case() as case:
        request, build_id, generation_id = await _ready(case, monkeypatch)
        async with case.sessions() as session, session.begin():
            await _accept(session, case, build_id)
        accepted = await _proof(case, build_id)
        async with case.engine.begin() as connection:
            await _reject_verifier_metadata_drift(connection, case.schema_name)
            await connection.run_sync(_install_parent, case.schema_name, 16_384)
            await connection.run_sync(_install_ranges, case.schema_name, 16_384)
            await connection.run_sync(_install, case.schema_name)
            with pytest.raises(RuntimeError, match="guard_identity_mismatch"):
                async with connection.begin_nested():
                    await connection.execute(
                        text(
                            f'ALTER TABLE "{case.schema_name}".custom_import_build_verification DROP CONSTRAINT custom_import_build_verification_acceptance'
                        )
                    )
                    await connection.execute(
                        text(
                            f'ALTER TABLE "{case.schema_name}".custom_import_build_verification ADD CONSTRAINT custom_import_build_verification_acceptance CHECK (true)'
                        )
                    )
                    await connection.run_sync(_install, case.schema_name)
        assert await _proof(case, build_id) == accepted
        assert await _step(case, request, build_id) == "complete"
        sealed = await output.build_output(case.sessions, request, build_id)
        assert sealed.generation_id == generation_id
