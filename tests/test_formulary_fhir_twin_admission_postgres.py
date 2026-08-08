# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proof for FHIR formulary twin evidence gates."""

from __future__ import annotations

import asyncio
import uuid

import asyncpg
import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from tests.formulary_fhir_twin_admission_pg_support import ADMISSION_PATH
from tests.formulary_fhir_twin_admission_pg_support import admission_insert
from tests.formulary_fhir_twin_admission_pg_support import assert_invalid_pointer_writes
from tests.formulary_fhir_twin_admission_pg_support import assert_sqlstate
from tests.formulary_fhir_twin_admission_pg_support import ATTEMPT_PATH
from tests.formulary_fhir_twin_admission_pg_support import attempt_insert
from tests.formulary_fhir_twin_admission_pg_support import connect
from tests.formulary_fhir_twin_admission_pg_support import database_url
from tests.formulary_fhir_twin_admission_pg_support import drop_schema
from tests.formulary_fhir_twin_admission_pg_support import FOUNDATION_PATH
from tests.formulary_fhir_twin_admission_pg_support import GUARDS_PATH
from tests.formulary_fhir_twin_admission_pg_support import HASHES
from tests.formulary_fhir_twin_admission_pg_support import load_migration
from tests.formulary_fhir_twin_admission_pg_support import quoted
from tests.formulary_fhir_twin_admission_pg_support import run_migration
from tests.formulary_fhir_twin_admission_pg_support import seed_content_graph
from tests.formulary_fhir_twin_admission_pg_support import seed_datasets
from tests.formulary_fhir_twin_admission_pg_support import set_source_enabled


async def _assert_catalog(connection: asyncpg.Connection, schema_name: str) -> None:
    expected_functions = {
        "guard_fhir_formulary_twin_attempt_insert",
        "guard_fhir_formulary_twin_attempt_immutable",
        "guard_fhir_formulary_twin_admission_insert",
        "guard_fhir_formulary_twin_admission_immutable",
        "guard_fhir_formulary_current_twin_admission",
        "assert_fhir_formulary_current_published",
        "guard_fhir_formulary_twin_dataset",
        "guard_fhir_formulary_current_source",
        "guard_fhir_formulary_cow_immutable",
        "guard_fhir_formulary_build_owner_insert",
        "guard_fhir_formulary_alias_content_insert",
    }
    routine_records = await connection.fetch(
        "SELECT routine.proname, routine.prosecdef, routine.proconfig, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') AS public_execute "
        "FROM pg_proc AS routine JOIN pg_namespace AS namespace "
        "ON namespace.oid = routine.pronamespace WHERE namespace.nspname = $1",
        schema_name,
    )
    guarded_routines = [
        routine_record
        for routine_record in routine_records
        if routine_record["proname"] in expected_functions
    ]
    assert {
        routine_record["proname"] for routine_record in guarded_routines
    } == expected_functions
    assert all(routine_record["prosecdef"] for routine_record in guarded_routines)
    assert all(
        routine_record["proconfig"] == ["search_path=pg_catalog"]
        for routine_record in guarded_routines
    )
    assert all(
        routine_record["public_execute"] is False
        for routine_record in guarded_routines
    )
    deferred_trigger_record = await connection.fetchrow(
        "SELECT trigger.tgdeferrable, trigger.tginitdeferred "
        "FROM pg_trigger AS trigger JOIN pg_class AS relation "
        "ON relation.oid = trigger.tgrelid JOIN pg_namespace AS namespace "
        "ON namespace.oid = relation.relnamespace WHERE namespace.nspname = $1 "
        "AND trigger.tgname = 'fhir_formulary_current_published_guard'",
        schema_name,
    )
    assert dict(deferred_trigger_record) == {
        "tgdeferrable": True,
        "tginitdeferred": True,
    }


async def _assert_failed_guard_upgrade(
    engine,
    guards,
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    with pytest.raises(Exception, match="fhir_formulary_preexisting_current_invalid"):
        await run_migration(engine, guards, "upgrade")
    assert await connection.fetchval(
        "SELECT to_regprocedure($1)",
        f"{schema_name}.guard_fhir_formulary_current_twin_admission()",
    ) is None


async def _assert_safe_chain_and_preflight(
    engine,
    attempt,
    admission,
    guards,
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    await run_migration(engine, guards, "downgrade")
    await run_migration(engine, admission, "downgrade")
    await run_migration(engine, attempt, "downgrade")
    assert await connection.fetchval(
        "SELECT to_regclass($1)", f"{schema_name}.fhir_formulary_twin_attempt"
    ) is None
    await run_migration(engine, attempt, "upgrade")
    await run_migration(engine, admission, "upgrade")
    invalid_pointers = (
        ("source-none", "none-current", "transaction_timestamp()"),
        ("source-building", "building-current", "transaction_timestamp()"),
        ("source-failed", "failed-current", "transaction_timestamp()"),
        ("source-ordinary", "ordinary-current", "transaction_timestamp()"),
        ("source-zero-seed", "zero-seed", "transaction_timestamp()"),
        ("source-real-published", "real-published-seed", "'2026-08-08T00:03Z'"),
    )
    for source_id, dataset_id, published_at in invalid_pointers:
        await connection.execute(
            f"INSERT INTO {schema}.fhir_formulary_current "
            f"(source_id, dataset_id, generation, published_at) VALUES "
            f"('{source_id}', '{dataset_id}', 1, {published_at})"
        )
        await _assert_failed_guard_upgrade(engine, guards, connection, schema_name)
        await connection.execute(
            f"DELETE FROM {schema}.fhir_formulary_current WHERE source_id = '{source_id}'"
        )
    await run_migration(engine, guards, "upgrade")
    await _assert_catalog(connection, schema_name)


async def _assert_seed_publication(connection, schema_name: str) -> None:
    """Prove seed scoping, source state, and atomic publication."""

    schema = quoted(schema_name)
    await assert_sqlstate(
        connection,
        "55000",
        f"INSERT INTO {schema}.fhir_formulary_current (source_id, dataset_id) "
        "VALUES ('source-real-seed', 'real-seed')",
    )
    await assert_sqlstate(
        connection,
        "55000",
        f"INSERT INTO {schema}.fhir_formulary_current (source_id, dataset_id) "
        "VALUES ('source-live-seed', 'live-seed')",
    )
    await connection.execute(
        f"UPDATE {schema}.fhir_formulary_source SET enabled = true, "
        "updated_at = transaction_timestamp() WHERE source_id = 'source-live-seed'"
    )
    await assert_sqlstate(
        connection,
        "55000",
        f"INSERT INTO {schema}.fhir_formulary_current (source_id, dataset_id) "
        "VALUES ('source-live-seed', 'live-seed')",
    )
    async with connection.transaction():
        await connection.execute(
            f"INSERT INTO {schema}.fhir_formulary_current "
            "(source_id, dataset_id, published_at) VALUES "
            "('source-live-seed', 'live-seed', transaction_timestamp())"
        )
        await connection.execute(
            f"UPDATE {schema}.fhir_formulary_dataset AS seed SET "
            "status = 'published', published_at = pointer.published_at "
            f"FROM {schema}.fhir_formulary_current AS pointer "
            "WHERE seed.source_id = pointer.source_id "
            "AND seed.dataset_id = pointer.dataset_id "
            "AND seed.dataset_id = 'live-seed'"
        )
    for enabled_value in (False, True):
        await connection.execute(
            f"UPDATE {schema}.fhir_formulary_source SET enabled = $1, "
            "updated_at = transaction_timestamp() WHERE source_id = 'source-live-seed'",
            enabled_value,
        )
    await assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {schema}.fhir_formulary_source SET metadata_json = '{{}}' "
        "WHERE source_id = 'source-live-seed'",
    )
    await connection.execute(
        f"UPDATE {schema}.fhir_formulary_source SET enabled = true, "
        "updated_at = transaction_timestamp() WHERE source_id = 'source-a'"
    )


async def _assert_content_graph_guards(connection, schema_name: str) -> None:
    schema = quoted(schema_name)
    key_by_table = {
        "fhir_formulary_coverage_plan": "public_id",
        "fhir_formulary_coverage_plan_version": "coverage_version_id",
        "fhir_formulary_dataset_coverage_plan": "source_id",
        "fhir_formulary_drug_plan_alias": "alias_id",
        "fhir_formulary_drug_plan_alias_version": "alias_version_id",
        "fhir_formulary_dataset_alias": "source_id",
        "fhir_formulary_medication": "medication_version_id",
        "fhir_formulary_alias_membership": "source_id",
        "fhir_formulary_alternative": "raw_reference",
    }
    for table_name, key_name in key_by_table.items():
        table = f"{schema}.{table_name}"
        for statement in (
            f"UPDATE {table} SET {key_name} = {key_name} WHERE false",
            f"DELETE FROM {table} WHERE false",
            f"TRUNCATE TABLE {table} CASCADE",
        ):
            await assert_sqlstate(connection, "55000", statement)
    await connection.execute(
        f"UPDATE {schema}.fhir_formulary_checkpoint SET fence_token = 2, "
        "processed_count = 1 WHERE dataset_id = 'graph-building'"
    )
    for statement in (
        f"DELETE FROM {schema}.fhir_formulary_checkpoint "
        "WHERE dataset_id = 'graph-building'",
        f"TRUNCATE TABLE {schema}.fhir_formulary_checkpoint CASCADE",
        f"INSERT INTO {schema}.fhir_formulary_dataset_coverage_plan "
        "(source_id, dataset_id, public_id, coverage_version_id) VALUES "
        "('source-a', 'graph-verified', 'fhir_aaaaaaaaaaaaaaaaaaaaaaaaaa', 'coverage-v1')",
        f"INSERT INTO {schema}.fhir_formulary_dataset_alias "
        "(source_id, dataset_id, alias_id, alias_version_id) VALUES "
        "('source-a', 'graph-verified', 'late-owner-alias', 'late-owner-av')",
        f"INSERT INTO {schema}.fhir_formulary_checkpoint "
        "(source_id, alias_id, source_plan_identifier, run_id, dataset_id, fence_token, "
        "cutoff_at, acquisition_mode) VALUES ('source-a', 'late-owner-alias', "
        "'late-owner-plan', 'run-graph-verified', 'graph-verified', 1, "
        "'2026-08-08Z', 'full')",
        f"INSERT INTO {schema}.fhir_formulary_alias_membership "
        "(source_id, alias_version_id, upstream_medication_id, medication_version_id, "
        f"variant_hash) VALUES ('source-a', 'late-member-av', 'med-one', 'med-v1', "
        f"'{HASHES['membership']}')",
        f"INSERT INTO {schema}.fhir_formulary_alternative "
        "(alias_version_id, upstream_medication_id, raw_reference) "
        "VALUES ('late-alt-av', 'med-one', 'late')",
    ):
        await assert_sqlstate(connection, "55000", statement)


async def _assert_content_verify_race(url, schema_name: str) -> None:
    schema = quoted(schema_name)
    writer = await connect(url)
    verifier = await connect(url)
    writer_tx = writer.transaction()
    await writer_tx.start()
    try:
        await writer.execute(
            f"INSERT INTO {schema}.fhir_formulary_alternative "
            "(alias_version_id, upstream_medication_id, raw_reference) "
            "VALUES ('build-av', 'med-one', 'race')"
        )

        async def verify_after_lock() -> int:
            async with verifier.transaction():
                await verifier.fetchrow(
                    f"SELECT dataset_id FROM {schema}.fhir_formulary_dataset "
                    "WHERE dataset_id = 'graph-building' FOR UPDATE"
                )
                count = await verifier.fetchval(
                    f"SELECT count(*) FROM {schema}.fhir_formulary_alternative "
                    "WHERE alias_version_id = 'build-av'"
                )
                await verifier.execute(
                    f"UPDATE {schema}.fhir_formulary_dataset SET status = 'verified' "
                    "WHERE dataset_id = 'graph-building'"
                )
                return count

        verify_task = asyncio.create_task(verify_after_lock())
        await asyncio.sleep(0.05)
        assert not verify_task.done()
        await writer_tx.commit()
        assert await verify_task == 2
        await assert_sqlstate(
            writer,
            "55000",
            f"INSERT INTO {schema}.fhir_formulary_alternative "
            "(alias_version_id, upstream_medication_id, raw_reference) "
            "VALUES ('build-av', 'med-one', 'after-verify')",
        )
    finally:
        if writer.is_in_transaction():
            await writer_tx.rollback()
        await writer.close()
        await verifier.close()


async def _assert_mismatch_attempt(connection, schema_name: str) -> None:
    """Prove mismatch persistence, no admission, and cross-role root burn."""

    mismatch = attempt_insert(
        schema_name,
        "baseline-mismatch",
        "run-baseline-mismatch",
        "candidate-mismatch",
        "run-candidate-mismatch",
        matched=False,
    )
    await connection.execute(mismatch)
    await assert_sqlstate(
        connection,
        "55000",
        admission_insert(
            schema_name,
            "baseline-mismatch",
            "run-baseline-mismatch",
            "candidate-mismatch",
            "run-candidate-mismatch",
            4,
        ),
    )
    for statement in (
        attempt_insert(
            schema_name,
            "baseline-mismatch",
            "run-baseline-mismatch",
            "candidate-a",
            "run-candidate-a",
            matched=True,
        ),
        attempt_insert(
            schema_name,
            "baseline-a",
            "run-baseline-a",
            "candidate-mismatch",
            "run-candidate-mismatch",
            matched=True,
        ),
    ):
        await assert_sqlstate(connection, "55000", statement)


async def _assert_concurrent_attempt_replay(url, connection, schema_name: str) -> None:
    """Prove two exact concurrent calls insert one replayable attempt."""

    replay = attempt_insert(
        schema_name,
        "baseline-a",
        "run-baseline-a",
        "candidate-a",
        "run-candidate-a",
        matched=True,
    ) + " ON CONFLICT DO NOTHING"
    first, second = await connect(url), await connect(url)
    try:
        insert_statuses = await asyncio.gather(
            first.execute(replay),
            second.execute(replay),
        )
    finally:
        await first.close()
        await second.close()
    assert sorted(insert_statuses) == ["INSERT 0 0", "INSERT 0 1"]
    schema = quoted(schema_name)
    assert await connection.fetchval(
        f"SELECT count(*) FROM {schema}.fhir_formulary_twin_attempt "
        "WHERE baseline_dataset_id = 'baseline-a'"
    ) == 1


async def _assert_admission_and_publication(connection, schema_name: str) -> None:
    schema = quoted(schema_name)
    pointer_update = (
        f"UPDATE {schema}.fhir_formulary_current SET dataset_id = 'candidate-a', "
        "generation = 2, published_at = transaction_timestamp() "
        "WHERE source_id = 'source-a'"
    )
    await assert_sqlstate(
        connection,
        "55000",
        f"UPDATE {schema}.fhir_formulary_dataset SET medication_count = 9 "
        "WHERE dataset_id = 'candidate-a'",
    )
    await connection.execute(
        admission_insert(
            schema_name,
            "baseline-a",
            "run-baseline-a",
            "candidate-a",
            "run-candidate-a",
            4,
        )
    )
    await set_source_enabled(connection, schema_name, "source-a", False)
    await assert_sqlstate(connection, "55000", pointer_update)
    await set_source_enabled(connection, schema_name, "source-a", True)
    await assert_sqlstate(connection, "55000", pointer_update)
    async with connection.transaction():
        await connection.execute(pointer_update)
        await connection.execute(
            f"UPDATE {schema}.fhir_formulary_dataset AS candidate SET "
            "status = 'published', published_at = pointer.published_at "
            f"FROM {schema}.fhir_formulary_current AS pointer "
            "WHERE candidate.source_id = pointer.source_id "
            "AND candidate.dataset_id = pointer.dataset_id "
            "AND candidate.dataset_id = 'candidate-a'"
        )
    for enabled_value in (False, True):
        await set_source_enabled(
            connection, schema_name, "source-a", enabled_value
        )
    for statement in (
        f"UPDATE {schema}.fhir_formulary_dataset SET status = 'failed' "
        "WHERE dataset_id = 'candidate-a'",
        f"UPDATE {schema}.fhir_formulary_dataset SET medication_count = 9 "
        "WHERE dataset_id = 'baseline-a'",
        f"DELETE FROM {schema}.fhir_formulary_dataset WHERE dataset_id = 'baseline-a'",
        f"UPDATE {schema}.fhir_formulary_dataset SET status = 'failed' "
        "WHERE dataset_id = 'seed-a'",
        f"DELETE FROM {schema}.fhir_formulary_current WHERE source_id = 'source-a'",
        f"UPDATE {schema}.fhir_formulary_source SET runtime_config_json = "
        "'{\"drift\": true}' WHERE source_id = 'source-a'",
    ):
        await assert_sqlstate(connection, "55000", statement)


async def _assert_evidence_and_downgrade_closed(
    engine,
    attempt,
    admission,
    guards,
    connection,
    schema_name: str,
) -> None:
    schema = quoted(schema_name)
    for table_name in ("fhir_formulary_twin_attempt", "fhir_formulary_twin_admission"):
        table = f"{schema}.{table_name}"
        for statement in (
            f"UPDATE {table} SET source_configuration_hash = '{'2' * 64}'",
            f"DELETE FROM {table}",
            f"TRUNCATE TABLE {table} CASCADE",
        ):
            await assert_sqlstate(connection, "55000", statement)
    for migration, marker in (
        (guards, "fhir_formulary_publication_guard_downgrade_forbidden"),
        (admission, "fhir_formulary_twin_admission_downgrade_forbidden"),
        (attempt, "fhir_formulary_twin_attempt_downgrade_forbidden"),
    ):
        with pytest.raises(Exception, match=marker):
            await run_migration(engine, migration, "downgrade")
    await _assert_catalog(connection, schema_name)


@pytest.mark.asyncio
async def test_twin_evidence_postgres_lifecycle(monkeypatch):
    url = database_url()
    schema_name = f"fhir_twin_test_{uuid.uuid4().hex}"
    foundation = load_migration(FOUNDATION_PATH, "fhir_twin_foundation")
    attempt = load_migration(ATTEMPT_PATH, "fhir_twin_attempt")
    admission = load_migration(ADMISSION_PATH, "fhir_twin_admission")
    guards = load_migration(GUARDS_PATH, "fhir_publication_guards")
    engine = create_async_engine(url.set(drivername="postgresql+asyncpg"))
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema_name)
    monkeypatch.setenv("DB_SCHEMA", schema_name)
    try:
        async with engine.begin() as engine_connection:
            await engine_connection.exec_driver_sql(f"CREATE SCHEMA {quoted(schema_name)}")
        await run_migration(engine, foundation, "upgrade")
        connection = await connect(url)
        try:
            await seed_datasets(connection, schema_name)
            await seed_content_graph(connection, schema_name)
            await run_migration(engine, attempt, "upgrade")
            await run_migration(engine, admission, "upgrade")
            await run_migration(engine, guards, "upgrade")
            await _assert_catalog(connection, schema_name)
            await _assert_safe_chain_and_preflight(
                engine, attempt, admission, guards, connection, schema_name
            )
            await _assert_seed_publication(connection, schema_name)
            await assert_invalid_pointer_writes(connection, schema_name)
            await _assert_content_graph_guards(connection, schema_name)
            await _assert_content_verify_race(url, schema_name)
            await _assert_mismatch_attempt(connection, schema_name)
            await _assert_concurrent_attempt_replay(url, connection, schema_name)
            await _assert_admission_and_publication(connection, schema_name)
            await _assert_evidence_and_downgrade_closed(
                engine, attempt, admission, guards, connection, schema_name
            )
        finally:
            await connection.close()
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()
