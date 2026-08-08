"""PostgreSQL proof for the durable Job-only V5 retirement guard."""

from __future__ import annotations

import asyncio
from copy import deepcopy
import hashlib

import pytest

from tests.ptg_wave_materialized_preclaim_postgres_support import (
    insert_materialized_supersession,
    materialized_evidence,
    seed_materialized_predecessor,
    successor_cohort,
)
from tests.test_ptg_wave_recovery_storage_postgres import (
    MATERIALIZED_PRECLAIM_PATH,
    _dsn,
    _insert_successor,
    _install_migration,
    _load_migration,
    _quote,
    _signed_evidence,
    asyncpg,
)


async def _admit_v5(
    connection,
    schema: str,
    descriptor: dict,
    successor_wave_id: str,
) -> dict:
    evidence, canonical = materialized_evidence(
        descriptor,
        successor_wave_id,
    )
    async with connection.transaction():
        await insert_materialized_supersession(
            connection,
            schema,
            descriptor["wave_id"],
            successor_wave_id,
            evidence,
            canonical,
        )
        await _insert_successor(
            connection,
            schema,
            successor_wave_id,
            "admitted",
            successor_cohort(successor_wave_id, evidence),
        )
    return evidence


async def _admit_v5_on_new_connection(
    schema: str,
    descriptor: dict,
    successor_wave_id: str,
) -> dict:
    connection = await asyncpg.connect(_dsn())
    try:
        return await _admit_v5(
            connection,
            schema,
            descriptor,
            successor_wave_id,
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_v5_accepts_one_atomic_successor_and_freezes_predecessor(
    monkeypatch,
):
    schema = "wave_recovery_materialized_acceptance"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        await _admit_v5(
            connection,
            schema,
            descriptor,
            "materialized-successor",
        )
        quoted = _quote(schema)
        retirement = await connection.fetchrow(
            f"""
            SELECT predecessor_wave_id, successor_wave_id, recovery_basis
              FROM {quoted}.ptg_import_wave_supersession
             WHERE predecessor_wave_id = $1
            """,
            descriptor["wave_id"],
        )
        assert tuple(retirement) == (
            descriptor["wave_id"],
            "materialized-successor",
            "materialized_preclaim_failure",
        )
        assert await connection.fetchval(
            f"SELECT state FROM {quoted}.ptg_import_wave WHERE wave_id = $1",
            descriptor["wave_id"],
        ) == "slots_waiting"
        with pytest.raises(asyncpg.PostgresError, match="QUARANTINED_IMMUTABLE"):
            await connection.execute(
                f"UPDATE {quoted}.ptg_import_wave SET state = 'failed' "
                "WHERE wave_id = $1",
                descriptor["wave_id"],
            )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


_RETIRED_WRITE_STATEMENTS = (
    "UPDATE {schema}.import_run SET started_at = clock_timestamp() "
    "WHERE run_id = 'materialized-run'",
    "INSERT INTO {schema}.import_run (run_id, importer, metrics) VALUES ("
    "'stale-materialized-run', 'ptg', "
    "'{{\"wave_id\":\"materialized-wave\"}}'::jsonb)",
    "DELETE FROM {schema}.ptg_import_wave_intent "
    "WHERE wave_id = 'materialized-wave'",
    "INSERT INTO {schema}.ptg_import_wave_claim (wave_id) "
    "VALUES ('materialized-wave')",
    "INSERT INTO {schema}.ptg_import_wave_outcome (wave_id) "
    "VALUES ('materialized-wave')",
    "INSERT INTO {schema}.ptg_source_attempt_event (outer_run_id, event_kind) "
    "VALUES ('materialized-run', 'worker_start_admitted')",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("write_statement", _RETIRED_WRITE_STATEMENTS)
async def test_v5_retirement_blocks_every_predecessor_work_path(
    monkeypatch,
    write_statement,
):
    statement_digest = hashlib.sha256(write_statement.encode()).hexdigest()
    schema = "wave_recovery_materialized_fence_" + statement_digest[:12]
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        await _admit_v5(connection, schema, descriptor, "fence-successor")
        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_RETIRED",
        ):
            await connection.execute(
                write_statement.format(schema=_quote(schema))
            )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_v5_guard_triggers_cannot_be_bypassed_by_replica_role(monkeypatch):
    schema = "wave_recovery_materialized_always_triggers"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        expected_names = {
            "ptg_import_wave_materialized_preclaim_guard",
            "ptg_import_wave_materialized_preclaim_binding_guard",
            "ptg_import_wave_intent_materialized_retirement_guard",
            "ptg_import_wave_claim_materialized_retirement_guard",
            "ptg_import_wave_outcome_materialized_retirement_guard",
            "ptg_import_wave_materialized_retired_run_guard",
            "ptg_import_wave_materialized_retired_event_guard",
        }
        enabled_modes = await connection.fetch(
            """
            SELECT trigger.tgname, trigger.tgenabled::text AS tgenabled
              FROM pg_catalog.pg_trigger AS trigger
              JOIN pg_catalog.pg_class AS relation
                ON relation.oid = trigger.tgrelid
              JOIN pg_catalog.pg_namespace AS namespace
                ON namespace.oid = relation.relnamespace
             WHERE namespace.nspname = $1
               AND trigger.tgname = ANY($2::text[])
            """,
            schema,
            list(expected_names),
        )
        assert {
            trigger_mode["tgname"]: trigger_mode["tgenabled"]
            for trigger_mode in enabled_modes
        } == {name: "A" for name in expected_names}
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_v5_concurrent_successors_leave_one_atomic_handoff(monkeypatch):
    schema = "wave_recovery_materialized_concurrency"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        successor_ids = ("concurrent-successor-a", "concurrent-successor-b")
        admission_outcomes = await asyncio.gather(
            *(
                _admit_v5_on_new_connection(schema, descriptor, successor_id)
                for successor_id in successor_ids
            ),
            return_exceptions=True,
        )
        assert sum(
            isinstance(admission_outcome, dict)
            for admission_outcome in admission_outcomes
        ) == 1
        errors = [
            admission_outcome
            for admission_outcome in admission_outcomes
            if isinstance(admission_outcome, BaseException)
        ]
        assert len(errors) == 1
        assert errors[0].sqlstate in {"23505", "40P01"}

        quoted = _quote(schema)
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave_quarantine "
            "WHERE predecessor_wave_id = $1",
            descriptor["wave_id"],
        ) == 1
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave_supersession "
            "WHERE predecessor_wave_id = $1 "
            "AND recovery_basis = 'materialized_preclaim_failure'",
            descriptor["wave_id"],
        ) == 1
        assert await connection.fetchval(
            f"SELECT count(*) FROM {quoted}.ptg_import_wave "
            "WHERE wave_id = ANY($1::text[])",
            list(successor_ids),
        ) == 1
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_v5_downgrade_blocks_after_retirement(monkeypatch):
    schema = "wave_recovery_materialized_downgrade"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        await _admit_v5(connection, schema, descriptor, "downgrade-successor")
        migration = _load_migration(MATERIALIZED_PRECLAIM_PATH)
        statements: list[str] = []
        monkeypatch.setattr(migration.op, "execute", statements.append)
        migration.downgrade()

        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_DOWNGRADE_BLOCKED",
        ):
            async with connection.transaction():
                for statement in statements:
                    await connection.execute(statement)
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


_INVALID_EVIDENCE_MUTATIONS = (
    lambda proof: proof["database"].pop("claim_count"),
    lambda proof: proof["predecessor"].__setitem__("extra", True),
    lambda proof: proof["predecessor"].__setitem__(
        "pinned_image_digest",
        "0" * 64,
    ),
    lambda proof: proof["kubernetes"].__setitem__("complete_condition", True),
    lambda proof: proof["kubernetes"].__setitem__("failed", 12.0),
    lambda proof: proof["redis"].pop("ready_slot_count"),
    lambda proof: proof["redis"].__setitem__("release_present", "false"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", _INVALID_EVIDENCE_MUTATIONS)
async def test_v5_rejects_partial_or_unbound_evidence(monkeypatch, mutation):
    mutation_name = hashlib.sha256(repr(mutation).encode()).hexdigest()[:12]
    schema = "wave_recovery_materialized_evidence_" + mutation_name
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        successor_wave_id = "invalid-successor"
        evidence, _canonical = materialized_evidence(
            descriptor,
            successor_wave_id,
        )
        unsigned_evidence_map = deepcopy(evidence)
        unsigned_evidence_map.pop("proof_digest")
        mutation(unsigned_evidence_map)
        evidence, canonical = _signed_evidence(unsigned_evidence_map)

        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_EVIDENCE_INVALID",
        ):
            async with connection.transaction():
                await _insert_successor(
                    connection,
                    schema,
                    successor_wave_id,
                    "admitted",
                    successor_cohort(successor_wave_id, evidence),
                )
                await insert_materialized_supersession(
                    connection,
                    schema,
                    descriptor["wave_id"],
                    successor_wave_id,
                    evidence,
                    canonical,
                )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


_PROGRESS_STATEMENTS = (
    "UPDATE {schema}.import_run SET started_at = clock_timestamp() "
    "WHERE run_id = 'materialized-run'",
    "UPDATE {schema}.ptg_import_wave SET "
    "kubernetes_ready_attestation_digest = '" + "f" * 64 + "' "
    "WHERE wave_id = 'materialized-wave'",
    "INSERT INTO {schema}.ptg_import_wave_claim (wave_id) "
    "VALUES ('materialized-wave')",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("progress_statement", _PROGRESS_STATEMENTS)
async def test_v5_rejects_any_locked_progress(monkeypatch, progress_statement):
    statement_digest = hashlib.sha256(progress_statement.encode()).hexdigest()
    schema = "wave_recovery_materialized_progress_" + statement_digest[:12]
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        successor_wave_id = "progress-successor"
        evidence, canonical = materialized_evidence(
            descriptor,
            successor_wave_id,
        )
        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_REQUIRED",
        ):
            async with connection.transaction():
                await connection.execute(
                    progress_statement.format(schema=_quote(schema))
                )
                await _insert_successor(
                    connection,
                    schema,
                    successor_wave_id,
                    "admitted",
                    successor_cohort(successor_wave_id, evidence),
                )
                await insert_materialized_supersession(
                    connection,
                    schema,
                    descriptor["wave_id"],
                    successor_wave_id,
                    evidence,
                    canonical,
                )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()


@pytest.mark.asyncio
async def test_v5_deferred_binding_rejects_an_unrelated_successor(monkeypatch):
    schema = "wave_recovery_materialized_binding"
    connection = await asyncpg.connect(_dsn())
    try:
        await _install_migration(connection, monkeypatch, schema)
        descriptor = await seed_materialized_predecessor(connection, schema)
        evidence, canonical = materialized_evidence(
            descriptor,
            "binding-successor",
        )
        wrong_cohort_map = successor_cohort("binding-successor", evidence)
        wrong_cohort_map["materialized_preclaim_supersession"] = {}
        with pytest.raises(
            asyncpg.PostgresError,
            match="MATERIALIZED_PRECLAIM_BINDING_INVALID",
        ):
            async with connection.transaction():
                await _insert_successor(
                    connection,
                    schema,
                    "binding-successor",
                    "admitted",
                    wrong_cohort_map,
                )
                await insert_materialized_supersession(
                    connection,
                    schema,
                    descriptor["wave_id"],
                    "binding-successor",
                    evidence,
                    canonical,
                )
    finally:
        await connection.execute(f"DROP SCHEMA IF EXISTS {_quote(schema)} CASCADE")
        await connection.close()
