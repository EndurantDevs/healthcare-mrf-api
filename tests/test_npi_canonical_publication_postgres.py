# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL 18 proof for atomic canonical-NPI publication receipts."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from pathlib import Path

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from process.npi_canonical_publication import (
    NpiCanonicalPublicationInput,
    receipt_metrics,
)
from process.npi_canonical_publication_store import (
    canonical_relation_oids,
    insert_npi_publication_receipt,
    load_committed_npi_publication,
    has_settled_npi_publication,
    mark_npi_publication_succeeded,
)
from tests.npi_canonical_publication_postgres_support import (
    CANONICAL_TABLES,
    assert_published_state_is_frozen,
    npi_publication_schema,
)
from tests.public_evidence_nppes_admission_postgres_support import (
    admit_chain,
    admit_replay,
    finished_chain_receipt,
    prepared_replay,
    qualified,
)
from tests.public_evidence_storage_postgres_support import connect
from tests.public_evidence_storage_postgres_support import run_migration_action


RUN_ID = "run_npi_publication_pg"
ATTEMPT_ID = RUN_ID + ":" + "c" * 32
ATTEMPT_STARTED_AT = "2026-08-09T01:04:05.678901+00:00"


async def _admitted_chain(
    connection: asyncpg.Connection,
    schema_name: str,
    root: Path,
):
    replay = await prepared_replay(root)
    admission = await admit_replay(connection, schema_name, replay)
    chain = finished_chain_receipt(replay, admission)
    return await admit_chain(connection, schema_name, chain)


async def _insert_running_attempt(
    connection: asyncpg.Connection,
    schema_name: str,
    run_id: str = RUN_ID,
) -> None:
    await connection.execute(
        f"INSERT INTO {qualified(schema_name, 'import_run')} "
        "(run_id, importer, status, phase_detail, heartbeat_at, progress, metrics) "
        "VALUES ($1, 'npi', 'running', 'process_data running', "
        "transaction_timestamp() AT TIME ZONE 'UTC', $2::jsonb, '{}'::jsonb)",
        run_id,
        json.dumps(
            {
                "attempt_id": ATTEMPT_ID,
                "attempt_started_at": ATTEMPT_STARTED_AT,
            }
        ),
    )


async def _publish(
    connection: asyncpg.Connection,
    schema_name: str,
    chain_ref: str,
    *,
    row_counts: tuple[int, ...] = (1, 2, 3, 4, 5, 6),
    include_evidence_metrics: bool = True,
):
    relation_oids = await canonical_relation_oids(connection, schema=schema_name)
    receipt = await insert_npi_publication_receipt(
        connection,
        schema=schema_name,
        publication_input=NpiCanonicalPublicationInput(
            RUN_ID,
            ATTEMPT_ID,
            ATTEMPT_STARTED_AT,
            chain_ref,
            "2026-08-09",
            relation_oids,
            row_counts,
        ),
    )
    metrics_by_name = {
        "npi_canonical_publication": receipt_metrics(receipt),
    }
    if include_evidence_metrics:
        metrics_by_name["nppes_public_evidence"] = {"chain_ref": chain_ref}
    await mark_npi_publication_succeeded(
        connection,
        schema=schema_name,
        receipt=receipt,
        progress_by_name={
            "unit": "rows",
            "done": 2,
            "total": 2,
            "pct": 100,
            "phase": "npi published",
            "message": "succeeded",
        },
        metrics_by_name=metrics_by_name,
    )
    return receipt


@pytest.mark.asyncio
async def test_atomic_publication_is_validated_sealed_and_frozen(tmp_path):
    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            chain = await _admitted_chain(connection, schema_name, tmp_path)
            await _insert_running_attempt(connection, schema_name)
            async with connection.transaction():
                receipt = await _publish(
                    connection,
                    schema_name,
                    chain.chain_ref,
                )

            stored = await connection.fetchrow(
                f"SELECT receipt.*, sealed.sealed_at, run.status, run.snapshot_id "
                f"FROM {qualified(schema_name, 'npi_canonical_publication_receipt')} "
                "AS receipt JOIN "
                f"{qualified(schema_name, 'npi_canonical_publication_receipt_seal')} "
                "AS sealed USING (publication_ref) JOIN "
                f"{qualified(schema_name, 'import_run')} AS run USING (run_id)"
            )
            assert stored is not None
            assert stored["publication_ref"] == receipt.publication_ref
            assert stored["status"] == "succeeded"
            assert stored["snapshot_id"] == receipt.publication_ref
            assert stored["sealed_at"].tzinfo is not None
            assert tuple(
                stored[f"{table_name}_row_count"]
                for table_name in CANONICAL_TABLES
            ) == (1, 2, 3, 4, 5, 6)

            await assert_published_state_is_frozen(connection, schema_name)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_forged_census_rolls_back_receipt_and_terminal_state(tmp_path):
    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            chain = await _admitted_chain(connection, schema_name, tmp_path)
            await _insert_running_attempt(connection, schema_name)
            with pytest.raises(asyncpg.CheckViolationError):
                async with connection.transaction():
                    await _publish(
                        connection,
                        schema_name,
                        chain.chain_ref,
                        row_counts=(9, 2, 3, 4, 5, 6),
                    )
            assert await connection.fetchval(
                f"SELECT count(*) FROM "
                f"{qualified(schema_name, 'npi_canonical_publication_receipt')}"
            ) == 0
            run = await connection.fetchrow(
                f"SELECT status, snapshot_id FROM {qualified(schema_name, 'import_run')} "
                "WHERE run_id=$1",
                RUN_ID,
            )
            assert tuple(run) == ("running", None)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_preinserted_seal_and_contradictory_terminal_metrics_roll_back(
    tmp_path,
):
    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            chain = await _admitted_chain(connection, schema_name, tmp_path)
            await _insert_running_attempt(connection, schema_name)
            with pytest.raises(asyncpg.PostgresError):
                async with connection.transaction():
                    receipt = await _publish(
                        connection,
                        schema_name,
                        chain.chain_ref,
                    )
                    await connection.execute(
                        f"INSERT INTO {qualified(schema_name, 'npi_canonical_publication_receipt_seal')} "
                        "(publication_ref, sealed_at) VALUES ($1, "
                        "transaction_timestamp() + interval '1 second')",
                        receipt.publication_ref,
                    )

            with pytest.raises(asyncpg.CheckViolationError):
                async with connection.transaction():
                    await _publish(
                        connection,
                        schema_name,
                        chain.chain_ref,
                        include_evidence_metrics=False,
                    )
            assert await connection.fetchval(
                f"SELECT count(*) FROM "
                f"{qualified(schema_name, 'npi_canonical_publication_receipt')}"
            ) == 0
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_early_constraint_validation_seals_against_late_table_writes(
    tmp_path,
):
    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            chain = await _admitted_chain(connection, schema_name, tmp_path)
            await _insert_running_attempt(connection, schema_name)
            with pytest.raises(asyncpg.ObjectNotInPrerequisiteStateError):
                async with connection.transaction():
                    await _publish(connection, schema_name, chain.chain_ref)
                    await connection.execute("SET CONSTRAINTS ALL IMMEDIATE")
                    await connection.execute(
                        f"INSERT INTO {qualified(schema_name, 'npi')} "
                        "(synthetic_id) VALUES (99)"
                    )
            assert await connection.fetchval(
                f"SELECT count(*) FROM "
                f"{qualified(schema_name, 'npi_canonical_publication_receipt')}"
            ) == 0
            assert await connection.fetchval(
                f"SELECT count(*) FROM {qualified(schema_name, 'npi')}"
            ) == 1
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_fresh_connection_settlement_waits_for_publication_commit(tmp_path):
    """Prove settlement blocks until the publication commit becomes visible."""

    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        publisher = await connect(database_url)
        reconciler = await connect(database_url)
        transaction = None
        try:
            chain = await _admitted_chain(publisher, schema_name, tmp_path)
            await _insert_running_attempt(publisher, schema_name)
            transaction = publisher.transaction()
            await transaction.start()
            receipt = await _publish(publisher, schema_name, chain.chain_ref)
            progress_by_name = {
                "unit": "rows",
                "done": 2,
                "total": 2,
                "pct": 100,
                "phase": "npi published",
                "message": "succeeded",
            }
            metrics_by_name = {
                "npi_canonical_publication": receipt_metrics(receipt),
                "nppes_public_evidence": {"chain_ref": chain.chain_ref},
            }

            async def reconcile_after_settlement():
                async with reconciler.transaction():
                    assert await has_settled_npi_publication(
                        reconciler,
                        schema=schema_name,
                        run_id=RUN_ID,
                    )
                    return await load_committed_npi_publication(
                        reconciler,
                        schema=schema_name,
                        receipt=receipt,
                        progress_by_name=progress_by_name,
                        metrics_by_name=metrics_by_name,
                    )

            reconcile_task = asyncio.create_task(reconcile_after_settlement())
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(reconcile_task), timeout=0.1)
            await transaction.commit()
            transaction = None
            committed = await asyncio.wait_for(reconcile_task, timeout=2)
            assert committed is not None
            assert committed.receipt == receipt
        finally:
            if transaction is not None:
                await transaction.rollback()
            await publisher.close()
            await reconciler.close()


async def _assert_publication_triggers(
    connection: asyncpg.Connection,
    schema_name: str,
) -> None:
    """Require the exact deferred validator and enabled-always write guards."""

    trigger_rows = await connection.fetch(
                "SELECT relation.relname, trigger.tgname, trigger.tgenabled, "
                "trigger.tgdeferrable, trigger.tginitdeferred "
                "FROM pg_catalog.pg_trigger AS trigger "
                "JOIN pg_catalog.pg_class AS relation ON relation.oid=trigger.tgrelid "
                "JOIN pg_catalog.pg_namespace AS namespace "
                "ON namespace.oid=relation.relnamespace "
                "WHERE namespace.nspname=$1 AND NOT trigger.tgisinternal "
                "AND (relation.relname LIKE 'npi_canonical_publication%' "
                "OR (relation.relname='import_run' "
                "AND trigger.tgname='npi_canonical_publication_import_run_guard'))",
                schema_name,
            )
    trigger_by_name = {
        trigger_row["tgname"]: trigger_row for trigger_row in trigger_rows
    }
    integrity_guard = trigger_by_name[
        "npi_canonical_publication_receipt_integrity_guard"
    ]
    assert integrity_guard["tgdeferrable"] is True
    assert integrity_guard["tginitdeferred"] is True
    assert {
        trigger_row["tgname"]: trigger_row["tgenabled"]
        for trigger_row in trigger_rows
    } == {
        "npi_canonical_publication_import_run_guard": b"A",
        "npi_canonical_publication_receipt_integrity_guard": b"A",
        "npi_canonical_publication_receipt_mutation_guard": b"A",
        "npi_canonical_publication_receipt_seal_mutation_guard": b"A",
        "npi_canonical_publication_receipt_seal_truncate_guard": b"A",
        "npi_canonical_publication_receipt_truncate_guard": b"A",
    }
    canonical_guard_rows = await connection.fetch(
                "SELECT relation.relname, trigger.tgname, trigger.tgenabled "
                "FROM pg_catalog.pg_trigger AS trigger "
                "JOIN pg_catalog.pg_class AS relation "
                "ON relation.oid=trigger.tgrelid "
                "JOIN pg_catalog.pg_namespace AS namespace "
                "ON namespace.oid=relation.relnamespace "
                "WHERE namespace.nspname=$1 AND NOT trigger.tgisinternal "
                "AND relation.relname=ANY($2::text[]) "
                "AND trigger.tgname LIKE "
                "'npi_canonical_publication_postseal_%_guard'",
                schema_name,
                list(CANONICAL_TABLES),
            )
    assert len(canonical_guard_rows) == 12
    assert all(
        trigger_row["tgenabled"] == b"A"
        for trigger_row in canonical_guard_rows
    )


@pytest.mark.asyncio
async def test_publication_catalog_has_always_guards_and_no_public_privileges():
    """Require enabled-always publication guards and closed public ACLs."""

    async with npi_publication_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            await _assert_publication_triggers(connection, schema_name)

            for table_name in (
                "npi_canonical_publication_receipt",
                "npi_canonical_publication_receipt_seal",
            ):
                privileges = await connection.fetchval(
                    "SELECT bool_or(has_table_privilege('public', "
                    "$1, privilege_name)) FROM unnest(ARRAY["
                    "'SELECT','INSERT','UPDATE','DELETE','TRUNCATE',"
                    "'REFERENCES','TRIGGER','MAINTAIN']) AS privilege_name",
                    f"{schema_name}.{table_name}",
                )
                assert privileges is False
            for function_name in (
                "validate_npi_canonical_publication_receipt()",
                "guard_npi_canonical_publication_run()",
                "guard_npi_canonical_publication_after_seal()",
            ):
                assert await connection.fetchval(
                    "SELECT has_function_privilege('public', $1, 'EXECUTE')",
                    f"{schema_name}.{function_name}",
                ) is False
            sequence_name = (
                f"{schema_name}."
                "npi_canonical_publication_receipt_publication_generation_seq"
            )
            for privilege_name in ("SELECT", "USAGE", "UPDATE"):
                assert await connection.fetchval(
                    "SELECT has_sequence_privilege('public', $1, $2)",
                    sequence_name,
                    privilege_name,
                ) is False
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_publication_downgrade_is_empty_only_and_reversible(tmp_path):
    async with npi_publication_schema() as (
        engine,
        database_url,
        schema_name,
        migration,
    ):
        await run_migration_action(engine, migration, "downgrade")
        connection = await connect(database_url)
        try:
            assert await connection.fetchval(
                "SELECT to_regclass($1)",
                f"{schema_name}.npi_canonical_publication_receipt",
            ) is None
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "upgrade")

        connection = await connect(database_url)
        try:
            chain = await _admitted_chain(connection, schema_name, tmp_path)
            await _insert_running_attempt(connection, schema_name)
            async with connection.transaction():
                receipt = await _publish(connection, schema_name, chain.chain_ref)
        finally:
            await connection.close()

        with pytest.raises(
            DBAPIError,
            match="npi_canonical_publication_downgrade_requires_empty",
        ):
            await run_migration_action(engine, migration, "downgrade")

        connection = await connect(database_url)
        try:
            stored = await connection.fetchrow(
                f"SELECT receipt.publication_ref, run.status, run.snapshot_id "
                f"FROM {qualified(schema_name, 'npi_canonical_publication_receipt')} "
                "AS receipt JOIN "
                f"{qualified(schema_name, 'npi_canonical_publication_receipt_seal')} "
                "AS sealed USING (publication_ref) JOIN "
                f"{qualified(schema_name, 'import_run')} AS run USING (run_id)"
            )
            assert tuple(stored) == (
                receipt.publication_ref,
                "succeeded",
                receipt.publication_ref,
            )
        finally:
            await connection.close()
