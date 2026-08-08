# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL 18 proof for dormant normalized NPI-enumeration storage."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest
from sqlalchemy.exc import DBAPIError

from tests.public_evidence_npi_enumeration_postgres_support import (
    EXPECTED_COLUMNS,
    EXPECTED_COLUMN_TYPES,
    TABLE_NAMES,
    assert_owned_roots,
    assert_stored_candidate,
    candidate_rows,
    extra_source_link,
    insert_candidate,
    insert_row,
    npi_candidate,
    npi_enumeration_schema,
    seed_owned_roots,
    source_root_parameters,
    wait_for_ungranted_advisory_lock,
)
from tests.public_evidence_reference_roots_postgres_support import (
    insert_reference_row,
)
from tests.public_evidence_storage_postgres_support import (
    connect,
    quoted,
    run_migration_action,
)


async def _connect(database_url) -> asyncpg.Connection:
    return await connect(database_url)


async def _assert_table_columns(
    connection: asyncpg.Connection, schema_name: str
) -> None:
    column_records = await connection.fetch(
        "SELECT relation.relname AS table_name, attribute.attname AS column_name, "
        "format_type(attribute.atttypid, attribute.atttypmod) AS column_type, "
        "attribute.attnotnull AS not_null, "
        "pg_get_expr(default_value.adbin, default_value.adrelid) AS default_expr "
        "FROM pg_class AS relation JOIN pg_namespace AS namespace "
        "ON namespace.oid=relation.relnamespace JOIN pg_attribute AS attribute "
        "ON attribute.attrelid=relation.oid LEFT JOIN pg_attrdef AS default_value "
        "ON default_value.adrelid=relation.oid AND default_value.adnum=attribute.attnum "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND attribute.attnum>0 AND NOT attribute.attisdropped "
        "ORDER BY relation.relname, attribute.attnum",
        schema_name,
        list(TABLE_NAMES),
    )
    columns_by_table = {
        table_name: tuple(
            column_record["column_name"]
            for column_record in column_records
            if column_record["table_name"] == table_name
        )
        for table_name in TABLE_NAMES
    }
    assert columns_by_table == EXPECTED_COLUMNS
    for column_record in column_records:
        column_name = column_record["column_name"]
        assert column_record["column_type"] == EXPECTED_COLUMN_TYPES[column_name]
        assert column_record["not_null"] is (column_name != "effective_end_at")
        expected_default = (
            "transaction_timestamp()" if column_name == "created_at" else None
        )
        assert column_record["default_expr"] == expected_default


async def _assert_table_triggers(
    connection: asyncpg.Connection, schema_name: str
) -> None:
    trigger_records = await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, "
        "trigger_record.tgenabled::text, trigger_record.tgdeferrable, "
        "trigger_record.tginitdeferred FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid=trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND NOT trigger_record.tgisinternal",
        schema_name,
        list(TABLE_NAMES),
    )
    assert len(trigger_records) == 9
    assert all(record["tgenabled"] == "A" for record in trigger_records)
    integrity_triggers = [
        record
        for record in trigger_records
        if record["tgname"].endswith("_integrity_guard")
    ]
    assert len(integrity_triggers) == 3
    assert all(
        record["tgdeferrable"] and record["tginitdeferred"]
        for record in integrity_triggers
    )


async def _assert_helper_functions(
    connection: asyncpg.Connection, schema_name: str
) -> None:
    function_records = await connection.fetch(
        "SELECT routine.proname, routine.provolatile::text, routine.proisstrict, "
        "routine.prosecdef, routine.proparallel::text, routine.proconfig, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') AS public_execute "
        "FROM pg_proc AS routine JOIN pg_namespace AS namespace "
        "ON namespace.oid=routine.pronamespace WHERE namespace.nspname=$1 "
        "AND routine.proname=ANY($2::text[])",
        schema_name,
        [
            "public_evidence_record_digest",
            "public_evidence_record_ref",
            "public_evidence_npi_valid",
            "validate_public_evidence_npi_record",
        ],
    )
    assert len(function_records) == 4
    assert all(
        not function_record["public_execute"] for function_record in function_records
    )
    assert all(
        function_record["proconfig"] == ["search_path=pg_catalog"]
        for function_record in function_records
    )
    validator = next(
        function_record
        for function_record in function_records
        if function_record["proname"] == "validate_public_evidence_npi_record"
    )
    assert validator["prosecdef"] and not validator["proisstrict"]
    assert validator["provolatile"] == "v" and validator["proparallel"] == "u"
    for helper in (
        function_record
        for function_record in function_records
        if function_record["proname"] != validator["proname"]
    ):
        assert helper["provolatile"] == "i"
        assert helper["proisstrict"] and not helper["prosecdef"]
        assert helper["proparallel"] == "s"


async def _assert_table_acl(connection: asyncpg.Connection, schema_name: str) -> None:
    for table_name in TABLE_NAMES:
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
            assert not await connection.fetchval(
                "SELECT has_table_privilege('public', $1, $2)",
                f"{schema_name}.{table_name}",
                privilege,
            )


async def _assert_catalog(connection: asyncpg.Connection, schema_name: str) -> None:
    """Prove the exact private tables, triggers, functions, and ACLs."""

    await _assert_table_columns(connection, schema_name)
    await _assert_table_triggers(connection, schema_name)
    await _assert_helper_functions(connection, schema_name)
    await _assert_table_acl(connection, schema_name)


async def _assert_mutation_rejected(
    connection: asyncpg.Connection, statement: str
) -> None:
    with pytest.raises(
        asyncpg.ObjectNotInPrerequisiteStateError,
        match="public_evidence_catalog_mutation_forbidden",
    ):
        async with connection.transaction():
            await connection.execute(statement)


@pytest.mark.asyncio
async def test_catalog_is_exact_private_deferred_and_always_enabled() -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await _connect(database_url)
        try:
            await _assert_catalog(connection, schema_name)
        finally:
            await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    (
        "enumeration_state",
        "npi_entity_type",
        "finite_active_interval",
        "table_order",
        "immediate",
    ),
    (
        ("active", "individual_type_1", False, TABLE_NAMES, False),
        ("active", "organization_type_2", True, tuple(reversed(TABLE_NAMES)), True),
        (
            "deactivated",
            "individual_type_1",
            False,
            (TABLE_NAMES[1], TABLE_NAMES[0], TABLE_NAMES[2]),
            False,
        ),
    ),
)
async def test_frozen_python_vectors_insert_in_any_deferred_order(
    enumeration_state: str,
    npi_entity_type: str,
    finite_active_interval: bool,
    table_order: tuple[str, ...],
    immediate: bool,
) -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await _connect(database_url)
        try:
            candidate = npi_candidate(
                enumeration_state=enumeration_state,
                npi_entity_type=npi_entity_type,
                finite_active_interval=finite_active_interval,
            )
            await seed_owned_roots(connection, schema_name, candidate)
            await insert_candidate(
                connection,
                schema_name,
                candidate,
                table_order=table_order,
                force_immediate=immediate,
            )
            await assert_stored_candidate(connection, schema_name, candidate)
        finally:
            await connection.close()


_TAMPER_CASES = (
    (TABLE_NAMES[0], "row_sha256", b"\x04" * 32),
    (TABLE_NAMES[1], "row_sha256", b"\x05" * 32),
    (TABLE_NAMES[2], "row_sha256", b"\x06" * 32),
    (TABLE_NAMES[2], "npi", "1234567890"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("table_name", "field_name", "replacement"), _TAMPER_CASES)
async def test_local_row_digest_and_npi_tamper_fails_closed(
    table_name: str,
    field_name: str,
    replacement: object,
) -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await _connect(database_url)
        try:
            candidate = npi_candidate()
            await seed_owned_roots(connection, schema_name, candidate)
            rows = candidate_rows(candidate)
            rows[table_name][0][field_name] = replacement
            with pytest.raises(asyncpg.PostgresError):
                await insert_candidate(connection, schema_name, candidate, rows=rows)
            for stored_table in TABLE_NAMES:
                assert (
                    await connection.fetchval(
                        f"SELECT count(*) FROM {quoted(schema_name)}.{quoted(stored_table)}"
                    )
                    == 0
                )
        finally:
            await connection.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_table", (TABLE_NAMES[1], TABLE_NAMES[2]))
async def test_incomplete_record_families_fail_at_deferred_commit(
    missing_table: str,
) -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await _connect(database_url)
        try:
            candidate = npi_candidate()
            await seed_owned_roots(connection, schema_name, candidate)
            rows = candidate_rows(candidate)
            rows[missing_table] = []
            with pytest.raises(
                asyncpg.CheckViolationError,
                match="public_evidence_npi_record_invalid",
            ):
                await insert_candidate(connection, schema_name, candidate, rows=rows)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_extra_owned_link_is_rejected_without_partial_rows() -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await _connect(database_url)
        try:
            candidate = npi_candidate()
            await seed_owned_roots(connection, schema_name, candidate)
            extra_record, extra_link = extra_source_link(candidate)
            await insert_reference_row(
                connection,
                schema_name,
                "public_evidence_source_record",
                source_root_parameters(candidate, extra_record),
            )
            table_rows = candidate_rows(candidate)
            table_rows[TABLE_NAMES[1]].append(extra_link)
            with pytest.raises(asyncpg.CheckViolationError):
                await insert_candidate(
                    connection, schema_name, candidate, rows=table_rows
                )
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_concurrent_extra_link_waits_then_fails_without_partial_state() -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        setup = await _connect(database_url)
        first = await _connect(database_url)
        second = await _connect(database_url)
        try:
            candidate = npi_candidate()
            await seed_owned_roots(setup, schema_name, candidate)
            extra_record, extra_link = extra_source_link(candidate)
            await insert_reference_row(
                setup,
                schema_name,
                "public_evidence_source_record",
                source_root_parameters(candidate, extra_record),
            )
            await insert_candidate(setup, schema_name, candidate)
            first_tx = first.transaction()
            second_tx = second.transaction()
            await first_tx.start()
            await first.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
                "healthporta.public-evidence-npi-record:"
                f"{schema_name}:{candidate.common_row.evidence_ref}",
            )
            await second_tx.start()
            second_pid = await second.fetchval("SELECT pg_backend_pid()")
            await insert_row(second, schema_name, TABLE_NAMES[1], extra_link)
            second_commit = asyncio.create_task(second_tx.commit())
            await wait_for_ungranted_advisory_lock(setup, second_pid)
            await first_tx.commit()
            with pytest.raises(asyncpg.PostgresError):
                await second_commit
            assert (
                await setup.fetchval(
                    f"SELECT count(*) FROM {quoted(schema_name)}.{quoted(TABLE_NAMES[1])}"
                )
                == 1
            )
        finally:
            for connection in (setup, first, second):
                if connection.is_in_transaction():
                    await connection.execute("ROLLBACK")
                await connection.close()


@pytest.mark.asyncio
async def test_immutable_mutations_and_populated_downgrade_fail_closed() -> None:
    async with npi_enumeration_schema() as (
        engine,
        database_url,
        schema_name,
        migration,
    ):
        connection = await _connect(database_url)
        try:
            candidate = npi_candidate()
            await seed_owned_roots(connection, schema_name, candidate)
            await insert_candidate(connection, schema_name, candidate)
            schema = quoted(schema_name)
            for table_name in TABLE_NAMES:
                table = f"{schema}.{quoted(table_name)}"
                truncate_target = table
                if table_name == TABLE_NAMES[0]:
                    truncate_target = ", ".join(
                        f"{schema}.{quoted(candidate_table)}"
                        for candidate_table in TABLE_NAMES
                    )
                for statement in (
                    f"UPDATE {table} SET created_at=created_at",
                    f"DELETE FROM {table}",
                    f"TRUNCATE {truncate_target}",
                ):
                    await _assert_mutation_rejected(connection, statement)
        finally:
            await connection.close()
        with pytest.raises(
            DBAPIError,
            match="public_evidence_downgrade_requires_empty_npi_records",
        ):
            await run_migration_action(engine, migration, "downgrade")
        survivor = await _connect(database_url)
        try:
            await assert_stored_candidate(survivor, schema_name, candidate)
            for table_name in TABLE_NAMES:
                assert (
                    await survivor.fetchval(
                        f"SELECT count(*) FROM {quoted(schema_name)}.{quoted(table_name)}"
                    )
                    == 1
                )
        finally:
            await survivor.close()


@pytest.mark.asyncio
async def test_empty_downgrade_and_reupgrade_preserve_parent_roots() -> None:
    async with npi_enumeration_schema() as (
        engine,
        database_url,
        schema_name,
        migration,
    ):
        connection = await _connect(database_url)
        try:
            candidate = npi_candidate()
            await seed_owned_roots(connection, schema_name, candidate)
            await assert_owned_roots(connection, schema_name, candidate)
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "downgrade")
        connection = await _connect(database_url)
        try:
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM pg_tables WHERE schemaname=$1",
                    schema_name,
                )
                == 7
            )
            await assert_owned_roots(connection, schema_name, candidate)
        finally:
            await connection.close()
        await run_migration_action(engine, migration, "upgrade")
        connection = await _connect(database_url)
        try:
            await assert_owned_roots(connection, schema_name, candidate)
        finally:
            await connection.close()
