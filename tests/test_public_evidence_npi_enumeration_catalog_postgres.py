# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL 18 constraint and index proof for NPI-enumeration storage."""

from __future__ import annotations

import asyncpg
import pytest

from tests.public_evidence_npi_enumeration_postgres_support import (
    EXPECTED_CONSTRAINT_FLAGS,
    EXPECTED_INDEX_NAMES,
    EXPECTED_SCHEMA_TABLES,
    TABLE_NAMES,
    npi_candidate,
    npi_enumeration_schema,
)
from tests.public_evidence_storage_postgres_support import connect, quoted


EXPECTED_TRIGGER_BINDINGS_BY_NAME = {
    **{
        f"{table_name}_integrity_guard": (
            table_name,
            5,
            "A",
            True,
            True,
            "validate_public_evidence_npi_record",
        )
        for table_name in TABLE_NAMES
    },
    **{
        f"{table_name}_mutation_guard": (
            table_name,
            27,
            "A",
            False,
            False,
            "guard_public_evidence_immutable_catalog",
        )
        for table_name in TABLE_NAMES
    },
    **{
        f"{table_name}_truncate_guard": (
            table_name,
            34,
            "A",
            False,
            False,
            "guard_public_evidence_immutable_catalog",
        )
        for table_name in TABLE_NAMES
    },
}


async def _constraint_records(
    connection: asyncpg.Connection, schema_name: str
) -> list[asyncpg.Record]:
    return await connection.fetch(
        "SELECT relation.relname, constraint_record.conname, "
        "constraint_record.contype::text, constraint_record.condeferrable, "
        "constraint_record.condeferred, constraint_record.convalidated, "
        "constraint_record.conenforced, "
        "pg_get_constraintdef(constraint_record.oid) AS definition "
        "FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid=constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND constraint_record.contype NOT IN ('n','t')",
        schema_name,
        list(TABLE_NAMES),
    )


def _assert_constraint_definitions(
    constraint_records: list[asyncpg.Record], schema_name: str
) -> None:
    constraint_flags_by_name = {
        entry["conname"]: (
            entry["relname"],
            entry["contype"],
            entry["condeferrable"],
            entry["condeferred"],
            entry["convalidated"],
            entry["conenforced"],
        )
        for entry in constraint_records
    }
    assert constraint_flags_by_name == EXPECTED_CONSTRAINT_FLAGS
    definitions_by_name = {
        entry["conname"]: entry["definition"] for entry in constraint_records
    }
    for constraint_name in (
        "public_evidence_record_shape_check",
        "public_evidence_record_source_link_shape_check",
        "public_evidence_npi_enumeration_shape_check",
    ):
        definition = definitions_by_name[constraint_name]
        assert definition.count("IS TRUE") == 1
        assert definition.rstrip(")").endswith("IS TRUE")
    record_owner = "(evidence_ref, source_release_ref, source_release_contract_sha256, source_kind)"
    source_owner = (
        "(source_record_ref, source_release_ref, "
        "source_release_contract_sha256, source_kind)"
    )
    common_reference = (
        f"REFERENCES {schema_name}.public_evidence_record{record_owner} "
        "ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED"
    )
    for constraint_name in (
        "public_evidence_record_source_link_record_fkey",
        "public_evidence_npi_enumeration_record_fkey",
    ):
        assert definitions_by_name[constraint_name] == (
            f"FOREIGN KEY {record_owner} {common_reference}"
        )
    assert definitions_by_name["public_evidence_record_source_link_source_fkey"] == (
        f"FOREIGN KEY {source_owner} REFERENCES {schema_name}."
        f"public_evidence_source_record{source_owner} "
        "ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED"
    )
    assert definitions_by_name["public_evidence_record_release_fkey"] == (
        "FOREIGN KEY (source_release_ref, source_release_contract_sha256, source_kind) "
        f"REFERENCES {schema_name}.public_evidence_source_release"
        "(source_release_ref, contract_sha256, source_kind) ON DELETE RESTRICT"
    )


async def _assert_indexes_and_tables(
    connection: asyncpg.Connection, schema_name: str
) -> None:
    index_records = await connection.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname=$1 "
        "AND tablename=ANY($2::text[])",
        schema_name,
        list(TABLE_NAMES),
    )
    assert {entry["indexname"] for entry in index_records} == EXPECTED_INDEX_NAMES
    table_records = await connection.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname=$1", schema_name
    )
    assert {entry["tablename"] for entry in table_records} == EXPECTED_SCHEMA_TABLES


async def _assert_trigger_bindings(
    connection: asyncpg.Connection, schema_name: str
) -> None:
    trigger_records = await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, "
        "trigger_record.tgtype::integer, trigger_record.tgenabled::text, "
        "trigger_record.tgdeferrable, trigger_record.tginitdeferred, "
        "procedure.proname, procedure.pronargs, "
        "procedure_namespace.nspname AS function_schema "
        "FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid=trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid=relation.relnamespace "
        "JOIN pg_proc AS procedure ON procedure.oid=trigger_record.tgfoid "
        "JOIN pg_namespace AS procedure_namespace "
        "ON procedure_namespace.oid=procedure.pronamespace "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND NOT trigger_record.tgisinternal",
        schema_name,
        list(TABLE_NAMES),
    )
    assert all(
        trigger_record["function_schema"] == schema_name
        and trigger_record["pronargs"] == 0
        for trigger_record in trigger_records
    )
    trigger_bindings_by_name = {
        trigger_record["tgname"]: (
            trigger_record["relname"],
            trigger_record["tgtype"],
            trigger_record["tgenabled"],
            trigger_record["tgdeferrable"],
            trigger_record["tginitdeferred"],
            trigger_record["proname"],
        )
        for trigger_record in trigger_records
    }
    assert trigger_bindings_by_name == EXPECTED_TRIGGER_BINDINGS_BY_NAME


async def _assert_public_acl_denied(
    connection: asyncpg.Connection, schema_name: str
) -> None:
    for table_name in TABLE_NAMES:
        for privilege in (
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "TRUNCATE",
            "REFERENCES",
            "TRIGGER",
            "MAINTAIN",
        ):
            assert not await connection.fetchval(
                "SELECT has_table_privilege('public', $1, $2)",
                f"{schema_name}.{table_name}",
                privilege,
            )


@pytest.mark.asyncio
async def test_constraints_indexes_and_table_scope_are_exact() -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            _assert_constraint_definitions(
                await _constraint_records(connection, schema_name), schema_name
            )
            await _assert_indexes_and_tables(connection, schema_name)
            await _assert_trigger_bindings(connection, schema_name)
            await _assert_public_acl_denied(connection, schema_name)
        finally:
            await connection.close()


@pytest.mark.asyncio
async def test_npi_helper_enforces_public_domain_and_cms_check_digit() -> None:
    async with npi_enumeration_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            helper = f"{quoted(schema_name)}.public_evidence_npi_valid"
            assert await connection.fetchval(
                f"SELECT {helper}($1)", npi_candidate().typed_row.npi
            )
            for invalid_npi in (
                "0000000000",
                "1234567890",
                "3000000000",
                "123456789",
                "abcdefghij",
            ):
                assert not await connection.fetchval(
                    f"SELECT {helper}($1)", invalid_npi
                )
            assert await connection.fetchval(f"SELECT {helper}(NULL)") is None
        finally:
            await connection.close()
