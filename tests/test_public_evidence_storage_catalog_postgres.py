# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL catalog-shape proof for public-evidence storage."""

from __future__ import annotations

import pytest

from public_evidence import evidence_record_token_policy as token_policy
from tests.public_evidence_storage_postgres_support import (
    EXPECTED_COLUMNS_BY_TABLE,
    EXPECTED_INDEX_NAMES,
    PTG_POLICY_ID,
    PUBLIC_POLICY_ID,
    connect,
    public_evidence_schema,
    quoted,
    release_parameters,
    token_policy_row,
)


TABLE_NAMES = {
    "public_evidence_source_identity",
    "public_evidence_source_release",
    "public_evidence_token_policy",
    "public_evidence_tax_identity",
}


async def _assert_trigger_shape(connection, schema_name: str) -> None:
    """Require both always-enabled immutable guards on every catalog table."""

    triggers = await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, "
        "trigger_record.tgenabled::text, trigger_record.tgtype "
        "FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid = trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = $1 AND NOT trigger_record.tgisinternal",
        schema_name,
    )
    expected_triggers = {
        (
            table_name,
            f"{table_name}_{suffix}_guard",
            "A",
            trigger_type,
        )
        for table_name in TABLE_NAMES
        for suffix, trigger_type in (("mutation", 27), ("truncate", 34))
    }
    assert {
        (
            trigger["relname"],
            trigger["tgname"],
            trigger["tgenabled"],
            trigger["tgtype"],
        )
        for trigger in triggers
    } == expected_triggers


async def _assert_routine_shape(connection, schema_name: str) -> None:
    """Require only the closed helper set with no public execution grant."""

    routines = await connection.fetch(
        "SELECT routine.proname, routine.provolatile, routine.prosecdef, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') "
        "AS public_execute FROM pg_proc AS routine "
        "JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = $1",
        schema_name,
    )
    routine_by_name = {routine["proname"]: routine for routine in routines}
    assert set(routine_by_name) == {
        "guard_public_evidence_immutable_catalog",
        "public_evidence_source_identity_ref",
        "public_evidence_source_release_valid",
        "public_evidence_tax_identity_ref",
        "public_evidence_token_policy_descriptor_sha256",
    }
    assert all(not routine["public_execute"] for routine in routines)
    assert routine_by_name["guard_public_evidence_immutable_catalog"]["prosecdef"]
    assert all(
        routine_by_name[name]["provolatile"] in ("i", b"i")
        for name in routine_by_name
        if name != "guard_public_evidence_immutable_catalog"
    )


async def _assert_public_privileges(connection, schema_name: str) -> None:
    """Require every public table privilege to remain revoked."""

    for table_name in TABLE_NAMES:
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
            assert not await connection.fetchval(
                "SELECT has_table_privilege('public', $1, $2)",
                f"{schema_name}.{table_name}",
                privilege,
            )


async def _assert_catalog_shape(connection, schema_name: str) -> None:
    """Prove exact catalog roots, immutable triggers, and closed privileges."""

    tables = await connection.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname = $1",
        schema_name,
    )
    assert {table["tablename"] for table in tables} == TABLE_NAMES
    await _assert_trigger_shape(connection, schema_name)
    await _assert_routine_shape(connection, schema_name)
    await _assert_public_privileges(connection, schema_name)


async def _assert_columns_indexes_and_foreign_keys(
    connection,
    schema_name: str,
) -> None:
    """Require exact columns, indexes, and ownership foreign keys."""

    columns = await connection.fetch(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = $1 ORDER BY table_name, ordinal_position",
        schema_name,
    )
    actual_columns_by_table = {
        table_name: tuple(
            column["column_name"]
            for column in columns
            if column["table_name"] == table_name
        )
        for table_name in TABLE_NAMES
    }
    assert actual_columns_by_table == EXPECTED_COLUMNS_BY_TABLE
    indexes = await connection.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1",
        schema_name,
    )
    assert {index["indexname"] for index in indexes} == EXPECTED_INDEX_NAMES
    foreign_keys = await connection.fetch(
        "SELECT constraint_name FROM information_schema.table_constraints "
        "WHERE constraint_schema = $1 AND constraint_type = 'FOREIGN KEY'",
        schema_name,
    )
    assert {foreign_key["constraint_name"] for foreign_key in foreign_keys} == {
        "public_evidence_source_release_artifact_fkey",
        "public_evidence_tax_identity_policy_fkey",
    }


async def _assert_exact_reference_vectors(connection, schema_name: str) -> None:
    """Require SQL helpers to reproduce every frozen Python reference vector."""

    for source_kind in (
        "tic",
        "public_provider_directory_fhir",
        "nppes_entity_address",
        "public_hpt",
    ):
        parameters = release_parameters(source_kind)
        actual_ref = await connection.fetchval(
            f"SELECT {quoted(schema_name)}."
            "public_evidence_source_identity_ref($1, $2, $3)",
            parameters["artifact_identity_kind"],
            parameters["artifact_content_identity_kind"],
            parameters["artifact_content_sha256"],
        )
        assert actual_ref == parameters["artifact_identity_ref"]
    for contract_id, policy_id in (
        (token_policy.PTG_V4_EIN_TOKEN_POLICY_CONTRACT, PTG_POLICY_ID),
        (token_policy.PUBLIC_EIN_NPI_TOKEN_POLICY_CONTRACT, PUBLIC_POLICY_ID),
    ):
        actual_digest = await connection.fetchval(
            f"SELECT {quoted(schema_name)}."
            "public_evidence_token_policy_descriptor_sha256($1, $2)",
            contract_id,
            policy_id,
        )
        assert actual_digest == token_policy_row(contract_id, policy_id)[2]


@pytest.mark.asyncio
async def test_public_evidence_catalog_and_exact_contract_vectors() -> None:
    """Prove exact tables, helpers, privileges, and Python-compatible vectors."""

    async with public_evidence_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            await _assert_catalog_shape(connection, schema_name)
            await _assert_columns_indexes_and_foreign_keys(connection, schema_name)
            await _assert_exact_reference_vectors(connection, schema_name)
        finally:
            await connection.close()
