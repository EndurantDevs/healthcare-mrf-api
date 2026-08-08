# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic helpers for the public-evidence reference-root PostgreSQL proof."""

from __future__ import annotations

import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator, Mapping
import uuid

import asyncpg
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from public_evidence import evidence_record_policies as record_policy
from public_evidence import evidence_record_primitives as record_primitive
from public_evidence import source_release_contract as release_contract
from tests.public_evidence_source_release_support import release_input
from tests.public_evidence_storage_postgres_support import (
    database_url,
    drop_schema,
    load_migration as load_foundation_migration,
    quoted,
    run_migration_action,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260808100000_public_evidence_reference_roots.py"
)
DISPOSABLE_SCHEMA_PREFIX = "public_evidence_test_"
LONG_PROTOCOL_ID = "a" + "b" * 94 + "_v" + "1" * 128
REFERENCE_TABLE_NAMES = (
    "public_evidence_source_record",
    "public_evidence_provider_group",
    "public_evidence_source_entity",
)
EXPECTED_COLUMNS_BY_TABLE = {
    "public_evidence_source_record": (
        "source_record_ref",
        "source_release_ref",
        "source_release_contract_sha256",
        "source_kind",
        "record_kind",
        "identity_contract_id",
        "record_hmac_sha256",
        "payload_sha256",
        "created_at",
    ),
    "public_evidence_provider_group": (
        "provider_group_ref",
        "source_release_ref",
        "source_release_contract_sha256",
        "source_kind",
        "identity_contract_id",
        "identity_sha256",
        "created_at",
    ),
    "public_evidence_source_entity": (
        "source_entity_ref",
        "source_release_ref",
        "source_release_contract_sha256",
        "source_kind",
        "entity_kind",
        "identity_contract_id",
        "identity_sha256",
        "created_at",
    ),
}


async def _assert_column_shape(connection, schema_name: str) -> None:
    """Require exact columns and an unbounded protocol-ID SQL type."""

    columns = await connection.fetch(
        "SELECT table_name, column_name, data_type, character_maximum_length "
        "FROM information_schema.columns WHERE table_schema = $1 "
        "AND table_name = ANY($2::text[]) "
        "ORDER BY table_name, ordinal_position",
        schema_name,
        list(REFERENCE_TABLE_NAMES),
    )
    actual_columns_by_table = {
        table_name: tuple(
            column["column_name"]
            for column in columns
            if column["table_name"] == table_name
        )
        for table_name in REFERENCE_TABLE_NAMES
    }
    assert actual_columns_by_table == EXPECTED_COLUMNS_BY_TABLE
    identity_columns = [
        column for column in columns if column["column_name"] == "identity_contract_id"
    ]
    assert len(identity_columns) == 3
    assert all(column["data_type"] == "text" for column in identity_columns)
    assert all(
        column["character_maximum_length"] is None for column in identity_columns
    )


async def _constraint_definitions(connection, schema_name: str) -> dict[str, str]:
    constraints = await connection.fetch(
        "SELECT relation.relname, constraint_record.conname, "
        "pg_get_constraintdef(constraint_record.oid) AS definition "
        "FROM pg_constraint AS constraint_record "
        "JOIN pg_class AS relation ON relation.oid = constraint_record.conrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = $1",
        schema_name,
    )
    return {
        constraint["conname"]: constraint["definition"] for constraint in constraints
    }


async def _assert_release_ownership(connection, schema_name: str) -> None:
    """Require every root to own the exact parent digest and source kind."""

    constraints_by_name = await _constraint_definitions(connection, schema_name)
    assert "public_evidence_source_release_kind_owner_key" in constraints_by_name
    for table_name in REFERENCE_TABLE_NAMES:
        foreign_key = constraints_by_name[f"{table_name}_release_fkey"]
        assert "source_release_contract_sha256" in foreign_key
        assert "contract_sha256" in foreign_key
        assert "source_kind" in foreign_key
        assert "ON DELETE RESTRICT" in foreign_key


async def _assert_guard_triggers(connection, schema_name: str) -> None:
    """Require both always-enabled immutable guards on every new root."""

    triggers = await connection.fetch(
        "SELECT relation.relname, trigger_record.tgname, "
        "trigger_record.tgenabled::text, trigger_record.tgtype "
        "FROM pg_trigger AS trigger_record "
        "JOIN pg_class AS relation ON relation.oid = trigger_record.tgrelid "
        "JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace "
        "WHERE namespace.nspname = $1 AND NOT trigger_record.tgisinternal "
        "AND relation.relname = ANY($2::text[])",
        schema_name,
        list(REFERENCE_TABLE_NAMES),
    )
    assert {
        (
            trigger["relname"],
            trigger["tgname"],
            trigger["tgenabled"],
            trigger["tgtype"],
        )
        for trigger in triggers
    } == {
        (table_name, f"{table_name}_{suffix}_guard", "A", trigger_type)
        for table_name in REFERENCE_TABLE_NAMES
        for suffix, trigger_type in (("mutation", 27), ("truncate", 34))
    }


async def _assert_private_routines_and_tables(connection, schema_name: str) -> None:
    """Require private immutable helpers and no PUBLIC table privileges."""

    function_names = (
        "public_evidence_source_record_ref",
        "public_evidence_provider_group_ref",
        "public_evidence_source_entity_ref",
    )
    routines = await connection.fetch(
        "SELECT routine.proname, routine.provolatile, "
        "has_function_privilege('public', routine.oid, 'EXECUTE') "
        "AS public_execute FROM pg_proc AS routine "
        "JOIN pg_namespace AS namespace ON namespace.oid = routine.pronamespace "
        "WHERE namespace.nspname = $1 AND routine.proname = ANY($2::text[])",
        schema_name,
        list(function_names),
    )
    assert {routine["proname"] for routine in routines} == set(function_names)
    assert all(routine["provolatile"] in ("i", b"i") for routine in routines)
    assert all(not routine["public_execute"] for routine in routines)
    for table_name in REFERENCE_TABLE_NAMES:
        for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"):
            assert not await connection.fetchval(
                "SELECT has_table_privilege('public', $1, $2)",
                f"{schema_name}.{table_name}",
                privilege,
            )


async def assert_reference_catalog_shape(connection, schema_name: str) -> None:
    """Prove exact columns, ownership, guards, helper ACLs, and table ACLs."""

    await _assert_column_shape(connection, schema_name)
    await _assert_release_ownership(connection, schema_name)
    await _assert_guard_triggers(connection, schema_name)
    await _assert_private_routines_and_tables(connection, schema_name)


def load_reference_migration() -> Any:
    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_reference_roots_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@asynccontextmanager
async def reference_roots_schema() -> (
    AsyncIterator[tuple[AsyncEngine, sa.URL, str, Any]]
):
    parsed_url = database_url()
    engine = create_async_engine(
        parsed_url.set(drivername="postgresql+asyncpg"),
        pool_pre_ping=True,
    )
    schema_name = f"{DISPOSABLE_SCHEMA_PREFIX}{uuid.uuid4().hex}"
    foundation_migration = load_foundation_migration()
    reference_migration = load_reference_migration()
    foundation_migration._schema = lambda: schema_name
    reference_migration._schema = lambda: schema_name
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"CREATE SCHEMA {quoted(schema_name)}")
        await run_migration_action(engine, foundation_migration, "upgrade")
        await run_migration_action(engine, reference_migration, "upgrade")
        yield engine, parsed_url, schema_name, reference_migration
    finally:
        await drop_schema(engine, schema_name)
        await engine.dispose()


def source_release(
    source_kind: str,
) -> release_contract.PublicEvidenceSourceReleaseDescriptor:
    return release_contract.build_public_evidence_source_release(
        release_input(source_kind)
    )


def source_record_parameters(
    source_kind: str,
    record_kind: str,
    *,
    identity_contract_id: str = "synthetic_record_hmac_v1",
    seed: int = 1,
) -> dict[str, object]:
    release = source_release(source_kind)
    record_hmac = f"{seed:02x}" * 32
    payload_digest = f"{seed + 64:02x}" * 32
    reference = record_primitive.build_evidence_source_record_reference(
        release,
        {
            "record_kind": record_kind,
            "identity_contract_id": identity_contract_id,
            "record_hmac_sha256": record_hmac,
            "payload_sha256": payload_digest,
        },
    )
    return {
        "source_record_ref": reference.source_record_ref,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": bytes.fromhex(release.contract_sha256),
        "source_kind": release.source_kind,
        "record_kind": reference.record_kind,
        "identity_contract_id": reference.identity_contract_id,
        "record_hmac_sha256": bytes.fromhex(reference.record_hmac_sha256),
        "payload_sha256": bytes.fromhex(reference.payload_sha256),
    }


def provider_group_parameters(
    source_kind: str = "tic",
    *,
    identity_contract_id: str = "synthetic_provider_group_digest_v1",
) -> dict[str, object]:
    release = source_release(source_kind)
    reference = record_policy.build_provider_group_reference(
        release,
        {
            "identity_contract_id": identity_contract_id,
            "identity_sha256": "b1" * 32,
        },
    )
    return {
        "provider_group_ref": reference.provider_group_ref,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": bytes.fromhex(release.contract_sha256),
        "source_kind": release.source_kind,
        "identity_contract_id": reference.identity_contract_id,
        "identity_sha256": bytes.fromhex(reference.identity_sha256),
    }


def source_entity_parameters(
    source_kind: str,
    entity_kind: str,
    *,
    identity_contract_id: str = "synthetic_entity_digest_v1",
    seed: int = 1,
) -> dict[str, object]:
    release = source_release(source_kind)
    reference = record_policy.build_opaque_source_entity_reference(
        release,
        {
            "entity_kind": entity_kind,
            "identity_contract_id": identity_contract_id,
            "identity_sha256": f"{seed + 192:02x}" * 32,
        },
    )
    return {
        "source_entity_ref": reference.source_entity_ref,
        "source_release_ref": release.source_release_ref,
        "source_release_contract_sha256": bytes.fromhex(release.contract_sha256),
        "source_kind": release.source_kind,
        "entity_kind": reference.entity_kind,
        "identity_contract_id": reference.identity_contract_id,
        "identity_sha256": bytes.fromhex(reference.identity_sha256),
    }


async def insert_reference_row(
    connection: asyncpg.Connection,
    schema_name: str,
    table_name: str,
    parameters: Mapping[str, object],
) -> None:
    column_names = tuple(parameters)
    placeholders = ", ".join(
        f"${ordinal}" for ordinal in range(1, len(column_names) + 1)
    )
    await connection.execute(
        f"INSERT INTO {quoted(schema_name)}.{quoted(table_name)} "
        f"({', '.join(column_names)}) VALUES ({placeholders})",
        *(parameters[column_name] for column_name in column_names),
    )
