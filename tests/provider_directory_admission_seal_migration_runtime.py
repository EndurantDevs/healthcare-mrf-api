# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable-PostgreSQL lifecycle runner for the admission-seal migration."""

from __future__ import annotations

import json
import os
import re
import uuid

import pytest

from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_sha256,
)
from tests.provider_directory_admission_seal_migration_fixture import (
    MIGRATION_PATH,
    POSTGRES_DSN_ENV,
    PROOF_MIGRATION_PATH,
    _assert_legacy_surface_contract,
    _assert_receipt_only_update_is_scoped,
    _digest_call,
    _insert_sealed,
    _install_legacy_dataset_surface,
    _load,
    _run_migration,
    asyncpg,
)
from tests.provider_directory_admission_seal_migration_write_assertions import (
    _assert_catalog_contract,
    _assert_invalid_complete_receipts,
    _assert_invalid_write_paths,
    _assert_sealed_mutations,
)


TEST_DATABASE_PATTERN = re.compile(r"(?:^|[_-])test(?:[_-]|$)", re.IGNORECASE)


async def _install_and_verify_upgrade(
    connection,
    schema: str,
    migration,
    proof_migration,
) -> None:
    """Install the predecessor surface and verify the scoped M1 upgrade."""

    await connection.execute(f'CREATE SCHEMA "{schema}"')
    await _install_legacy_dataset_surface(connection, schema, migration)
    await connection.execute(
        proof_migration._payload_canonical_json_function_sql(schema)
    )
    await connection.execute(proof_migration._payload_sha256_function_sql(schema))
    await _run_migration(migration, "upgrade", connection)
    await _assert_catalog_contract(connection, schema)
    await _assert_legacy_surface_contract(
        connection,
        schema,
        migration,
        scoped=True,
    )
    await _assert_receipt_only_update_is_scoped(connection, schema)


async def _assert_digest_and_sealed_mutations(connection, schema: str) -> None:
    """Verify digest parity, insert one seal, and exercise immutable guards."""

    summary_by_field = {
        "endpoint": "synthetic",
        "large_integer": 10000000000000000000000000001,
        "negative_zero": -0.0,
        "unicode": "Příklad 🙂",
    }
    proof_sha256 = "a" * 64
    resource_types = ["Location", "Organization"]
    database_digest = await connection.fetchval(
        f"SELECT {_digest_call(schema)}",
        json.dumps(summary_by_field, ensure_ascii=False),
        1,
        "generic",
        proof_sha256,
        resource_types,
    )
    assert database_digest == canonical_payload_sha256(
        {
            "contract": "provider-directory-admission-seal-v1",
            "metadata_summary": summary_by_field,
            "admission_version": 1,
            "admission_kind": "generic",
            "proof_sha256": proof_sha256,
            "resource_types": resource_types,
        }
    )
    await _insert_sealed(
        connection,
        schema,
        "dataset_sealed",
        summary_by_field,
        proof_sha256,
        resource_types,
    )
    await _assert_sealed_mutations(connection, schema)


async def _assert_downgrade_and_reupgrade(connection, schema: str, migration) -> None:
    """Block a populated downgrade, then prove clean downgrade and re-upgrade."""

    table = f'"{schema}".provider_directory_endpoint_dataset'
    with pytest.raises(
        asyncpg.PostgresError,
        match="provider_directory_endpoint_dataset_admission_downgrade_blocked",
    ):
        await _run_migration(migration, "downgrade", connection)
    await connection.execute(f"DELETE FROM {table}")
    await _run_migration(migration, "downgrade", connection)
    await _assert_legacy_surface_contract(
        connection,
        schema,
        migration,
        scoped=False,
    )
    assert await connection.fetchval(
        "SELECT pg_catalog.to_regprocedure($1) IS NULL",
        f'"{schema}".'
        "provider_directory_endpoint_dataset_admission_metadata_sha256"
        "(jsonb,smallint,text,text,character varying[])",
    )
    assert not await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM information_schema.columns
             WHERE table_schema = $1
               AND table_name = 'provider_directory_endpoint_dataset'
               AND column_name LIKE '%admission%'
        )
        """,
        schema,
    )
    await _run_migration(migration, "upgrade", connection)
    await _assert_catalog_contract(connection, schema)


async def run_postgres_contract(monkeypatch) -> None:
    """Exercise upgrade, write guards, downgrade, and re-upgrade on test DB."""

    database_dsn = os.getenv(POSTGRES_DSN_ENV)
    if not database_dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    connection = await asyncpg.connect(database_dsn)
    database_name = str(await connection.fetchval("SELECT current_database()"))
    if TEST_DATABASE_PATTERN.search(database_name) is None:
        await connection.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")
    schema = "admission_seal_" + uuid.uuid4().hex
    migration = _load(MIGRATION_PATH, "admission_seal_postgres_migration")
    proof_migration = _load(
        PROOF_MIGRATION_PATH,
        "admission_seal_prerequisite_postgres_migration",
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        await _install_and_verify_upgrade(
            connection, schema, migration, proof_migration
        )
        table = f'"{schema}".provider_directory_endpoint_dataset'
        await connection.execute(
            f"INSERT INTO {table} (dataset_id) VALUES ('dataset_legacy')"
        )
        await _assert_invalid_write_paths(connection, schema)
        await _assert_invalid_complete_receipts(connection, schema)
        await _assert_digest_and_sealed_mutations(connection, schema)
        await _assert_downgrade_and_reupgrade(connection, schema, migration)
    finally:
        await connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await connection.close()
