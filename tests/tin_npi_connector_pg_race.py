# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL dataset-resource serialization race scenarios."""

from __future__ import annotations

import asyncio
import uuid

import pytest

from tests.tin_npi_connector_postgres_support import (
    asyncpg,
    create_fence_tables,
    load_migration,
    open_test_connection,
    run_migration,
)


async def prove_dataset_validation_races(monkeypatch):
    admin_connection = await open_test_connection()
    writer_connection = await open_test_connection()
    validator_connection = await open_test_connection()
    schema = f"tin_npi_connector_test_{uuid.uuid4().hex}"
    quoted_schema = f'"{schema}"'
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    try:
        await admin_connection.execute(f"CREATE SCHEMA {quoted_schema}")
        await create_fence_tables(admin_connection, schema)
        await run_migration(load_migration(), "upgrade", admin_connection)
        await _insert_race_datasets(admin_connection, quoted_schema)
        await _prove_validation_first(
            admin_connection,
            writer_connection,
            validator_connection,
            quoted_schema,
        )
        await _prove_writer_first(
            admin_connection,
            writer_connection,
            validator_connection,
            quoted_schema,
        )
    finally:
        await writer_connection.close()
        await validator_connection.close()
        await admin_connection.execute(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await admin_connection.close()


async def _insert_race_datasets(connection, quoted_schema):
    await connection.execute(
        f"""
        INSERT INTO {quoted_schema}.provider_directory_endpoint_dataset (
            dataset_id,
            endpoint_id,
            status,
            is_current,
            resource_count
        ) VALUES
            ('dataset-validation-first', 'endpoint-a', 'acquiring', false, 0),
            ('dataset-writer-first', 'endpoint-a', 'acquiring', false, 0)
        """
    )


async def _prove_validation_first(
    admin_connection,
    writer_connection,
    validator_connection,
    quoted_schema,
):
    validation_transaction = validator_connection.transaction()
    await validation_transaction.start()
    await validator_connection.execute(
        f"""
        UPDATE {quoted_schema}.provider_directory_endpoint_dataset
           SET status = 'validated'
         WHERE dataset_id = 'dataset-validation-first'
        """
    )
    blocked_writer = asyncio.create_task(
        writer_connection.execute(
            f"""
            INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
                dataset_id, resource_type, resource_id, payload_hash, payload_json
            ) VALUES (
                'dataset-validation-first', 'Organization', 'organization-late',
                $1, '{{"id":"organization-late"}}'::jsonb
            )
            """,
            "11" * 32,
        )
    )
    await asyncio.sleep(0.1)
    assert blocked_writer.done() is False
    await validation_transaction.commit()
    with pytest.raises(
        asyncpg.PostgresError,
        match="tin_npi_connector_dataset_resource_parent_immutable",
    ):
        await blocked_writer
    resource_count = await admin_connection.fetchval(
        f"""
        SELECT COUNT(*)
          FROM {quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-validation-first'
        """
    )
    assert resource_count == 0


async def _prove_writer_first(
    admin_connection,
    writer_connection,
    validator_connection,
    quoted_schema,
):
    writer_transaction = writer_connection.transaction()
    await writer_transaction.start()
    await writer_connection.execute(
        f"""
        INSERT INTO {quoted_schema}.provider_directory_dataset_resource (
            dataset_id, resource_type, resource_id, payload_hash, payload_json
        ) VALUES (
            'dataset-writer-first', 'Organization', 'organization-early',
            $1, '{{"id":"organization-early"}}'::jsonb
        )
        """,
        "22" * 32,
    )
    blocked_validator = asyncio.create_task(
        validator_connection.execute(
            f"""
            UPDATE {quoted_schema}.provider_directory_endpoint_dataset
               SET status = 'validated'
             WHERE dataset_id = 'dataset-writer-first'
            """
        )
    )
    await asyncio.sleep(0.1)
    assert blocked_validator.done() is False
    await writer_transaction.commit()
    await blocked_validator
    await _assert_writer_first_state(admin_connection, quoted_schema)


async def _assert_writer_first_state(connection, quoted_schema):
    resource_count = await connection.fetchval(
        f"""
        SELECT COUNT(*)
          FROM {quoted_schema}.provider_directory_dataset_resource
         WHERE dataset_id = 'dataset-writer-first'
        """
    )
    dataset_status = await connection.fetchval(
        f"""
        SELECT status
          FROM {quoted_schema}.provider_directory_endpoint_dataset
         WHERE dataset_id = 'dataset-writer-first'
        """
    )
    assert resource_count == 1
    assert dataset_status == "validated"
