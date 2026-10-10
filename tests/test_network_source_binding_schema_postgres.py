# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native source binding storage stays durable and enforces bounded int4 IDs."""

import json
from uuid import uuid4

import asyncpg
import pytest

from db.models.registry_network_binding import RegistryNetworkBinding, RegistryNetworkBindingBatch
from tests.test_registry_record_store_postgres import record_db

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def network_binding_db(record_db):
    return record_db


def _binding(**changes):
    return {
        "binding_id": uuid4(),
        "source_system": "ptg",
        "source_id": "synthetic-source",
        "dataset_schema": "synthetic_dataset",
        "dataset_id": "dataset-one",
        "producer_id": "producer-one",
        "edition_id": "edition-one",
        "source_key": "source-network-one",
        "source_scope_json": json.dumps(
            {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"}
        ),
        "binding_key": "a" * 64,
        "network_id": 2147483647,
        "evidence_id": "synthetic-review",
        "evidence_sha256": "b" * 64,
        **changes,
    }


async def _insert(connection, schema, fields):
    columns = tuple(fields)
    return await connection.fetchrow(
        f'INSERT INTO "{schema}".registry_network_binding ({",".join(columns)}) '
        f"VALUES ({','.join('$' + str(index) for index in range(1, len(columns) + 1))}) RETURNING *",
        *fields.values(),
    )


async def test_migration_and_models_have_exact_native_columns_and_no_row_hooks(network_binding_db):
    connection, schema, _ = network_binding_db
    for model in (RegistryNetworkBinding, RegistryNetworkBindingBatch):
        columns = await connection.fetch(
            "SELECT attname::text FROM pg_attribute WHERE attrelid=to_regclass($1) AND attnum>0 AND NOT attisdropped ORDER BY attnum",
            f'"{schema}".{model.__tablename__}',
        )
        assert [column["attname"] for column in columns] == list(model.__table__.columns.keys())
        assert model.__runtime_schema_sync__ is False
        assert not await connection.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=to_regclass($1) AND NOT tgisinternal)",
            f'"{schema}".{model.__tablename__}',
        )
    stored = await _insert(connection, schema, _binding())
    assert type(stored["network_id"]) is int and stored["network_id"] == 2147483647
    assert stored["archived"] is False and stored["revision"] == 1
    assert stored["created_at"].tzinfo is not None
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)


@pytest.mark.parametrize(
    "changes",
    [
        {"network_id": 0},
        {"network_id": -1},
        {"network_id": 2147483648},
        {"source_system": "label"},
        {"source_id": ""},
        {"binding_key": "A" * 64},
        {"evidence_sha256": "bad"},
        {"source_scope_json": "[]"},
        {"revision": 0},
    ],
)
async def test_native_constraints_reject_invalid_bindings_without_partial_rows(network_binding_db, changes):
    connection, schema, _ = network_binding_db
    expected_error = asyncpg.DataError if changes.get("network_id") == 2147483648 else asyncpg.CheckViolationError
    with pytest.raises(expected_error):
        async with connection.transaction():
            await _insert(connection, schema, _binding(**changes))
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 0


async def test_scope_reuse_and_closed_binding_identity_remain_explicit(network_binding_db):
    connection, schema, _ = network_binding_db
    first = await _insert(connection, schema, _binding())
    second = await _insert(connection, schema, _binding(edition_id="edition-two", binding_key="c" * 64, network_id=1))
    assert first["source_key"] == second["source_key"] and first["binding_id"] != second["binding_id"]
    await connection.execute(
        f'UPDATE "{schema}".registry_network_binding SET archived=true WHERE binding_id=$1', first["binding_id"]
    )
    with pytest.raises(asyncpg.UniqueViolationError):
        async with connection.transaction():
            await _insert(connection, schema, _binding())
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_network_binding') == 2
