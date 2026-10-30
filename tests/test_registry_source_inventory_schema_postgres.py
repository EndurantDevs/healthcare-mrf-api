# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The forward CHECK retains old arrays and refuses malformed inventory metadata."""

import importlib.util
import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from process.registry_source_inventory_contract import source_inventory_root
from process.registry_source_inventory_reference import SourceInventoryReference
from tests.test_network_serving_schema_postgres import _candidate
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


def _migration():
    path = Path(__file__).parents[1] / "alembic/versions/20261029010000_registry_source_inventory.py"
    spec = importlib.util.spec_from_file_location("inventory_schema_migration", path)
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    return migration


async def _update(connection, schema, identity, document):
    await connection.execute(
        f'UPDATE "{schema}".network_membership_candidate SET source_recipes_json=$2::jsonb WHERE candidate_id=$1',
        identity,
        json.dumps(document),
    )


async def test_inventory_reference_upgrade_retains_legacy_and_matches_closed_root(serving_schema):
    connection, schema, _ = serving_schema
    identity = uuid4()
    await _candidate(connection, schema, identity)
    await _update(connection, schema, identity, list(range(100)))
    await connection.execute(_migration()._ddl(schema))
    stored = await connection.fetchval(
        f'SELECT source_recipes_json::text FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
        identity,
    )
    assert json.loads(stored) == list(range(100))
    root = source_inventory_root(())
    for count in (0, 101, 1000):
        metadata = replace(root, entry_count=count, encoded_bytes=100000, recipe_bytes=90000)
        reference = SourceInventoryReference(metadata).as_dict()
        await _update(connection, schema, identity, reference)
        assert (
            json.loads(
                await connection.fetchval(
                    f'SELECT source_recipes_json::text FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
                    identity,
                )
            )
            == reference
        )
    await _update(connection, schema, identity, [])


async def test_inventory_reference_check_rejects_open_or_unbounded_metadata(serving_schema):
    connection, schema, _ = serving_schema
    identity = uuid4()
    await _candidate(connection, schema, identity)
    await connection.execute(_migration()._ddl(schema))
    reference = SourceInventoryReference(source_inventory_root(())).as_dict()
    invalid_documents = [
        None,
        {},
        list(range(101)),
        {**reference, "extra": True},
        {**reference, "table_name": "other.heap"},
    ]
    for field, field_value in (
        ("entry_count", 1001),
        ("entry_count", None),
        ("entry_count", True),
        ("entry_count", 1.5),
        ("contribution_count", 1),
        ("derivation_count", 1),
        ("encoded_bytes", 67108865),
        ("recipe_bytes", 3),
        ("recipes_sha256", "x" * 64),
        ("entries_sha256", None),
        ("extra", 1),
    ):
        changed = deepcopy(reference)
        changed["root"][field] = field_value
        invalid_documents.append(changed)
    missing = deepcopy(reference)
    del missing["root"]["entries_sha256"]
    invalid_documents.append(missing)
    for document in invalid_documents:
        transaction = connection.transaction()
        await transaction.start()
        try:
            with pytest.raises(asyncpg.PostgresError):
                await _update(connection, schema, identity, document)
        finally:
            await transaction.rollback()
    assert (
        await connection.fetchval(
            f'SELECT source_recipes_json::text FROM "{schema}".network_membership_candidate WHERE candidate_id=$1',
            identity,
        )
        == "[]"
    )


async def test_inventory_schema_target_cannot_select_another_sql_namespace():
    migration = _migration()
    for schema in (None, "", "schema.table", 'scope"; SELECT 1', "a" * 64):
        with pytest.raises(ValueError):
            migration._ddl(schema)
