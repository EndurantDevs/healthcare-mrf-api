# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read the complete named CMS input group within a caller-owned transaction."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db.connection import Database
from db.plan_release_storage import storage_identifier
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

_ORIGIN_SQL = """SELECT current_setting('transaction_read_only') AS readonly,
 current_setting('transaction_isolation') AS isolation,
 current_setting('session_replication_role') AS replication_role"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_prepared_context_" + code)


def _copy(value):
    return json.loads(json.dumps(value, allow_nan=False))


def _inputs(source_binding, source_policy, prepared_policy, inventory):
    _require(
        type(source_binding) is FHIRSourceStorageBinding
        and type(source_policy) is GenericSourcePolicy
        and type(prepared_policy) is GenericSourcePolicy,
        "explicit_coordinates_required",
    )
    _require(source_policy.schema == prepared_policy.schema == source_binding.storage_schema, "schema_changed")
    names = (
        source_policy.owner_role,
        source_policy.publisher_role,
        prepared_policy.owner_role,
        prepared_policy.publisher_role,
    )
    _require(len(set(names)) == 4, "roles_overlap")
    _require(type(inventory) is dict and set(inventory) == {"relations"}, "inventory_invalid")
    relations = inventory["relations"]
    _require(type(relations) is list and len(relations) == 8, "inventory_invalid")
    for relation in relations:
        _require(type(relation) is dict and set(relation) == {"schema_name", "relation_name"}, "inventory_invalid")
        storage_identifier(relation["relation_name"])
        _require(relation["schema_name"] == source_binding.storage_schema, "frozen_coordinates_changed")
    _require(len({relation["relation_name"] for relation in relations}) == 8, "inventory_invalid")
    return _copy(
        {
            "source_storage": asdict(source_binding),
            "source_policy": asdict(source_policy),
            "prepared_policy": asdict(prepared_policy),
            "frozen_inventory": {"relations": relations},
        }
    )


async def _physical(database, session, connection, task, transaction):
    """Keep the application transaction/task boundary without comparing database identities."""
    _require(asyncio.current_task() is task, "original_task_required")
    _require(session.in_transaction() and not session.in_nested_transaction(), "original_transaction_required")
    _require(database._transaction_binding(borrowed_only=True) is not None, "borrowed_session_required")
    if not connection.is_in_transaction():
        await session.execute(text("SELECT 1"))
    _require(connection.is_in_transaction(), "native_transaction_required")


async def _origin(connection, source_binding, prepared_policy):
    row = await connection.fetchrow(_ORIGIN_SQL)
    _require(
        row is not None
        and row["readonly"] == "off"
        and row["replication_role"] == "origin"
        and row["isolation"] in {"read committed", "repeatable read", "serializable"},
        "writable_transaction_required",
    )
    return {key: row[key] for key in ("readonly", "isolation", "replication_role")}


async def _observe(connection, source_binding, source_policy, prepared_policy, inventory):
    """Require real SELECT access to every qualified input through the named kernel."""
    from process.provider_directory_generic_source_storage import _capture

    names = [relation["relation_name"] for relation in inventory["relations"]]
    await _capture(connection, prepared_policy, names)
    return _copy(inventory)


@asynccontextmanager
async def observe_prepared_cms_inputs(
    database, session, connection, *, source_binding, source_policy, prepared_policy, frozen_inventory
):
    """Read named inputs without granting rights or taking over the caller's transaction.

    Import orchestration closes candidate writes before this operation. PostgreSQL
    enforces access; preparation still revalidates the complete candidate content.
    The caller owns commit, rollback, cancellation and the configured SQL roles.
    """
    _require(isinstance(database, Database), "database_required")
    _require(isinstance(session, AsyncSession), "session_required")
    expected = _inputs(source_binding, source_policy, prepared_policy, frozen_inventory)
    task, transaction = asyncio.current_task(), session.get_transaction()
    _require(task is not None and transaction is not None, "original_transaction_required")

    async def observe():
        """Read the unchanged input names within the caller transaction."""
        _require(
            _inputs(source_binding, source_policy, prepared_policy, frozen_inventory) == expected, "inputs_changed"
        )
        await _physical(database, session, connection, task, transaction)
        modes = await _origin(connection, source_binding, prepared_policy)
        inventory = await _observe(
            connection, source_binding, source_policy, prepared_policy, expected["frozen_inventory"]
        )
        return {**expected, "origin": modes, "native_coordinates": inventory}

    original = await observe()
    yield _copy(original)
    _require(await observe() == original, "inputs_changed")
