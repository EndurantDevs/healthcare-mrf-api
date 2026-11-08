# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Observe prepared CMS inputs on a borrowed session without granting authority.

The caller supplies the issued Source coordinates and authenticated frozen
inventory separately from the prepared publication policy. These checks compare
physical identities only. They establish neither immutable content nor writer,
callable, claim, profile or mutation-fence authority.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict

from sqlalchemy.ext.asyncio import AsyncSession

from db.connection import Database
from db.plan_release_storage import storage_identifier
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

_ORIGIN_SQL = """SELECT session_user::text AS session,current_user::text AS current,
 pg_catalog.pg_backend_pid() AS backend,
 (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=pg_catalog.current_database()) AS database_oid,
 pg_catalog.current_setting('transaction_read_only') AS readonly,
 pg_catalog.current_setting('transaction_isolation') AS isolation,
 pg_catalog.current_setting('session_replication_role') AS replication_role,
 (SELECT virtualtransaction FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid()
  AND locktype='virtualxid' AND granted) AS transaction"""
_COORDINATES_SQL = """SELECT c.oid::bigint AS catalog_schema_oid,s.oid::bigint AS storage_schema_oid,
 s.nspowner::bigint AS namespace_owner_oid,so.oid::bigint AS source_owner_oid,
 so.rolcanlogin AS source_owner_login,sp.rolcanlogin AS source_publisher_login,
 po.oid::bigint AS prepared_owner_oid,po.rolcanlogin AS prepared_owner_login,
 pp.rolcanlogin AS prepared_publisher_login
 FROM pg_catalog.pg_namespace c,pg_catalog.pg_namespace s,
 pg_catalog.pg_roles so,pg_catalog.pg_roles sp,pg_catalog.pg_roles po,pg_catalog.pg_roles pp
 WHERE c.nspname=$1 AND s.nspname=$2 AND so.rolname=$3 AND sp.rolname=$4
 AND po.rolname=$5 AND pp.rolname=$6"""
_INVENTORY_SQL = """SELECT c.oid::bigint AS relation_oid,c.relfilenode::bigint AS relfilenode,
 c.relowner::bigint AS owner_oid,n.oid::bigint AS schema_oid,n.nspowner::bigint AS schema_owner_oid,
 n.nspname AS schema_name,c.relname AS relation_name,
 c.relkind='r' AND c.relpersistence='p' AND NOT c.relispartition
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AS supported
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=ANY($1::oid[]) ORDER BY c.oid"""
_RELATION_FIELDS = {
    "schema_name",
    "relation_name",
    "relation_oid",
    "schema_oid",
    "schema_owner_oid",
    "owner_oid",
    "relfilenode",
}


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
    _require(type(inventory) is dict and set(inventory) == {"database_oid", "relations"}, "inventory_invalid")
    _require(inventory["database_oid"] == source_binding.database_oid, "inventory_invalid")
    relations = inventory["relations"]
    _require(type(relations) is list and len(relations) == 8, "inventory_invalid")
    for inventory_relation in relations:
        _require(type(inventory_relation) is dict and set(inventory_relation) == _RELATION_FIELDS, "inventory_invalid")
        _require(
            all(
                type(inventory_relation[key]) is int and 0 < inventory_relation[key] < 2**32
                for key in _RELATION_FIELDS - {"schema_name", "relation_name"}
            ),
            "inventory_invalid",
        )
        storage_identifier(inventory_relation["relation_name"])
        _require(
            inventory_relation["schema_name"] == source_binding.storage_schema
            and inventory_relation["schema_oid"] == source_binding.storage_schema_oid
            and inventory_relation["schema_owner_oid"] == source_binding.namespace_owner_oid
            and inventory_relation["owner_oid"] != source_binding.owner_oid,
            "frozen_coordinates_changed",
        )
    _require(
        len({inventory_relation["relation_oid"] for inventory_relation in relations})
        == len({inventory_relation["relation_name"] for inventory_relation in relations})
        == 8,
        "inventory_invalid",
    )
    return _copy(
        {
            "source_storage": asdict(source_binding),
            "source_policy": asdict(source_policy),
            "prepared_policy": asdict(prepared_policy),
            "frozen_inventory": {
                "database_oid": inventory["database_oid"],
                "relations": sorted(relations, key=lambda inventory_relation: inventory_relation["relation_oid"]),
            },
        }
    )


async def _physical(database, session, connection, task, transaction):
    _require(asyncio.current_task() is task, "original_task_required")
    _require(
        session.in_transaction() and not session.in_nested_transaction() and session.get_transaction() is transaction,
        "original_transaction_required",
    )
    binding = database._transaction_binding(borrowed_only=True)
    _require(binding is not None and binding.session is session, "borrowed_session_required")
    physical = await session.connection()
    _require(
        (await physical.get_raw_connection()).driver_connection is connection
        and not connection.is_closed()
        and connection.is_in_transaction(),
        "physical_connection_changed",
    )


async def _origin(connection, source_binding, prepared_policy):
    row = await connection.fetchrow(_ORIGIN_SQL)
    _require(
        row is not None
        and row["session"] == row["current"] == prepared_policy.publisher_role
        and row["backend"] == connection.get_server_pid()
        and row["database_oid"] == source_binding.database_oid
        and row["readonly"] == "off"
        and row["replication_role"] == "origin"
        and row["isolation"] in {"read committed", "repeatable read", "serializable"}
        and type(row["transaction"]) is str
        and bool(row["transaction"]),
        "original_publisher_required",
    )
    return dict(row)


async def _observe(connection, source_binding, source_policy, prepared_policy, inventory):
    coordinates = await connection.fetchrow(
        _COORDINATES_SQL,
        source_binding.catalog_schema,
        source_binding.storage_schema,
        source_policy.owner_role,
        source_policy.publisher_role,
        prepared_policy.owner_role,
        prepared_policy.publisher_role,
    )
    _require(
        coordinates is not None
        and coordinates["catalog_schema_oid"] == source_binding.catalog_schema_oid
        and coordinates["storage_schema_oid"] == source_binding.storage_schema_oid
        and coordinates["namespace_owner_oid"] == source_binding.namespace_owner_oid
        and coordinates["source_owner_oid"] == source_binding.owner_oid
        and coordinates["prepared_owner_oid"] not in {source_binding.owner_oid, source_binding.namespace_owner_oid}
        and coordinates["source_owner_login"] is False
        and coordinates["prepared_owner_login"] is False
        and coordinates["source_publisher_login"] is True
        and coordinates["prepared_publisher_login"] is True,
        "native_coordinates_changed",
    )
    native_relations = [
        dict(native_relation)
        for native_relation in await connection.fetch(
            _INVENTORY_SQL, [native_relation["relation_oid"] for native_relation in inventory["relations"]]
        )
    ]
    _require(
        all(
            native_relation.pop("supported") is True
            and native_relation["owner_oid"] == coordinates["prepared_owner_oid"]
            for native_relation in native_relations
        )
        and native_relations == inventory["relations"],
        "frozen_inventory_changed",
    )
    return dict(coordinates)


@asynccontextmanager
async def observe_prepared_cms_inputs(
    database, session, connection, *, source_binding, source_policy, prepared_policy, frozen_inventory
):
    """Compare native input identities within an existing prepared-session bridge.

    Supply the original AsyncSession and driver from the trusted publisher bridge,
    unchanged Source manifest coordinates/policy, separate prepared policy, and
    the authenticated preparation's frozen inventory. No verifier callback or
    detached inventory can supply provenance here. The caller must authenticate
    those inputs independently and owns commit, rollback and cancellation.

    The yielded detached data is an observation, never a capability. No locks,
    SOURCE contexts or role changes are installed. A real finisher still needs
    qualified CMS writer/callable custody and a fence held through receipt commit.
    """
    _require(isinstance(database, Database), "database_required")
    _require(isinstance(session, AsyncSession), "session_required")
    expected = _inputs(source_binding, source_policy, prepared_policy, frozen_inventory)
    task, transaction = asyncio.current_task(), session.get_transaction()
    _require(task is not None and transaction is not None, "original_transaction_required")

    async def observe():
        """Recheck the same physical session, actor and frozen input identities."""
        _require(
            _inputs(source_binding, source_policy, prepared_policy, frozen_inventory) == expected, "inputs_changed"
        )
        await _physical(database, session, connection, task, transaction)
        before = await _origin(connection, source_binding, prepared_policy)
        coordinates = await _observe(
            connection, source_binding, source_policy, prepared_policy, expected["frozen_inventory"]
        )
        _require(await _origin(connection, source_binding, prepared_policy) == before, "native_transaction_changed")
        await _physical(database, session, connection, task, transaction)
        _require(
            _inputs(source_binding, source_policy, prepared_policy, frozen_inventory) == expected, "inputs_changed"
        )
        return {**expected, "origin": before, "native_coordinates": coordinates}

    original = await observe()
    yield _copy(original)
    _require(await observe() == original, "native_evidence_changed")
