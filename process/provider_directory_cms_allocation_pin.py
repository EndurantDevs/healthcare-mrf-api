# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin exact allocation targets through parent read rights, without child grants."""

import asyncio

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_policy import _savepoint

_LOCKS_SQL = """SELECT DISTINCT c.oid::bigint FROM pg_catalog.pg_locks l
 JOIN pg_catalog.pg_class c ON c.oid=l.relation
 JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE l.locktype='relation' AND l.pid=pg_catalog.pg_backend_pid() AND l.granted
   AND l.mode<>'SIReadLock' AND n.nspname=$1 AND c.relkind IN ('r','p')"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_allocation_pin_" + code)


async def _locks(connection, schema):
    return {entry["oid"] for entry in await connection.fetch(_LOCKS_SQL, schema)}


async def _bounded_settings(connection):
    settings_by_name = dict(
        await connection.fetchrow(
            "SELECT current_setting('lock_timeout') AS lock_timeout,"
            "current_setting('statement_timeout') AS statement_timeout"
        )
    )
    for name, bound in (("lock_timeout", "250ms"), ("statement_timeout", "2500ms")):
        await connection.fetchval(
            "SELECT pg_catalog.set_config($1,CASE WHEN current_setting($1)::interval=interval '0' "
            "OR current_setting($1)::interval>($2::text)::interval THEN $2::text ELSE current_setting($1) END,true)",
            name,
            bound,
        )
    return settings_by_name


async def pin_cms_allocation_targets(workspace, inventory):
    """Keep selected native objects pinned while preserving prior locks and settings.

    An exact constant permits static partition pruning even when a prepared
    parameter query would switch to a generic plan and lock unrelated children.
    Verify actual lock acquisition rather than assuming the planner pruned it.
    This pins objects, not their data, roles or callable implementation.
    """
    connection, schema = workspace.connection, workspace.catalog_schema
    expected_oids = {entry["oid"] for entry in inventory}
    parents_by_name = {entry["relname"]: entry for entry in inventory if entry["relname"] in TABLES[:4]}
    _require(set(parents_by_name) == set(TABLES[:4]), "roots_required")
    async with _savepoint(connection), asyncio.timeout(3):
        settings_by_name = await _bounded_settings(connection)
        before = await _locks(connection, schema)
        literal = await connection.fetchval(
            "SELECT pg_catalog.quote_literal($1::text)", workspace.identity["dataset_id"]
        )
        _require(type(literal) is str, "dataset_literal_required")
        for root in sorted(parents_by_name):
            parent = storage_relation(schema, root)
            await connection.execute("LOCK TABLE ONLY " + parent + " IN ACCESS SHARE MODE NOWAIT")
            await connection.execute(f"SELECT dataset_id FROM {parent} WHERE dataset_id={literal} LIMIT 0")
        after = await _locks(connection, schema)
        _require(expected_oids <= after and after - before <= expected_oids, "scope_changed")
        for name, setting in settings_by_name.items():
            await connection.fetchval("SELECT pg_catalog.set_config($1,$2,true)", name, setting)
