# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pin exact allocation targets through parent read rights, without child grants."""

import asyncio

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_policy import _savepoint


def _require(condition, code):
    if not condition:
        raise ValueError("cms_allocation_pin_" + code)


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
    This pins objects, not their data, roles or callable implementation.
    """
    connection, schema = workspace.connection, workspace.catalog_schema
    parents_by_name = {
        entry.get("name", entry.get("relname")): entry
        for entry in inventory
        if entry.get("name", entry.get("relname")) in TABLES[:4]
    }
    _require(set(parents_by_name) == set(TABLES[:4]), "roots_required")
    from process.provider_directory_cms_prepared_allocation import generic_leaf_name

    expected_names = set(TABLES[:4]) | {
        generic_leaf_name(root, workspace.identity["dataset_id"])
        for root, parent in parents_by_name.items()
        if parent.get("kind", parent.get("relkind")) == "p"
    }
    _require(
        len(inventory) == len(expected_names)
        and {entry.get("name", entry.get("relname")) for entry in inventory} == expected_names
        and all(entry.get("schema", entry.get("nspname")) == schema for entry in inventory),
        "scope_changed",
    )
    async with _savepoint(connection), asyncio.timeout(3):
        settings_by_name = await _bounded_settings(connection)
        literal = await connection.fetchval(
            "SELECT pg_catalog.quote_literal($1::text)", workspace.identity["dataset_id"]
        )
        _require(type(literal) is str, "dataset_literal_required")
        for root in sorted(parents_by_name):
            parent = storage_relation(schema, root)
            await connection.execute("LOCK TABLE ONLY " + parent + " IN ACCESS SHARE MODE NOWAIT")
            await connection.execute(f"SELECT dataset_id FROM {parent} WHERE dataset_id={literal} LIMIT 0")
        for name, setting in settings_by_name.items():
            await connection.fetchval("SELECT pg_catalog.set_config($1,$2,true)", name, setting)
