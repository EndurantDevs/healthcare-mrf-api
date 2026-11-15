# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real public named-preparation bootstrap for disposable native fixtures."""

import json
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

from db.migration_named_preparation import TABLES, upgrade_named_preparation_catalog
from process.registry_named_preparation_records import PREPARATION, local_uuid


async def prepare_registry_catalog_foundation(fixture, monkeypatch=None):
    """Use the production metadata bootstrap and register schema cleanup before creation."""
    exists = await fixture.connection.fetchval("SELECT to_regnamespace('hp_snapshot_retention') IS NOT NULL")
    present_tables = await fixture.connection.fetch(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='hp_snapshot_retention' "
        "AND table_name::text=ANY($1::text[])",
        list(TABLES),
    )
    if not hasattr(fixture, "named_preparation_created_tables"):
        fixture.named_preparation_created_tables = []
    if not present_tables:
        fixture.named_preparation_created_tables.extend(TABLES)
    if not exists:
        fixture.created_control_namespaces.append("hp_snapshot_retention")
    async with fixture.engine.begin() as connection:
        await connection.run_sync(upgrade_named_preparation_catalog)


@asynccontextmanager
async def registry_preparation_foundation(fixture):
    """Install the catalog for one fixture scope and retire exactly what this scope created."""
    exists = await fixture.connection.fetchval("SELECT to_regnamespace('hp_snapshot_retention') IS NOT NULL")
    present = await fixture.connection.fetchval(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='hp_snapshot_retention' "
        "AND table_name::text=ANY($1::text[])",
        list(TABLES),
    )
    async with fixture.engine.begin() as connection:
        await connection.run_sync(upgrade_named_preparation_catalog)
    try:
        yield
    finally:
        scope = SimpleNamespace(
            connection=fixture.connection,
            named_preparation_created_tables=[] if present else list(TABLES),
            created_control_namespaces=[] if exists else ["hp_snapshot_retention"],
        )
        await cleanup_registry_preparation_foundation(scope)


async def freeze_named_preparation(engine, handoff, preparation_roles):
    """Run the separately owned administrator freeze for one handed-off named preparation."""
    from process.registry_named_preparation_administration import _freeze_registry_generation

    config_by_field = {
        **{field: handoff[field] for field in ("kind", "candidate_id", "catalog_schema", "storage_schema")},
        "preparation_roles": preparation_roles,
    }
    async with engine.connect() as administrator:
        frozen = await _freeze_registry_generation(administrator, config_by_field, handoff["candidate_id"])
    assert frozen["preparation_id"] == handoff["candidate_id"]
    assert frozen["state"] in {"frozen", "validated", "published"}
    return frozen


async def freeze_and_validate_network_candidate(fixture, copy_target, roles, **options):
    """Hand off, freeze as the administrator and validate one candidate, as the worker and operator do."""
    from process.network_membership_publication import (
        NetworkPreparationPending,
        load_named_network_preparation,
        validate_named_network_preparation,
    )

    option_by_name = {"control_schema": fixture.control_schema, "roles": roles, **options}
    try:
        return await validate_named_network_preparation(fixture.connection, copy_target, **option_by_name)
    except NetworkPreparationPending:
        handoff = await load_named_network_preparation(
            fixture.connection, copy_target.candidate_id, control_schema=fixture.control_schema
        )
    assert handoff is not None and handoff["candidate_id"] == str(copy_target.candidate_id)
    await freeze_named_preparation(fixture.engine, handoff, roles)
    return await validate_named_network_preparation(fixture.connection, copy_target, **option_by_name)


@asynccontextmanager
async def network_publication_lifecycle(fixture, roles=None):
    """Give a single-connection fixture the catalog, role names and administrator step publication requires."""
    from process.registry_publication_worker import CONFIG_ENV

    roles = roles or {
        "namespace_owner_role": fixture.roles["owner"],
        "publisher_role": fixture.roles["reader"],
        "application_roles": [fixture.roles["reader"]],
    }

    async def seal_candidate(copy_target, **_options):
        return await freeze_and_validate_network_candidate(fixture, copy_target, roles)

    previous = os.environ.get(CONFIG_ENV)
    os.environ[CONFIG_ENV] = json.dumps({"preparation_roles": roles})
    try:
        async with registry_preparation_foundation(fixture):
            yield SimpleNamespace(preparation_roles=roles, seal_candidate=seal_candidate)
    finally:
        if previous is None:
            os.environ.pop(CONFIG_ENV, None)
        else:
            os.environ[CONFIG_ENV] = previous


async def retire_empty_registry_catalog_placeholder(connection):
    """Remove only the original empty placeholder; a catalog an outer fixture installed stays."""
    is_installed = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_catalog.pg_class WHERE relnamespace=to_regnamespace('hp_snapshot_retention'))"
    )
    if not is_installed:
        await connection.execute("DROP SCHEMA hp_snapshot_retention")


async def cleanup_registry_network_preparations(connection, candidate_ids, control_schema):
    """Delete only exact fixture candidates after locking and checking their logical scope."""
    identities = list(dict.fromkeys(local_uuid(candidate_id) for candidate_id in candidate_ids))
    if not identities:
        return
    async with connection.transaction():
        preparations = await connection.fetch(
            f"SELECT preparation_id,node_id,importer_id FROM {PREPARATION} "
            "WHERE preparation_id=ANY($1::uuid[]) FOR UPDATE NOWAIT",
            identities,
        )
        if any(row["node_id"] != control_schema or row["importer_id"] != "network" for row in preparations):
            raise RuntimeError("registry fixture preparation scope differs")
        for table in (PREPARATION + "_relation", PREPARATION + "_claim", PREPARATION):
            await connection.execute(f"DELETE FROM {table} WHERE preparation_id=ANY($1::uuid[])", identities)


async def cleanup_registry_preparation_foundation(fixture, *, primary=None):
    """Retire only the exact tables registered before this fixture's bootstrap."""
    try:
        created_tables = getattr(fixture, "named_preparation_created_tables", ())
        if any(table not in TABLES for table in created_tables):
            raise RuntimeError("named preparation fixture cleanup scope differs")
        for table in ("preparation_relation", "preparation_claim", "preparation", "relation"):
            if table in created_tables:
                await fixture.connection.execute(f'DROP TABLE IF EXISTS hp_snapshot_retention."{table}" RESTRICT')
        for namespace in reversed(fixture.created_control_namespaces):
            if namespace != "hp_snapshot_retention":
                raise RuntimeError("named preparation fixture cleanup namespace differs")
            await fixture.connection.execute('DROP SCHEMA IF EXISTS "hp_snapshot_retention" RESTRICT')
    except BaseException as cleanup_error:
        if primary is None:
            raise
        primary.add_note("Named preparation fixture cleanup also failed.")
        raise primary from cleanup_error
