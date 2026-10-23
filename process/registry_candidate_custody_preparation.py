# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare only missing candidate custody without repeating committed registry changes."""

from sqlalchemy import text

from db.migration_network_candidate_custody import (
    REVISION,
    TABLE,
    candidate_custody_parent,
    validate_prepared_candidate_custody,
)
from db.migration_registry_preparation import registry_migration
from process.network_address_projection import _identifier
from process.registry_management_permissions import assume_registry_source_owner


def prepare_candidate_custody_table(connection, schema):
    """Create once or validate an incumbent; never repair its ownership, shape, or ACL."""
    namespace = _identifier(schema)
    connection.exec_driver_sql(f"LOCK TABLE {namespace}.network_membership_candidate IN SHARE MODE NOWAIT")
    before = candidate_custody_parent(connection, schema)
    exists = connection.execute(text("SELECT to_regclass(:name)"), {"name": namespace + "." + TABLE}).scalar_one()
    if exists is None:
        registry_migration(REVISION)._create(connection, schema)
    identity = validate_prepared_candidate_custody(connection, schema)
    if candidate_custody_parent(connection, schema) != before:
        raise RuntimeError("Candidate custody preparation changed existing parent identity or privileges")
    return {
        "component": "registry_candidate_custody_preparation",
        "prepared_revisions": [REVISION],
        "created": exists is None,
        **identity,
    }


async def prepare_registry_candidate_custody(connection, *, owner_role, control_schema):
    """Use existing maintenance authority after scoped writer admission and physical drain."""
    if not connection.in_transaction():
        raise ValueError("Candidate custody preparation requires a caller transaction")
    async with connection.begin_nested():
        raw = await connection.get_raw_connection()
        driver = raw.driver_connection
        caller = await driver.fetchval("SELECT quote_ident(current_user)")
        await assume_registry_source_owner(driver, owner_role, control_schema=control_schema)
        receipt = await connection.run_sync(prepare_candidate_custody_table, control_schema)
        await driver.execute(f"SET LOCAL ROLE {caller}")
        return receipt
