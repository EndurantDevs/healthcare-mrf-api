# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrative setup and transfer of the existing candidate control ledger."""

from sqlalchemy import text

from db.migration_network_candidate_custody import REVISION, TABLE
from db.migration_registry_preparation import registry_migration
from db.models.network_serving import NetworkCandidateCustody
from process.network_address_projection import _identifier
from process.registry_management_permissions import _administrative_role


def prepare_candidate_custody_table(connection, schema):
    """Create the existing ledger when absent and retain its fixed data columns."""
    namespace = _identifier(schema)
    connection.exec_driver_sql(f"LOCK TABLE {namespace}.network_membership_candidate IN SHARE MODE NOWAIT")
    exists = connection.scalar(text("SELECT to_regclass(:name) IS NOT NULL"), {"name": namespace + "." + TABLE})
    if not exists:
        registry_migration(REVISION)._create(connection, schema)
    columns = list(
        connection.execute(
            text(
                "SELECT a.attname FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid "
                "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relname=:table "
                "AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
            ),
            {"schema": schema, "table": TABLE},
        ).scalars()
    )
    if tuple(columns) != tuple(NetworkCandidateCustody.__table__.columns.keys()):
        raise RuntimeError("Candidate control ledger columns differ")
    return {
        "component": "registry_candidate_custody_preparation",
        "prepared_revisions": [REVISION],
        "created": not exists,
        "schema_name": schema,
        "table_name": TABLE,
    }


async def prepare_registry_candidate_custody(connection, *, owner_role, control_schema):
    """Apply existing administrator role permissions in the caller-owned transaction."""
    if not connection.in_transaction():
        raise ValueError("Candidate preparation requires a caller transaction")
    async with connection.begin_nested():
        driver = (await connection.get_raw_connection()).driver_connection
        async with _administrative_role(driver, owner_role):
            return await connection.run_sync(prepare_candidate_custody_table, control_schema)


def _empty_candidate_controls(connection, schema):
    """Keep the original empty publication precondition under bounded name-based locks."""
    namespace = _identifier(schema)
    for table in (TABLE, "network_membership_candidate", "network_serving_control", "network_serving_manifest"):
        connection.exec_driver_sql(f"LOCK TABLE {namespace}.{_identifier(table)} IN ACCESS EXCLUSIVE MODE NOWAIT")
    for table in (TABLE, "network_membership_candidate", "network_serving_manifest"):
        if connection.scalar(text(f"SELECT EXISTS(SELECT FROM {namespace}.{_identifier(table)})")):
            raise RuntimeError("Candidate ledger transfer requires empty publication storage")
    if (
        connection.scalar(
            text(
                f"SELECT count(*)=1 AND bool_and(id=1 AND generation_id IS NULL) FROM {namespace}.network_serving_control"
            )
        )
        is not True
    ):
        raise RuntimeError("Candidate ledger transfer requires the original empty serving head")


def _split_empty_candidate_custody(connection, schema, custody_owner_role, caller):
    """Transfer the empty ledger and preserve existing direct SELECT recipients."""
    namespace = _identifier(schema)
    _empty_candidate_controls(connection, schema)
    readers = list(
        connection.execute(
            text(
                "SELECT DISTINCT role.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) acl "
                "JOIN pg_roles role ON role.oid=acl.grantee WHERE n.nspname=:schema AND c.relname=:table "
                "AND acl.privilege_type='SELECT' ORDER BY role.rolname LIMIT 129"
            ),
            {"schema": schema, "table": TABLE},
        ).scalars()
    )
    if len(readers) > 128:
        raise RuntimeError("Candidate ledger reader list exceeds its bound")
    connection.exec_driver_sql(f"SET LOCAL ROLE {caller}")
    connection.exec_driver_sql(f"ALTER TABLE {namespace}.{TABLE} OWNER TO {_identifier(custody_owner_role)}")
    connection.exec_driver_sql(f"SET LOCAL ROLE {_identifier(custody_owner_role)}")
    for reader in readers:
        connection.exec_driver_sql(f"GRANT SELECT ON {namespace}.{TABLE} TO {_identifier(reader)}")
    return {"schema_name": schema, "table_name": TABLE, "reader_roles": readers}


async def split_registry_candidate_custody(connection, *, owner_role, custody_owner_role, control_schema):
    """Use the existing administrator route for an atomic empty-ledger owner transfer."""
    if not connection.in_transaction():
        raise ValueError("Candidate ledger transfer requires a caller transaction")
    for name in (owner_role, custody_owner_role, control_schema):
        _identifier(name)
    if owner_role == custody_owner_role:
        raise ValueError("Candidate ledger transfer requires a separate owner")
    async with connection.begin_nested():
        driver = (await connection.get_raw_connection()).driver_connection
        caller = await driver.fetchval("SELECT quote_ident(current_user)")
        async with _administrative_role(driver, owner_role):
            receipt = await connection.run_sync(
                _split_empty_candidate_custody, control_schema, custody_owner_role, caller
            )
        return {
            "component": "registry_candidate_custody_owner_split",
            "revision": 1,
            "owner_role": owner_role,
            "custody_owner_role": custody_owner_role,
            **receipt,
        }
