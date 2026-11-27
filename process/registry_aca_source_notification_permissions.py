# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plan narrow accepted ACA inbox grants inside an authenticated operator transaction."""

from db.registry_aca_source_notification import INSERT_COLUMNS, TABLE
from process.network_address_projection import _identifier
from process.registry_cms_source_notifications import _INBOX_SHAPE_SQL

# The inbox follows the serving control owner; the two flags preflight this caller's ALTER OWNER and GRANT.
_OWNER_SQL = """SELECT o.rolname,t.oid AS inbox_oid,
 pg_has_role(current_user,o.oid,'SET') AS can_assume_owner,
 has_schema_privilege(current_user,n.oid,'USAGE') AS schema_usage
 FROM pg_namespace n JOIN pg_class h ON h.relnamespace=n.oid
 JOIN pg_roles o ON o.oid=h.relowner
 JOIN pg_class t ON t.relnamespace=n.oid AND t.relname='registry_aca_source_notification'
 WHERE n.nspname=$1 AND h.relname='network_serving_control'
 AND h.relkind='r' AND h.relpersistence='p'"""


async def plan_registry_aca_source_notification_grants(
    connection, *, control_schema, publisher_role, reader_roles, runtime_roles
):
    """Return SQL only; the original operator applies it in this same transaction."""
    _identifier(control_schema)
    _identifier(publisher_role)
    if (
        type(reader_roles) is not tuple
        or not 1 <= len(reader_roles) <= 32
        or any(type(role) is not str for role in reader_roles)
        or tuple(sorted(set(reader_roles))) != reader_roles
        or publisher_role in reader_roles
        or type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 128
        or any(type(role) is not str for role in runtime_roles)
        or tuple(sorted(set(runtime_roles))) != runtime_roles
        or not {publisher_role, *reader_roles} <= set(runtime_roles)
        or not connection.is_in_transaction()
    ):
        raise ValueError("registry_aca_source_grants_invalid")
    for role in runtime_roles:
        _identifier(role)
    owner = await connection.fetchrow(_OWNER_SQL, control_schema)
    roles = tuple(sorted({publisher_role, *reader_roles}))
    if (
        owner is None
        or owner["rolname"] in roles
        or owner["can_assume_owner"] is not True
        or owner["schema_usage"] is not True
    ):
        raise ValueError("registry_aca_source_grants_invalid")
    if not await connection.fetchval(_INBOX_SHAPE_SQL, control_schema, TABLE):
        raise ValueError("registry_aca_source_grants_invalid")
    relation = _identifier(control_schema) + "." + TABLE
    configured = ",".join(_identifier(role) for role in roles)
    columns = ",".join(_identifier(column) for column in INSERT_COLUMNS)
    return (
        f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE",
        f"ALTER TABLE {relation} OWNER TO {_identifier(owner['rolname'])}",
        f"REVOKE ALL ON TABLE {relation} FROM PUBLIC",
        f"GRANT USAGE ON SCHEMA {_identifier(control_schema)} TO {configured}",
        f"GRANT SELECT ON TABLE {relation} TO {configured}",
        f"GRANT INSERT ({columns}) ON {relation} TO {_identifier(publisher_role)}",
    )


async def install_registry_aca_source_notification_permissions(
    connection, *, control_schema, publisher_role, reader_roles, runtime_roles
):
    """Apply one native grant plan under the caller transaction."""
    arguments_by_name = dict(
        control_schema=control_schema,
        publisher_role=publisher_role,
        reader_roles=reader_roles,
        runtime_roles=runtime_roles,
    )
    if not connection.is_in_transaction():
        raise ValueError("registry_aca_source_grants_invalid")
    async with connection.transaction():
        plan = await plan_registry_aca_source_notification_grants(connection, **arguments_by_name)
        for statement in plan:
            await connection.execute(statement)
        relation = _identifier(control_schema) + "." + TABLE
        return {
            "component": "registry_aca_source_notification_permissions",
            "revision": 1,
            "runtime_role_count": len(runtime_roles),
            "table_oid": await connection.fetchval("SELECT to_regclass($1)::oid", relation),
        }


def run_command(arguments=None):
    """Enroll only explicit operator roles using existing protected database config."""
    import asyncio
    import json
    import sys

    from db.connection import Database
    from process.custom_import.cli import _receipt_only_database_output, _set_engine_echo
    from process.registry_source_import import _disconnect_database, _error_json, _ReceiptArgumentParser

    parser = _ReceiptArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--control-schema", required=True)
    parser.add_argument("--publisher-role", required=True)
    parser.add_argument("--reader-role", action="append", required=True)
    parser.add_argument("--runtime-role", action="append", required=True)
    parsed = parser.parse_args(arguments)

    async def install():
        """Install under the original configured operator connection and transaction."""
        database = Database()
        with _receipt_only_database_output(database):
            try:
                await database.connect()
                _set_engine_echo(database, False)
                async with database.acquire_driver() as connection, connection.transaction(isolation="repeatable_read"):
                    return await install_registry_aca_source_notification_permissions(
                        connection,
                        control_schema=parsed.control_schema,
                        publisher_role=parsed.publisher_role,
                        reader_roles=tuple(sorted(parsed.reader_role)),
                        runtime_roles=tuple(sorted(parsed.runtime_role)),
                    )
            finally:
                await _disconnect_database(database)

    try:
        receipt = asyncio.run(install())
    except KeyboardInterrupt, asyncio.CancelledError:
        print(_error_json("canceled"), file=sys.stderr)
        return 130
    except Exception:
        print(_error_json("permission_enrollment_failed"), file=sys.stderr)
        return 1
    print(json.dumps({"status": "ok", **receipt}, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(run_command())
