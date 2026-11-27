# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit operator plans and the fail-closed immutable lock-column shape."""

from sqlalchemy import text

from process.network_address_projection import _identifier
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError

LOCK_COLUMN = "registry_ptg_read_lock"
HEADER_TABLES = (
    "ptg2_snapshot",
    "ptg2_import_run",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_layout",
    "ptg2_snapshot_pin",
)
PIN_INSERT_COLUMNS = ("owner_type", "owner_id", "snapshot_id", "reason", "created_at")


def published_lock_provisioning_statements(schema_name, reader_role, approval_role):
    """Return explicit operator SQL only; never grant during a request or startup."""
    schema = _identifier(schema_name)
    readers = (_identifier(reader_role), _identifier(approval_role))
    if reader_role == approval_role:
        raise ValueError("registry_ptg_scope_store_unprotected")
    statements = []
    for table in HEADER_TABLES:
        relation = schema + "." + _identifier(table)
        statements.append(
            f"ALTER TABLE {relation} ADD COLUMN {LOCK_COLUMN} smallint NOT NULL DEFAULT 0 "
            f"CONSTRAINT {table}_registry_read_lock CHECK({LOCK_COLUMN}=0)"
        )
        statements.append(f"GRANT UPDATE({LOCK_COLUMN}) ON {relation} TO {','.join(readers)}")
    statements.append(f"GRANT INSERT({','.join(PIN_INSERT_COLUMNS)}) ON {schema}.ptg2_snapshot_pin TO {readers[1]}")
    return tuple(statements)


_LOCK_COLUMN_SQL = """
SELECT c.relname,
 a.attnotnull AND a.atttypid='smallint'::regtype AND NOT a.attisdropped
 AND EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='c' AND k.convalidated
    AND k.conkey=ARRAY[a.attnum]::smallint[]
    AND regexp_replace(pg_get_expr(k.conbin,k.conrelid),'[()\\s]','','g')=:constant_expression) AS protected
FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=:lock_column
WHERE n.nspname=:schema_name AND c.relname=ANY(CAST(:tables AS text[]))
ORDER BY c.relname
"""


async def require_published_lock_privileges(session, schema_name):
    """Require the constant lock column on every header; PostgreSQL enforces the grants."""
    _identifier(schema_name)
    if not session.in_transaction():
        raise RegistryPTGProducerScopeError("registry_ptg_scope_transaction_required")
    headers = (
        (
            await session.execute(
                text(_LOCK_COLUMN_SQL),
                {
                    "schema_name": schema_name,
                    "tables": HEADER_TABLES,
                    "lock_column": LOCK_COLUMN,
                    "constant_expression": LOCK_COLUMN + "=0",
                },
            )
        )
        .mappings()
        .all()
    )
    if (
        {header["relname"] for header in headers} != set(HEADER_TABLES)
        or len(headers) != len(HEADER_TABLES)
        or any(header["protected"] is not True for header in headers)
    ):
        raise RegistryPTGProducerScopeError("registry_ptg_published_lock_privileges_unprotected")


async def install_registry_ptg_office_preparation_permissions(connection, *, profile, store, reader_roles):
    """Install ordinary office preparation permissions in the caller's administrator transaction."""
    from types import SimpleNamespace

    from process.registry_fhir_catalog_permissions import (
        _grant_named_preparation_control,
        _revoke_named_preparation_writes,
    )
    from process.registry_ptg_office_custody import office_preparation_roles
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore

    if not connection.is_in_transaction() or type(store) is not RegistryPTGProducerScopeStore:
        raise ValueError("registry_ptg_office_profile_unavailable")
    roles = office_preparation_roles(
        SimpleNamespace(
            office_custody=profile,
            owner_role=profile.owner_role,
            reader_roles=tuple(sorted({*reader_roles, store.approval_role})),
        )
    )
    publisher = _identifier(roles["publisher_role"])
    owner = _identifier(roles["namespace_owner_role"])
    applications = ",".join(_identifier(role) for role in roles["application_roles"])
    await connection.execute(f"REVOKE {owner} FROM {applications}")
    database = await connection.fetchval("SELECT quote_ident(current_database())")
    await connection.execute(f"GRANT CREATE ON DATABASE {database} TO {publisher}")
    await _grant_named_preparation_control(connection, roles["publisher_role"], store.approval_role)
    readers = sorted(set(reader_roles) - {roles["publisher_role"], store.approval_role})
    if readers:
        recipients = ",".join(_identifier(role) for role in readers)
        await connection.execute(f"GRANT USAGE ON SCHEMA hp_snapshot_retention TO {recipients}")
        tables = "hp_snapshot_retention.preparation,hp_snapshot_retention.preparation_claim,hp_snapshot_retention.preparation_relation,hp_snapshot_retention.relation"
        await connection.execute(f"REVOKE INSERT,UPDATE,DELETE,TRUNCATE ON {tables} FROM {recipients}")
        await _revoke_named_preparation_writes(connection, *readers)
        await connection.execute(f"GRANT SELECT ON {tables} TO {recipients}")
