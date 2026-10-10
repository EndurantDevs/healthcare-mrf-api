# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit operator plans and fail-closed immutable-column lock privileges."""

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


_PERMISSION_SQL = """
SELECT c.relname,
 a.attnotnull AND a.atttypid='smallint'::regtype AND NOT a.attisdropped
 AND EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='c' AND k.convalidated
    AND k.conkey=ARRAY[a.attnum]::smallint[]
    AND regexp_replace(pg_get_expr(k.conbin,k.conrelid),'[()\\s]','','g')=:constant_expression)
 AND has_table_privilege(current_user,c.oid,'SELECT')
 AND has_column_privilege(current_user,c.oid,a.attnum,'UPDATE')
 AND NOT has_table_privilege(current_user,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 AND NOT EXISTS(SELECT FROM pg_attribute other WHERE other.attrelid=c.oid AND other.attnum>0 AND NOT other.attisdropped
    AND other.attname<>:lock_column AND has_column_privilege(current_user,c.oid,other.attnum,'UPDATE,REFERENCES'))
 AND NOT EXISTS(SELECT FROM pg_attribute inserted WHERE inserted.attrelid=c.oid AND inserted.attnum>0
    AND NOT inserted.attisdropped AND has_column_privilege(current_user,c.oid,inserted.attnum,'INSERT')
    AND NOT(:pin_writer AND c.relname='ptg2_snapshot_pin' AND inserted.attname=ANY(CAST(:pin_columns AS text[]))))
 AND NOT has_schema_privilege(current_user,n.oid,'CREATE')
 AND NOT EXISTS(SELECT FROM pg_class fact WHERE fact.relnamespace=n.oid AND fact.relkind IN('r','p')
    AND (has_table_privilege(current_user,fact.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
      OR EXISTS(SELECT FROM pg_attribute field WHERE field.attrelid=fact.oid AND field.attnum>0
        AND NOT field.attisdropped AND (
          (has_column_privilege(current_user,fact.oid,field.attnum,'UPDATE,REFERENCES')
           AND NOT(fact.relname=ANY(CAST(:tables AS text[])) AND field.attname=:lock_column))
          OR (has_column_privilege(current_user,fact.oid,field.attnum,'INSERT')
              AND NOT(:pin_writer AND fact.relname='ptg2_snapshot_pin'
                AND field.attname=ANY(CAST(:pin_columns AS text[])))))))) AS protected
FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid
JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=:lock_column
WHERE n.nspname=:schema_name AND c.relname=ANY(CAST(:tables AS text[]))
ORDER BY c.relname
"""


async def require_published_lock_privileges(session, schema_name, *, pin_writer=False):
    """Refuse broad/inherited header or pin writes before using original row locks."""
    _identifier(schema_name)
    if type(pin_writer) is not bool:
        raise ValueError("registry_ptg_published_lock_privileges_unprotected")
    if not session.in_transaction():
        raise RegistryPTGProducerScopeError("registry_ptg_scope_transaction_required")
    headers = (
        (
            await session.execute(
                text(_PERMISSION_SQL),
                {
                    "schema_name": schema_name,
                    "tables": HEADER_TABLES,
                    "lock_column": LOCK_COLUMN,
                    "constant_expression": LOCK_COLUMN + "=0",
                    "pin_writer": pin_writer,
                    "pin_columns": PIN_INSERT_COLUMNS,
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
