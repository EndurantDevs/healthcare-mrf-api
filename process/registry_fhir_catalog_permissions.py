# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only publication privileges, without manual approval or Source writes."""

from db.plan_release_storage import storage_identifier
from process.registry_management_permissions import REGISTRY_TABLES
from process.registry_publication_permissions import LIFECYCLE_COLUMNS
from process.registry_publication_worker_permissions import (
    publication_permission_context,
    verify_publication_queue_shape,
)

_UPDATES = {
    "registry_publication_request": tuple(sorted(LIFECYCLE_COLUMNS)),
    "network_serving_control": ("metadata_generation",),
    "registry_fhir_catalog_snapshot": ("published_at",),
    "registry_revision_control": ("id",),
}


async def _verify_revision_lock(connection, namespace):
    """The singleton key permits row locks without permitting revision changes."""
    valid = await connection.fetchval(
        "SELECT c.relkind='r' AND NOT c.relispartition AND a.attnotnull AND a.atttypid='int4'::regtype "
        "AND EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='p' "
        "AND k.convalidated AND k.conkey=ARRAY[a.attnum]) "
        "AND EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='c' "
        "AND k.convalidated AND k.conenforced AND k.conkey=ARRAY[a.attnum] "
        "AND pg_get_expr(k.conbin,k.conrelid)='(id = 1)') "
        "AND NOT EXISTS(SELECT FROM pg_trigger t WHERE t.tgrelid=c.oid AND NOT t.tgisinternal "
        "AND (t.tgtype & 16)=16) "
        "AND NOT EXISTS(SELECT FROM pg_rewrite r WHERE r.ev_class=c.oid) "
        "FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname='id' AND NOT a.attisdropped "
        "WHERE c.oid=to_regclass($1)",
        storage_identifier(namespace) + ".registry_revision_control",
    )
    if valid is not True:
        raise ValueError("registry_fhir_catalog_revision_lock_invalid")


async def verify_fhir_catalog_publication_permissions(connection, config, *, role=None):
    """Verify effective column rights for both existing queue worker identities."""
    if not connection.is_in_transaction() or config.metadata_runtime is None or not config.metadata_only:
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    if role is not None and (
        role not in {config.lifecycle_role, config.execution_role}
        or not await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", role)
    ):
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    namespace, oid = await publication_permission_context(
        connection, **config.permission_roles, control_schema=config.control_schema
    )
    await _verify_revision_lock(connection, namespace)
    column_privileges = await connection.fetch(
        """SELECT configured.rolname,r.oid=configured.oid AS direct_role,
      c.relname,a.attname,has_column_privilege(r.oid,c.oid,a.attnum,'SELECT') AS can_read,
      has_column_privilege(r.oid,c.oid,a.attnum,'INSERT') AS can_insert,
      has_column_privilege(r.oid,c.oid,a.attnum,'UPDATE') AS can_update,
      has_column_privilege(r.oid,c.oid,a.attnum,'REFERENCES') AS can_reference,
      has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN') AS broad_write
      FROM pg_roles configured JOIN pg_roles r ON r.oid=configured.oid OR pg_has_role(configured.oid,r.oid,'SET')
      CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      WHERE configured.rolname=ANY($1::text[]) AND n.oid=$2::oid AND c.relname=ANY($3::text[])""",
        [config.lifecycle_role, config.execution_role],
        oid,
        sorted(REGISTRY_TABLES),
    )
    for column in column_privileges:
        _validate_column(column, config)
    for actor in (config.lifecycle_role, config.execution_role):
        await verify_publication_queue_shape(connection, namespace, oid, actor)
        if await connection.fetchval(
            "SELECT pg_has_role($1::name,$2::name,'MEMBER') OR pg_has_role($1::name,$2::name,'SET') "
            "OR has_database_privilege($1::name,current_database(),'CREATE')",
            actor,
            config.physical_owner_role,
        ):
            raise ValueError("registry_fhir_catalog_permissions_invalid")


def _validate_column(column, config):
    is_execution_role = column["rolname"] == config.execution_role
    table, name = column["relname"], column["attname"]
    update = name in _UPDATES.get(table, ()) and (is_execution_role or table == "registry_publication_request")
    read = is_execution_role or table == "registry_publication_request"
    if column["can_insert"] or column["can_reference"] or column["broad_write"] or column["can_update"] and not update:
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    if column["direct_role"] and (column["can_read"] != read or column["can_update"] != update):
        raise ValueError("registry_fhir_catalog_permissions_invalid")


async def install_fhir_catalog_publication_permissions(connection, config):
    """Add only precise grants to explicitly selected, separately prepared identities."""
    if not connection.is_in_transaction() or config.metadata_runtime is None or not config.metadata_only:
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    namespace, _ = await publication_permission_context(
        connection, **config.permission_roles, control_schema=config.control_schema
    )
    await _verify_revision_lock(connection, namespace)
    schema = storage_identifier(namespace)
    execution, lifecycle = storage_identifier(config.execution_role), storage_identifier(config.lifecycle_role)
    await connection.execute(f"GRANT USAGE ON SCHEMA {schema} TO {execution},{lifecycle}")
    for table in sorted(REGISTRY_TABLES):
        await connection.execute(f"GRANT SELECT ON {schema}.{storage_identifier(table)} TO {execution}")
    await connection.execute(f"GRANT SELECT ON {schema}.registry_publication_request TO {lifecycle}")
    for table, columns in _UPDATES.items():
        recipients = f"{execution},{lifecycle}" if table == "registry_publication_request" else execution
        selected = ",".join(storage_identifier(column) for column in columns)
        await connection.execute(f"GRANT UPDATE({selected}) ON {schema}.{storage_identifier(table)} TO {recipients}")
    await verify_fhir_catalog_publication_permissions(connection, config)
