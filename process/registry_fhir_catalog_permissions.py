# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Metadata-only publication privileges, without manual approval or Source writes."""

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.network_membership_writer_closure import is_protected_writer_owner
from process.registry_management_permissions import REGISTRY_TABLES, verify_registry_management_permissions
from process.registry_publication_permissions import INSERT_COLUMNS, LIFECYCLE_COLUMNS
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

_PREPARATION_READS = frozenset(
    {
        "registry_revision_control",
        "registry_approved_record",
        "network_registry_identity",
        "network_registry_alias",
        "registry_fhir_catalog_snapshot",
        "network_serving_control",
        "network_serving_manifest",
        "network_membership_candidate",
        "network_candidate_custody",
        "registry_publication_request",
    }
)
_LOADER_INSERTS = {
    "network_registry_identity": ("allocation_key",),
    "network_registry_alias": (
        "source_system",
        "source_id",
        "alias_type",
        "alias_value",
        "scope_key",
        "network_id",
        "evidence_id",
    ),
    "registry_publication_request": tuple(sorted(INSERT_COLUMNS)),
}
_SNAPSHOT_INSERTS = tuple(column for column in SNAPSHOT_COLUMNS if column != "published_at")
_IDENTITY_SEQUENCE = "network_registry_identity_network_id_seq"


def _preparation_roles(config, loader_role, sealer_role):
    if config.metadata_runtime is None or not config.metadata_only:
        raise ValueError("registry_fhir_catalog_preparation_permissions_invalid")
    roles = (loader_role, sealer_role, *config.permission_roles.values())
    for role in roles:
        storage_identifier(role)
    if len(set(roles)) != len(roles) or loader_role not in config.loader_roles:
        raise ValueError("registry_fhir_catalog_preparation_permissions_invalid")
    return roles


async def _verify_preparation_roles(connection, config, loader_role, sealer_role, *, pending):
    for role in (loader_role, sealer_role):
        safe = await connection.fetchval(
            "SELECT r.rolcanlogin AND NOT EXISTS(SELECT FROM pg_roles reachable WHERE "
            "(reachable.oid=r.oid OR pg_has_role(r.oid,reachable.oid,'MEMBER')) AND "
            "(reachable.rolsuper OR reachable.rolcreatedb OR reachable.rolcreaterole "
            "OR reachable.rolreplication OR reachable.rolbypassrls)) "
            "AND NOT pg_has_role(r.oid,$2::name,'MEMBER') "
            "AND NOT EXISTS(SELECT FROM pg_auth_members m WHERE m.admin_option "
            "AND (m.member=r.oid OR pg_has_role(r.oid,m.member,'MEMBER'))) "
            "FROM pg_roles r WHERE r.rolname=$1",
            role,
            config.queue_owner_role,
        )
        if safe is not True:
            raise ValueError("registry_fhir_catalog_preparation_role_invalid")
    physical = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", config.physical_owner_role)
    if not is_protected_writer_owner(physical):
        raise ValueError("registry_fhir_catalog_preparation_owner_invalid")
    for member_role, inherit, can_set in ((loader_role, True, False), (config.physical_owner_role, False, True)):
        edges = await connection.fetch(
            "SELECT inherit_option,set_option,admin_option FROM pg_auth_members "
            "WHERE member=(SELECT oid FROM pg_roles WHERE rolname=$1) "
            "AND roleid=(SELECT oid FROM pg_roles WHERE rolname=$2)",
            sealer_role,
            member_role,
        )
        if any(
            (edge["inherit_option"], edge["set_option"], edge["admin_option"]) != (inherit, can_set, False)
            for edge in edges
        ):
            raise ValueError("registry_fhir_catalog_preparation_membership_invalid")
        actual = await connection.fetchrow(
            "SELECT pg_has_role($1::name,$2::name,'USAGE') AS inherited,"
            "pg_has_role($1::name,$2::name,'SET') AS can_set",
            sealer_role,
            member_role,
        )
        if (
            actual["inherited"]
            and not inherit
            or actual["can_set"]
            and not can_set
            or not pending
            and (actual["inherited"], actual["can_set"]) != (inherit, can_set)
        ):
            raise ValueError("registry_fhir_catalog_preparation_membership_invalid")
    if await connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'MEMBER') OR pg_has_role($2::name,$3::name,'MEMBER')",
        loader_role,
        config.physical_owner_role,
        config.queue_owner_role,
    ):
        raise ValueError("registry_fhir_catalog_preparation_membership_invalid")


async def _verify_preparation_columns(connection, oid, loader_role, sealer_role, *, pending):
    columns = await connection.fetch(
        """SELECT configured.rolname,r.oid=configured.oid AS direct_role,c.relname,a.attname,
        has_column_privilege(r.oid,c.oid,a.attnum,'SELECT') AS can_read,
        has_column_privilege(r.oid,c.oid,a.attnum,'INSERT') AS can_insert,
        has_column_privilege(r.oid,c.oid,a.attnum,'UPDATE,REFERENCES') AS extra_column,
        has_column_privilege(r.oid,c.oid,a.attnum,
          'SELECT WITH GRANT OPTION,INSERT WITH GRANT OPTION,UPDATE WITH GRANT OPTION,REFERENCES WITH GRANT OPTION') AS can_grant,
        has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN') AS broad_write
        FROM pg_roles configured JOIN pg_roles r ON r.oid=configured.oid
          OR pg_has_role(configured.oid,r.oid,'USAGE') OR pg_has_role(configured.oid,r.oid,'SET')
        CROSS JOIN pg_class c JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
        WHERE configured.rolname=ANY($1::text[]) AND c.relnamespace=$2::oid AND c.relkind IN ('r','p','v','m','f')""",
        [loader_role, sealer_role],
        oid,
    )
    for column in columns:
        table = column["relname"]
        inserts = (
            _SNAPSHOT_INSERTS
            if column["rolname"] == sealer_role and table == "registry_fhir_catalog_snapshot"
            else _LOADER_INSERTS.get(table, ())
        )
        expected = (table in _PREPARATION_READS, column["attname"] in inserts)
        actual = (column["can_read"], column["can_insert"])
        if (
            column["extra_column"]
            or column["can_grant"]
            or column["broad_write"]
            or any(allowed and not intended for allowed, intended in zip(actual, expected, strict=True))
            or column["direct_role"]
            and not pending
            and actual != expected
        ):
            raise ValueError("registry_fhir_catalog_preparation_columns_invalid")
    sequences = await connection.fetch(
        """SELECT r.oid=configured.oid AS direct_role,c.relname,
        has_sequence_privilege(r.oid,c.oid,'USAGE') AS can_use,
        has_sequence_privilege(r.oid,c.oid,'SELECT,UPDATE,USAGE WITH GRANT OPTION') AS extra
        FROM pg_roles configured JOIN pg_roles r ON r.oid=configured.oid
          OR pg_has_role(configured.oid,r.oid,'USAGE') OR pg_has_role(configured.oid,r.oid,'SET')
        CROSS JOIN pg_class c WHERE configured.rolname=ANY($1::text[]) AND c.relnamespace=$2::oid AND c.relkind='S'""",
        [loader_role, sealer_role],
        oid,
    )
    for sequence in sequences:
        is_identity_sequence = sequence["relname"] == _IDENTITY_SEQUENCE
        if (
            sequence["extra"]
            or sequence["can_use"]
            and not is_identity_sequence
            or sequence["direct_role"]
            and not pending
            and sequence["can_use"] != is_identity_sequence
        ):
            raise ValueError("registry_fhir_catalog_preparation_sequence_invalid")


async def _verify_native_identity(connection, sealer_role, execution_role, *, pending):
    for role in (sealer_role, execution_role):
        rights = await connection.fetchrow(
            "SELECT has_function_privilege($1::name,'pg_catalog.pg_control_system()','EXECUTE') AS can_execute,"
            "EXISTS(SELECT FROM pg_roles r WHERE (r.rolname=$1 "
            "OR pg_has_role($1::name,r.oid,'USAGE') OR pg_has_role($1::name,r.oid,'SET')) "
            "AND has_function_privilege(r.oid,'pg_catalog.pg_control_system()','EXECUTE WITH GRANT OPTION')) AS can_grant",
            role,
        )
        if rights["can_grant"] or not pending and not rights["can_execute"]:
            raise ValueError("registry_fhir_catalog_preparation_native_identity_invalid")


async def _verify_preparation(connection, config, loader_role, sealer_role, *, pending):
    """Check the existing control plane and permit only explicitly missing additive rights."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_preparation_transaction_required")
    _preparation_roles(config, loader_role, sealer_role)
    management = await verify_registry_management_permissions(
        connection,
        api_role=config.api_role,
        owner_role=config.queue_owner_role,
        control_schema=config.control_schema,
        catalog_snapshot_pending=pending,
    )
    namespace, oid = management["schema_name"], management["schema_oid"]
    await _verify_preparation_roles(connection, config, loader_role, sealer_role, pending=pending)
    await _verify_preparation_columns(connection, oid, loader_role, sealer_role, pending=pending)
    for role in (loader_role, sealer_role):
        rights = await connection.fetch(
            """SELECT r.oid=configured.oid AS direct_role,
            has_schema_privilege(r.oid,$2::oid,'USAGE') AS can_read,
            has_schema_privilege(r.oid,$2::oid,'CREATE,USAGE WITH GRANT OPTION') AS extra_schema,
            has_database_privilege(r.oid,current_database(),'CREATE') AS can_create,
            has_database_privilege(r.oid,current_database(),'TEMP') AS can_temp,
            has_database_privilege(r.oid,current_database(),'CREATE WITH GRANT OPTION,TEMP WITH GRANT OPTION') AS can_grant
            FROM pg_roles configured JOIN pg_roles r ON r.oid=configured.oid
              OR pg_has_role(configured.oid,r.oid,'USAGE') OR pg_has_role(configured.oid,r.oid,'SET')
            WHERE configured.rolname=$1""",
            role,
            oid,
        )
        if any(
            privilege["extra_schema"]
            or privilege["can_grant"]
            or privilege["direct_role"]
            and not pending
            and not all(privilege[name] for name in ("can_read", "can_create", "can_temp"))
            for privilege in rights
        ):
            raise ValueError("registry_fhir_catalog_preparation_namespace_invalid")
    if not pending and not await connection.fetchval(
        "SELECT has_database_privilege($1::name,current_database(),'CREATE')",
        config.physical_owner_role,
    ):
        raise ValueError("registry_fhir_catalog_preparation_owner_invalid")
    await _verify_native_identity(connection, sealer_role, config.execution_role, pending=pending)
    if not pending:
        await publication_permission_context(connection, **config.permission_roles, control_schema=namespace)
        for role in (loader_role, sealer_role):
            await verify_publication_queue_shape(connection, namespace, oid, role)
    return {
        "control_schema": namespace,
        "loader_role": loader_role,
        "sealer_role": sealer_role,
        "physical_owner_role": config.physical_owner_role,
    }


async def verify_fhir_catalog_preparation_permissions(connection, config, *, loader_role, sealer_role, role=None):
    """Verify operator-prepared catalog identities without granting or touching Source."""
    if role is not None and (
        role not in {loader_role, sealer_role}
        or not await connection.fetchval(
            "SELECT current_user=session_user AND current_user::text=$1",
            role,
        )
    ):
        raise ValueError("registry_fhir_catalog_preparation_direct_login_required")
    return await _verify_preparation(connection, config, loader_role, sealer_role, pending=False)


async def install_fhir_catalog_preparation_permissions(connection, config, *, loader_role, sealer_role):
    """Add exact catalog grants to existing operator roles; refuse drift atomically."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_preparation_transaction_required")
    async with connection.transaction():
        receipt = await _verify_preparation(connection, config, loader_role, sealer_role, pending=True)
        caller = await connection.fetchval("SELECT quote_ident(current_user)")
        namespace = storage_identifier(receipt["control_schema"])
        loader, sealer = storage_identifier(loader_role), storage_identifier(sealer_role)
        database = await connection.fetchval("SELECT quote_ident(current_database())")
        await connection.execute(
            f"GRANT CREATE ON DATABASE {database} TO {loader},{sealer},{storage_identifier(config.physical_owner_role)}"
        )
        await connection.execute(f"GRANT TEMP ON DATABASE {database} TO {loader},{sealer}")
        await connection.execute(f"GRANT {loader} TO {sealer} WITH ADMIN FALSE, INHERIT TRUE, SET FALSE")
        await connection.execute(
            f"GRANT {storage_identifier(config.physical_owner_role)} TO {sealer} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE"
        )
        for role in (sealer_role, config.execution_role):
            if not await connection.fetchval(
                "SELECT has_function_privilege($1::name,'pg_catalog.pg_control_system()','EXECUTE')", role
            ):
                await connection.execute(
                    f"GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO {storage_identifier(role)}"
                )
        await connection.execute(f"SET LOCAL ROLE {storage_identifier(config.queue_owner_role)}")
        await connection.execute(
            f"GRANT SELECT ON {namespace}.registry_fhir_catalog_snapshot TO {storage_identifier(config.api_role)}"
        )
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {loader},{sealer}")
        for table in sorted(_PREPARATION_READS):
            owner = await connection.fetchval(
                "SELECT quote_ident(pg_get_userbyid(relowner)) FROM pg_class WHERE oid=to_regclass($1)",
                namespace + "." + table,
            )
            await connection.execute(f"SET LOCAL ROLE {caller}")
            await connection.execute(f"SET LOCAL ROLE {owner}")
            await connection.execute(f"GRANT SELECT ON {namespace}.{storage_identifier(table)} TO {loader},{sealer}")
        await connection.execute(f"SET LOCAL ROLE {caller}")
        await connection.execute(f"SET LOCAL ROLE {storage_identifier(config.queue_owner_role)}")
        for table, columns in {**_LOADER_INSERTS, "registry_fhir_catalog_snapshot": _SNAPSHOT_INSERTS}.items():
            selected = ",".join(storage_identifier(column) for column in columns)
            recipient = sealer if table == "registry_fhir_catalog_snapshot" else loader
            await connection.execute(
                f"GRANT INSERT({selected}) ON {namespace}.{storage_identifier(table)} TO {recipient}"
            )
        await connection.execute(f"GRANT USAGE ON SEQUENCE {namespace}.{_IDENTITY_SEQUENCE} TO {loader}")
        await connection.execute(f"SET LOCAL ROLE {caller}")
        return await verify_fhir_catalog_preparation_permissions(
            connection, config, loader_role=loader_role, sealer_role=sealer_role
        )


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


async def _verify_publication_sequences(connection, schema_oid, config):
    if await connection.fetchval(
        """SELECT EXISTS(SELECT FROM pg_roles configured JOIN pg_roles r ON r.oid=configured.oid
        OR pg_has_role(configured.oid,r.oid,'USAGE') OR pg_has_role(configured.oid,r.oid,'SET')
        CROSS JOIN pg_class c WHERE configured.rolname=ANY($1::text[]) AND c.relnamespace=$2::oid
        AND c.relkind='S' AND has_sequence_privilege(r.oid,c.oid,'SELECT,USAGE,UPDATE'))""",
        [config.lifecycle_role, config.execution_role],
        schema_oid,
    ):
        raise ValueError("registry_fhir_catalog_publication_sequence_invalid")


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
    await _verify_publication_sequences(connection, oid, config)
    column_privileges = await connection.fetch(
        """SELECT configured.rolname,r.oid=configured.oid AS direct_role,
      c.relname,a.attname,has_column_privilege(r.oid,c.oid,a.attnum,'SELECT') AS can_read,
      has_column_privilege(r.oid,c.oid,a.attnum,'INSERT') AS can_insert,
      has_column_privilege(r.oid,c.oid,a.attnum,'UPDATE') AS can_update,
      has_column_privilege(r.oid,c.oid,a.attnum,'REFERENCES') AS can_reference,
      has_column_privilege(r.oid,c.oid,a.attnum,
        'SELECT WITH GRANT OPTION,INSERT WITH GRANT OPTION,UPDATE WITH GRANT OPTION,REFERENCES WITH GRANT OPTION') AS can_grant,
      has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN') AS broad_write
      FROM pg_roles configured JOIN pg_roles r ON r.oid=configured.oid
        OR pg_has_role(configured.oid,r.oid,'USAGE') OR pg_has_role(configured.oid,r.oid,'SET')
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
    if (
        column["can_insert"]
        or column["can_reference"]
        or column["can_grant"]
        or column["broad_write"]
        or column["can_update"]
        and not update
    ):
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    if column["direct_role"] and (column["can_read"] != read or column["can_update"] != update):
        raise ValueError("registry_fhir_catalog_permissions_invalid")


async def install_fhir_catalog_publication_permissions(connection, config):
    """Add only precise grants to explicitly selected, separately prepared identities."""
    if not connection.is_in_transaction() or config.metadata_runtime is None or not config.metadata_only:
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    async with connection.transaction():
        namespace, oid = await publication_permission_context(
            connection, **config.permission_roles, control_schema=config.control_schema
        )
        await _verify_revision_lock(connection, namespace)
        await _verify_publication_sequences(connection, oid, config)
        schema = storage_identifier(namespace)
        execution, lifecycle = storage_identifier(config.execution_role), storage_identifier(config.lifecycle_role)
        await connection.execute(f"GRANT USAGE ON SCHEMA {schema} TO {execution},{lifecycle}")
        for table in sorted(REGISTRY_TABLES):
            await connection.execute(f"GRANT SELECT ON {schema}.{storage_identifier(table)} TO {execution}")
        await connection.execute(f"GRANT SELECT ON {schema}.registry_publication_request TO {lifecycle}")
        for table, columns in _UPDATES.items():
            recipients = f"{execution},{lifecycle}" if table == "registry_publication_request" else execution
            selected = ",".join(storage_identifier(column) for column in columns)
            await connection.execute(
                f"GRANT UPDATE({selected}) ON {schema}.{storage_identifier(table)} TO {recipients}"
            )
        await verify_fhir_catalog_publication_permissions(connection, config)
