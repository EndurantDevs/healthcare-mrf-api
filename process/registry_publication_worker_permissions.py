# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Precise native grants for separate publication lifecycle and execution logins."""

import asyncpg

from process.network_address_projection import _identifier
from process.network_membership_writer_closure import _protected_owner
from process.registry_management_permissions import (
    _TABLES,
    _catalog,
    _custody_ledger_owner,
    _roles,
    verify_registry_management_permissions,
)
from process.registry_publication_permissions import _CUSTODY_SQL, INSERT_COLUMNS, LIFECYCLE_COLUMNS

_INSERT = {
    "registry_approval_history": (
        "approved_revision",
        "previous_approved_revision",
        "expected_draft_revision",
        "actor_key",
        "actor_json",
        "selection_json",
        "reason",
        "idempotency_key",
        "request_sha256",
    ),
    "registry_approved_record": (
        "approved_revision",
        "record_kind",
        "record_key",
        "record_revision",
        "custom_revision",
        "record_json",
    ),
    "network_membership_candidate": (
        "candidate_id",
        "dataset_id",
        "schema_id",
        "producer_id",
        "schema_name",
        "source_generations",
        "approved_custom_revision",
        "expected_head",
        "expected_rows",
        "source_recipes_json",
        "validation_json",
    ),
    "network_serving_manifest": (
        "generation_id",
        "candidate_id",
        "schema_revision",
        "source_generations",
        "approved_custom_revision",
        "manifest_sha256",
    ),
}
_UPDATE = {
    "registry_publication_request": tuple(sorted(LIFECYCLE_COLUMNS)),
    "registry_revision_control": ("draft_revision", "approved_revision"),
    "network_membership_candidate": ("accepted_rows", "state", "validation_json", "index_ready"),
    "network_serving_control": ("generation_id",),
}
_SEQUENCE = "network_serving_manifest_generation_id_seq"


class RegistryPublicationWorkerPermissionError(ValueError):
    """Native worker identities or exact publication privileges are unsafe."""


async def _context(
    connection, lifecycle_role, execution_role, api_role, queue_owner_role, physical_owner_role, control_schema
):
    from process.network_membership_candidate_lifecycle import _control_namespace

    names = (lifecycle_role, execution_role, api_role, queue_owner_role, physical_owner_role)
    for name in names:
        _identifier(name)
    if len(set(names)) != len(names):
        raise RegistryPublicationWorkerPermissionError("Publication roles must be distinct")
    namespace = _control_namespace(control_schema)[1:-1]
    await verify_registry_management_permissions(
        connection, api_role=api_role, owner_role=queue_owner_role, control_schema=namespace
    )
    for role in (lifecycle_role, execution_role):
        await _roles(connection, namespace, role, queue_owner_role)
        flags = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", role)
        if (
            flags is None
            or not flags["rolcanlogin"]
            or any(
                flags[name] for name in ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
            )
        ):
            raise RegistryPublicationWorkerPermissionError("Workers must be existing safe direct LOGIN roles")
    physical = await connection.fetchrow("SELECT * FROM pg_roles WHERE rolname=$1", physical_owner_role)
    if not _protected_owner(physical) or await connection.fetchval(
        "SELECT pg_has_role($1::name,$2::name,'MEMBER')", physical_owner_role, queue_owner_role
    ):
        raise RegistryPublicationWorkerPermissionError(
            "Physical owner must be protected and separate from queue ownership"
        )
    for role in (api_role, lifecycle_role):
        if await connection.fetchval("SELECT pg_has_role($1::name,$2::name,'MEMBER')", role, physical_owner_role):
            raise RegistryPublicationWorkerPermissionError("Runtime roles cannot assume the physical owner")
    oid = await connection.fetchval("SELECT oid FROM pg_namespace WHERE nspname=$1", namespace)
    return namespace, oid


async def _verify_columns(connection, namespace, lifecycle_role, execution_role):
    columns = await connection.fetch(
        """SELECT configured.rolname,r.oid=configured.oid AS direct_role,c.relname,a.attname,
      has_column_privilege(r.oid,c.oid,a.attnum,'SELECT') AS can_read,
      has_column_privilege(r.oid,c.oid,a.attnum,'INSERT') AS can_insert,
      has_column_privilege(r.oid,c.oid,a.attnum,'UPDATE') AS can_update,
      has_column_privilege(r.oid,c.oid,a.attnum,'REFERENCES') AS can_reference,
      has_table_privilege(r.oid,c.oid,'DELETE,TRUNCATE,TRIGGER,MAINTAIN') AS broad_write
      FROM pg_roles configured JOIN pg_roles r
        ON r.oid=configured.oid OR pg_has_role(configured.oid,r.oid,'SET')
      CROSS JOIN pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
      WHERE configured.rolname=ANY($1::text[]) AND n.nspname=$2 AND c.relname=ANY($3::text[])""",
        [lifecycle_role, execution_role],
        namespace,
        sorted(_TABLES),
    )
    for column in columns:
        is_execution = column["rolname"] == execution_role
        table, name = column["relname"], column["attname"]
        insert_columns = _INSERT.get(table, ()) if is_execution else ()
        update_columns = _UPDATE.get(table, ()) if is_execution or table == "registry_publication_request" else ()
        # A SET-only membership does not contribute to the LOGIN's effective ACLs.
        # They must still stay within its write policy when assumed explicitly.
        if (
            column["direct_role"]
            and (
                column["can_read"] != (is_execution or table == "registry_publication_request")
                or column["can_insert"] != (name in insert_columns)
                or column["can_update"] != (name in update_columns)
            )
            or column["can_insert"]
            and name not in insert_columns
            or column["can_update"]
            and name not in update_columns
            or column["can_reference"]
            or column["broad_write"]
        ):
            raise RegistryPublicationWorkerPermissionError("Worker effective column privileges differ")


async def _verify_queue(connection, namespace, schema_oid, role):
    queue = await connection.fetchrow(
        "SELECT c.oid::bigint,c.relowner::bigint,r.oid::bigint AS runtime_oid FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN pg_roles r WHERE n.nspname=$1 AND c.relname='registry_publication_request' AND r.rolname=$2",
        namespace,
        role,
    )
    if not await connection.fetchval(
        _CUSTODY_SQL,
        queue["oid"],
        schema_oid,
        queue["relowner"],
        queue["runtime_oid"],
        sorted(LIFECYCLE_COLUMNS),
        sorted(INSERT_COLUMNS),
    ):
        raise RegistryPublicationWorkerPermissionError("Queue audit custody is not closed")


async def _verify_sequences(connection, sequences, lifecycle_role, execution_role):
    """Constrain both direct grants and every identity workers may SET ROLE to."""
    privileges = await connection.fetch(
        """SELECT configured.rolname,r.oid=configured.oid AS direct_role,c.relname,
      has_sequence_privilege(r.oid,c.oid,'USAGE') AS use_sequence,
      has_sequence_privilege(r.oid,c.oid,'SELECT,UPDATE') AS extra
      FROM pg_roles configured JOIN pg_roles r
        ON r.oid=configured.oid OR pg_has_role(configured.oid,r.oid,'SET')
      CROSS JOIN pg_class c
      WHERE configured.rolname=ANY($1::text[]) AND c.oid=ANY($2::oid[])""",
        [lifecycle_role, execution_role],
        [sequence["oid"] for sequence in sequences],
    )
    for privilege in privileges:
        is_execution = privilege["rolname"] == execution_role and privilege["relname"] == _SEQUENCE
        if (
            privilege["extra"]
            or privilege["use_sequence"]
            and not is_execution
            or privilege["direct_role"]
            and privilege["use_sequence"] != is_execution
        ):
            raise RegistryPublicationWorkerPermissionError("Worker sequence privileges differ")


async def verify_registry_publication_worker_permissions(
    connection,
    *,
    lifecycle_role,
    execution_role,
    api_role,
    queue_owner_role,
    physical_owner_role,
    control_schema=None,
    role=None,
):
    """Check existing identities, queue custody and exact effective worker grants."""
    if not connection.is_in_transaction():
        raise RegistryPublicationWorkerPermissionError("Worker permission verification requires a caller transaction")
    if role is not None and (
        role not in {lifecycle_role, execution_role}
        or not await connection.fetchval("SELECT current_user=session_user AND current_user::text=$1", role)
    ):
        raise RegistryPublicationWorkerPermissionError("Worker connection must use its exact direct LOGIN identity")
    namespace, oid = await _context(
        connection, lifecycle_role, execution_role, api_role, queue_owner_role, physical_owner_role, control_schema
    )
    _, sequences = await _catalog(connection, oid)
    await _verify_columns(connection, namespace, lifecycle_role, execution_role)
    for role in (lifecycle_role, execution_role):
        await _verify_queue(connection, namespace, oid, role)
        if not await connection.fetchval("SELECT has_schema_privilege($1::name,$2::oid,'USAGE')", role, oid):
            raise RegistryPublicationWorkerPermissionError("Worker schema usage is unavailable")
    rights = await connection.fetchrow(
        """SELECT pg_has_role($1::name,$2::name,'SET') AS can_set,
      pg_has_role($1::name,$2::name,'USAGE') AS can_inherit,
      has_database_privilege($2::name,current_database(),'CREATE') AS can_create,
      EXISTS(SELECT 1 FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.roleid JOIN pg_roles member ON member.oid=m.member WHERE r.rolname=$2 AND member.rolname=$1 AND m.admin_option) AS can_admin""",
        execution_role,
        physical_owner_role,
    )
    if not rights["can_set"] or not rights["can_inherit"] or not rights["can_create"] or rights["can_admin"]:
        raise RegistryPublicationWorkerPermissionError("Execution physical ownership rights differ")
    await _verify_sequences(connection, sequences, lifecycle_role, execution_role)
    return {
        "control_schema": namespace,
        "lifecycle_role": lifecycle_role,
        "execution_role": execution_role,
        "queue_owner_role": queue_owner_role,
        "physical_owner_role": physical_owner_role,
    }


async def _grant_worker(connection, namespace, tables, role, *, execution, custody_owner_role=None):
    grantee = _identifier(role)
    await connection.execute(f"REVOKE ALL ON SCHEMA {_identifier(namespace)} FROM {grantee}")
    await connection.execute(f"GRANT USAGE ON SCHEMA {_identifier(namespace)} TO {grantee}")
    for table in tables:
        caller = await connection.fetchval("SELECT quote_ident(current_user)")
        if custody_owner_role and table["relname"] == "network_candidate_custody":
            await connection.execute(f"SET LOCAL ROLE {_identifier(custody_owner_role)}")
        relation = _identifier(namespace) + "." + _identifier(table["relname"])
        await connection.execute(f"REVOKE ALL ON TABLE {relation} FROM {grantee}")
        columns = ",".join(_identifier(name) for name in table["columns"])
        await connection.execute(f"REVOKE ALL({columns}) ON {relation} FROM {grantee}")
        if execution or table["relname"] == "registry_publication_request":
            await connection.execute(f"GRANT SELECT ON {relation} TO {grantee}")
        for privilege, policy in (("INSERT", _INSERT), ("UPDATE", _UPDATE)):
            allowed = (
                policy.get(table["relname"], ())
                if execution or table["relname"] == "registry_publication_request" and privilege == "UPDATE"
                else ()
            )
            if allowed:
                columns = ",".join(_identifier(name) for name in allowed)
                await connection.execute(f"GRANT {privilege}({columns}) ON {relation} TO {grantee}")
        await connection.execute(f"SET LOCAL ROLE {caller}")


async def _install_worker_metadata(connection, namespace, tables, sequences, roles, custody_owner):
    """Issue each metadata grant as its existing protected native owner."""
    queue_owner = roles["queue_owner_role"]
    lifecycle_role, execution_role = roles["lifecycle_role"], roles["execution_role"]
    original_role = await connection.fetchval("SELECT quote_ident(current_user)")
    await connection.execute(f"SET LOCAL ROLE {_identifier(queue_owner)}")
    for table in tables:
        if custody_owner and table["relname"] == "network_candidate_custody":
            await connection.execute(f"SET LOCAL ROLE {_identifier(custody_owner)}")
        await connection.execute(
            f"LOCK TABLE {_identifier(namespace)}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
        await connection.execute(f"SET LOCAL ROLE {_identifier(queue_owner)}")
    for role in (lifecycle_role, execution_role):
        await _grant_worker(
            connection,
            namespace,
            tables,
            role,
            execution=role == execution_role,
            custody_owner_role=custody_owner,
        )
        for sequence in sequences:
            await connection.execute(
                f"REVOKE ALL ON SEQUENCE {_identifier(namespace)}.{_identifier(sequence['relname'])} FROM {_identifier(role)}"
            )
    await connection.execute(
        f"GRANT USAGE ON SEQUENCE {_identifier(namespace)}.{_identifier(_SEQUENCE)} TO {_identifier(execution_role)}"
    )
    await connection.execute(f"SET LOCAL ROLE {original_role}")


async def install_registry_publication_worker_permissions(
    connection, *, lifecycle_role, execution_role, api_role, queue_owner_role, physical_owner_role, control_schema=None
):
    """Install one atomic exact policy on pre-existing roles; never create logins or credentials.

    A trusted operator owns the caller transaction and can assume both protected
    owners. Source-reader memberships and retained source ACLs are provisioned
    separately. Qualified composition uses no legacy batch receipt writes.
    """
    if not connection.is_in_transaction():
        raise RegistryPublicationWorkerPermissionError("Worker permission installation requires a caller transaction")
    options_by_name = dict(
        lifecycle_role=lifecycle_role,
        execution_role=execution_role,
        api_role=api_role,
        queue_owner_role=queue_owner_role,
        physical_owner_role=physical_owner_role,
        control_schema=control_schema,
    )
    try:
        async with connection.transaction():
            namespace, oid = await _context(connection, **options_by_name)
            tables, sequences = await _catalog(connection, oid)
            custody_owner = await _custody_ledger_owner(connection, namespace, tables, queue_owner_role)
            for owner in (queue_owner_role, physical_owner_role, *([custody_owner] if custody_owner else [])):
                if not await connection.fetchval("SELECT pg_has_role(current_user,$1::name,'SET')", owner):
                    raise RegistryPublicationWorkerPermissionError("Trusted installer cannot assume protected owners")
            await _install_worker_metadata(connection, namespace, tables, sequences, options_by_name, custody_owner)
            await connection.execute(
                f"GRANT CREATE ON DATABASE {_identifier(await connection.fetchval('SELECT current_database()'))} TO {_identifier(physical_owner_role)}"
            )
            await connection.execute(
                f"GRANT {_identifier(physical_owner_role)} TO {_identifier(execution_role)} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE"
            )
            return await verify_registry_publication_worker_permissions(connection, **options_by_name)
    except asyncpg.PostgresError:
        raise RegistryPublicationWorkerPermissionError("Worker permission installation was refused") from None
