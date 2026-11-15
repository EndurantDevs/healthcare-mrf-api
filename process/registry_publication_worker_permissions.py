# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrative grants for separate publication lifecycle and execution logins."""

import asyncpg

from process.network_address_projection import _identifier
from process.network_membership_candidate_lifecycle import _control_namespace
from process.registry_management_permissions import _administrative_role, _catalog
from process.registry_publication_permissions import LIFECYCLE_COLUMNS

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
    "network_membership_batch": (
        "candidate_id",
        "batch_id",
        "row_count",
        "input_sha256",
        "copy_sha256",
        "input_bytes",
        "copy_bytes",
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
    """Publication role configuration or permission installation is invalid."""


async def _grant_worker(connection, namespace, tables, role, *, execution):
    """Apply the existing column grants without inspecting effective role closure."""
    grantee = _identifier(role)
    for table in tables:
        await _grant_worker_table(connection, namespace, table, grantee, execution=execution)


async def _grant_worker_table(connection, namespace, table, grantee, *, execution):
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


async def _install_worker_metadata(connection, namespace, tables, sequences, roles):
    """Issue metadata grants through their existing administrative owner roles."""
    lifecycle_role, execution_role = roles["lifecycle_role"], roles["execution_role"]
    for table in tables:
        await connection.execute(
            f"LOCK TABLE {_identifier(namespace)}.{_identifier(table['relname'])} IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
    await connection.execute(
        f"ALTER TABLE {_identifier(namespace)}.network_candidate_custody OWNER TO {_identifier(roles['queue_owner_role'])}"
    )
    await connection.execute(
        f"GRANT USAGE ON SCHEMA {_identifier(namespace)} TO {_identifier(roles['queue_owner_role'])}"
    )
    for role in (lifecycle_role, execution_role):
        await connection.execute(f"REVOKE ALL ON SCHEMA {_identifier(namespace)} FROM {_identifier(role)}")
        await connection.execute(f"GRANT USAGE ON SCHEMA {_identifier(namespace)} TO {_identifier(role)}")
    async with _administrative_role(connection, roles["queue_owner_role"]):
        for role in (lifecycle_role, execution_role):
            await _grant_worker(connection, namespace, tables, role, execution=role == execution_role)
            for sequence in sequences:
                await connection.execute(
                    f"REVOKE ALL ON SEQUENCE {_identifier(namespace)}.{_identifier(sequence['relname'])} FROM {_identifier(role)}"
                )
        await connection.execute(
            f"GRANT USAGE ON SEQUENCE {_identifier(namespace)}.{_identifier(_SEQUENCE)} TO {_identifier(execution_role)}"
        )


async def install_registry_publication_worker_permissions(
    connection, *, lifecycle_role, execution_role, api_role, queue_owner_role, physical_owner_role, control_schema=None
):
    """Apply exact grants on existing roles atomically under PostgreSQL administrator rights."""
    from process.registry_fhir_catalog_permissions import _grant_named_preparation_control

    if not connection.is_in_transaction():
        raise RegistryPublicationWorkerPermissionError("Worker permission installation requires a caller transaction")
    roles_by_name = dict(
        lifecycle_role=lifecycle_role,
        execution_role=execution_role,
        api_role=api_role,
        queue_owner_role=queue_owner_role,
        physical_owner_role=physical_owner_role,
    )
    for role in roles_by_name.values():
        _identifier(role)
    if len(set(roles_by_name.values())) != len(roles_by_name):
        raise RegistryPublicationWorkerPermissionError("Publication roles must be distinct")
    namespace = _control_namespace(control_schema)[1:-1]
    try:
        async with connection.transaction():
            tables, sequences = await _catalog(connection, namespace)
            await connection.execute(f"REVOKE CREATE ON SCHEMA {_identifier(namespace)} FROM PUBLIC")
            applications = ",".join(_identifier(role) for role in (api_role, lifecycle_role, execution_role))
            await connection.execute(f"REVOKE {_identifier(queue_owner_role)} FROM {applications}")
            await connection.execute(
                f"REVOKE {_identifier(physical_owner_role)} FROM {_identifier(api_role)},{_identifier(lifecycle_role)}"
            )
            await _install_worker_metadata(connection, namespace, tables, sequences, roles_by_name)
            database = _identifier(await connection.fetchval("SELECT current_database()"))
            await connection.execute(f"GRANT CREATE ON DATABASE {database} TO {_identifier(physical_owner_role)}")
            await connection.execute(f"GRANT TEMPORARY ON DATABASE {database} TO {_identifier(execution_role)}")
            await connection.execute(
                f"GRANT {_identifier(physical_owner_role)} TO {_identifier(execution_role)} WITH ADMIN FALSE, INHERIT TRUE, SET TRUE"
            )
            await _grant_named_preparation_control(connection, execution_role)
            return {"control_schema": namespace, **roles_by_name}
    except asyncpg.PostgresError:
        raise RegistryPublicationWorkerPermissionError("Worker permission installation was refused") from None
