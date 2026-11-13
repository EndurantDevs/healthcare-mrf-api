# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Administrative grants for catalog loading, validation and atomic publication."""

from db.plan_release_storage import storage_identifier
from db.registry_fhir_catalog import SNAPSHOT_COLUMNS
from process.registry_management_permissions import REGISTRY_TABLES
from process.registry_publication_permissions import INSERT_COLUMNS, LIFECYCLE_COLUMNS

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


def _configured_roles(config, *roles):
    if not config.metadata_only or config.metadata_runtime is None:
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    for role in (*roles, *config.permission_roles.values()):
        storage_identifier(role)
    return storage_identifier(config.control_schema)


_PREPARATION_ADMISSION_COLUMNS = (
    "preparation_id,node_id,importer_id,dataset_key,run_id,generation_id,publication_fence,"
    "admission_sha256,admission,expected_generation_id,expected_fence_token,predecessor_pin_id"
)
_PREPARATION_LIFECYCLE_COLUMNS = (
    "state,attempt_id,handoff,intake_inventory,intake_sha256,validation,validation_sha256,"
    "publication,publication_sha256,published_at,published_fence_token,cancellation_requested,"
    "abandonment_sha256,claim_attempt,claim_token,claim_expires_at,updated_at"
)
_PREPARATION_COLUMNS = (
    _PREPARATION_ADMISSION_COLUMNS + "," + _PREPARATION_LIFECYCLE_COLUMNS + ",frozen_inventory,frozen_sha256,created_at"
)


async def _revoke_named_preparation_writes(connection, *roles):
    """Remove both old table grants and column grants before a role changes function."""
    recipients = ",".join(storage_identifier(role) for role in sorted(set(roles)))
    table = "hp_snapshot_retention.preparation"
    await connection.execute(f"REVOKE INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES ON {table} FROM {recipients}")
    columns = _PREPARATION_COLUMNS
    await connection.execute(
        f"REVOKE INSERT({columns}),UPDATE({columns}),REFERENCES({columns}) ON {table} FROM {recipients}"
    )


async def _grant_named_preparation_control(connection, *roles):
    """Use pre-installed metadata; only administrators may record the permission freeze."""
    recipients = ",".join(storage_identifier(role) for role in sorted(set(roles)))
    await connection.execute(f"GRANT USAGE ON SCHEMA hp_snapshot_retention TO {recipients}")
    await _revoke_named_preparation_writes(connection, *roles)
    await connection.execute(
        f"GRANT SELECT,INSERT({_PREPARATION_ADMISSION_COLUMNS}),UPDATE({_PREPARATION_LIFECYCLE_COLUMNS}) "
        f"ON hp_snapshot_retention.preparation TO {recipients}"
    )
    await connection.execute(
        "GRANT SELECT,INSERT ON hp_snapshot_retention.preparation_claim,"
        "hp_snapshot_retention.preparation_relation TO " + recipients
    )
    await connection.execute("GRANT SELECT ON hp_snapshot_retention.relation TO " + recipients)


async def install_fhir_catalog_preparation_permissions(
    connection, config, *, loader_role, sealer_role, preparation_roles=None
):
    """Give the builder candidate DDL and the validator direct control-table access."""
    from process.registry_fhir_catalog_store import named_catalog_preparation_roles

    if not connection.is_in_transaction() or loader_role not in config.loader_roles:
        raise ValueError("registry_fhir_catalog_preparation_permissions_invalid")
    namespace = _configured_roles(config, loader_role, sealer_role)
    profile = preparation_roles or {
        "namespace_owner_role": config.physical_owner_role,
        "publisher_role": sealer_role,
        "application_roles": sorted(
            {
                loader_role,
                sealer_role,
                config.lifecycle_role,
                config.execution_role,
                config.api_role,
                *config.loader_roles,
                *config.reader_roles,
            }
        ),
    }
    profile = named_catalog_preparation_roles(profile)
    if profile["namespace_owner_role"] != config.physical_owner_role or profile["publisher_role"] != sealer_role:
        raise ValueError("registry_fhir_catalog_preparation_permissions_invalid")
    loader, sealer = storage_identifier(loader_role), storage_identifier(sealer_role)
    owner = storage_identifier(profile["namespace_owner_role"])
    applications = ",".join(storage_identifier(role) for role in profile["application_roles"])
    async with connection.transaction():
        database = await connection.fetchval("SELECT quote_ident(current_database())")
        await connection.execute(f"REVOKE {owner} FROM {applications}")
        await connection.execute(f"REVOKE {loader} FROM {sealer}")
        await connection.execute(f"REVOKE CREATE ON DATABASE {database} FROM {sealer}")
        await connection.execute(f"GRANT CREATE ON DATABASE {database} TO {loader}")
        await connection.execute(f"GRANT TEMP ON DATABASE {database} TO {loader},{sealer}")
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {loader},{sealer}")
        await connection.execute(f"REVOKE CREATE ON SCHEMA {namespace} FROM {loader},{sealer}")
        for table in sorted(_PREPARATION_READS):
            await connection.execute(f"REVOKE ALL ON {namespace}.{storage_identifier(table)} FROM {loader},{sealer}")
            await connection.execute(f"GRANT SELECT ON {namespace}.{storage_identifier(table)} TO {loader},{sealer}")
        for table, columns in {**_LOADER_INSERTS, "registry_fhir_catalog_snapshot": _SNAPSHOT_INSERTS}.items():
            selected = ",".join(storage_identifier(column) for column in columns)
            recipient = sealer if table == "registry_fhir_catalog_snapshot" else loader
            await connection.execute(
                f"GRANT INSERT({selected}) ON {namespace}.{storage_identifier(table)} TO {recipient}"
            )
        await connection.execute(
            f"GRANT UPDATE(custody_json,accounting_json) ON {namespace}.registry_fhir_catalog_snapshot TO {sealer}"
        )
        await connection.execute(f"GRANT USAGE ON SEQUENCE {namespace}.{_IDENTITY_SEQUENCE} TO {loader}")
        await connection.execute(
            f"GRANT SELECT ON {namespace}.registry_fhir_catalog_snapshot TO {storage_identifier(config.api_role)}"
        )
        await _grant_named_preparation_control(connection, sealer_role, config.execution_role)
    return {"control_schema": config.control_schema, "loader_role": loader_role, "sealer_role": sealer_role}


async def install_fhir_catalog_publication_permissions(connection, config):
    """Grant the existing queue actors the exact catalog pointer and queue updates."""
    if not connection.is_in_transaction():
        raise ValueError("registry_fhir_catalog_permissions_invalid")
    schema = _configured_roles(config)
    execution, lifecycle = storage_identifier(config.execution_role), storage_identifier(config.lifecycle_role)
    async with connection.transaction():
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
        await _grant_named_preparation_control(connection, config.execution_role)
