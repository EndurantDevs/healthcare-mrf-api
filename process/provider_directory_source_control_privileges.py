# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed low-volume admission rights inside the verified native-admin source cut."""

from db.plan_release_storage import storage_identifier, storage_relation

ADMISSION_GRANTS = (
    ("provider_directory_api_endpoint", "SELECT(endpoint_id),UPDATE(endpoint_id)"),
    (
        "provider_directory_source",
        "SELECT(source_id,endpoint_id,metadata_json),UPDATE(source_id)",
    ),
    (
        "import_run",
        "SELECT(run_id,retry_of_run_id,engine,node_id,importer,status,finished_at,params),UPDATE(run_id)",
    ),
)
SOURCE_CONTROL_READS = ("provider_directory_source", "provider_directory_api_endpoint")
_CONTROL_READ_COLUMNS = {
    "provider_directory_source": "source_id,endpoint_id,metadata_json",
    "provider_directory_api_endpoint": "endpoint_id",
}
_DISPOSITION_RIGHTS = (
    "SELECT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,observed_at),"
    "INSERT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,dataset_hash)"
)
METADATA_FUNCTIONS = (
    "provider_directory_endpoint_dataset_admission_metadata_sha256(jsonb,smallint,text,text,character varying[])",
    "provider_directory_subset_canonical_sha256(jsonb)",
    "provider_directory_subset_completion_proof_pair_valid(jsonb,text)",
    "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)",
    "provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)",
)


async def grant_source_admission_controls(connection, policy):
    """Grant existing control reads, narrow row-lock columns and header-proof validators.

    The source provisioner calls this after independent-admin full-profile
    verification, inside its original savepoint. These functions validate one
    header transition; none is installed on or invoked by payload COPY.
    """
    owner = storage_identifier(policy.owner_role)
    for root, privileges in ADMISSION_GRANTS:
        await connection.execute(f"GRANT {privileges} ON {storage_relation(policy.schema, root)} TO {owner}")
    await _grant_control_reads(connection, policy)
    await _grant_disposition_controls(connection, policy)
    for signature in METADATA_FUNCTIONS:
        name, _, arguments = signature.partition("(")
        await connection.execute(
            f"GRANT EXECUTE ON FUNCTION {storage_relation(policy.schema, name)}({arguments} TO {owner}"
        )


async def _grant_control_reads(connection, policy):
    owner = storage_identifier(policy.owner_role)
    for root in SOURCE_CONTROL_READS:
        # Pinning and the final alias fence compare complete low-volume rows.
        # PostgreSQL REVOKE SELECT also removes column reads. Preserve their
        # existing exact native ACL contract when repairing a revoked read.
        await connection.execute(
            f"GRANT SELECT,SELECT({_CONTROL_READ_COLUMNS[root]}) ON {storage_relation(policy.schema, root)} TO {owner}"
        )


async def _grant_disposition_controls(connection, policy):
    """Read and append the immutable low-volume disposition; never modify it."""
    await connection.execute(
        f"GRANT {_DISPOSITION_RIGHTS} ON "
        f"{storage_relation(policy.schema, 'provider_directory_cms_npd_stale_candidate')} "
        f"TO {storage_identifier(policy.owner_role)}"
    )


async def provision_source_control_reads(connection, policy, *, verify_role_profile):
    """Repair fixed admission rights on a protected family in a genuine admin cut."""
    from process.plan_release_storage_custody import storage_roles
    from process.provider_directory_generic_source_policy import GenericSourcePolicy
    from process.provider_directory_generic_source_provision import (
        verify_generic_source_family,
    )

    if (
        type(policy) is not GenericSourcePolicy
        or connection.is_in_transaction() is not True
        or not callable(verify_role_profile)
    ):
        raise RuntimeError("provider_directory_source_control_admin_required")
    actor = await connection.fetchrow(
        "SELECT session_user::text AS session,current_user::text AS current,"
        "(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS native_admin"
    )
    if (
        not actor["native_admin"]
        or actor["session"] != actor["current"]
        or actor["current"]
        in (
            policy.owner_role,
            policy.publisher_role,
            *policy.runtime_roles,
        )
    ):
        raise RuntimeError("provider_directory_source_control_independent_admin_required")
    async with connection.transaction():
        await storage_roles(connection, policy.schema, **policy.role_arguments())
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        await verify_generic_source_family(connection, policy)
        await grant_source_admission_controls(connection, policy)
        await verify_generic_source_family(connection, policy)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())


async def lock_source_publication_controls(database, schema):
    """Retain native alias guards while independent SOURCE cuts overlap."""
    from process.provider_directory_generic_source_policy import require_source_publication_transaction

    publisher = await require_source_publication_transaction(database)
    if publisher.policy.schema != schema:
        raise RuntimeError("provider_directory_source_publication_schema_changed")
    endpoint_ref = storage_relation(schema, "provider_directory_api_endpoint")
    source_ref = storage_relation(schema, "provider_directory_source")
    # Keep the FK stable while its row locks fence alias inserts and moves.
    await database.status(f"LOCK TABLE {endpoint_ref} IN ROW EXCLUSIVE MODE NOWAIT;")
    await database.status(f"LOCK TABLE {source_ref} IN ROW EXCLUSIVE MODE NOWAIT;")
    alias_fence_ready = await database.scalar(
        """SELECT current_setting('session_replication_role')='origin' AND EXISTS(
         SELECT FROM pg_catalog.pg_constraint k
         WHERE k.conrelid=to_regclass(:source_ref) AND k.confrelid=to_regclass(:endpoint_ref)
          AND k.contype='f' AND k.convalidated AND k.conenforced AND NOT k.condeferrable
          AND k.conkey=ARRAY(SELECT attnum FROM pg_catalog.pg_attribute
           WHERE attrelid=k.conrelid AND attname='endpoint_id' AND NOT attisdropped)
          AND k.confkey=ARRAY(SELECT attnum FROM pg_catalog.pg_attribute
           WHERE attrelid=k.confrelid AND attname='endpoint_id' AND NOT attisdropped)
          AND (SELECT count(*)=4 AND bool_and(tgisinternal AND tgenabled='A')
           FROM pg_catalog.pg_trigger WHERE tgconstraint=k.oid)
        )""",
        source_ref=source_ref,
        endpoint_ref=endpoint_ref,
    )
    if alias_fence_ready is not True:
        raise RuntimeError("provider_directory_source_publication_alias_fence_changed")
