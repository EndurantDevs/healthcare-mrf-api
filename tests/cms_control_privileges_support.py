# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed CMS control and metadata-read grants that PostgreSQL fixtures apply as administrator."""

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_control_privileges import ADMISSION_GRANTS, SOURCE_CONTROL_READS

IMPORT_RUN_SELECT = (
    "run_id",
    "engine",
    "node_id",
    "importer",
    "family",
    "status",
    "phase_detail",
    "params",
    "idempotency_key",
    "triggered_by",
    "schedule_id",
    "subscription_id",
    "source_file_import_id",
    "created_at",
    "started_at",
    "finished_at",
    "progress",
    "metrics",
    "error",
    "snapshot_id",
    "import_id",
    "retry_of_run_id",
)
IMPORT_RUN_PROGRESS_UPDATE = (
    "status",
    "phase_detail",
    "heartbeat_at",
    "progress",
    "error",
    "metrics",
    "started_at",
    "finished_at",
)
# Shared invoker guards inspect these columns before accepting ordinary progress.
IMPORT_RUN_GUARD_SELECT = (
    (
        "ptg_import_wave",
        (
            "cleanup_evidence_digest",
            "cleanup_summary",
            "cohort_attestation",
            "cohort_attestation_digest",
            "failure_receipt",
            "failure_receipt_digest",
            "idempotency_key",
            "intent_count",
            "jobs_digest",
            "k8s_post_started_at",
            "k8s_post_ticket",
            "kubernetes_config_identity",
            "kubernetes_delete_evidence",
            "kubernetes_delete_evidence_digest",
            "kubernetes_delete_started_at",
            "kubernetes_delete_ticket",
            "kubernetes_job_receipt",
            "kubernetes_job_receipt_digest",
            "kubernetes_job_uid",
            "kubernetes_manifest_identity",
            "kubernetes_ready_attestation",
            "kubernetes_ready_attestation_digest",
            "linkage_ack",
            "linkage_ack_digest",
            "manifest_digest",
            "outcomes_digest",
            "pinned_image_digest",
            "pinned_image_reference",
            "protocol_identity",
            "queue",
            "redis_cleanup_evidence",
            "redis_cleanup_evidence_digest",
            "redis_cleanup_started_at",
            "redis_cleanup_ticket",
            "redis_release_attestation",
            "redis_release_attestation_digest",
            "redis_release_started_at",
            "redis_release_ticket",
            "release_queue",
            "request_digest",
            "resolved_at",
            "resource_class",
            "runtime_image_identity",
            "state",
            "terminal_evidence_digest",
            "terminal_summary",
            "uncertainty_resume_state",
            "wave_digest",
            "wave_id",
            "worker_class",
            "worker_limit",
        ),
    ),
    (
        "ptg_import_wave_admission_rollback",
        (
            "recovery_basis",
            "recovery_evidence",
            "recovery_evidence_sha256",
            "successor_wave_id",
        ),
    ),
    (
        "ptg_import_wave_claim",
        ("wave_id",),
    ),
    (
        "ptg_import_wave_intent",
        (
            "job_id",
            "ordinal",
            "run_id",
            "source_file_import_id",
            "wave_id",
        ),
    ),
    (
        "ptg_import_wave_ordinary_terminal_receipt",
        ("run_id",),
    ),
    (
        "ptg_import_wave_outcome",
        ("wave_id",),
    ),
    (
        "ptg_import_wave_quarantine",
        (
            "predecessor_wave_id",
            "recovery_basis",
        ),
    ),
    (
        "ptg_import_wave_supersession",
        (
            "predecessor_wave_id",
            "recovery_basis",
            "recovery_evidence",
            "recovery_evidence_sha256",
            "successor_wave_id",
        ),
    ),
    (
        "ptg_source_attempt_event",
        (
            "event_kind",
            "outer_run_id",
        ),
    ),
)
PROFILE_GENERATION_UPDATE = (
    "status",
    "operation",
    "control_generation",
    "generation_id",
    "selection_proof_id",
    "authority_revision",
    "profile_schema_version",
    "profile_strategy_version",
    "source_vector_hash",
    "source_vector_json",
    "source_context_vector_hash",
    "source_context_vector_json",
    "executable_plan_hash",
    "profile_as_of",
    "capacity_geometry_status",
    "capacity_geometry_hash",
    "capacity_geometry_json",
    "cutover_forecast_hash",
    "evidence_rows",
    "profile_rows",
    "published_at",
    "updated_at",
)
PROFILE_GENERATION_INSERT = (
    "singleton_key",
    *PROFILE_GENERATION_UPDATE,
    "targets_json",
)
PROFILE_DELTA_INSERT = (
    "build_id",
    "executable_plan_hash",
    "from_capacity_geometry_status",
    "from_capacity_geometry_hash",
    "from_capacity_geometry_json",
    "capacity_geometry_status",
    "capacity_geometry_hash",
    "capacity_geometry_json",
    "from_source_vector_hash",
    "to_source_vector_hash",
    "from_source_context_vector_hash",
    "to_source_context_vector_hash",
    "from_generation_id",
    "generation_id",
    "operation",
    "profile_as_of",
    "selection_proof_id",
    "control_generation",
    "authority_revision",
    "targets_json",
    "evidence_rows",
    "profile_rows",
    "evidence_inserted",
    "evidence_deleted",
    "profile_inserted",
    "profile_deleted",
    "cutover_forecast_hash",
    "cutover_forecast_json",
    "cutover_actual_hash",
    "cutover_actual_json",
    "cutover_wal_start_lsn",
    "cutover_wal_observed_lsn",
    "cutover_wal_bytes",
    "evidence_target_bytes_before",
    "evidence_target_bytes_after",
    "evidence_target_growth_bytes",
    "profile_target_bytes_before",
    "profile_target_bytes_after",
    "profile_target_growth_bytes",
    "committed_at",
)
PROFILE_INITIAL_INSERT = (
    "build_id",
    "attestation_id",
    "generation_id",
    "run_id",
    "contract_id",
    "payload",
    "payload_sha256",
    "committed_at",
)
PROFILE_CHECKPOINT_UPDATE = (
    "cutover_forecast_status",
    "cutover_forecast_hash",
    "cutover_forecast_json",
    "updated_at",
)
ADDRESS_GENERATION_SELECT = (
    "singleton",
    "local_lineage_id",
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
    "relation_oids",
)
ADDRESS_GENERATION_UPDATE = (
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
    "relation_oids",
)
GEO_STATE_UPDATE = (
    "active_geo_assurance_version",
    "active_table_oid",
    "active_relation_signature",
    "active_dependency_bindings",
    "candidate_geo_assurance_version",
    "candidate_table_oid",
    "candidate_relation_signature",
    "candidate_dependency_bindings",
    "candidate_projected_rows",
)
ALIAS_ARTIFACT_COLUMNS = ("artifact_name", "generation", "updated_at")

TABLE_GRANTS = (
    ("provider_directory_profile_serving_generation", "SELECT"),
    ("provider_directory_profile_delta_receipt", "SELECT"),
    ("provider_directory_profile_initial_receipt", "SELECT"),
    ("provider_directory_profile_build_checkpoint", "SELECT,DELETE"),
    ("provider_directory_profile_capacity_preflight_receipt", "SELECT"),
    ("provider_directory_profile_capacity_lease_consumption", "SELECT"),
)
COLUMN_GRANTS = (
    ("provider_directory_source", "UPDATE", ("endpoint_id", "updated_at")),
    ("import_run", "SELECT", IMPORT_RUN_SELECT),
    ("import_run", "UPDATE", IMPORT_RUN_PROGRESS_UPDATE),
    *((root, "SELECT", columns) for root, columns in IMPORT_RUN_GUARD_SELECT),
    ("provider_directory_profile_serving_generation", "INSERT", PROFILE_GENERATION_INSERT),
    ("provider_directory_profile_serving_generation", "UPDATE", PROFILE_GENERATION_UPDATE),
    ("provider_directory_profile_delta_receipt", "INSERT", PROFILE_DELTA_INSERT),
    # Native row locks require UPDATE on a column; the attested immutable guard rejects DML.
    ("provider_directory_profile_delta_receipt", "UPDATE", ("build_id",)),
    ("provider_directory_profile_initial_receipt", "INSERT", PROFILE_INITIAL_INSERT),
    ("provider_directory_profile_build_checkpoint", "UPDATE", PROFILE_CHECKPOINT_UPDATE),
    ("entity_address_result_generation", "SELECT", ADDRESS_GENERATION_SELECT),
    ("entity_address_result_generation", "UPDATE", ADDRESS_GENERATION_UPDATE),
    ("entity_address_geo_assurance_state", "SELECT", ("singleton", *GEO_STATE_UPDATE)),
    ("entity_address_geo_assurance_state", "UPDATE", GEO_STATE_UPDATE),
    ("address_alias_state_v1", "SELECT", ("singleton", "schema_version", "active_ruleset_version", "generation")),
    # The singleton CHECK prevents key mutation; alias generation remains operator-owned.
    ("address_alias_state_v1", "UPDATE", ("singleton",)),
    ("address_alias_artifact_state_v1", "SELECT", ALIAS_ARTIFACT_COLUMNS),
    ("address_alias_artifact_state_v1", "INSERT", ALIAS_ARTIFACT_COLUMNS),
    ("address_alias_artifact_state_v1", "UPDATE", ("generation", "updated_at")),
)
MAINTAIN_TABLES = (
    "provider_directory_source",
    "provider_directory_api_endpoint",
    "import_run",
    "provider_directory_profile_build_checkpoint",
    "provider_directory_profile_capacity_lease_consumption",
    "provider_directory_profile_serving_generation",
    "provider_directory_profile_capacity_preflight_receipt",
    "provider_directory_profile_initial_receipt",
    "reference_family_result_generation",
)


async def grant_cms_control_privileges(connection, policy):
    """Apply fixed Owner grants inside the caller's fully verified admin savepoint.

    Existing admission, SOURCE epoch, and Doctors grants remain separate. This
    adds only fixed control and guard reads, with no preparation issuer, Reader
    or membership authority.
    Receipt custody already supplies Owner rights; redundant column ACLs are forbidden.
    MAINTAIN permits the existing brief table fences without table-wide UPDATE.
    Errors and cancellation propagate to the caller's whole-cut rollback.
    """
    owner = storage_identifier(policy.owner_role)
    table_statements = tuple(
        f"GRANT {privileges} ON {storage_relation(policy.schema, root)} TO {owner}" for root, privileges in TABLE_GRANTS
    )
    column_statements = tuple(
        f"GRANT {privilege}({','.join(storage_identifier(name) for name in columns)}) "
        f"ON {storage_relation(policy.schema, root)} TO {owner}"
        for root, privilege, columns in COLUMN_GRANTS
    )
    maintain_statements = tuple(
        f"GRANT MAINTAIN ON {storage_relation(policy.schema, root)} TO {owner}" for root in MAINTAIN_TABLES
    )
    await grant_npi_generation_read(connection, policy)
    for statement in (
        *table_statements,
        *column_statements,
        *maintain_statements,
        f"GRANT SET ON PARAMETER temp_file_limit TO {owner}",
    ):
        await connection.execute(statement)


READ_COLUMN_GRANTS = {
    "provider_directory_source": ("source_id", "endpoint_id"),
    "provider_directory_cms_serving_receipt": (
        "receipt_id",
        "payload",
        "publication_xid",
        "profile_generation_id",
        "registry_source_pair_json",
        "address_lineage_id",
        "address_generation",
    ),
    "entity_address_result_generation": ADDRESS_GENERATION_SELECT,
    "provider_directory_profile_serving_generation": (
        "singleton_key",
        "status",
        "operation",
        "control_generation",
        "generation_id",
        "selection_proof_id",
        "authority_revision",
        "profile_schema_version",
        "profile_strategy_version",
        "source_vector_hash",
        "source_context_vector_hash",
        "executable_plan_hash",
        "profile_as_of",
        "targets_json",
        "evidence_rows",
        "profile_rows",
    ),
    "reference_family_result_generation": (
        "importer_id",
        "local_lineage_id",
        "local_generation",
        "origin_lineage_id",
        "origin_generation",
        "published_at",
        "serving_schema",
        "serving_relation_names",
    ),
    "address_alias_state_v1": ("singleton", "generation"),
}

ORIGIN_METADATA_COLUMNS = (
    "receipt_id",
    "payload",
    "publication_xid",
    "profile_generation_id",
    "registry_source_pair_json",
)


def cms_origin_metadata_read_roles(policy, roles):
    """Keep explicit origin recipients separate from the admitted SOURCE role tuple."""
    if type(policy) is not GenericSourcePolicy:
        raise ValueError("cms_origin_metadata_read_policy_required")
    if type(roles) is not tuple or any(type(role) is not str for role in roles) or len(set(roles)) != len(roles):
        raise ValueError("cms_origin_metadata_read_roles_required")
    for role in roles:
        storage_identifier(role)
    if set(roles) & {policy.owner_role, policy.publisher_role, *policy.runtime_roles, *policy.control_runtime_roles}:
        raise ValueError("cms_origin_metadata_read_source_overlap")
    return tuple(sorted(roles))


async def grant_cms_origin_metadata_read_privileges(connection, policy, *, metadata_read_roles):
    """Apply only the fixed receipt projection inside the attested original-admin read cut."""
    roles = cms_origin_metadata_read_roles(policy, metadata_read_roles)
    if not roles:
        raise ValueError("cms_origin_metadata_read_roles_required")
    columns = ",".join(storage_identifier(column) for column in ORIGIN_METADATA_COLUMNS)
    table = storage_relation(policy.schema, "provider_directory_cms_serving_receipt")
    for role in roles:
        await connection.execute(f"GRANT SELECT({columns}) ON {table} TO {storage_identifier(role)}")


def cms_control_read_roles(policy):
    """Use the caller's admitted SOURCE policy, never observed ACL beneficiaries."""
    if type(policy) is not GenericSourcePolicy:
        raise ValueError("cms_control_read_policy_required")
    return (policy.publisher_role, *(role for role in policy.runtime_roles if role not in policy.control_runtime_roles))


async def grant_cms_control_read_privileges(connection, policy):
    """Run only inside the same fully attested original-admin cut as writer grants."""
    grantees = tuple(storage_identifier(role) for role in cms_control_read_roles(policy))
    statements = tuple(
        f"GRANT SELECT({','.join(storage_identifier(column) for column in columns)}) "
        f"ON {storage_relation(policy.schema, root)} TO {grantee}"
        for root, columns in READ_COLUMN_GRANTS.items()
        for grantee in grantees
    )
    for statement in statements:
        await connection.execute(statement)


_CONTROL_READ_COLUMNS = {
    "provider_directory_source": "source_id,endpoint_id,metadata_json",
    "provider_directory_api_endpoint": "endpoint_id",
}
_DISPOSITION_RIGHTS = (
    "SELECT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,observed_at),"
    "INSERT(dataset_id,endpoint_id,acquisition_root_run_id,vector_sha256,prior_status,dataset_hash)"
)
_METADATA_FUNCTIONS = (
    "provider_directory_endpoint_dataset_admission_metadata_sha256(jsonb,smallint,text,text,character varying[])",
    "provider_directory_subset_canonical_sha256(jsonb)",
    "provider_directory_subset_completion_proof_pair_valid(jsonb,text)",
    "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)",
    "provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)",
)


async def grant_source_admission_controls(connection, policy):
    """Grant the fixed control reads, row-lock columns and header-proof validators to the Source Owner."""
    owner = storage_identifier(policy.owner_role)
    for root, privileges in ADMISSION_GRANTS:
        await connection.execute(f"GRANT {privileges} ON {storage_relation(policy.schema, root)} TO {owner}")
    for root in SOURCE_CONTROL_READS:
        # Mirror the table and column ACL entries that the original provisioning wrote.
        await connection.execute(
            f"GRANT SELECT,SELECT({_CONTROL_READ_COLUMNS[root]}) ON {storage_relation(policy.schema, root)} TO {owner}"
        )
    await connection.execute(
        f"GRANT {_DISPOSITION_RIGHTS} ON "
        f"{storage_relation(policy.schema, 'provider_directory_cms_npd_stale_candidate')} TO {owner}"
    )
    for signature in _METADATA_FUNCTIONS:
        name, _, arguments = signature.partition("(")
        await connection.execute(
            f"GRANT EXECUTE ON FUNCTION {storage_relation(policy.schema, name)}({arguments} TO {owner}"
        )
    await _grant_address_artifact_revision(connection, policy)
    await grant_npi_generation_read(connection, policy)


async def _grant_address_artifact_revision(connection, policy):
    """Maintain installed logical artifact revisions through the configured Source Owner."""
    installed = await connection.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class relation "
        "JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname='address_alias_artifact_state_v1')",
        policy.schema,
    )
    if installed:
        await connection.execute(
            "GRANT SELECT(artifact_name,generation,updated_at),INSERT(artifact_name,generation,updated_at),"
            "UPDATE(generation,updated_at) ON "
            f"{storage_relation(policy.schema, 'address_alias_artifact_state_v1')} "
            f"TO {storage_identifier(policy.owner_role)}"
        )


async def grant_npi_generation_read(connection, policy):
    """Read installed canonical NPI origin metadata through the configured Source Owner."""
    installed = await connection.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class relation "
        "JOIN pg_catalog.pg_namespace namespace ON namespace.oid=relation.relnamespace "
        "WHERE namespace.nspname=$1 AND relation.relname='npi_result_generation')",
        policy.schema,
    )
    if installed:
        await connection.execute(
            "GRANT SELECT(origin_lineage_id,origin_generation) ON "
            f"{storage_relation(policy.schema, 'npi_result_generation')} TO {storage_identifier(policy.owner_role)}"
        )
