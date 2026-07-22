"""Add source-neutral physical Provider Directory projections.

Revision ID: 20260721170000_provider_directory_physical_projection
Revises: 20260721160000_provider_directory_retained_artifact_acquisition
"""

from __future__ import annotations

import os
import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from db.migration_adoption import create_table_or_validate
from db.migration_expression_adoption import _normalized_expression
from db.migration_index_adoption import create_index_if_missing


revision = "20260721170000_provider_directory_physical_projection"
down_revision = "20260721160000_provider_directory_retained_artifact_acquisition"
branch_labels = None
depends_on = None


_CHECKS_BY_TABLE = {
    "provider_directory_physical_projection": {
        "pd_physical_projection_digest_check": (
            "physical_projection_id ~ '^[0-9a-f]{64}$' "
            "AND canonical_row_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND transform_context_hash ~ '^[0-9a-f]{64}$' "
            "AND completeness_manifest_hash ~ '^[0-9a-f]{64}$' "
            "AND dataset_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_profile_hash ~ '^[0-9a-f]{64}$' "
            "AND scope_contract_id <> ''"
        ),
        "pd_physical_projection_json_check": (
            "jsonb_typeof(selected_resources_json) = 'array' "
            "AND jsonb_array_length(selected_resources_json) > 0 "
            "AND jsonb_typeof(required_resources_json) = 'array' "
            "AND jsonb_typeof(transform_context_json) = 'object' "
            "AND jsonb_typeof(completeness_manifest_json) = 'object' "
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(proof_json) = 'object'"
        ),
        "pd_physical_projection_state_check": (
            "resource_count > 0 AND storage_schema <> '' "
            "AND storage_relation <> '' AND storage_relation_oid > 0 "
            "AND storage_trigger_oid > 0 "
            "AND ((membership_storage_schema IS NULL "
            "AND membership_storage_relation IS NULL "
            "AND membership_storage_relation_oid IS NULL "
            "AND membership_storage_trigger_oid IS NULL) OR "
            "(membership_storage_schema <> '' "
            "AND membership_storage_relation <> '' "
            "AND membership_storage_relation_oid > 0 "
            "AND membership_storage_trigger_oid > 0)) "
            "AND ((profile_contribution_storage_schema IS NULL "
            "AND profile_contribution_storage_relation IS NULL "
            "AND profile_contribution_storage_relation_oid IS NULL "
            "AND profile_contribution_storage_trigger_oid IS NULL) OR "
            "(profile_contribution_storage_schema <> '' "
            "AND profile_contribution_storage_relation <> '' "
            "AND profile_contribution_storage_relation_oid > 0 "
            "AND profile_contribution_storage_trigger_oid > 0)) "
            "AND status IN ('sealed', 'retiring') "
            "AND retain_until >= sealed_at "
            "AND ((status = 'sealed' AND retiring_at IS NULL) "
            "OR (status = 'retiring' AND retiring_at IS NOT NULL))"
        ),
    },
    "provider_directory_projection_recipe": {
        "pd_projection_recipe_digest_check": (
            "recipe_id ~ '^[0-9a-f]{64}$' "
            "AND input_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND transform_context_hash ~ '^[0-9a-f]{64}$' "
            "AND completeness_manifest_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_profile_hash ~ '^[0-9a-f]{64}$' "
            "AND scope_contract_id <> '' "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')"
        ),
        "pd_projection_recipe_json_check": (
            "jsonb_typeof(selected_resources_json) = 'array' "
            "AND jsonb_array_length(selected_resources_json) > 0 "
            "AND jsonb_typeof(required_resources_json) = 'array'"
            " AND jsonb_typeof(transform_context_json) = 'object'"
            " AND jsonb_typeof(completeness_manifest_json) = 'object'"
            " AND (prepared_proof_json IS NULL OR "
            "jsonb_typeof(prepared_proof_json) = 'object')"
            " AND (native_campaign_proof_json IS NULL OR "
            "jsonb_typeof(native_campaign_proof_json) = 'object')"
        ),
        "pd_projection_recipe_state_check": (
            "attempt > 0 AND status IN "
            "('building', 'proof_ready', 'sealed', 'failed', 'retired') "
            "AND ((status IN ('building', 'proof_ready') "
            "AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND physical_projection_id IS NULL AND sealed_at IS NULL) "
            "OR (status = 'sealed' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL "
            "AND physical_projection_id IS NOT NULL AND sealed_at IS NOT NULL) "
            "OR (status IN ('failed', 'retired') "
            "AND lease_token IS NULL AND lease_expires_at IS NULL)) "
            "AND (status NOT IN ('proof_ready', 'sealed') "
            "OR workset_registered_at IS NOT NULL) "
            "AND (status <> 'proof_ready' OR prepared_proof_json IS NOT NULL)"
        ),
        "pd_projection_recipe_stage_check": (
            "(stage_schema IS NULL AND stage_relation IS NULL "
            "AND stage_relation_oid IS NULL) OR "
            "(stage_schema <> '' AND stage_relation <> '' "
            "AND stage_relation_oid > 0)"
        ),
        "pd_projection_recipe_membership_stage_check": (
            "(membership_stage_schema IS NULL "
            "AND membership_stage_relation IS NULL "
            "AND membership_stage_relation_oid IS NULL) OR "
            "(membership_stage_schema <> '' "
            "AND membership_stage_relation <> '' "
            "AND membership_stage_relation_oid > 0)"
        ),
        "pd_projection_recipe_profile_stage_check": (
            "(profile_contribution_stage_schema IS NULL "
            "AND profile_contribution_stage_relation IS NULL "
            "AND profile_contribution_stage_relation_oid IS NULL) OR "
            "(profile_contribution_stage_schema <> '' "
            "AND profile_contribution_stage_relation <> '' "
            "AND profile_contribution_stage_relation_oid > 0)"
        ),
        "pd_projection_recipe_workset_check": (
            "(input_block_set_sha256 IS NULL AND input_block_count IS NULL "
            "AND partition_set_sha256 IS NULL AND partition_count IS NULL "
            "AND workset_registered_at IS NULL) OR "
            "(input_block_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_block_count > 0 "
            "AND partition_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_count > 0 AND workset_registered_at IS NOT NULL)"
        ),
    },
    "provider_directory_projection_input_block": {
        "pd_projection_input_block_digest_check": (
            "block_id ~ '^[0-9a-f]{64}$' "
            "AND upstream_artifact_id ~ '^[0-9a-f]{64}$' "
            "AND source_object_id ~ '^[0-9a-f]{64}$' "
            "AND content_sha256 ~ '^[0-9a-f]{64}$' "
            "AND payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND block_proof_sha256 ~ '^[0-9a-f]{64}$'"
        ),
        "pd_projection_input_block_values_check": (
            "block_ordinal >= 0 AND source_partition_ordinal >= 0 "
            "AND record_start >= 0 AND record_count > 0 "
            "AND payload_bytes > 0 AND upstream_artifact_id <> '' "
            "AND source_object_id <> '' AND block_kind <> '' "
            "AND input_contract_id <> '' "
            "AND jsonb_typeof(summary_json) = 'object' "
            "AND (retained_range_ordinal IS NULL "
            "OR retained_range_ordinal >= 0)"
        ),
    },
    "provider_directory_projection_proof_shard": {
        "pd_projection_proof_shard_digest_check": (
            "partition_id ~ '^[0-9a-f]{64}$' "
            "AND partition_key ~ '^[0-9a-f]{64}$' "
            "AND input_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (recipe_lease_token IS NULL OR "
            "recipe_lease_token ~ '^[0-9a-f]{64}$') "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$') "
            "AND (canonical_row_sha256 IS NULL OR "
            "canonical_row_sha256 ~ '^[0-9a-f]{64}$')"
        ),
        "pd_projection_proof_shard_values_check": (
            "attempt > 0 AND partition_ordinal >= 0 "
            "AND partition_key <> '' AND resource_type <> '' "
            "AND partition_attempt >= 0 "
            "AND status IN ('pending', 'building', 'complete') "
            "AND ((status = 'pending' AND partition_attempt = 0 "
            "AND recipe_lease_token IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND canonical_row_sha256 IS NULL AND resource_count IS NULL "
            "AND first_identity_json IS NULL AND last_identity_json IS NULL "
            "AND proof_json IS NULL AND completed_at IS NULL) OR "
            "(status = 'building' AND partition_attempt > 0 "
            "AND recipe_lease_token IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL "
            "AND lease_heartbeat_at IS NOT NULL "
            "AND canonical_row_sha256 IS NULL AND resource_count IS NULL "
            "AND first_identity_json IS NULL AND last_identity_json IS NULL "
            "AND proof_json IS NULL AND completed_at IS NULL) OR "
            "(status = 'complete' AND partition_attempt > 0 "
            "AND recipe_lease_token IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND canonical_row_sha256 IS NOT NULL AND resource_count > 0 "
            "AND jsonb_typeof(first_identity_json) = 'array' "
            "AND jsonb_typeof(last_identity_json) = 'array' "
            "AND jsonb_typeof(proof_json) = 'object' "
            "AND completed_at IS NOT NULL))"
        ),
    },
    "provider_directory_physical_projection_resource": {
        "pd_physical_projection_resource_values_check": (
            "length(physical_projection_id) = 64 "
            "AND physical_projection_id !~ '[^0-9a-f]' "
            "AND length(proof_partition_id) = 64 "
            "AND proof_partition_id !~ '[^0-9a-f]' "
            "AND length(payload_hash) = 64 "
            "AND payload_hash !~ '[^0-9a-f]' "
            "AND resource_type <> '' AND resource_id <> '' "
            "AND source_rank <> '' "
            "AND (summary_npi IS NULL OR "
            "summary_npi BETWEEN 1000000000 AND 2999999999) "
            "AND summary_address_count >= 0 "
            "AND summary_network_link_count >= 0 "
            "AND summary_affiliation_link_count >= 0 "
            "AND summary_addressed_location = "
            "(resource_type = 'Location' AND summary_address_count > 0) "
            "AND (NOT summary_geocoded_location OR "
            "summary_addressed_location) "
            "AND (resource_type = 'InsurancePlan' OR "
            "summary_network_link_count = 0) "
            "AND (resource_type = 'OrganizationAffiliation' OR "
            "summary_affiliation_link_count = 0) "
            "AND (effective_start IS NULL OR effective_start <> '') "
            "AND (effective_end IS NULL OR effective_end <> '') "
            "AND (observed_at IS NULL OR observed_at <> '') "
            "AND (profile_evidence_json IS NULL OR ("
            "jsonb_typeof(profile_evidence_json) = 'object' "
            "AND profile_evidence_json <> '{}'::jsonb "
            "AND profile_evidence_json - ARRAY['names', 'specialties', "
            "'contacts', 'addresses', 'references'] = '{}'::jsonb "
            "AND (NOT profile_evidence_json ? 'names' OR "
            "(jsonb_typeof(profile_evidence_json -> 'names') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'names') > 0)) "
            "AND (NOT profile_evidence_json ? 'specialties' OR "
            "(jsonb_typeof(profile_evidence_json -> 'specialties') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'specialties') > 0)) "
            "AND (NOT profile_evidence_json ? 'contacts' OR "
            "(jsonb_typeof(profile_evidence_json -> 'contacts') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'contacts') > 0)) "
            "AND (NOT profile_evidence_json ? 'addresses' OR "
            "(jsonb_typeof(profile_evidence_json -> 'addresses') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'addresses') > 0)) "
            "AND (NOT profile_evidence_json ? 'references' OR "
            "(jsonb_typeof(profile_evidence_json -> 'references') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'references') > 0))))"
        ),
    },
    "provider_directory_physical_projection_resource_membership": {
        "pd_physical_projection_membership_values_check": (
            "physical_projection_id ~ '^[0-9a-f]{64}$' "
            "AND membership_partition_id ~ '^[0-9a-f]{64}$' "
            "AND resource_type <> '' AND resource_id <> ''"
        ),
    },
    "provider_directory_physical_projection_profile_contribution": {
        "pd_physical_profile_contribution_values_check": (
            "physical_projection_id ~ '^[0-9a-f]{64}$' "
            "AND proof_partition_id ~ '^[0-9a-f]{64}$' "
            "AND payload_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_type <> '' AND resource_id <> '' "
            "AND source_rank <> '' "
            "AND (direct_npi IS NULL OR "
            "direct_npi BETWEEN 1000000000 AND 2999999999) "
            "AND (effective_start IS NULL OR effective_start <> '') "
            "AND (effective_end IS NULL OR effective_end <> '') "
            "AND (observed_at IS NULL OR observed_at <> '')"
        ),
        "pd_physical_profile_contribution_json_check": (
            "jsonb_typeof(names_json) = 'array' "
            "AND jsonb_typeof(specialties_json) = 'array' "
            "AND jsonb_typeof(contacts_json) = 'array' "
            "AND jsonb_typeof(addresses_json) = 'array' "
            "AND jsonb_typeof(references_json) = 'array'"
        ),
    },
    "provider_directory_physical_projection_source_summary": {
        "pd_physical_source_summary_values_check": (
            "canonical_row_sha256 ~ '^[0-9a-f]{64}$' "
            "AND dataset_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_count > 0 "
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(proof_json) = 'object'"
        ),
    },
    "provider_directory_physical_projection_partition": {
        "pd_physical_partition_values_check": (
            "proof_partition_id ~ '^[0-9a-f]{64}$' "
            "AND canonical_row_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_ordinal >= 0 AND resource_type <> '' "
            "AND resource_count > 0 AND jsonb_typeof(proof_json) = 'object'"
        ),
    },
    "provider_directory_physical_projection_reference": {
        "pd_physical_projection_reference_values_check": (
            "owner_kind IN ('dataset', 'build', 'artifact') "
            "AND owner_id <> '' "
            "AND reference_identity_hash ~ '^[0-9a-f]{64}$' "
            "AND lease_token ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(reference_proof_json) = 'object' "
            "AND lease_expires_at >= created_at "
            "AND lease_heartbeat_at >= created_at "
            "AND (released_at IS NULL OR released_at >= created_at) "
            "AND (retain_until IS NULL OR retain_until >= created_at)"
        ),
    },
}


_TRIGGER_TYPE_BY_TIMING = {
    "BEFORE INSERT OR UPDATE OR DELETE": 31,
    "BEFORE UPDATE OR DELETE": 27,
}


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _is_offline_mode() -> bool:
    get_context = getattr(op, "get_context", None)
    return bool(get_context and get_context().as_sql)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _timestamp(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(
        name,
        sa.TIMESTAMP(timezone=True),
        nullable=nullable,
        server_default=None if nullable else sa.func.now(),
    )


def _quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _canonical_check_definitions(
    schema: str,
    table_name: str,
    expected_checks: dict[str, str],
) -> dict[str, str]:
    connection = op.get_bind()
    temporary_table = f"mrf_projection_check_{uuid.uuid4().hex}"
    quoted_temporary = _quoted_identifier(temporary_table)
    quoted_source = f"{_quoted_identifier(schema)}.{_quoted_identifier(table_name)}"
    connection.execute(
        sa.text(
            f"CREATE TEMPORARY TABLE {quoted_temporary} "
            f"(LIKE {quoted_source} INCLUDING DEFAULTS) ON COMMIT DROP"
        )
    )
    try:
        for constraint_name, expression in expected_checks.items():
            connection.execute(
                sa.text(
                    f"ALTER TABLE {quoted_temporary} ADD CONSTRAINT "
                    f"{_quoted_identifier(constraint_name)} CHECK ({expression})"
                )
            )
        rows = connection.execute(
            sa.text(
                """
                SELECT constraint_record.conname,
                       pg_get_constraintdef(constraint_record.oid, true)
                  FROM pg_constraint AS constraint_record
                 WHERE constraint_record.conrelid = to_regclass(:table_name)
                   AND constraint_record.contype = 'c'
                """
            ),
            {"table_name": temporary_table},
        ).all()
        return {str(name): str(definition) for name, definition in rows}
    finally:
        connection.execute(sa.text(f"DROP TABLE IF EXISTS {quoted_temporary}"))


def _actual_check_definitions(schema: str, table_name: str) -> dict[str, str]:
    rows = (
        op.get_bind()
        .execute(
            sa.text(
                """
            SELECT constraint_record.conname,
                   pg_get_constraintdef(constraint_record.oid, true)
              FROM pg_constraint AS constraint_record
              JOIN pg_class AS table_record
                ON table_record.oid = constraint_record.conrelid
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = table_record.relnamespace
             WHERE namespace_record.nspname = :schema
               AND table_record.relname = :table_name
               AND constraint_record.contype = 'c'
            """
            ),
            {"schema": schema, "table_name": table_name},
        )
        .all()
    )
    return {str(name): str(definition) for name, definition in rows}


def _validate_check_constraints(schema: str) -> None:
    if _is_offline_mode():
        return
    for table_name, expected_checks in _CHECKS_BY_TABLE.items():
        expected_definitions = _canonical_check_definitions(
            schema,
            table_name,
            expected_checks,
        )
        actual_definitions = _actual_check_definitions(schema, table_name)
        for constraint_name, expected_definition in expected_definitions.items():
            if _normalized_expression(actual_definitions.get(constraint_name)) != (
                _normalized_expression(expected_definition)
            ):
                raise RuntimeError(
                    "existing_schema_check_constraint_mismatch:"
                    f"{schema}.{table_name}.{constraint_name}"
                )


def _validate_trigger(
    schema: str,
    trigger_name: str,
    table_name: str,
    timing: str,
    function_name: str,
) -> None:
    if _is_offline_mode():
        return
    trigger_record = (
        op.get_bind()
        .execute(
            sa.text(
                """
            SELECT function_record.proname,
                   function_schema.nspname,
                   trigger_record.tgenabled,
                   trigger_record.tgtype,
                   trigger_record.tgattr::text,
                   pg_get_expr(
                       trigger_record.tgqual,
                       trigger_record.tgrelid,
                       true
                   ),
                   encode(trigger_record.tgargs, 'hex'),
                   trigger_record.tgoldtable,
                   trigger_record.tgnewtable,
                   pg_get_triggerdef(trigger_record.oid, true)
              FROM pg_trigger AS trigger_record
              JOIN pg_class AS table_record
                ON table_record.oid = trigger_record.tgrelid
              JOIN pg_namespace AS table_schema
                ON table_schema.oid = table_record.relnamespace
              JOIN pg_proc AS function_record
                ON function_record.oid = trigger_record.tgfoid
              JOIN pg_namespace AS function_schema
                ON function_schema.oid = function_record.pronamespace
             WHERE table_schema.nspname = :schema
               AND table_record.relname = :table_name
               AND trigger_record.tgname = :trigger_name
               AND NOT trigger_record.tgisinternal
            """
            ),
            {
                "schema": schema,
                "table_name": table_name,
                "trigger_name": trigger_name,
            },
        )
        .first()
    )
    if trigger_record is None:
        raise RuntimeError(
            f"existing_schema_trigger_missing:{schema}.{table_name}.{trigger_name}"
        )
    (
        actual_function,
        function_schema,
        enabled,
        trigger_type,
        update_columns,
        trigger_condition,
        trigger_arguments,
        old_transition_table,
        new_transition_table,
        trigger_definition,
    ) = tuple(trigger_record)
    if isinstance(enabled, bytes):
        enabled = enabled.decode("ascii")
    normalized_definition = " ".join(
        str(trigger_definition).replace('"', "").lower().split()
    )
    expected_relation = f" on {schema}.{table_name} "
    expected_function = f"execute function {schema}.{function_name}()"
    if (
        str(actual_function) != function_name
        or str(function_schema) != schema
        or str(enabled) != "O"
        or int(trigger_type) != _TRIGGER_TYPE_BY_TIMING[timing]
        or str(update_columns or "") != ""
        or trigger_condition is not None
        or str(trigger_arguments or "") != ""
        or old_transition_table is not None
        or new_transition_table is not None
        or not normalized_definition.startswith(
            f"create trigger {trigger_name.lower()} "
        )
        or expected_relation.lower() not in normalized_definition
        or expected_function.lower() not in normalized_definition
        or " when " in normalized_definition
        or " update of " in normalized_definition
    ):
        raise RuntimeError(
            f"existing_schema_trigger_mismatch:{schema}.{table_name}.{trigger_name}"
        )


def _create_partitioned_resource_table(schema: str) -> None:
    """Create the partitioned resource parent or validate an adopted parent."""

    connection = op.get_bind()
    relation_kind = None
    if not _is_offline_mode():
        relation_kind = connection.execute(
            sa.text(
                """
            SELECT class_record.relkind,
                   partition_record.partstrat,
                   pg_get_partkeydef(class_record.oid)
              FROM pg_class AS class_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = class_record.relnamespace
              LEFT JOIN pg_partitioned_table AS partition_record
                ON partition_record.partrelid = class_record.oid
             WHERE namespace_record.nspname = :schema
               AND class_record.relname =
                   'provider_directory_physical_projection_resource'
                """
            ),
            {"schema": schema},
        ).first()
    elements = (
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=256), nullable=False),
        sa.Column("proof_partition_id", sa.String(length=64), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(), nullable=False),
        sa.Column(
            "source_rank",
            sa.Text(),
            nullable=False,
        ),
        sa.Column("summary_npi", sa.BigInteger(), nullable=True),
        sa.Column("summary_address_count", sa.Integer(), nullable=False),
        sa.Column(
            "summary_addressed_location",
            sa.Boolean(),
            nullable=False,
        ),
        sa.Column(
            "summary_geocoded_location",
            sa.Boolean(),
            nullable=False,
        ),
        sa.Column(
            "summary_network_link_count",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column(
            "summary_affiliation_link_count",
            sa.Integer(),
            nullable=False,
        ),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("effective_start", sa.String(length=64), nullable=True),
        sa.Column("effective_end", sa.String(length=64), nullable=True),
        sa.Column("observed_at", sa.String(length=64), nullable=True),
        sa.Column("profile_evidence_json", postgresql.JSONB(), nullable=True),
        sa.CheckConstraint(
            "length(physical_projection_id) = 64 "
            "AND physical_projection_id !~ '[^0-9a-f]' "
            "AND length(proof_partition_id) = 64 "
            "AND proof_partition_id !~ '[^0-9a-f]' "
            "AND length(payload_hash) = 64 "
            "AND payload_hash !~ '[^0-9a-f]' "
            "AND resource_type <> '' AND resource_id <> '' "
            "AND source_rank <> '' "
            "AND (summary_npi IS NULL OR "
            "summary_npi BETWEEN 1000000000 AND 2999999999) "
            "AND summary_address_count >= 0 "
            "AND summary_network_link_count >= 0 "
            "AND summary_affiliation_link_count >= 0 "
            "AND summary_addressed_location = "
            "(resource_type = 'Location' AND summary_address_count > 0) "
            "AND (NOT summary_geocoded_location OR "
            "summary_addressed_location) "
            "AND (resource_type = 'InsurancePlan' OR "
            "summary_network_link_count = 0) "
            "AND (resource_type = 'OrganizationAffiliation' OR "
            "summary_affiliation_link_count = 0) "
            "AND (effective_start IS NULL OR effective_start <> '') "
            "AND (effective_end IS NULL OR effective_end <> '') "
            "AND (observed_at IS NULL OR observed_at <> '') "
            "AND (profile_evidence_json IS NULL OR ("
            "jsonb_typeof(profile_evidence_json) = 'object' "
            "AND profile_evidence_json <> '{}'::jsonb "
            "AND profile_evidence_json - ARRAY['names', 'specialties', "
            "'contacts', 'addresses', 'references'] = '{}'::jsonb "
            "AND (NOT profile_evidence_json ? 'names' OR "
            "(jsonb_typeof(profile_evidence_json -> 'names') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'names') > 0)) "
            "AND (NOT profile_evidence_json ? 'specialties' OR "
            "(jsonb_typeof(profile_evidence_json -> 'specialties') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'specialties') > 0)) "
            "AND (NOT profile_evidence_json ? 'contacts' OR "
            "(jsonb_typeof(profile_evidence_json -> 'contacts') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'contacts') > 0)) "
            "AND (NOT profile_evidence_json ? 'addresses' OR "
            "(jsonb_typeof(profile_evidence_json -> 'addresses') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'addresses') > 0)) "
            "AND (NOT profile_evidence_json ? 'references' OR "
            "(jsonb_typeof(profile_evidence_json -> 'references') = 'array' "
            "AND jsonb_array_length(profile_evidence_json -> 'references') > 0))))",
            name="pd_physical_projection_resource_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [
                f"{schema}.provider_directory_physical_projection."
                "physical_projection_id"
            ],
            name="pd_physical_projection_resource_projection_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            "resource_type",
            "resource_id",
            name="provider_directory_physical_projection_resource_pkey",
        ),
    )
    if relation_kind is None:
        op.create_table(
            "provider_directory_physical_projection_resource",
            *elements,
            schema=schema,
            postgresql_partition_by="LIST (physical_projection_id)",
        )
    else:
        relation_kind_value, partition_strategy, partition_key = tuple(relation_kind)
        if isinstance(relation_kind_value, bytes):
            relation_kind_value = relation_kind_value.decode("ascii")
        if isinstance(partition_strategy, bytes):
            partition_strategy = partition_strategy.decode("ascii")
        if (str(relation_kind_value), str(partition_strategy)) != (
            "p",
            "l",
        ) or _normalized_expression(partition_key) != _normalized_expression(
            "LIST (physical_projection_id)"
        ):
            raise RuntimeError(
                "existing_schema_partition_mismatch:"
                f"{schema}.provider_directory_physical_projection_resource"
            )
        create_table_or_validate(
            op,
            "provider_directory_physical_projection_resource",
            *elements,
            schema=schema,
        )
    create_index_if_missing(
        op,
        "pd_physical_projection_resource_identity_idx",
        "provider_directory_physical_projection_resource",
        ["resource_type", "resource_id", "physical_projection_id"],
        schema=schema,
    )


def _create_partitioned_membership_table(schema: str) -> None:
    """Create the optional fact-to-semantic-membership edge parent."""

    connection = op.get_bind()
    table_name = "provider_directory_physical_projection_resource_membership"
    relation_kind = None
    if not _is_offline_mode():
        relation_kind = connection.execute(
            sa.text(
                """
            SELECT class_record.relkind, partition_record.partstrat,
                   pg_get_partkeydef(class_record.oid)
              FROM pg_class AS class_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = class_record.relnamespace
              LEFT JOIN pg_partitioned_table AS partition_record
                ON partition_record.partrelid = class_record.oid
             WHERE namespace_record.nspname = :schema
               AND class_record.relname = :table_name
                """
            ),
            {"schema": schema, "table_name": table_name},
        ).first()
    elements = (
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("membership_partition_id", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=256), nullable=False),
        sa.CheckConstraint(
            "physical_projection_id ~ '^[0-9a-f]{64}$' "
            "AND membership_partition_id ~ '^[0-9a-f]{64}$' "
            "AND resource_type <> '' AND resource_id <> ''",
            name="pd_physical_projection_membership_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [f"{schema}.provider_directory_physical_projection.physical_projection_id"],
            name="pd_physical_projection_membership_projection_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id", "resource_type", "resource_id"],
            [
                f"{schema}.provider_directory_physical_projection_resource.physical_projection_id",
                f"{schema}.provider_directory_physical_projection_resource.resource_type",
                f"{schema}.provider_directory_physical_projection_resource.resource_id",
            ],
            name="pd_physical_projection_membership_resource_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            "membership_partition_id",
            "resource_type",
            "resource_id",
            name="pd_physical_projection_resource_membership_pkey",
        ),
    )
    if relation_kind is None:
        op.create_table(
            table_name,
            *elements,
            schema=schema,
            postgresql_partition_by="LIST (physical_projection_id)",
        )
    else:
        relation_kind_value, partition_strategy, partition_key = tuple(relation_kind)
        if isinstance(relation_kind_value, bytes):
            relation_kind_value = relation_kind_value.decode("ascii")
        if isinstance(partition_strategy, bytes):
            partition_strategy = partition_strategy.decode("ascii")
        if (str(relation_kind_value), str(partition_strategy)) != (
            "p",
            "l",
        ) or _normalized_expression(partition_key) != _normalized_expression(
            "LIST (physical_projection_id)"
        ):
            raise RuntimeError(
                "existing_schema_partition_mismatch:" f"{schema}.{table_name}"
            )
        create_table_or_validate(op, table_name, *elements, schema=schema)
    create_index_if_missing(
        op,
        "pd_physical_projection_membership_resource_idx",
        table_name,
        [
            "physical_projection_id",
            "resource_type",
            "resource_id",
            "membership_partition_id",
        ],
        schema=schema,
    )


def _create_partitioned_profile_contribution_table(schema: str) -> None:
    """Create the source-neutral Profile-contribution partition parent."""

    connection = op.get_bind()
    table_name = "provider_directory_physical_projection_profile_contribution"
    relation_kind = None
    if not _is_offline_mode():
        relation_kind = connection.execute(
            sa.text(
                """
            SELECT class_record.relkind, partition_record.partstrat,
                   pg_get_partkeydef(class_record.oid)
              FROM pg_class AS class_record
              JOIN pg_namespace AS namespace_record
                ON namespace_record.oid = class_record.relnamespace
              LEFT JOIN pg_partitioned_table AS partition_record
                ON partition_record.partrelid = class_record.oid
             WHERE namespace_record.nspname = :schema
               AND class_record.relname = :table_name
                """
            ),
            {"schema": schema, "table_name": table_name},
        ).first()
    elements = (
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("resource_id", sa.String(length=256), nullable=False),
        sa.Column("proof_partition_id", sa.String(length=64), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("source_rank", sa.Text(), nullable=False),
        sa.Column("direct_npi", sa.BigInteger(), nullable=True),
        sa.Column("active", sa.Boolean(), nullable=True),
        sa.Column("effective_start", sa.String(length=64), nullable=True),
        sa.Column("effective_end", sa.String(length=64), nullable=True),
        sa.Column("observed_at", sa.String(length=64), nullable=True),
        sa.Column("names_json", postgresql.JSONB(), nullable=False),
        sa.Column("specialties_json", postgresql.JSONB(), nullable=False),
        sa.Column("contacts_json", postgresql.JSONB(), nullable=False),
        sa.Column("addresses_json", postgresql.JSONB(), nullable=False),
        sa.Column("references_json", postgresql.JSONB(), nullable=False),
        sa.CheckConstraint(
            "physical_projection_id ~ '^[0-9a-f]{64}$' "
            "AND proof_partition_id ~ '^[0-9a-f]{64}$' "
            "AND payload_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_type <> '' AND resource_id <> '' "
            "AND source_rank <> '' "
            "AND (direct_npi IS NULL OR "
            "direct_npi BETWEEN 1000000000 AND 2999999999) "
            "AND (effective_start IS NULL OR effective_start <> '') "
            "AND (effective_end IS NULL OR effective_end <> '') "
            "AND (observed_at IS NULL OR observed_at <> '')",
            name="pd_physical_profile_contribution_values_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(names_json) = 'array' "
            "AND jsonb_typeof(specialties_json) = 'array' "
            "AND jsonb_typeof(contacts_json) = 'array' "
            "AND jsonb_typeof(addresses_json) = 'array' "
            "AND jsonb_typeof(references_json) = 'array'",
            name="pd_physical_profile_contribution_json_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [f"{schema}.provider_directory_physical_projection.physical_projection_id"],
            name="pd_physical_profile_contribution_projection_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id", "resource_type", "resource_id"],
            [
                f"{schema}.provider_directory_physical_projection_resource.physical_projection_id",
                f"{schema}.provider_directory_physical_projection_resource.resource_type",
                f"{schema}.provider_directory_physical_projection_resource.resource_id",
            ],
            name="pd_physical_profile_contribution_resource_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            "resource_type",
            "resource_id",
            name="pd_physical_profile_contribution_pkey",
        ),
    )
    if relation_kind is None:
        op.create_table(
            table_name,
            *elements,
            schema=schema,
            postgresql_partition_by="LIST (physical_projection_id)",
        )
    else:
        relation_kind_value, partition_strategy, partition_key = tuple(relation_kind)
        if isinstance(relation_kind_value, bytes):
            relation_kind_value = relation_kind_value.decode("ascii")
        if isinstance(partition_strategy, bytes):
            partition_strategy = partition_strategy.decode("ascii")
        if (str(relation_kind_value), str(partition_strategy)) != (
            "p",
            "l",
        ) or _normalized_expression(partition_key) != _normalized_expression(
            "LIST (physical_projection_id)"
        ):
            raise RuntimeError(
                "existing_schema_partition_mismatch:" f"{schema}.{table_name}"
            )
        create_table_or_validate(op, table_name, *elements, schema=schema)
    create_index_if_missing(
        op,
        "pd_physical_profile_contribution_npi_idx",
        table_name,
        ["physical_projection_id", "direct_npi", "resource_type", "resource_id"],
        schema=schema,
    )


def _create_tables(schema: str) -> None:
    create_table_or_validate(
        op,
        "provider_directory_physical_projection",
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("canonical_row_sha256", sa.String(length=64), nullable=False),
        sa.Column("content_hash_contract_id", sa.String(length=128), nullable=False),
        sa.Column("decoder_contract_id", sa.String(length=128), nullable=False),
        sa.Column("input_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("transform_contract_id", sa.String(length=128), nullable=False),
        sa.Column("scope_contract_id", sa.String(length=128), nullable=False),
        sa.Column("transform_context_hash", sa.String(length=64), nullable=False),
        sa.Column("transform_context_json", postgresql.JSONB(), nullable=False),
        sa.Column("completeness_manifest_hash", sa.String(length=64), nullable=False),
        sa.Column("completeness_manifest_json", postgresql.JSONB(), nullable=False),
        sa.Column("dataset_hash", sa.String(length=64), nullable=False),
        sa.Column("resource_profile_hash", sa.String(length=64), nullable=False),
        sa.Column("selected_resources_json", postgresql.JSONB(), nullable=False),
        sa.Column("required_resources_json", postgresql.JSONB(), nullable=False),
        sa.Column("resource_count", sa.BigInteger(), nullable=False),
        sa.Column("resource_counts_json", postgresql.JSONB(), nullable=False),
        sa.Column("proof_json", postgresql.JSONB(), nullable=False),
        sa.Column("storage_schema", sa.String(length=63), nullable=False),
        sa.Column("storage_relation", sa.String(length=63), nullable=False),
        sa.Column("storage_relation_oid", sa.BigInteger(), nullable=False),
        sa.Column("storage_trigger_oid", sa.BigInteger(), nullable=False),
        sa.Column("membership_storage_schema", sa.String(length=63), nullable=True),
        sa.Column("membership_storage_relation", sa.String(length=63), nullable=True),
        sa.Column("membership_storage_relation_oid", sa.BigInteger(), nullable=True),
        sa.Column("membership_storage_trigger_oid", sa.BigInteger(), nullable=True),
        sa.Column(
            "profile_contribution_storage_schema",
            sa.String(length=63),
            nullable=True,
        ),
        sa.Column(
            "profile_contribution_storage_relation",
            sa.String(length=63),
            nullable=True,
        ),
        sa.Column(
            "profile_contribution_storage_relation_oid",
            sa.BigInteger(),
            nullable=True,
        ),
        sa.Column(
            "profile_contribution_storage_trigger_oid",
            sa.BigInteger(),
            nullable=True,
        ),
        sa.Column("status", sa.String(length=16), nullable=False),
        _timestamp("created_at"),
        _timestamp("sealed_at"),
        _timestamp("retain_until"),
        _timestamp("retiring_at", nullable=True),
        sa.CheckConstraint(
            "physical_projection_id ~ '^[0-9a-f]{64}$' "
            "AND canonical_row_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND transform_context_hash ~ '^[0-9a-f]{64}$' "
            "AND completeness_manifest_hash ~ '^[0-9a-f]{64}$' "
            "AND dataset_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_profile_hash ~ '^[0-9a-f]{64}$' "
            "AND scope_contract_id <> ''",
            name="pd_physical_projection_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(selected_resources_json) = 'array' "
            "AND jsonb_array_length(selected_resources_json) > 0 "
            "AND jsonb_typeof(required_resources_json) = 'array' "
            "AND jsonb_typeof(transform_context_json) = 'object' "
            "AND jsonb_typeof(completeness_manifest_json) = 'object' "
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(proof_json) = 'object'",
            name="pd_physical_projection_json_check",
        ),
        sa.CheckConstraint(
            "resource_count > 0 AND storage_schema <> '' "
            "AND storage_relation <> '' AND storage_relation_oid > 0 "
            "AND storage_trigger_oid > 0 "
            "AND ((membership_storage_schema IS NULL "
            "AND membership_storage_relation IS NULL "
            "AND membership_storage_relation_oid IS NULL "
            "AND membership_storage_trigger_oid IS NULL) OR "
            "(membership_storage_schema <> '' "
            "AND membership_storage_relation <> '' "
            "AND membership_storage_relation_oid > 0 "
            "AND membership_storage_trigger_oid > 0)) "
            "AND ((profile_contribution_storage_schema IS NULL "
            "AND profile_contribution_storage_relation IS NULL "
            "AND profile_contribution_storage_relation_oid IS NULL "
            "AND profile_contribution_storage_trigger_oid IS NULL) OR "
            "(profile_contribution_storage_schema <> '' "
            "AND profile_contribution_storage_relation <> '' "
            "AND profile_contribution_storage_relation_oid > 0 "
            "AND profile_contribution_storage_trigger_oid > 0)) "
            "AND status IN ('sealed', 'retiring') "
            "AND retain_until >= sealed_at "
            "AND ((status = 'sealed' AND retiring_at IS NULL) "
            "OR (status = 'retiring' AND retiring_at IS NOT NULL))",
            name="pd_physical_projection_state_check",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            name="provider_directory_physical_projection_pkey",
        ),
        sa.UniqueConstraint(
            "storage_schema",
            "storage_relation",
            name="pd_physical_projection_storage_name_key",
        ),
        sa.UniqueConstraint(
            "storage_relation_oid",
            name="pd_physical_projection_storage_oid_key",
        ),
        sa.UniqueConstraint(
            "membership_storage_schema",
            "membership_storage_relation",
            name="pd_physical_projection_membership_storage_name_key",
        ),
        sa.UniqueConstraint(
            "membership_storage_relation_oid",
            name="pd_physical_projection_membership_storage_oid_key",
        ),
        sa.UniqueConstraint(
            "profile_contribution_storage_schema",
            "profile_contribution_storage_relation",
            name="pd_physical_projection_profile_storage_name_key",
        ),
        sa.UniqueConstraint(
            "profile_contribution_storage_relation_oid",
            name="pd_physical_projection_profile_storage_oid_key",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_physical_projection_gc_idx",
        "provider_directory_physical_projection",
        ["status", "retain_until"],
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_physical_projection_source_summary",
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("canonical_row_sha256", sa.String(length=64), nullable=False),
        sa.Column("dataset_hash", sa.String(length=64), nullable=False),
        sa.Column("resource_count", sa.BigInteger(), nullable=False),
        sa.Column("resource_counts_json", postgresql.JSONB(), nullable=False),
        sa.Column("proof_json", postgresql.JSONB(), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "canonical_row_sha256 ~ '^[0-9a-f]{64}$' "
            "AND dataset_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_count > 0 "
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(proof_json) = 'object'",
            name="pd_physical_source_summary_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [
                f"{schema}.provider_directory_physical_projection."
                "physical_projection_id"
            ],
            name="pd_physical_source_summary_projection_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            name="provider_directory_physical_projection_source_summary_pkey",
        ),
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_physical_projection_partition",
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("proof_partition_id", sa.String(length=64), nullable=False),
        sa.Column("partition_ordinal", sa.Integer(), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("canonical_row_sha256", sa.String(length=64), nullable=False),
        sa.Column("resource_count", sa.BigInteger(), nullable=False),
        sa.Column("proof_json", postgresql.JSONB(), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "proof_partition_id ~ '^[0-9a-f]{64}$' "
            "AND canonical_row_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_ordinal >= 0 AND resource_type <> '' "
            "AND resource_count > 0 AND jsonb_typeof(proof_json) = 'object'",
            name="pd_physical_partition_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [f"{schema}.provider_directory_physical_projection.physical_projection_id"],
            name="pd_physical_partition_projection_fkey",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            "proof_partition_id",
            name="provider_directory_physical_projection_partition_pkey",
        ),
        sa.UniqueConstraint(
            "physical_projection_id",
            "resource_type",
            "partition_ordinal",
            name="pd_physical_partition_ordinal_key",
        ),
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_projection_recipe",
        sa.Column("recipe_id", sa.String(length=64), nullable=False),
        sa.Column("decoder_contract_id", sa.String(length=128), nullable=False),
        sa.Column("input_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("transform_contract_id", sa.String(length=128), nullable=False),
        sa.Column("scope_contract_id", sa.String(length=128), nullable=False),
        sa.Column("transform_context_hash", sa.String(length=64), nullable=False),
        sa.Column("transform_context_json", postgresql.JSONB(), nullable=False),
        sa.Column("completeness_manifest_hash", sa.String(length=64), nullable=False),
        sa.Column("completeness_manifest_json", postgresql.JSONB(), nullable=False),
        sa.Column("resource_profile_hash", sa.String(length=64), nullable=False),
        sa.Column("selected_resources_json", postgresql.JSONB(), nullable=False),
        sa.Column("required_resources_json", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("lease_token", sa.String(length=64)),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("lease_heartbeat_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("physical_projection_id", sa.String(length=64)),
        sa.Column("stage_schema", sa.String(length=63)),
        sa.Column("stage_relation", sa.String(length=63)),
        sa.Column("stage_relation_oid", sa.BigInteger()),
        sa.Column("membership_stage_schema", sa.String(length=63)),
        sa.Column("membership_stage_relation", sa.String(length=63)),
        sa.Column("membership_stage_relation_oid", sa.BigInteger()),
        sa.Column("profile_contribution_stage_schema", sa.String(length=63)),
        sa.Column("profile_contribution_stage_relation", sa.String(length=63)),
        sa.Column("profile_contribution_stage_relation_oid", sa.BigInteger()),
        sa.Column("input_block_set_sha256", sa.String(length=64)),
        sa.Column("input_block_count", sa.Integer()),
        sa.Column("partition_set_sha256", sa.String(length=64)),
        sa.Column("partition_count", sa.Integer()),
        sa.Column("native_campaign_proof_json", postgresql.JSONB()),
        sa.Column("prepared_proof_json", postgresql.JSONB()),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        _timestamp("workset_registered_at", nullable=True),
        _timestamp("sealed_at", nullable=True),
        sa.CheckConstraint(
            "recipe_id ~ '^[0-9a-f]{64}$' "
            "AND input_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND transform_context_hash ~ '^[0-9a-f]{64}$' "
            "AND completeness_manifest_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_profile_hash ~ '^[0-9a-f]{64}$' "
            "AND scope_contract_id <> '' "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')",
            name="pd_projection_recipe_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(selected_resources_json) = 'array' "
            "AND jsonb_array_length(selected_resources_json) > 0 "
            "AND jsonb_typeof(required_resources_json) = 'array' "
            "AND jsonb_typeof(transform_context_json) = 'object' "
            "AND jsonb_typeof(completeness_manifest_json) = 'object' "
            "AND (prepared_proof_json IS NULL OR "
            "jsonb_typeof(prepared_proof_json) = 'object') "
            "AND (native_campaign_proof_json IS NULL OR "
            "jsonb_typeof(native_campaign_proof_json) = 'object')",
            name="pd_projection_recipe_json_check",
        ),
        sa.CheckConstraint(
            "attempt > 0 AND status IN "
            "('building', 'proof_ready', 'sealed', 'failed', 'retired') "
            "AND ((status IN ('building', 'proof_ready') "
            "AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL "
            "AND physical_projection_id IS NULL AND sealed_at IS NULL) "
            "OR (status = 'sealed' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL "
            "AND physical_projection_id IS NOT NULL AND sealed_at IS NOT NULL) "
            "OR (status IN ('failed', 'retired') "
            "AND lease_token IS NULL AND lease_expires_at IS NULL)) "
            "AND (status NOT IN ('proof_ready', 'sealed') "
            "OR workset_registered_at IS NOT NULL) "
            "AND (status <> 'proof_ready' OR prepared_proof_json IS NOT NULL)",
            name="pd_projection_recipe_state_check",
        ),
        sa.CheckConstraint(
            "(stage_schema IS NULL AND stage_relation IS NULL "
            "AND stage_relation_oid IS NULL) OR "
            "(stage_schema <> '' AND stage_relation <> '' "
            "AND stage_relation_oid > 0)",
            name="pd_projection_recipe_stage_check",
        ),
        sa.CheckConstraint(
            "(membership_stage_schema IS NULL "
            "AND membership_stage_relation IS NULL "
            "AND membership_stage_relation_oid IS NULL) OR "
            "(membership_stage_schema <> '' "
            "AND membership_stage_relation <> '' "
            "AND membership_stage_relation_oid > 0)",
            name="pd_projection_recipe_membership_stage_check",
        ),
        sa.CheckConstraint(
            "(profile_contribution_stage_schema IS NULL "
            "AND profile_contribution_stage_relation IS NULL "
            "AND profile_contribution_stage_relation_oid IS NULL) OR "
            "(profile_contribution_stage_schema <> '' "
            "AND profile_contribution_stage_relation <> '' "
            "AND profile_contribution_stage_relation_oid > 0)",
            name="pd_projection_recipe_profile_stage_check",
        ),
        sa.CheckConstraint(
            "(input_block_set_sha256 IS NULL AND input_block_count IS NULL "
            "AND partition_set_sha256 IS NULL AND partition_count IS NULL "
            "AND workset_registered_at IS NULL) OR "
            "(input_block_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_block_count > 0 "
            "AND partition_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_count > 0 AND workset_registered_at IS NOT NULL)",
            name="pd_projection_recipe_workset_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [
                f"{schema}.provider_directory_physical_projection."
                "physical_projection_id"
            ],
            name="pd_projection_recipe_physical_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "recipe_id",
            name="provider_directory_projection_recipe_pkey",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_recipe_lease_idx",
        "provider_directory_projection_recipe",
        ["status", "lease_expires_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_recipe_physical_idx",
        "provider_directory_projection_recipe",
        ["physical_projection_id"],
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_projection_input_block",
        sa.Column("recipe_id", sa.String(length=64), nullable=False),
        sa.Column("block_id", sa.String(length=64), nullable=False),
        sa.Column("block_ordinal", sa.Integer(), nullable=False),
        sa.Column("upstream_artifact_id", sa.String(length=64), nullable=False),
        sa.Column("source_object_id", sa.String(length=64), nullable=False),
        sa.Column("block_kind", sa.String(length=64), nullable=False),
        sa.Column("input_contract_id", sa.String(length=128), nullable=False),
        sa.Column("source_partition_ordinal", sa.Integer(), nullable=False),
        sa.Column("record_start", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_bytes", sa.BigInteger(), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(), nullable=False),
        sa.Column("retained_campaign_id", sa.String(length=64), nullable=False),
        sa.Column(
            "retained_campaign_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("retained_source_item_id", sa.String(length=64), nullable=False),
        sa.Column("retained_range_ordinal", sa.Integer(), nullable=True),
        sa.Column("block_proof_sha256", sa.String(length=64), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "block_id ~ '^[0-9a-f]{64}$' "
            "AND upstream_artifact_id ~ '^[0-9a-f]{64}$' "
            "AND source_object_id ~ '^[0-9a-f]{64}$' "
            "AND content_sha256 ~ '^[0-9a-f]{64}$' "
            "AND payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND block_proof_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_projection_input_block_digest_check",
        ),
        sa.CheckConstraint(
            "block_ordinal >= 0 AND source_partition_ordinal >= 0 "
            "AND record_start >= 0 AND record_count > 0 "
            "AND payload_bytes > 0 AND upstream_artifact_id <> '' "
            "AND source_object_id <> '' AND block_kind <> '' "
            "AND input_contract_id <> '' "
            "AND jsonb_typeof(summary_json) = 'object' "
            "AND (retained_range_ordinal IS NULL "
            "OR retained_range_ordinal >= 0)",
            name="pd_projection_input_block_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id"],
            [f"{schema}.provider_directory_projection_recipe.recipe_id"],
            name="pd_projection_input_block_recipe_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id", "retained_source_item_id"],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item.source_item_id",
            ],
            name="pd_projection_input_block_retained_item_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "recipe_id",
            "block_id",
            name="provider_directory_projection_input_block_pkey",
        ),
        sa.UniqueConstraint(
            "recipe_id",
            "block_ordinal",
            name="pd_projection_input_block_ordinal_key",
        ),
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_projection_proof_shard",
        sa.Column("recipe_id", sa.String(length=64), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("partition_id", sa.String(length=64), nullable=False),
        sa.Column("partition_ordinal", sa.Integer(), nullable=False),
        sa.Column("partition_key", sa.String(length=64), nullable=False),
        sa.Column("input_block_id", sa.String(length=64), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("input_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("partition_attempt", sa.Integer(), nullable=False),
        sa.Column("recipe_lease_token", sa.String(length=64)),
        sa.Column("lease_token", sa.String(length=64)),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("lease_heartbeat_at", sa.TIMESTAMP(timezone=True)),
        sa.Column("canonical_row_sha256", sa.String(length=64)),
        sa.Column("resource_count", sa.BigInteger()),
        sa.Column("first_identity_json", postgresql.JSONB()),
        sa.Column("last_identity_json", postgresql.JSONB()),
        sa.Column("proof_json", postgresql.JSONB()),
        _timestamp("created_at"),
        _timestamp("completed_at", nullable=True),
        sa.CheckConstraint(
            "partition_id ~ '^[0-9a-f]{64}$' "
            "AND partition_key ~ '^[0-9a-f]{64}$' "
            "AND input_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (recipe_lease_token IS NULL OR "
            "recipe_lease_token ~ '^[0-9a-f]{64}$') "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$') "
            "AND (canonical_row_sha256 IS NULL OR "
            "canonical_row_sha256 ~ '^[0-9a-f]{64}$')",
            name="pd_projection_proof_shard_digest_check",
        ),
        sa.CheckConstraint(
            "attempt > 0 AND partition_ordinal >= 0 "
            "AND partition_key <> '' AND resource_type <> '' "
            "AND partition_attempt >= 0 "
            "AND status IN ('pending', 'building', 'complete') "
            "AND ((status = 'pending' AND partition_attempt = 0 "
            "AND recipe_lease_token IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND canonical_row_sha256 IS NULL AND resource_count IS NULL "
            "AND first_identity_json IS NULL AND last_identity_json IS NULL "
            "AND proof_json IS NULL AND completed_at IS NULL) OR "
            "(status = 'building' AND partition_attempt > 0 "
            "AND recipe_lease_token IS NOT NULL AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL "
            "AND lease_heartbeat_at IS NOT NULL "
            "AND canonical_row_sha256 IS NULL AND resource_count IS NULL "
            "AND first_identity_json IS NULL AND last_identity_json IS NULL "
            "AND proof_json IS NULL AND completed_at IS NULL) OR "
            "(status = 'complete' AND partition_attempt > 0 "
            "AND recipe_lease_token IS NULL AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND canonical_row_sha256 IS NOT NULL AND resource_count > 0 "
            "AND jsonb_typeof(first_identity_json) = 'array' "
            "AND jsonb_typeof(last_identity_json) = 'array' "
            "AND jsonb_typeof(proof_json) = 'object' "
            "AND completed_at IS NOT NULL))",
            name="pd_projection_proof_shard_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id"],
            [f"{schema}.provider_directory_projection_recipe.recipe_id"],
            name="pd_projection_proof_shard_recipe_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id", "input_block_id"],
            [
                f"{schema}.provider_directory_projection_input_block.recipe_id",
                f"{schema}.provider_directory_projection_input_block.block_id",
            ],
            name="pd_projection_proof_shard_input_block_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "recipe_id",
            "attempt",
            "partition_id",
            name="provider_directory_projection_proof_shard_pkey",
        ),
        sa.UniqueConstraint(
            "recipe_id",
            "attempt",
            "resource_type",
            "partition_ordinal",
            name="pd_projection_proof_shard_ordinal_key",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_proof_shard_order_idx",
        "provider_directory_projection_proof_shard",
        ["recipe_id", "attempt", "resource_type", "partition_ordinal"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_proof_shard_claim_idx",
        "provider_directory_projection_proof_shard",
        ["recipe_id", "attempt", "status", "lease_expires_at"],
        schema=schema,
    )

    _create_partitioned_resource_table(schema)
    _create_partitioned_membership_table(schema)
    _create_partitioned_profile_contribution_table(schema)

    create_table_or_validate(
        op,
        "provider_directory_physical_projection_reference",
        sa.Column("physical_projection_id", sa.String(length=64), nullable=False),
        sa.Column("owner_kind", sa.String(length=32), nullable=False),
        sa.Column("owner_id", sa.String(length=128), nullable=False),
        sa.Column("reference_identity_hash", sa.String(length=64), nullable=False),
        sa.Column("reference_proof_json", postgresql.JSONB(), nullable=False),
        sa.Column("lease_token", sa.String(length=64), nullable=False),
        _timestamp("created_at"),
        _timestamp("lease_expires_at"),
        _timestamp("lease_heartbeat_at"),
        _timestamp("retain_until", nullable=True),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "owner_kind IN ('dataset', 'build', 'artifact') "
            "AND owner_id <> '' "
            "AND reference_identity_hash ~ '^[0-9a-f]{64}$' "
            "AND lease_token ~ '^[0-9a-f]{64}$' "
            "AND jsonb_typeof(reference_proof_json) = 'object' "
            "AND lease_expires_at >= created_at "
            "AND lease_heartbeat_at >= created_at "
            "AND (released_at IS NULL OR released_at >= created_at) "
            "AND (retain_until IS NULL OR retain_until >= created_at)",
            name="pd_physical_projection_reference_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["physical_projection_id"],
            [
                f"{schema}.provider_directory_physical_projection."
                "physical_projection_id"
            ],
            name="pd_physical_projection_reference_projection_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "physical_projection_id",
            "owner_kind",
            "owner_id",
            name="provider_directory_physical_projection_reference_pkey",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_physical_projection_reference_active_idx",
        "provider_directory_physical_projection_reference",
        [
            "physical_projection_id",
            "lease_expires_at",
            "owner_kind",
            "owner_id",
        ],
        schema=schema,
        postgresql_where=sa.text("released_at IS NULL"),
    )


def _create_fences(schema: str) -> None:
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    function_block = f"""
        CREATE OR REPLACE FUNCTION
        {quoted_schema}.reject_provider_directory_projection_resource_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'UPDATE'
               AND current_setting(
                    'healthporta.provider_directory_projection_prepare', true
               ) = 'on'
               AND OLD.physical_projection_id IS NULL
               AND NEW.physical_projection_id ~ '^[0-9a-f]{{64}}$'
               AND OLD.resource_type = NEW.resource_type
               AND OLD.resource_id = NEW.resource_id
               AND OLD.proof_partition_id = NEW.proof_partition_id
               AND OLD.payload_hash = NEW.payload_hash
               AND OLD.payload_json = NEW.payload_json
               AND OLD.source_rank = NEW.source_rank
               AND OLD.summary_npi IS NOT DISTINCT FROM NEW.summary_npi
               AND OLD.summary_address_count = NEW.summary_address_count
               AND OLD.summary_addressed_location =
                   NEW.summary_addressed_location
               AND OLD.summary_geocoded_location =
                   NEW.summary_geocoded_location
               AND OLD.summary_network_link_count =
                   NEW.summary_network_link_count
               AND OLD.summary_affiliation_link_count =
                   NEW.summary_affiliation_link_count
               AND OLD.active IS NOT DISTINCT FROM NEW.active
               AND OLD.effective_start IS NOT DISTINCT FROM NEW.effective_start
               AND OLD.effective_end IS NOT DISTINCT FROM NEW.effective_end
               AND OLD.observed_at IS NOT DISTINCT FROM NEW.observed_at
               AND OLD.profile_evidence_json IS NOT DISTINCT FROM
                   NEW.profile_evidence_json THEN
                RETURN NEW;
            END IF;
            IF TG_OP IN ('UPDATE', 'DELETE') THEN
                RAISE EXCEPTION 'provider_directory_projection_resource_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF EXISTS (
                SELECT 1
                  FROM {quoted_schema}.provider_directory_physical_projection
                 WHERE physical_projection_id = NEW.physical_projection_id
            ) THEN
                RAISE EXCEPTION 'provider_directory_projection_resource_sealed'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.reject_provider_directory_projection_stage_mutation()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'provider_directory_projection_stage_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_stage_is_real(
            candidate_schema text,
            candidate_relation text,
            candidate_relation_oid bigint
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT candidate_schema IS NOT NULL
               AND candidate_relation IS NOT NULL
               AND candidate_relation_oid IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM pg_class AS relation_record
                      JOIN pg_namespace AS relation_schema
                        ON relation_schema.oid = relation_record.relnamespace
                     WHERE relation_record.oid::bigint = candidate_relation_oid
                       AND relation_schema.nspname = candidate_schema
                       AND relation_record.relname = candidate_relation
                       AND relation_record.relkind IN ('r', 'p')
               );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_stage_matches_parent(
            candidate_schema text,
            candidate_relation text,
            candidate_relation_oid bigint,
            parent_relation text,
            require_logged boolean
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT candidate_schema = {_sql_literal(schema)}
               AND candidate_relation IS NOT NULL
               AND candidate_relation_oid IS NOT NULL
               AND parent_relation IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM pg_class AS relation_record
                      JOIN pg_namespace AS relation_schema
                        ON relation_schema.oid = relation_record.relnamespace
                      JOIN pg_class AS parent_record
                        ON parent_record.relname = parent_relation
                      JOIN pg_namespace AS parent_schema
                        ON parent_schema.oid = parent_record.relnamespace
                     WHERE relation_record.oid::bigint = candidate_relation_oid
                       AND relation_schema.nspname = candidate_schema
                       AND relation_record.relname = candidate_relation
                       AND relation_record.relkind IN ('r', 'p')
                       AND NOT relation_record.relrowsecurity
                       AND NOT relation_record.relforcerowsecurity
                       AND parent_schema.nspname = {_sql_literal(schema)}
                       AND parent_record.relkind = 'p'
                       AND (
                            NOT require_logged
                            OR (
                                relation_record.relpersistence = 'p'
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM pg_partition_tree(
                                          relation_record.oid::regclass
                                      ) AS stage_tree
                                      JOIN pg_class AS stage_tree_relation
                                        ON stage_tree_relation.oid =
                                           stage_tree.relid
                                     WHERE stage_tree_relation.relpersistence <>
                                           'p'
                                        OR stage_tree_relation.relkind NOT IN
                                           ('r', 'p')
                                        OR stage_tree_relation.relrowsecurity
                                        OR stage_tree_relation.relforcerowsecurity
                                )
                            )
                       )
                       AND (
                            SELECT count(*)
                              FROM pg_attribute AS candidate_attribute
                             WHERE candidate_attribute.attrelid =
                                   relation_record.oid
                               AND candidate_attribute.attnum > 0
                               AND NOT candidate_attribute.attisdropped
                       ) = (
                            SELECT count(*)
                              FROM pg_attribute AS parent_attribute
                             WHERE parent_attribute.attrelid = parent_record.oid
                               AND parent_attribute.attnum > 0
                               AND NOT parent_attribute.attisdropped
                       )
                       AND NOT EXISTS (
                            SELECT 1
                              FROM pg_attribute AS candidate_attribute
                             WHERE candidate_attribute.attrelid =
                                   relation_record.oid
                               AND candidate_attribute.attnum > 0
                               AND NOT candidate_attribute.attisdropped
                               AND NOT EXISTS (
                                   SELECT 1
                                     FROM pg_attribute AS parent_attribute
                                    WHERE parent_attribute.attrelid =
                                          parent_record.oid
                                      AND parent_attribute.attnum > 0
                                      AND NOT parent_attribute.attisdropped
                                      AND parent_attribute.attname =
                                          candidate_attribute.attname
                                      AND parent_attribute.attnum =
                                          candidate_attribute.attnum
                                      AND parent_attribute.atttypid =
                                          candidate_attribute.atttypid
                                      AND parent_attribute.atttypmod =
                                          candidate_attribute.atttypmod
                                      AND parent_attribute.attnotnull =
                                          candidate_attribute.attnotnull
                                      AND parent_attribute.attcollation =
                                          candidate_attribute.attcollation
                                      AND parent_attribute.attidentity =
                                          candidate_attribute.attidentity
                                      AND parent_attribute.attgenerated =
                                          candidate_attribute.attgenerated
                                      AND parent_attribute.attstorage =
                                          candidate_attribute.attstorage
                                      AND parent_attribute.attcompression =
                                          candidate_attribute.attcompression
                               )
                       )
                       AND NOT EXISTS (
                            SELECT 1
                              FROM pg_constraint AS parent_constraint
                             WHERE parent_constraint.conrelid = parent_record.oid
                               AND parent_constraint.contype = 'c'
                               AND NOT EXISTS (
                                   SELECT 1
                                     FROM pg_constraint AS candidate_constraint
                                    WHERE candidate_constraint.conrelid =
                                          relation_record.oid
                                      AND candidate_constraint.contype = 'c'
                                      AND candidate_constraint.convalidated
                                      AND candidate_constraint.conname =
                                          parent_constraint.conname
                                      AND pg_get_expr(
                                          candidate_constraint.conbin,
                                          candidate_constraint.conrelid
                                      ) = pg_get_expr(
                                          parent_constraint.conbin,
                                          parent_constraint.conrelid
                                      )
                               )
                       )
                       AND NOT EXISTS (
                            SELECT 1
                              FROM pg_constraint AS candidate_constraint
                             WHERE candidate_constraint.conrelid =
                                   relation_record.oid
                               AND candidate_constraint.contype = 'c'
                               AND NOT candidate_constraint.convalidated
                       )
               );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_stage_has_index(
            candidate_relation_oid bigint,
            required_columns text[],
            require_unique boolean
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT EXISTS (
                SELECT 1
                  FROM pg_index AS index_record
                 WHERE index_record.indrelid = candidate_relation_oid::oid
                   AND index_record.indisvalid
                   AND index_record.indisready
                   AND index_record.indislive
                   AND index_record.indisunique = require_unique
                   AND index_record.indpred IS NULL
                   AND index_record.indexprs IS NULL
                   AND index_record.indnkeyatts =
                       cardinality(required_columns)
                   AND index_record.indnatts =
                       cardinality(required_columns)
                   AND ARRAY(
                       SELECT pg_get_indexdef(
                           index_record.indexrelid,
                           column_ordinal,
                           true
                       )
                         FROM generate_series(
                             1,
                             index_record.indnkeyatts
                         ) AS column_ordinal
                        ORDER BY column_ordinal
                   ) = required_columns
            );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_stage_matches_proof(
            candidate_schema text,
            candidate_relation text,
            candidate_relation_oid bigint,
            candidate_trigger_oid bigint,
            physical_projection_id text,
            prepared_proof jsonb
        ) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
        DECLARE
            stage_matches boolean := false;
        BEGIN
            IF NOT {quoted_schema}.
                provider_directory_projection_stage_matches_parent(
                    candidate_schema,
                    candidate_relation,
                    candidate_relation_oid,
                    'provider_directory_physical_projection_resource',
                    true
                )
               OR NOT {quoted_schema}.
                  provider_directory_projection_stage_is_immutable(
                      candidate_schema,
                      candidate_relation,
                      candidate_relation_oid,
                      candidate_trigger_oid
                  )
               OR NOT {quoted_schema}.
                  provider_directory_projection_stage_has_index(
                      candidate_relation_oid,
                      ARRAY[
                          'physical_projection_id',
                          'resource_type',
                          'resource_id'
                      ],
                      true
                  )
               OR NOT {quoted_schema}.
                  provider_directory_projection_stage_has_index(
                      candidate_relation_oid,
                      ARRAY[
                          'resource_type',
                          'resource_id',
                          'physical_projection_id'
                      ],
                      false
                  ) THEN
                RETURN false;
            END IF;
            EXECUTE format(
                $stage_query$
                WITH actual_resource_counts AS (
                    SELECT resource_type, count(*)::bigint AS resource_count
                      FROM %I.%I
                     GROUP BY resource_type
                ),
                expected_resource_counts AS (
                    SELECT resource_type,
                           (count_value #>> '{{}}')::bigint AS resource_count
                      FROM jsonb_each($2 -> 'resource_counts')
                           AS expected(resource_type, count_value)
                     WHERE (count_value #>> '{{}}')::bigint > 0
                ),
                actual_partition_counts AS (
                    SELECT proof_partition_id,
                           count(*)::bigint AS resource_count
                      FROM %I.%I
                     GROUP BY proof_partition_id
                ),
                expected_partitions AS (
                    SELECT shard ->> 'partition_id' AS partition_id,
                           shard ->> 'resource_type' AS resource_type,
                           (shard ->> 'resource_count')::bigint
                               AS resource_count
                      FROM jsonb_array_elements($2 -> 'raw_shards') AS shard
                )
                SELECT
                    (SELECT count(*) FROM %I.%I) =
                        ($2 ->> 'resource_count')::bigint
                    AND NOT EXISTS (
                        SELECT 1 FROM %I.%I AS resource_record
                         WHERE resource_record.physical_projection_id <> $1
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM actual_resource_counts AS actual
                          FULL JOIN expected_resource_counts AS expected
                            USING (resource_type)
                         WHERE actual.resource_count IS DISTINCT FROM
                               expected.resource_count
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM actual_partition_counts AS actual
                          FULL JOIN expected_partitions AS expected
                            ON expected.partition_id =
                               actual.proof_partition_id
                         WHERE actual.resource_count IS DISTINCT FROM
                               expected.resource_count
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM %I.%I AS resource_record
                         WHERE NOT EXISTS (
                             SELECT 1
                               FROM expected_partitions AS expected
                              WHERE expected.partition_id =
                                    resource_record.proof_partition_id
                                AND (
                                    expected.resource_type = '__mixed__'
                                    OR expected.resource_type =
                                       resource_record.resource_type
                                )
                         )
                    );
                $stage_query$,
                candidate_schema,
                candidate_relation,
                candidate_schema,
                candidate_relation,
                candidate_schema,
                candidate_relation,
                candidate_schema,
                candidate_relation,
                candidate_schema,
                candidate_relation
            ) INTO stage_matches USING physical_projection_id, prepared_proof;
            RETURN COALESCE(stage_matches, false);
        EXCEPTION WHEN OTHERS THEN
            RETURN false;
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_membership_matches_proof(
            candidate_schema text,
            candidate_relation text,
            candidate_relation_oid bigint,
            candidate_trigger_oid bigint,
            physical_projection_id text,
            prepared_proof jsonb,
            resource_schema text,
            resource_relation text
        ) RETURNS boolean LANGUAGE plpgsql STABLE AS $$
        DECLARE
            stage_matches boolean := false;
            membership_proof jsonb := prepared_proof -> 'semantic_membership';
        BEGIN
            IF jsonb_typeof(membership_proof) <> 'object'
               OR membership_proof ->> 'contract_id' <>
                  'healthporta.provider-directory.semantic-membership.v1'
               OR jsonb_typeof(
                   membership_proof -> 'membership_partition_resource_counts'
               ) <> 'object'
               OR NOT {quoted_schema}.
                  provider_directory_projection_stage_matches_parent(
                      candidate_schema,
                      candidate_relation,
                      candidate_relation_oid,
                      'provider_directory_physical_projection_resource_membership',
                      true
                  )
               OR NOT {quoted_schema}.
                  provider_directory_projection_stage_is_immutable(
                      candidate_schema,
                      candidate_relation,
                      candidate_relation_oid,
                      candidate_trigger_oid
                  )
               OR NOT {quoted_schema}.
                  provider_directory_projection_stage_has_index(
                      candidate_relation_oid,
                      ARRAY[
                          'physical_projection_id',
                          'membership_partition_id',
                          'resource_type',
                          'resource_id'
                      ],
                      true
                  ) THEN
                RETURN false;
            END IF;
            EXECUTE format(
                $membership_query$
                WITH actual_partition_counts AS (
                    SELECT membership_partition_id,
                           count(*)::bigint AS resource_count
                      FROM %I.%I
                     GROUP BY membership_partition_id
                ),
                expected_partition_counts AS (
                    SELECT partition_id,
                           (count_value #>> '{{}}')::bigint AS resource_count
                      FROM jsonb_each(
                          $2 -> 'semantic_membership' ->
                          'membership_partition_resource_counts'
                      ) AS expected(partition_id, count_value)
                )
                SELECT
                    (SELECT count(*) FROM %I.%I) =
                        ($2 -> 'semantic_membership' ->>
                         'membership_edge_count')::bigint
                    AND NOT EXISTS (
                        SELECT 1
                          FROM actual_partition_counts AS actual
                          FULL JOIN expected_partition_counts AS expected
                            ON expected.partition_id =
                               actual.membership_partition_id
                         WHERE actual.resource_count IS DISTINCT FROM
                               expected.resource_count
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM %I.%I AS membership_record
                         WHERE membership_record.physical_projection_id <> $1
                            OR NOT EXISTS (
                                SELECT 1
                                  FROM %I.%I AS resource_record
                                 WHERE resource_record.physical_projection_id = $1
                                   AND resource_record.resource_type =
                                       membership_record.resource_type
                                   AND resource_record.resource_id =
                                       membership_record.resource_id
                            )
                    );
                $membership_query$,
                candidate_schema,
                candidate_relation,
                candidate_schema,
                candidate_relation,
                candidate_schema,
                candidate_relation,
                resource_schema,
                resource_relation
            ) INTO stage_matches USING physical_projection_id, prepared_proof;
            RETURN COALESCE(stage_matches, false);
        EXCEPTION WHEN OTHERS THEN
            RETURN false;
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_stage_is_immutable(
            candidate_schema text,
            candidate_relation text,
            candidate_relation_oid bigint,
            candidate_trigger_oid bigint
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT candidate_schema IS NOT NULL
               AND candidate_relation IS NOT NULL
               AND candidate_relation_oid IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM pg_class AS relation_record
                      JOIN pg_namespace AS relation_schema
                        ON relation_schema.oid = relation_record.relnamespace
                      JOIN pg_trigger AS trigger_record
                        ON trigger_record.tgrelid = relation_record.oid
                      JOIN pg_proc AS function_record
                        ON function_record.oid = trigger_record.tgfoid
                      JOIN pg_namespace AS function_schema
                        ON function_schema.oid = function_record.pronamespace
                     WHERE relation_record.oid::bigint = candidate_relation_oid
                       AND relation_schema.nspname = candidate_schema
                       AND relation_record.relname = candidate_relation
                       AND relation_record.relkind IN ('r', 'p')
                       AND trigger_record.tgname =
                           'provider_directory_projection_stage_immutable'
                       AND NOT trigger_record.tgisinternal
                       AND trigger_record.tgenabled = 'O'
                       AND trigger_record.tgtype = 31
                       AND trigger_record.tgqual IS NULL
                       AND trigger_record.tgnargs = 0
                       AND trigger_record.tgoldtable IS NULL
                       AND trigger_record.tgnewtable IS NULL
                       AND trigger_record.tgattr::text = ''
                       AND function_schema.nspname = candidate_schema
                       AND function_record.proname =
                           'reject_provider_directory_projection_stage_mutation'
                       AND (
                            candidate_trigger_oid IS NULL
                            OR trigger_record.oid::bigint = candidate_trigger_oid
                       )
                       AND NOT EXISTS (
                            SELECT 1
                              FROM pg_partition_tree(
                                  relation_record.oid::regclass
                              ) AS stage_tree
                              JOIN pg_class AS stage_tree_relation
                                ON stage_tree_relation.oid = stage_tree.relid
                             WHERE stage_tree.level > 0
                               AND (
                                   NOT stage_tree.isleaf
                                   OR stage_tree_relation.relkind <> 'r'
                                   OR stage_tree_relation.relpersistence <> 'p'
                                   OR stage_tree_relation.relrowsecurity
                                   OR stage_tree_relation.relforcerowsecurity
                                   OR NOT EXISTS (
                                       SELECT 1
                                         FROM pg_trigger AS leaf_trigger
                                        WHERE leaf_trigger.tgrelid =
                                              stage_tree.relid
                                          AND leaf_trigger.tgparentid =
                                              trigger_record.oid
                                          AND leaf_trigger.tgname =
                                              trigger_record.tgname
                                          AND leaf_trigger.tgfoid =
                                              trigger_record.tgfoid
                                          AND NOT leaf_trigger.tgisinternal
                                          AND leaf_trigger.tgenabled = 'O'
                                          AND leaf_trigger.tgtype = 31
                                          AND leaf_trigger.tgqual IS NULL
                                          AND leaf_trigger.tgnargs = 0
                                          AND leaf_trigger.tgoldtable IS NULL
                                          AND leaf_trigger.tgnewtable IS NULL
                                          AND leaf_trigger.tgattr::text = ''
                                   )
                               )
                       )
               );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_input_block()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            recipe_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_lease_token',
                true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF action_setting = 'gc'
                   AND recipe_id_setting = OLD.recipe_id
                   AND physical_id_setting = OLD.recipe_id
                   AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
                   AND COALESCE(recipe_lease_setting, '') = ''
                   AND EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_physical_projection AS physical
                          JOIN {quoted_schema}.
                               provider_directory_projection_recipe AS recipe
                            ON recipe.recipe_id = physical.physical_projection_id
                         WHERE physical.physical_projection_id = OLD.recipe_id
                           AND physical.status = 'retiring'
                           AND physical.retain_until <= clock_timestamp()
                           AND recipe.status = 'retired'
                           AND recipe.attempt =
                               recipe_attempt_setting::integer
                           AND recipe.physical_projection_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_physical_projection_reference
                                      AS active_reference
                                WHERE active_reference.physical_projection_id =
                                      OLD.recipe_id
                                  AND active_reference.released_at IS NULL
                           )
                         FOR SHARE OF physical, recipe
                   ) THEN
                    RETURN OLD;
                END IF;
                RAISE EXCEPTION
                    'provider_directory_projection_input_block_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF action_setting <> 'workset_register'
                   OR recipe_id_setting <> NEW.recipe_id
                   OR recipe_attempt_setting !~ '^[1-9][0-9]*$'
                   OR recipe_lease_setting !~ '^[0-9a-f]{{64}}$'
                   OR NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                     WHERE recipe_id = NEW.recipe_id
                       AND attempt = recipe_attempt_setting::integer
                       AND status = 'building'
                       AND lease_token = recipe_lease_setting
                       AND lease_expires_at > clock_timestamp()
                       AND workset_registered_at IS NULL
                     FOR SHARE
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_workset_write_unfenced'
                        USING ERRCODE = '55000';
                END IF;
            ELSIF TG_OP = 'UPDATE'
                  AND action_setting = 'retained_rebind'
                  AND recipe_id_setting = NEW.recipe_id
                  AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
                  AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
                  AND OLD.recipe_id = NEW.recipe_id
                  AND OLD.block_id = NEW.block_id
                  AND OLD.block_ordinal = NEW.block_ordinal
                  AND OLD.upstream_artifact_id = NEW.upstream_artifact_id
                  AND OLD.source_object_id = NEW.source_object_id
                  AND OLD.block_kind = NEW.block_kind
                  AND OLD.input_contract_id = NEW.input_contract_id
                  AND OLD.source_partition_ordinal = NEW.source_partition_ordinal
                  AND OLD.record_start = NEW.record_start
                  AND OLD.record_count = NEW.record_count
                  AND OLD.content_sha256 = NEW.content_sha256
                  AND OLD.payload_sha256 = NEW.payload_sha256
                  AND OLD.payload_bytes = NEW.payload_bytes
                  AND OLD.summary_json = NEW.summary_json
                  AND OLD.block_proof_sha256 = NEW.block_proof_sha256
                  AND OLD.created_at = NEW.created_at THEN
                IF recipe_lease_setting IS NULL OR NOT EXISTS (
                    SELECT 1
                     FROM {quoted_schema}.provider_directory_projection_recipe
                     WHERE recipe_id = NEW.recipe_id
                       AND attempt = recipe_attempt_setting::integer
                       AND status IN ('building', 'proof_ready')
                       AND lease_token = recipe_lease_setting
                       AND lease_expires_at > clock_timestamp()
                       AND workset_registered_at IS NOT NULL
                     FOR SHARE
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_binding_write_unfenced'
                        USING ERRCODE = '55000';
                END IF;
            ELSE
                RAISE EXCEPTION
                    'provider_directory_projection_input_block_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF NEW.retained_range_ordinal IS NULL THEN
                PERFORM 1
                  FROM {quoted_schema}.provider_directory_retained_artifact_campaign
                       AS campaign
                  JOIN {quoted_schema}.provider_directory_retained_artifact_consumer
                       AS consumer
                    ON consumer.campaign_id = campaign.campaign_id
                   AND consumer.consumer_recipe_id = NEW.recipe_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact_consumer_reference
                       AS reference
                    ON reference.campaign_id = consumer.campaign_id
                   AND reference.consumer_recipe_id = consumer.consumer_recipe_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact_campaign_item
                       AS campaign_item
                    ON campaign_item.campaign_id = reference.campaign_id
                   AND campaign_item.source_item_id = reference.source_item_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact
                       AS artifact
                    ON artifact.artifact_sha256 = reference.artifact_sha256
                  JOIN {quoted_schema}.provider_directory_retained_artifact_layout
                       AS layout
                    ON layout.layout_sha256 = reference.layout_sha256
                   AND layout.artifact_sha256 = reference.artifact_sha256
                 WHERE campaign.campaign_id = NEW.retained_campaign_id
                   AND campaign.state = 'complete'
                   AND campaign.complete
                   AND campaign.campaign_sha256 = NEW.retained_campaign_sha256
                   AND campaign.released_at IS NULL
                   AND consumer.claimed_campaign_sha256 = campaign.campaign_sha256
                   AND consumer.released_at IS NULL
                   AND reference.claim_generation = consumer.claim_generation
                   AND reference.released_at IS NULL
                   AND reference.source_item_id = NEW.retained_source_item_id
                   AND reference.artifact_sha256 = NEW.upstream_artifact_id
                   AND reference.layout_sha256 = NEW.source_object_id
                   AND campaign_item.status = 'admitted'
                   AND campaign_item.artifact_sha256 = reference.artifact_sha256
                   AND campaign_item.layout_sha256 = reference.layout_sha256
                   AND artifact.registry_status = 'verified'
                   AND artifact.released_at IS NULL
                   AND layout.registry_status = 'verified'
                   AND layout.released_at IS NULL
                   AND NEW.record_start = 0
                   AND NEW.record_count = layout.artifact_record_count
                   AND NEW.payload_sha256 = artifact.artifact_sha256
                   AND NEW.payload_bytes = artifact.artifact_byte_count
                   AND NEW.content_sha256 = layout.manifest_sha256
                 FOR SHARE OF campaign, consumer, reference, campaign_item,
                              artifact, layout;
            ELSE
                PERFORM 1
                  FROM {quoted_schema}.provider_directory_retained_artifact_campaign
                       AS campaign
                  JOIN {quoted_schema}.provider_directory_retained_artifact_consumer
                       AS consumer
                    ON consumer.campaign_id = campaign.campaign_id
                   AND consumer.consumer_recipe_id = NEW.recipe_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact_consumer_reference
                       AS reference
                    ON reference.campaign_id = consumer.campaign_id
                   AND reference.consumer_recipe_id = consumer.consumer_recipe_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact_campaign_item
                       AS campaign_item
                    ON campaign_item.campaign_id = reference.campaign_id
                   AND campaign_item.source_item_id = reference.source_item_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact
                       AS artifact
                    ON artifact.artifact_sha256 = reference.artifact_sha256
                  JOIN {quoted_schema}.provider_directory_retained_artifact_layout
                       AS layout
                    ON layout.layout_sha256 = reference.layout_sha256
                   AND layout.artifact_sha256 = reference.artifact_sha256
                  JOIN {quoted_schema}.provider_directory_retained_artifact_range
                       AS retained_range
                    ON retained_range.layout_sha256 = layout.layout_sha256
                   AND retained_range.range_ordinal = NEW.retained_range_ordinal
                 WHERE campaign.campaign_id = NEW.retained_campaign_id
                   AND campaign.state = 'complete'
                   AND campaign.complete
                   AND campaign.campaign_sha256 = NEW.retained_campaign_sha256
                   AND campaign.released_at IS NULL
                   AND consumer.claimed_campaign_sha256 = campaign.campaign_sha256
                   AND consumer.released_at IS NULL
                   AND reference.claim_generation = consumer.claim_generation
                   AND reference.released_at IS NULL
                   AND reference.source_item_id = NEW.retained_source_item_id
                   AND reference.artifact_sha256 = NEW.upstream_artifact_id
                   AND reference.layout_sha256 = NEW.source_object_id
                   AND campaign_item.status = 'admitted'
                   AND campaign_item.artifact_sha256 = reference.artifact_sha256
                   AND campaign_item.layout_sha256 = reference.layout_sha256
                   AND artifact.registry_status = 'verified'
                   AND artifact.released_at IS NULL
                   AND layout.registry_status = 'verified'
                   AND layout.released_at IS NULL
                   AND retained_range.artifact_sha256 = NEW.upstream_artifact_id
                   AND retained_range.record_start = NEW.record_start
                   AND retained_range.record_count = NEW.record_count
                   AND retained_range.raw_sha256 = NEW.payload_sha256
                   AND retained_range.raw_byte_count = NEW.payload_bytes
                   AND retained_range.canonical_sha256 = NEW.content_sha256
                 FOR SHARE OF campaign, consumer, reference, campaign_item,
                              artifact, layout, retained_range;
            END IF;
            IF NOT FOUND THEN
                RAISE EXCEPTION
                    'provider_directory_projection_retained_binding_invalid'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_proof_shard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            recipe_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_lease_token',
                true
            );
            partition_id_setting text := current_setting(
                'healthporta.provider_directory_projection_partition_id', true
            );
            partition_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_partition_attempt',
                true
            );
            shard_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_shard_lease_token',
                true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
        BEGIN
            IF TG_OP = 'DELETE' THEN
                IF action_setting = 'gc'
                   AND recipe_id_setting = OLD.recipe_id
                   AND physical_id_setting = OLD.recipe_id
                   AND recipe_attempt_setting = OLD.attempt::text
                   AND COALESCE(recipe_lease_setting, '') = ''
                   AND EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_physical_projection AS physical
                          JOIN {quoted_schema}.
                               provider_directory_projection_recipe AS recipe
                            ON recipe.recipe_id = physical.physical_projection_id
                         WHERE physical.physical_projection_id = OLD.recipe_id
                           AND physical.status = 'retiring'
                           AND physical.retain_until <= clock_timestamp()
                           AND recipe.status = 'retired'
                           AND recipe.attempt = OLD.attempt
                           AND recipe.physical_projection_id IS NULL
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_physical_projection_reference
                                      AS active_reference
                                WHERE active_reference.physical_projection_id =
                                      OLD.recipe_id
                                  AND active_reference.released_at IS NULL
                           )
                         FOR SHARE OF physical, recipe
                   ) THEN
                    RETURN OLD;
                END IF;
                RAISE EXCEPTION 'provider_directory_projection_proof_shard_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF action_setting = 'workset_register'
                   AND recipe_id_setting = NEW.recipe_id
                   AND recipe_attempt_setting = NEW.attempt::text
                   AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
                   AND NEW.status = 'pending'
                   AND NEW.partition_attempt = 0
                   AND NEW.recipe_lease_token IS NULL
                   AND NEW.lease_token IS NULL
                   AND NEW.lease_expires_at IS NULL
                   AND NEW.lease_heartbeat_at IS NULL
                   AND NEW.canonical_row_sha256 IS NULL
                   AND NEW.resource_count IS NULL
                   AND NEW.first_identity_json IS NULL
                   AND NEW.last_identity_json IS NULL
                   AND NEW.proof_json IS NULL
                   AND NEW.completed_at IS NULL
                   AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                     WHERE recipe_id = NEW.recipe_id
                       AND attempt = NEW.attempt
                       AND status = 'building'
                       AND lease_token = recipe_lease_setting
                       AND lease_expires_at > clock_timestamp()
                       AND workset_registered_at IS NULL
                     FOR SHARE
                ) THEN
                    RETURN NEW;
                END IF;
                RAISE EXCEPTION
                    'provider_directory_projection_workset_write_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.recipe_id IS DISTINCT FROM NEW.recipe_id
               OR OLD.attempt IS DISTINCT FROM NEW.attempt
               OR OLD.partition_id IS DISTINCT FROM NEW.partition_id
               OR OLD.partition_ordinal IS DISTINCT FROM NEW.partition_ordinal
               OR OLD.partition_key IS DISTINCT FROM NEW.partition_key
               OR OLD.input_block_id IS DISTINCT FROM NEW.input_block_id
               OR OLD.resource_type IS DISTINCT FROM NEW.resource_type
               OR OLD.input_sha256 IS DISTINCT FROM NEW.input_sha256
               OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
                RAISE EXCEPTION 'provider_directory_projection_shard_identity_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.status = 'complete' THEN
                RAISE EXCEPTION 'provider_directory_projection_proof_shard_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF recipe_id_setting <> NEW.recipe_id
               OR recipe_attempt_setting <> NEW.attempt::text
               OR recipe_lease_setting !~ '^[0-9a-f]{{64}}$'
               OR partition_id_setting <> NEW.partition_id
               OR partition_attempt_setting !~ '^[0-9]+$'
               OR NOT EXISTS (
                SELECT 1
                  FROM {quoted_schema}.provider_directory_projection_recipe
                 WHERE recipe_id = NEW.recipe_id
                   AND attempt = NEW.attempt
                   AND status = 'building'
                   AND lease_token = recipe_lease_setting
                   AND lease_expires_at > clock_timestamp()
                   AND workset_registered_at IS NOT NULL
                 FOR SHARE
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_projection_shard_write_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            IF action_setting = 'shard_claim'
               AND NEW.status = 'building'
               AND NEW.partition_attempt = OLD.partition_attempt + 1
               AND OLD.partition_attempt = partition_attempt_setting::integer
               AND NEW.recipe_lease_token = recipe_lease_setting
               AND NEW.lease_token = shard_lease_setting
               AND shard_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND NEW.lease_expires_at > clock_timestamp()
               AND NEW.lease_heartbeat_at IS NOT NULL
               AND (
                   OLD.status = 'pending'
                   OR OLD.lease_expires_at <= clock_timestamp()
                   OR OLD.recipe_lease_token IS DISTINCT FROM
                       recipe_lease_setting
               )
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'partition_attempt', 'recipe_lease_token',
                   'lease_token', 'lease_expires_at', 'lease_heartbeat_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'partition_attempt', 'recipe_lease_token',
                   'lease_token', 'lease_expires_at', 'lease_heartbeat_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'shard_heartbeat'
               AND OLD.status = 'building' AND NEW.status = 'building'
               AND NEW.partition_attempt = OLD.partition_attempt
               AND OLD.partition_attempt = partition_attempt_setting::integer
               AND OLD.recipe_lease_token = recipe_lease_setting
               AND NEW.recipe_lease_token = OLD.recipe_lease_token
               AND OLD.lease_token = shard_lease_setting
               AND NEW.lease_token = OLD.lease_token
               AND shard_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.lease_expires_at > clock_timestamp()
               AND to_jsonb(NEW) - ARRAY[
                   'lease_expires_at', 'lease_heartbeat_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'lease_expires_at', 'lease_heartbeat_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'shard_complete'
               AND OLD.status = 'building' AND NEW.status = 'complete'
               AND NEW.partition_attempt = OLD.partition_attempt
               AND OLD.partition_attempt = partition_attempt_setting::integer
               AND OLD.recipe_lease_token = recipe_lease_setting
               AND OLD.lease_token = shard_lease_setting
               AND shard_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.proof_json ?& ARRAY[
                   'contract_id', 'recipe_id', 'attempt',
                   'partition_attempt', 'partition_id', 'partition_ordinal',
                   'resource_type', 'input_sha256',
                   'canonical_row_sha256', 'resource_count',
                   'resource_counts', 'first_identity', 'last_identity'
               ]
               AND NEW.proof_json - ARRAY[
                   'contract_id', 'recipe_id', 'attempt',
                   'partition_attempt', 'partition_id', 'partition_ordinal',
                   'resource_type', 'input_sha256',
                   'canonical_row_sha256', 'resource_count',
                   'resource_counts', 'first_identity', 'last_identity',
                   'producer_proof'
               ] = '{{}}'::jsonb
               AND NEW.proof_json ->> 'contract_id' =
                   'healthporta.provider-directory.projection-proof-shard.v1'
               AND NEW.proof_json ->> 'recipe_id' = NEW.recipe_id
               AND NEW.proof_json -> 'attempt' = to_jsonb(NEW.attempt)
               AND NEW.proof_json -> 'partition_attempt' =
                   to_jsonb(NEW.partition_attempt)
               AND NEW.proof_json ->> 'partition_id' = NEW.partition_id
               AND NEW.proof_json -> 'partition_ordinal' =
                   to_jsonb(NEW.partition_ordinal)
               AND NEW.proof_json ->> 'resource_type' = NEW.resource_type
               AND NEW.proof_json ->> 'input_sha256' = NEW.input_sha256
               AND NEW.proof_json ->> 'canonical_row_sha256' =
                   NEW.canonical_row_sha256
               AND NEW.proof_json -> 'resource_count' =
                   to_jsonb(NEW.resource_count)
               AND NEW.proof_json -> 'first_identity' =
                   NEW.first_identity_json
               AND NEW.proof_json -> 'last_identity' =
                   NEW.last_identity_json
               AND jsonb_typeof(
                   NEW.proof_json -> 'resource_counts'
               ) = 'object'
               AND (
                   NOT (NEW.proof_json ? 'producer_proof')
                   OR jsonb_typeof(NEW.proof_json -> 'producer_proof') =
                      'object'
               )
               AND EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.provider_directory_projection_recipe
                          AS recipe
                    WHERE recipe.recipe_id = NEW.recipe_id
                      AND recipe.attempt = NEW.attempt
                      AND (
                          SELECT array_agg(resource_name ORDER BY resource_name)
                            FROM jsonb_object_keys(
                                NEW.proof_json -> 'resource_counts'
                            ) AS resource_name
                      ) = (
                          SELECT array_agg(resource_name ORDER BY resource_name)
                            FROM jsonb_array_elements_text(
                                recipe.selected_resources_json
                            ) AS resource_name
                      )
                      AND NOT EXISTS (
                          SELECT 1
                            FROM jsonb_each(
                                NEW.proof_json -> 'resource_counts'
                            ) AS resource_count(resource_name, count_value)
                           WHERE jsonb_typeof(count_value) <> 'number'
                              OR (count_value #>> '{{}}')::bigint < 0
                      )
                      AND (
                          SELECT COALESCE(
                              sum((count_value #>> '{{}}')::bigint), 0
                          )
                            FROM jsonb_each(
                                NEW.proof_json -> 'resource_counts'
                            ) AS resource_count(resource_name, count_value)
                      ) = NEW.resource_count
                      AND (
                          NEW.resource_type = '__mixed__'
                          OR NEW.proof_json -> 'resource_counts' ->
                             NEW.resource_type = to_jsonb(NEW.resource_count)
                      )
                    FOR SHARE OF recipe
               )
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'recipe_lease_token', 'lease_token',
                   'lease_expires_at', 'lease_heartbeat_at',
                   'canonical_row_sha256', 'resource_count',
                   'first_identity_json', 'last_identity_json',
                   'proof_json', 'completed_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'recipe_lease_token', 'lease_token',
                   'lease_expires_at', 'lease_heartbeat_at',
                   'canonical_row_sha256', 'resource_count',
                   'first_identity_json', 'last_identity_json',
                   'proof_json', 'completed_at'
               ] THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION
                'provider_directory_projection_shard_transition_invalid'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_reference()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
            owner_kind_setting text := current_setting(
                'healthporta.provider_directory_projection_reference_owner_kind',
                true
            );
            owner_id_setting text := current_setting(
                'healthporta.provider_directory_projection_reference_owner_id',
                true
            );
            identity_hash_setting text := current_setting(
                'healthporta.provider_directory_projection_reference_identity_hash',
                true
            );
            reference_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_reference_lease_token',
                true
            );
            previous_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_reference_previous_lease_token',
                true
            );
            projection_is_sealed boolean := false;
        BEGIN
            IF TG_OP IN ('INSERT', 'UPDATE')
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$' THEN
                SELECT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection AS physical
                      JOIN {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                        ON recipe.recipe_id = physical.physical_projection_id
                     WHERE physical.physical_projection_id =
                           NEW.physical_projection_id
                       AND physical.status = 'sealed'
                       AND recipe.recipe_id = recipe_id_setting
                       AND recipe.attempt = recipe_attempt_setting::integer
                       AND recipe.status = 'sealed'
                       AND recipe.physical_projection_id =
                           physical.physical_projection_id
                     FOR SHARE OF physical, recipe
                ) INTO projection_is_sealed;
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF action_setting = 'reference_insert'
                   AND recipe_id_setting = NEW.physical_projection_id
                   AND physical_id_setting = NEW.physical_projection_id
                   AND owner_kind_setting = NEW.owner_kind
                   AND owner_id_setting = NEW.owner_id
                   AND identity_hash_setting = NEW.reference_identity_hash
                   AND reference_lease_setting = NEW.lease_token
                   AND reference_lease_setting ~ '^[0-9a-f]{{64}}$'
                   AND COALESCE(previous_lease_setting, '') = ''
                   AND NEW.released_at IS NULL
                   AND NEW.lease_expires_at > clock_timestamp()
                   AND NEW.lease_expires_at <=
                       clock_timestamp() + interval '1 day'
                   AND NEW.lease_heartbeat_at <= clock_timestamp()
                   AND projection_is_sealed THEN
                    RETURN NEW;
                END IF;
                RAISE EXCEPTION
                    'provider_directory_projection_reference_insert_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'UPDATE'
               AND recipe_id_setting = OLD.physical_projection_id
               AND physical_id_setting = OLD.physical_projection_id
               AND owner_kind_setting = OLD.owner_kind
               AND owner_id_setting = OLD.owner_id
               AND identity_hash_setting = OLD.reference_identity_hash
               AND OLD.physical_projection_id = NEW.physical_projection_id
               AND OLD.owner_kind = NEW.owner_kind
               AND OLD.owner_id = NEW.owner_id
               AND OLD.reference_identity_hash = NEW.reference_identity_hash
               AND OLD.reference_proof_json = NEW.reference_proof_json
               AND OLD.created_at = NEW.created_at
               AND OLD.retain_until IS NOT DISTINCT FROM NEW.retain_until THEN
                IF action_setting = 'reference_release'
                   AND OLD.released_at IS NOT NULL
                   AND reference_lease_setting = OLD.lease_token
                   AND COALESCE(previous_lease_setting, '') = ''
                   AND to_jsonb(NEW) = to_jsonb(OLD) THEN
                    RETURN NEW;
                END IF;
                IF action_setting = 'reference_heartbeat'
                   AND projection_is_sealed
                   AND COALESCE(previous_lease_setting, '') = ''
                   AND reference_lease_setting = OLD.lease_token
                   AND NEW.lease_token = OLD.lease_token
                   AND OLD.released_at IS NULL
                   AND NEW.released_at IS NULL
                   AND OLD.lease_expires_at > clock_timestamp()
                   AND NEW.lease_expires_at > OLD.lease_expires_at
                   AND NEW.lease_expires_at <=
                       clock_timestamp() + interval '1 day'
                   AND NEW.lease_heartbeat_at >= OLD.lease_heartbeat_at
                   AND NEW.lease_heartbeat_at <= clock_timestamp()
                   AND to_jsonb(NEW) - ARRAY[
                       'lease_expires_at', 'lease_heartbeat_at'
                   ] = to_jsonb(OLD) - ARRAY[
                       'lease_expires_at', 'lease_heartbeat_at'
                   ] THEN
                    RETURN NEW;
                END IF;
                IF action_setting = 'reference_release'
                   AND projection_is_sealed
                   AND COALESCE(previous_lease_setting, '') = ''
                   AND reference_lease_setting = OLD.lease_token
                   AND NEW.lease_token = OLD.lease_token
                   AND (
                       (
                           OLD.released_at IS NULL
                           AND NEW.released_at IS NOT NULL
                           AND NEW.lease_expires_at <=
                               NEW.lease_heartbeat_at
                           AND NEW.lease_heartbeat_at >=
                               OLD.lease_heartbeat_at
                           AND to_jsonb(NEW) - ARRAY[
                               'released_at', 'lease_expires_at',
                               'lease_heartbeat_at'
                           ] = to_jsonb(OLD) - ARRAY[
                               'released_at', 'lease_expires_at',
                               'lease_heartbeat_at'
                           ]
                       )
                   ) THEN
                    RETURN NEW;
                END IF;
                IF action_setting = 'reference_reclaim'
                   AND projection_is_sealed
                   AND previous_lease_setting = OLD.lease_token
                   AND reference_lease_setting = NEW.lease_token
                   AND reference_lease_setting ~ '^[0-9a-f]{{64}}$'
                   AND NEW.lease_token <> OLD.lease_token
                   AND (
                       OLD.released_at IS NOT NULL
                       OR OLD.lease_expires_at <= clock_timestamp()
                   )
                   AND NEW.released_at IS NULL
                   AND NEW.lease_expires_at > clock_timestamp()
                   AND NEW.lease_expires_at <=
                       clock_timestamp() + interval '1 day'
                   AND NEW.lease_heartbeat_at >= OLD.lease_heartbeat_at
                   AND NEW.lease_heartbeat_at <= clock_timestamp()
                   AND to_jsonb(NEW) - ARRAY[
                       'lease_token', 'released_at', 'lease_expires_at',
                       'lease_heartbeat_at'
                   ] = to_jsonb(OLD) - ARRAY[
                       'lease_token', 'released_at', 'lease_expires_at',
                       'lease_heartbeat_at'
                   ] THEN
                    RETURN NEW;
                END IF;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.physical_projection_id
               AND physical_id_setting = OLD.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND OLD.released_at IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_physical_projection
                           AS physical
                      JOIN {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                        ON recipe.recipe_id = physical.physical_projection_id
                     WHERE physical.physical_projection_id =
                           OLD.physical_projection_id
                       AND physical.status = 'retiring'
                       AND physical.retain_until <= clock_timestamp()
                       AND recipe.status = 'retired'
                       AND recipe.attempt = recipe_attempt_setting::integer
                       AND recipe.physical_projection_id IS NULL
                       AND NOT EXISTS (
                           SELECT 1
                             FROM {quoted_schema}.
                                  provider_directory_physical_projection_reference
                                  AS active_reference
                            WHERE active_reference.physical_projection_id =
                                  OLD.physical_projection_id
                              AND active_reference.released_at IS NULL
                       )
                     FOR SHARE OF physical, recipe
               ) THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'provider_directory_projection_reference_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_recipe()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            recipe_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_lease_token',
                true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
        BEGIN
            IF TG_OP = 'INSERT' THEN
                IF action_setting <> 'recipe_insert'
                   OR recipe_id_setting <> NEW.recipe_id
                   OR recipe_attempt_setting <> NEW.attempt::text
                   OR recipe_lease_setting <> NEW.lease_token THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_recipe_insert_unfenced'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.status <> 'building'
                   OR NEW.attempt <> 1
                   OR NEW.lease_token !~ '^[0-9a-f]{{64}}$'
                   OR NEW.lease_expires_at <= clock_timestamp() THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_recipe_insert_state_invalid'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.physical_projection_id IS NOT NULL
                   OR NEW.stage_schema IS NOT NULL
                   OR NEW.stage_relation IS NOT NULL
                   OR NEW.stage_relation_oid IS NOT NULL
                   OR NEW.membership_stage_schema IS NOT NULL
                   OR NEW.membership_stage_relation IS NOT NULL
                   OR NEW.membership_stage_relation_oid IS NOT NULL
                   OR NEW.profile_contribution_stage_schema IS NOT NULL
                   OR NEW.profile_contribution_stage_relation IS NOT NULL
                   OR NEW.profile_contribution_stage_relation_oid IS NOT NULL THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_recipe_insert_stage_invalid'
                        USING ERRCODE = '55000';
                END IF;
                IF NEW.workset_registered_at IS NOT NULL
                   OR NEW.native_campaign_proof_json IS NOT NULL
                   OR NEW.prepared_proof_json IS NOT NULL
                   OR NEW.sealed_at IS NOT NULL THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_recipe_insert_state_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                IF action_setting = 'gc'
                   AND recipe_id_setting = OLD.recipe_id
                   AND recipe_attempt_setting = OLD.attempt::text
                   AND physical_id_setting = OLD.recipe_id
                   AND COALESCE(recipe_lease_setting, '') = ''
                   AND OLD.status = 'retired'
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {quoted_schema}.
                              provider_directory_physical_projection
                        WHERE physical_projection_id = OLD.recipe_id
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {quoted_schema}.
                              provider_directory_physical_projection_reference
                        WHERE physical_projection_id = OLD.recipe_id
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {quoted_schema}.
                              provider_directory_projection_proof_shard
                        WHERE recipe_id = OLD.recipe_id
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {quoted_schema}.
                              provider_directory_projection_input_block
                        WHERE recipe_id = OLD.recipe_id
                   ) THEN
                    RETURN OLD;
                END IF;
                RAISE EXCEPTION 'provider_directory_projection_recipe_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.recipe_id IS DISTINCT FROM NEW.recipe_id
               OR OLD.decoder_contract_id IS DISTINCT FROM NEW.decoder_contract_id
               OR OLD.input_set_sha256 IS DISTINCT FROM NEW.input_set_sha256
               OR OLD.transform_contract_id IS DISTINCT FROM NEW.transform_contract_id
               OR OLD.scope_contract_id IS DISTINCT FROM NEW.scope_contract_id
               OR OLD.transform_context_hash IS DISTINCT FROM
                    NEW.transform_context_hash
               OR OLD.transform_context_json IS DISTINCT FROM
                    NEW.transform_context_json
               OR OLD.completeness_manifest_hash IS DISTINCT FROM
                    NEW.completeness_manifest_hash
               OR OLD.completeness_manifest_json IS DISTINCT FROM
                    NEW.completeness_manifest_json
               OR OLD.resource_profile_hash IS DISTINCT FROM
                    NEW.resource_profile_hash
               OR OLD.selected_resources_json IS DISTINCT FROM
                    NEW.selected_resources_json
               OR OLD.required_resources_json IS DISTINCT FROM
                    NEW.required_resources_json
               OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
                RAISE EXCEPTION 'provider_directory_projection_recipe_identity_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF recipe_id_setting <> OLD.recipe_id
               OR recipe_attempt_setting <> OLD.attempt::text THEN
                RAISE EXCEPTION 'provider_directory_projection_recipe_write_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            IF action_setting = 'recipe_reclaim'
               AND OLD.status IN ('building', 'proof_ready')
               AND OLD.lease_expires_at <= clock_timestamp()
               AND NEW.status = 'building'
               AND NEW.attempt = OLD.attempt
               AND NEW.lease_token = recipe_lease_setting
               AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND NEW.lease_expires_at > clock_timestamp()
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'lease_token', 'lease_expires_at',
                   'lease_heartbeat_at', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'lease_token', 'lease_expires_at',
                   'lease_heartbeat_at', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'recipe_heartbeat'
               AND OLD.status IN ('building', 'proof_ready')
               AND NEW.status = OLD.status
               AND OLD.lease_token = recipe_lease_setting
               AND NEW.lease_token = OLD.lease_token
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.lease_expires_at > clock_timestamp()
               AND to_jsonb(NEW) - ARRAY[
                   'lease_expires_at', 'lease_heartbeat_at', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'lease_expires_at', 'lease_heartbeat_at', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'workset_register'
               AND OLD.status = 'building' AND NEW.status = 'building'
               AND OLD.lease_token = recipe_lease_setting
               AND NEW.lease_token = OLD.lease_token
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.lease_expires_at = OLD.lease_expires_at
               AND OLD.workset_registered_at IS NULL
               AND NEW.workset_registered_at IS NOT NULL
               AND NEW.input_block_set_sha256 IS NOT NULL
               AND NEW.input_block_count > 0
               AND NEW.partition_set_sha256 IS NOT NULL
               AND NEW.partition_count > 0
               AND to_jsonb(NEW) - ARRAY[
                   'input_block_set_sha256', 'input_block_count',
                   'partition_set_sha256', 'partition_count',
                   'workset_registered_at', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'input_block_set_sha256', 'input_block_count',
                   'partition_set_sha256', 'partition_count',
                   'workset_registered_at', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'stage_bind'
               AND OLD.status = 'building' AND NEW.status = 'building'
               AND OLD.lease_token = recipe_lease_setting
               AND NEW.lease_token = OLD.lease_token
               AND OLD.lease_expires_at > clock_timestamp()
               AND OLD.workset_registered_at IS NOT NULL
               AND {quoted_schema}.
                   provider_directory_projection_stage_matches_parent(
                       NEW.stage_schema,
                       NEW.stage_relation,
                       NEW.stage_relation_oid,
                       'provider_directory_physical_projection_resource',
                       false
                   )
               AND (
                    OLD.stage_schema IS NULL
                    OR (
                        OLD.stage_schema = NEW.stage_schema
                        AND OLD.stage_relation = NEW.stage_relation
                        AND OLD.stage_relation_oid = NEW.stage_relation_oid
                    )
               )
               AND (
                    (
                        OLD.membership_stage_schema IS NULL
                        AND NEW.membership_stage_schema IS NULL
                    )
                    OR (
                        {quoted_schema}.
                        provider_directory_projection_stage_matches_parent(
                            NEW.membership_stage_schema,
                            NEW.membership_stage_relation,
                            NEW.membership_stage_relation_oid,
                            'provider_directory_physical_projection_resource_membership',
                            false
                        )
                        AND (
                            OLD.membership_stage_schema IS NULL
                            OR (
                                OLD.membership_stage_schema =
                                    NEW.membership_stage_schema
                                AND OLD.membership_stage_relation =
                                    NEW.membership_stage_relation
                                AND OLD.membership_stage_relation_oid =
                                    NEW.membership_stage_relation_oid
                            )
                        )
                    )
               )
               AND (
                    (
                        OLD.profile_contribution_stage_schema IS NULL
                        AND NEW.profile_contribution_stage_schema IS NULL
                    )
                    OR (
                        {quoted_schema}.
                        provider_directory_projection_stage_matches_parent(
                            NEW.profile_contribution_stage_schema,
                            NEW.profile_contribution_stage_relation,
                            NEW.profile_contribution_stage_relation_oid,
                            'provider_directory_physical_projection_profile_contribution',
                            false
                        )
                        AND (
                            OLD.profile_contribution_stage_schema IS NULL
                            OR (
                                OLD.profile_contribution_stage_schema =
                                    NEW.profile_contribution_stage_schema
                                AND OLD.profile_contribution_stage_relation =
                                    NEW.profile_contribution_stage_relation
                                AND OLD.profile_contribution_stage_relation_oid =
                                    NEW.profile_contribution_stage_relation_oid
                            )
                        )
                    )
               )
               AND to_jsonb(NEW) - ARRAY[
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'membership_stage_schema', 'membership_stage_relation',
                   'membership_stage_relation_oid',
                   'profile_contribution_stage_schema',
                   'profile_contribution_stage_relation',
                   'profile_contribution_stage_relation_oid', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'membership_stage_schema', 'membership_stage_relation',
                   'membership_stage_relation_oid',
                   'profile_contribution_stage_schema',
                   'profile_contribution_stage_relation',
                   'profile_contribution_stage_relation_oid', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'proof_ready'
               AND OLD.status = 'building' AND NEW.status = 'proof_ready'
               AND OLD.lease_token = recipe_lease_setting
               AND NEW.lease_token = OLD.lease_token
               AND OLD.lease_expires_at > clock_timestamp()
               AND OLD.workset_registered_at IS NOT NULL
               AND NEW.prepared_proof_json IS NOT NULL
               AND NEW.prepared_proof_json ->> 'physical_projection_id' =
                   NEW.recipe_id
               AND jsonb_typeof(
                       NEW.prepared_proof_json -> 'raw_shards'
                   ) = 'array'
               AND NOT (NEW.prepared_proof_json ? 'shards')
               AND jsonb_array_length(
                       NEW.prepared_proof_json -> 'raw_shards'
                   ) > 0
               AND NEW.partition_count = jsonb_array_length(
                       NEW.prepared_proof_json -> 'raw_shards'
                   )
               AND (
                   SELECT count(DISTINCT expected_shard)
                     FROM jsonb_array_elements(
                         NEW.prepared_proof_json -> 'raw_shards'
                     ) AS expected_shard
               ) = NEW.partition_count
               AND (
                   SELECT count(*)
                     FROM {quoted_schema}.
                          provider_directory_projection_proof_shard
                          AS proof_shard
                    WHERE proof_shard.recipe_id = NEW.recipe_id
                      AND proof_shard.attempt = NEW.attempt
               ) = NEW.partition_count
               AND NOT EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.
                          provider_directory_projection_proof_shard
                          AS proof_shard
                    WHERE proof_shard.recipe_id = NEW.recipe_id
                      AND proof_shard.attempt = NEW.attempt
                      AND proof_shard.status <> 'complete'
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM jsonb_array_elements(
                         NEW.prepared_proof_json -> 'raw_shards'
                     ) AS expected_shard
                    WHERE NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_proof_shard
                               AS proof_shard
                         WHERE proof_shard.recipe_id = NEW.recipe_id
                           AND proof_shard.attempt = NEW.attempt
                           AND proof_shard.status = 'complete'
                           AND proof_shard.partition_id =
                               expected_shard ->> 'partition_id'
                           AND to_jsonb(proof_shard.partition_ordinal) =
                               expected_shard -> 'partition_ordinal'
                           AND proof_shard.resource_type =
                               expected_shard ->> 'resource_type'
                           AND proof_shard.input_sha256 =
                               expected_shard ->> 'input_sha256'
                           AND proof_shard.canonical_row_sha256 =
                               expected_shard ->> 'canonical_row_sha256'
                           AND to_jsonb(proof_shard.resource_count) =
                               expected_shard -> 'resource_count'
                           AND proof_shard.first_identity_json =
                               expected_shard -> 'first_identity'
                           AND proof_shard.last_identity_json =
                               expected_shard -> 'last_identity'
                    )
               )
               AND NOT EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.
                          provider_directory_projection_proof_shard
                          AS proof_shard
                    WHERE proof_shard.recipe_id = NEW.recipe_id
                      AND proof_shard.attempt = NEW.attempt
                      AND NOT EXISTS (
                          SELECT 1
                            FROM jsonb_array_elements(
                                NEW.prepared_proof_json -> 'raw_shards'
                            ) AS expected_shard
                           WHERE proof_shard.partition_id =
                                 expected_shard ->> 'partition_id'
                             AND to_jsonb(proof_shard.partition_ordinal) =
                                 expected_shard -> 'partition_ordinal'
                             AND proof_shard.resource_type =
                                 expected_shard ->> 'resource_type'
                             AND proof_shard.input_sha256 =
                                 expected_shard ->> 'input_sha256'
                             AND proof_shard.canonical_row_sha256 =
                                 expected_shard ->> 'canonical_row_sha256'
                             AND to_jsonb(proof_shard.resource_count) =
                                 expected_shard -> 'resource_count'
                             AND proof_shard.first_identity_json =
                                 expected_shard -> 'first_identity'
                             AND proof_shard.last_identity_json =
                                 expected_shard -> 'last_identity'
                      )
               )
               AND jsonb_typeof(
                       NEW.prepared_proof_json -> 'source_summary'
                   ) = 'object'
               AND {quoted_schema}.
                   provider_directory_projection_stage_matches_proof(
                       NEW.stage_schema,
                       NEW.stage_relation,
                       NEW.stage_relation_oid,
                       NULL,
                       NEW.recipe_id,
                       NEW.prepared_proof_json
                   )
               AND (
                    NEW.membership_stage_schema IS NULL
                    OR {quoted_schema}.
                       provider_directory_projection_membership_matches_proof(
                           NEW.membership_stage_schema,
                           NEW.membership_stage_relation,
                           NEW.membership_stage_relation_oid,
                           NULL,
                           NEW.recipe_id,
                           NEW.prepared_proof_json,
                           NEW.stage_schema,
                           NEW.stage_relation
                       )
               )
               AND NEW.profile_contribution_stage_schema IS NULL
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'native_campaign_proof_json',
                   'prepared_proof_json', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'native_campaign_proof_json',
                   'prepared_proof_json', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'seal'
               AND OLD.status = 'proof_ready' AND NEW.status = 'sealed'
               AND OLD.lease_token = recipe_lease_setting
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.physical_projection_id = physical_id_setting
               AND physical_id_setting ~ '^[0-9a-f]{{64}}$'
               AND OLD.prepared_proof_json ->> 'physical_projection_id' =
                   physical_id_setting
               AND NEW.lease_token IS NULL
               AND NEW.lease_expires_at IS NULL
               AND NEW.lease_heartbeat_at IS NULL
               AND NEW.sealed_at IS NOT NULL
               AND {quoted_schema}.
                   provider_directory_projection_stage_matches_proof(
                       OLD.stage_schema,
                       OLD.stage_relation,
                       OLD.stage_relation_oid,
                       NULL,
                       physical_id_setting,
                       OLD.prepared_proof_json
                   )
               AND (
                    OLD.membership_stage_schema IS NULL
                    OR {quoted_schema}.
                       provider_directory_projection_membership_matches_proof(
                           OLD.membership_stage_schema,
                           OLD.membership_stage_relation,
                           OLD.membership_stage_relation_oid,
                           NULL,
                           physical_id_setting,
                           OLD.prepared_proof_json,
                           OLD.stage_schema,
                           OLD.stage_relation
                       )
               )
               AND OLD.profile_contribution_stage_schema IS NULL
               AND EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.provider_directory_physical_projection
                          AS physical
                    WHERE physical.physical_projection_id = physical_id_setting
                      AND physical.proof_json = OLD.prepared_proof_json
                      AND physical.status = 'sealed'
                      AND {quoted_schema}.
                          provider_directory_projection_stage_matches_proof(
                              physical.storage_schema,
                              physical.storage_relation,
                              physical.storage_relation_oid,
                              physical.storage_trigger_oid,
                              physical_id_setting,
                              OLD.prepared_proof_json
                          )
                      AND (
                          physical.membership_storage_schema IS NULL
                          OR {quoted_schema}.
                             provider_directory_projection_membership_matches_proof(
                                 physical.membership_storage_schema,
                                 physical.membership_storage_relation,
                                 physical.membership_storage_relation_oid,
                                 physical.membership_storage_trigger_oid,
                                 physical_id_setting,
                                 OLD.prepared_proof_json,
                                 physical.storage_schema,
                                 physical.storage_relation
                             )
                      )
                      AND physical.profile_contribution_storage_schema IS NULL
                      AND EXISTS (
                          SELECT 1
                            FROM {quoted_schema}.
                                 provider_directory_physical_projection_source_summary
                                 AS source_summary
                           WHERE source_summary.physical_projection_id =
                                 physical_id_setting
                             AND source_summary.proof_json =
                                 OLD.prepared_proof_json -> 'source_summary'
                             AND source_summary.canonical_row_sha256 =
                                 OLD.prepared_proof_json -> 'source_summary'
                                     ->> 'canonical_row_sha256'
                             AND source_summary.dataset_hash =
                                 OLD.prepared_proof_json -> 'source_summary'
                                     ->> 'dataset_hash'
                             AND to_jsonb(source_summary.resource_count) =
                                 OLD.prepared_proof_json -> 'source_summary'
                                     -> 'resource_count'
                             AND source_summary.resource_counts_json =
                                 OLD.prepared_proof_json -> 'source_summary'
                                     -> 'resource_counts'
                      )
                      AND (
                          SELECT count(*)
                            FROM {quoted_schema}.
                                 provider_directory_physical_projection_partition
                                 AS partition_record
                           WHERE partition_record.physical_projection_id =
                                 physical_id_setting
                      ) = jsonb_array_length(
                          OLD.prepared_proof_json -> 'raw_shards'
                      )
                      AND (
                          SELECT count(DISTINCT expected_shard)
                            FROM jsonb_array_elements(
                                OLD.prepared_proof_json -> 'raw_shards'
                            ) AS expected_shard
                      ) = jsonb_array_length(
                          OLD.prepared_proof_json -> 'raw_shards'
                      )
                      AND NOT EXISTS (
                          SELECT 1
                            FROM jsonb_array_elements(
                                OLD.prepared_proof_json -> 'raw_shards'
                            ) AS expected_shard
                           WHERE NOT EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_physical_projection_partition
                                      AS partition_record
                                WHERE partition_record.physical_projection_id =
                                      physical_id_setting
                                  AND partition_record.proof_json = expected_shard
                                  AND partition_record.proof_partition_id =
                                      expected_shard ->> 'partition_id'
                                  AND to_jsonb(
                                      partition_record.partition_ordinal
                                  ) = expected_shard -> 'partition_ordinal'
                                  AND partition_record.resource_type =
                                      expected_shard ->> 'resource_type'
                                  AND partition_record.canonical_row_sha256 =
                                      expected_shard ->> 'canonical_row_sha256'
                                  AND to_jsonb(partition_record.resource_count) =
                                      expected_shard -> 'resource_count'
                           )
                      )
                      AND NOT EXISTS (
                          SELECT 1
                            FROM {quoted_schema}.
                                 provider_directory_physical_projection_partition
                                 AS partition_record
                           WHERE partition_record.physical_projection_id =
                                 physical_id_setting
                             AND NOT EXISTS (
                                 SELECT 1
                                   FROM jsonb_array_elements(
                                       OLD.prepared_proof_json -> 'raw_shards'
                                   ) AS expected_shard
                                  WHERE expected_shard =
                                        partition_record.proof_json
                                    AND partition_record.proof_partition_id =
                                        expected_shard ->> 'partition_id'
                                    AND to_jsonb(
                                        partition_record.partition_ordinal
                                    ) = expected_shard -> 'partition_ordinal'
                                    AND partition_record.resource_type =
                                        expected_shard ->> 'resource_type'
                                    AND partition_record.canonical_row_sha256 =
                                        expected_shard ->>
                                            'canonical_row_sha256'
                                    AND to_jsonb(
                                        partition_record.resource_count
                                    ) = expected_shard -> 'resource_count'
                             )
                      )
                     FOR SHARE OF physical
               )
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'physical_projection_id', 'lease_token',
                   'lease_expires_at', 'lease_heartbeat_at', 'sealed_at',
                   'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'physical_projection_id', 'lease_token',
                   'lease_expires_at', 'lease_heartbeat_at', 'sealed_at',
                   'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'fail'
               AND OLD.status IN ('building', 'proof_ready')
               AND NEW.status = 'failed'
               AND OLD.lease_token = recipe_lease_setting
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.lease_token IS NULL
               AND NEW.lease_expires_at IS NULL
               AND NEW.lease_heartbeat_at IS NULL
               AND NEW.physical_projection_id IS NULL
               AND NEW.sealed_at IS NULL
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'lease_token', 'lease_expires_at',
                   'lease_heartbeat_at', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'lease_token', 'lease_expires_at',
                   'lease_heartbeat_at', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'gc'
               AND OLD.status = 'sealed' AND NEW.status = 'retired'
               AND recipe_id_setting = OLD.recipe_id
               AND recipe_attempt_setting = OLD.attempt::text
               AND physical_id_setting = OLD.physical_projection_id
               AND COALESCE(recipe_lease_setting, '') = ''
               AND NEW.physical_projection_id IS NULL
               AND NEW.stage_schema IS NULL
               AND NEW.stage_relation IS NULL
               AND NEW.stage_relation_oid IS NULL
               AND NEW.membership_stage_schema IS NULL
               AND NEW.membership_stage_relation IS NULL
               AND NEW.membership_stage_relation_oid IS NULL
               AND NEW.profile_contribution_stage_schema IS NULL
               AND NEW.profile_contribution_stage_relation IS NULL
               AND NEW.profile_contribution_stage_relation_oid IS NULL
               AND NEW.lease_token IS NULL
               AND NEW.lease_expires_at IS NULL
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_physical_projection
                           AS physical
                     WHERE physical.physical_projection_id =
                           OLD.physical_projection_id
                       AND physical.status = 'retiring'
                       AND physical.retain_until <= clock_timestamp()
                       AND NOT EXISTS (
                           SELECT 1
                             FROM {quoted_schema}.
                                  provider_directory_physical_projection_reference
                                  AS reference_record
                            WHERE reference_record.physical_projection_id =
                                  OLD.physical_projection_id
                              AND reference_record.released_at IS NULL
                       )
                     FOR SHARE OF physical
               )
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'physical_projection_id',
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'membership_stage_schema', 'membership_stage_relation',
                   'membership_stage_relation_oid',
                   'profile_contribution_stage_schema',
                   'profile_contribution_stage_relation',
                   'profile_contribution_stage_relation_oid', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'physical_projection_id',
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'membership_stage_schema', 'membership_stage_relation',
                   'membership_stage_relation_oid',
                   'profile_contribution_stage_schema',
                   'profile_contribution_stage_relation',
                   'profile_contribution_stage_relation_oid', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'provider_directory_projection_recipe_transition_invalid'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_physical_source_summary()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            recipe_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_lease_token',
                true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
        BEGIN
            IF TG_OP = 'INSERT'
               AND action_setting = 'seal'
               AND recipe_id_setting = NEW.physical_projection_id
               AND physical_id_setting = NEW.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.provider_directory_projection_recipe
                          AS recipe
                     JOIN {quoted_schema}.provider_directory_physical_projection
                          AS physical
                       ON physical.physical_projection_id = recipe.recipe_id
                    WHERE recipe.recipe_id = NEW.physical_projection_id
                      AND recipe.attempt = recipe_attempt_setting::integer
                      AND recipe.status = 'proof_ready'
                      AND recipe.lease_token = recipe_lease_setting
                      AND recipe.lease_expires_at > clock_timestamp()
                      AND recipe.prepared_proof_json ->> 'physical_projection_id'
                          = NEW.physical_projection_id
                      AND physical.canonical_row_sha256 =
                          NEW.canonical_row_sha256
                      AND physical.dataset_hash = NEW.dataset_hash
                      AND physical.resource_count = NEW.resource_count
                      AND physical.resource_counts_json =
                          NEW.resource_counts_json
                      AND physical.proof_json -> 'source_summary' =
                          NEW.proof_json
                      AND NEW.canonical_row_sha256 =
                          NEW.proof_json ->> 'canonical_row_sha256'
                      AND NEW.dataset_hash =
                          NEW.proof_json ->> 'dataset_hash'
                      AND to_jsonb(NEW.resource_count) =
                          NEW.proof_json -> 'resource_count'
                      AND NEW.resource_counts_json =
                          NEW.proof_json -> 'resource_counts'
                    FOR SHARE OF recipe, physical
               ) THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.physical_projection_id
               AND physical_id_setting = OLD.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_physical_projection
                           AS physical
                      JOIN {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                        ON recipe.recipe_id = physical.physical_projection_id
                     WHERE physical.physical_projection_id =
                           OLD.physical_projection_id
                       AND physical.status = 'retiring'
                       AND physical.retain_until <= clock_timestamp()
                       AND recipe.status = 'retired'
                       AND recipe.attempt = recipe_attempt_setting::integer
                       AND recipe.physical_projection_id IS NULL
                       AND NOT EXISTS (
                           SELECT 1
                             FROM {quoted_schema}.
                                  provider_directory_physical_projection_reference
                                  AS active_reference
                            WHERE active_reference.physical_projection_id =
                                  OLD.physical_projection_id
                              AND active_reference.released_at IS NULL
                       )
                     FOR SHARE OF physical, recipe
               ) THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'provider_directory_projection_source_summary_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_physical_partition()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            recipe_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_lease_token',
                true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
        BEGIN
            IF TG_OP = 'INSERT'
               AND action_setting = 'seal'
               AND recipe_id_setting = NEW.physical_projection_id
               AND physical_id_setting = NEW.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.provider_directory_projection_recipe
                          AS recipe
                     JOIN {quoted_schema}.provider_directory_physical_projection
                          AS physical
                       ON physical.physical_projection_id = recipe.recipe_id
                    WHERE recipe.recipe_id = NEW.physical_projection_id
                      AND recipe.attempt = recipe_attempt_setting::integer
                      AND recipe.status = 'proof_ready'
                      AND recipe.lease_token = recipe_lease_setting
                      AND recipe.lease_expires_at > clock_timestamp()
                      AND recipe.prepared_proof_json ->> 'physical_projection_id'
                          = NEW.physical_projection_id
                      AND physical.canonical_row_sha256 =
                          recipe.prepared_proof_json ->> 'canonical_row_sha256'
                      AND physical.dataset_hash =
                          recipe.prepared_proof_json ->> 'dataset_hash'
                      AND (
                          NEW.resource_type = '__mixed__'
                          OR recipe.selected_resources_json ? NEW.resource_type
                      )
                      AND EXISTS (
                          SELECT 1
                            FROM jsonb_array_elements(
                                recipe.prepared_proof_json -> 'raw_shards'
                            ) AS expected_shard
                           WHERE expected_shard = NEW.proof_json
                             AND (
                                 SELECT array_agg(object_key ORDER BY object_key)
                                   FROM jsonb_object_keys(expected_shard)
                                        AS object_key
                             ) = ARRAY[
                                 'canonical_row_sha256', 'first_identity',
                                 'input_sha256', 'last_identity', 'partition_id',
                                 'partition_ordinal', 'resource_count',
                                 'resource_type'
                             ]::text[]
                             AND NEW.proof_partition_id =
                                 expected_shard ->> 'partition_id'
                             AND to_jsonb(NEW.partition_ordinal) =
                                 expected_shard -> 'partition_ordinal'
                             AND NEW.resource_type =
                                 expected_shard ->> 'resource_type'
                             AND NEW.canonical_row_sha256 =
                                 expected_shard ->> 'canonical_row_sha256'
                             AND to_jsonb(NEW.resource_count) =
                                 expected_shard -> 'resource_count'
                             AND expected_shard ->> 'input_sha256' ~
                                 '^[0-9a-f]{{64}}$'
                             AND jsonb_typeof(
                                 expected_shard -> 'first_identity'
                             ) = 'array'
                             AND jsonb_array_length(
                                 expected_shard -> 'first_identity'
                             ) = 2
                             AND jsonb_typeof(
                                 expected_shard -> 'first_identity' -> 0
                             ) = 'string'
                             AND jsonb_typeof(
                                 expected_shard -> 'first_identity' -> 1
                             ) = 'string'
                             AND jsonb_typeof(
                                 expected_shard -> 'last_identity'
                             ) = 'array'
                             AND jsonb_array_length(
                                 expected_shard -> 'last_identity'
                             ) = 2
                             AND jsonb_typeof(
                                 expected_shard -> 'last_identity' -> 0
                             ) = 'string'
                             AND jsonb_typeof(
                                 expected_shard -> 'last_identity' -> 1
                             ) = 'string'
                      )
                    FOR SHARE OF recipe, physical
               ) THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.physical_projection_id
               AND physical_id_setting = OLD.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_physical_projection
                           AS physical
                      JOIN {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                        ON recipe.recipe_id = physical.physical_projection_id
                     WHERE physical.physical_projection_id =
                           OLD.physical_projection_id
                       AND physical.status = 'retiring'
                       AND physical.retain_until <= clock_timestamp()
                       AND recipe.status = 'retired'
                       AND recipe.attempt = recipe_attempt_setting::integer
                       AND recipe.physical_projection_id IS NULL
                       AND NOT EXISTS (
                           SELECT 1
                             FROM {quoted_schema}.
                                  provider_directory_physical_projection_reference
                                  AS active_reference
                            WHERE active_reference.physical_projection_id =
                                  OLD.physical_projection_id
                              AND active_reference.released_at IS NULL
                       )
                     FOR SHARE OF physical, recipe
               ) THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'provider_directory_projection_partition_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_physical_projection()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            recipe_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_attempt', true
            );
            recipe_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_lease_token',
                true
            );
            physical_id_setting text := current_setting(
                'healthporta.provider_directory_projection_physical_id', true
            );
        BEGIN
            IF TG_OP = 'INSERT'
               AND action_setting = 'seal'
               AND recipe_id_setting = NEW.physical_projection_id
               AND physical_id_setting = NEW.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.provider_directory_projection_recipe
                          AS recipe
                    WHERE recipe.recipe_id = NEW.physical_projection_id
                      AND recipe.attempt = recipe_attempt_setting::integer
                      AND recipe.status = 'proof_ready'
                      AND recipe.lease_token = recipe_lease_setting
                      AND recipe.lease_expires_at > clock_timestamp()
                      AND recipe.prepared_proof_json = NEW.proof_json
                      AND recipe.prepared_proof_json ->> 'physical_projection_id'
                          = NEW.physical_projection_id
                      AND recipe.prepared_proof_json ->> 'canonical_row_sha256'
                          = NEW.canonical_row_sha256
                      AND recipe.prepared_proof_json ->> 'dataset_hash' =
                          NEW.dataset_hash
                      AND recipe.decoder_contract_id = NEW.decoder_contract_id
                      AND recipe.input_set_sha256 = NEW.input_set_sha256
                      AND recipe.transform_contract_id =
                          NEW.transform_contract_id
                      AND recipe.scope_contract_id = NEW.scope_contract_id
                      AND recipe.transform_context_hash =
                          NEW.transform_context_hash
                      AND recipe.transform_context_json =
                          NEW.transform_context_json
                      AND recipe.completeness_manifest_hash =
                          NEW.completeness_manifest_hash
                      AND recipe.completeness_manifest_json =
                          NEW.completeness_manifest_json
                      AND recipe.resource_profile_hash =
                          NEW.resource_profile_hash
                      AND recipe.selected_resources_json =
                          NEW.selected_resources_json
                      AND recipe.required_resources_json =
                          NEW.required_resources_json
                      AND recipe.prepared_proof_json -> 'resource_counts' =
                          NEW.resource_counts_json
                      AND (recipe.prepared_proof_json ->> 'resource_count')::bigint
                          = NEW.resource_count
                      AND recipe.stage_schema = NEW.storage_schema
                      AND recipe.stage_relation = NEW.storage_relation
                      AND recipe.stage_relation_oid = NEW.storage_relation_oid
                      AND recipe.membership_stage_schema IS NOT DISTINCT FROM
                          NEW.membership_storage_schema
                      AND recipe.membership_stage_relation IS NOT DISTINCT FROM
                          NEW.membership_storage_relation
                      AND recipe.membership_stage_relation_oid IS NOT DISTINCT FROM
                          NEW.membership_storage_relation_oid
                      AND recipe.profile_contribution_stage_schema
                          IS NOT DISTINCT FROM
                          NEW.profile_contribution_storage_schema
                      AND recipe.profile_contribution_stage_relation
                          IS NOT DISTINCT FROM
                          NEW.profile_contribution_storage_relation
                      AND recipe.profile_contribution_stage_relation_oid
                          IS NOT DISTINCT FROM
                          NEW.profile_contribution_storage_relation_oid
                      AND {quoted_schema}.
                          provider_directory_projection_stage_matches_proof(
                              NEW.storage_schema,
                              NEW.storage_relation,
                              NEW.storage_relation_oid,
                              NEW.storage_trigger_oid,
                              NEW.physical_projection_id,
                              NEW.proof_json
                          )
                      AND (
                          NEW.membership_storage_schema IS NULL
                          OR {quoted_schema}.
                             provider_directory_projection_membership_matches_proof(
                                 NEW.membership_storage_schema,
                                 NEW.membership_storage_relation,
                                 NEW.membership_storage_relation_oid,
                                 NEW.membership_storage_trigger_oid,
                                 NEW.physical_projection_id,
                                 NEW.proof_json,
                                 NEW.storage_schema,
                                 NEW.storage_relation
                             )
                      )
                      AND NEW.profile_contribution_storage_schema IS NULL
                    FOR SHARE OF recipe
               ) THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'UPDATE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.physical_projection_id
               AND physical_id_setting = OLD.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND COALESCE(recipe_lease_setting, '') = ''
               AND OLD.status = 'sealed' AND NEW.status = 'retiring'
               AND NEW.retiring_at IS NOT NULL
               AND NEW.retiring_at >= OLD.sealed_at
               AND NEW.retiring_at <= clock_timestamp()
               AND OLD.retain_until <= clock_timestamp()
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                     WHERE recipe.recipe_id = OLD.physical_projection_id
                       AND recipe.attempt = recipe_attempt_setting::integer
                       AND recipe.status = 'sealed'
                       AND recipe.physical_projection_id =
                           OLD.physical_projection_id
                     FOR SHARE OF recipe
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection_reference
                           AS reference_record
                     WHERE reference_record.physical_projection_id =
                           OLD.physical_projection_id
                       AND reference_record.released_at IS NULL
               )
               AND to_jsonb(NEW) - ARRAY['status', 'retiring_at'] =
                   to_jsonb(OLD) - ARRAY['status', 'retiring_at'] THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.physical_projection_id
               AND physical_id_setting = OLD.physical_projection_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND COALESCE(recipe_lease_setting, '') = ''
               AND OLD.status = 'retiring'
               AND OLD.retain_until <= clock_timestamp()
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection_reference
                           AS reference_record
                     WHERE reference_record.physical_projection_id =
                           OLD.physical_projection_id
                       AND reference_record.released_at IS NULL
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection_reference
                     WHERE physical_projection_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection_source_summary
                     WHERE physical_projection_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection_partition
                     WHERE physical_projection_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_projection_proof_shard
                     WHERE recipe_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_projection_input_block
                     WHERE recipe_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM pg_class AS storage_relation
                     WHERE storage_relation.oid IN (
                         OLD.storage_relation_oid::oid,
                         COALESCE(
                             OLD.membership_storage_relation_oid,
                             0
                         )::oid,
                         COALESCE(
                             OLD.profile_contribution_storage_relation_oid,
                             0
                         )::oid
                     )
               )
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                     WHERE recipe.recipe_id = OLD.physical_projection_id
                       AND recipe.attempt = recipe_attempt_setting::integer
                       AND recipe.status = 'retired'
                       AND recipe.physical_projection_id IS NULL
                     FOR SHARE OF recipe
               ) THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION 'provider_directory_physical_projection_immutable'
                USING ERRCODE = '55000';
        END;
        $$;
        """
    function_marker = "CREATE OR REPLACE FUNCTION"
    for function_tail in function_block.split(function_marker)[1:]:
        op.execute(function_marker + function_tail)
    trigger_specs = (
        (
            "provider_directory_projection_source_summary_guard",
            "provider_directory_physical_projection_source_summary",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_physical_source_summary",
        ),
        (
            "provider_directory_projection_partition_guard",
            "provider_directory_physical_projection_partition",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_physical_partition",
        ),
        (
            "provider_directory_projection_resource_guard",
            "provider_directory_physical_projection_resource",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "reject_provider_directory_projection_resource_mutation",
        ),
        (
            "provider_directory_projection_membership_guard",
            "provider_directory_physical_projection_resource_membership",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "reject_provider_directory_projection_stage_mutation",
        ),
        (
            "provider_directory_projection_profile_contribution_guard",
            "provider_directory_physical_projection_profile_contribution",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "reject_provider_directory_projection_stage_mutation",
        ),
        (
            "provider_directory_projection_input_block_guard",
            "provider_directory_projection_input_block",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_input_block",
        ),
        (
            "provider_directory_projection_proof_shard_guard",
            "provider_directory_projection_proof_shard",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_proof_shard",
        ),
        (
            "provider_directory_projection_reference_guard",
            "provider_directory_physical_projection_reference",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_reference",
        ),
        (
            "provider_directory_projection_recipe_guard",
            "provider_directory_projection_recipe",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_recipe",
        ),
        (
            "provider_directory_physical_projection_guard",
            "provider_directory_physical_projection",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_physical_projection",
        ),
    )
    for trigger_name, table_name, timing, function_name in trigger_specs:
        op.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                      FROM pg_trigger AS trigger_record
                      JOIN pg_class AS table_record
                        ON table_record.oid = trigger_record.tgrelid
                      JOIN pg_namespace AS namespace_record
                        ON namespace_record.oid = table_record.relnamespace
                     WHERE namespace_record.nspname = {_sql_literal(schema)}
                       AND table_record.relname = {_sql_literal(table_name)}
                       AND trigger_record.tgname = {_sql_literal(trigger_name)}
                       AND NOT trigger_record.tgisinternal
                ) THEN
                    CREATE TRIGGER {trigger_name}
                    {timing} ON {quoted_schema}.{table_name}
                    FOR EACH ROW EXECUTE FUNCTION
                        {quoted_schema}.{function_name}();
                END IF;
            END;
            $$;
            """
        )
        _validate_trigger(
            schema,
            trigger_name,
            table_name,
            timing,
            function_name,
        )


def upgrade() -> None:
    schema = _schema()
    _create_tables(schema)
    _validate_check_constraints(schema)
    _create_fences(schema)


def downgrade() -> None:
    schema = _schema()
    quoted_schema = '"' + schema.replace('"', '""') + '"'
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM
                    {quoted_schema}.provider_directory_physical_projection
            ) OR EXISTS (
                SELECT 1 FROM
                    {quoted_schema}.provider_directory_physical_projection_reference
                 WHERE released_at IS NULL
            ) OR EXISTS (
                SELECT 1 FROM
                    {quoted_schema}.provider_directory_projection_recipe
            ) OR EXISTS (
                SELECT 1
                  FROM pg_inherits AS inheritance_record
                 WHERE inheritance_record.inhparent =
                       '{schema}.provider_directory_physical_projection_resource'
                       ::regclass
            ) OR EXISTS (
                SELECT 1
                  FROM pg_inherits AS inheritance_record
                 WHERE inheritance_record.inhparent =
                       '{schema}.provider_directory_physical_projection_resource_membership'
                       ::regclass
            ) OR EXISTS (
                SELECT 1
                  FROM pg_inherits AS inheritance_record
                 WHERE inheritance_record.inhparent =
                       '{schema}.provider_directory_physical_projection_profile_contribution'
                       ::regclass
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_projection_downgrade_has_live_state'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$;
        """
    )
    for trigger_name, table_name in (
        (
            "provider_directory_physical_projection_guard",
            "provider_directory_physical_projection",
        ),
        (
            "provider_directory_projection_recipe_guard",
            "provider_directory_projection_recipe",
        ),
        (
            "provider_directory_projection_reference_guard",
            "provider_directory_physical_projection_reference",
        ),
        (
            "provider_directory_projection_proof_shard_guard",
            "provider_directory_projection_proof_shard",
        ),
        (
            "provider_directory_projection_input_block_guard",
            "provider_directory_projection_input_block",
        ),
        (
            "provider_directory_projection_resource_guard",
            "provider_directory_physical_projection_resource",
        ),
        (
            "provider_directory_projection_membership_guard",
            "provider_directory_physical_projection_resource_membership",
        ),
        (
            "provider_directory_projection_profile_contribution_guard",
            "provider_directory_physical_projection_profile_contribution",
        ),
        (
            "provider_directory_projection_source_summary_guard",
            "provider_directory_physical_projection_source_summary",
        ),
        (
            "provider_directory_projection_partition_guard",
            "provider_directory_physical_projection_partition",
        ),
    ):
        op.execute(
            f"DROP TRIGGER IF EXISTS {trigger_name} " f"ON {quoted_schema}.{table_name}"
        )
    for table_name in (
        "provider_directory_physical_projection_reference",
        "provider_directory_physical_projection_source_summary",
        "provider_directory_physical_projection_partition",
        "provider_directory_physical_projection_profile_contribution",
        "provider_directory_physical_projection_resource_membership",
        "provider_directory_physical_projection_resource",
        "provider_directory_projection_proof_shard",
        "provider_directory_projection_input_block",
        "provider_directory_projection_recipe",
        "provider_directory_physical_projection",
    ):
        op.drop_table(table_name, schema=schema)
    for function_name in (
        "guard_provider_directory_physical_projection",
        "guard_provider_directory_physical_source_summary",
        "guard_provider_directory_physical_partition",
        "guard_provider_directory_projection_recipe",
        "guard_provider_directory_projection_reference",
        "guard_provider_directory_projection_proof_shard",
        "guard_provider_directory_projection_input_block",
        "reject_provider_directory_projection_stage_mutation",
        "reject_provider_directory_projection_resource_mutation",
    ):
        op.execute(f"DROP FUNCTION {quoted_schema}.{function_name}()")
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_membership_matches_proof("
        "text, text, bigint, bigint, text, jsonb, text, text)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_stage_matches_proof("
        "text, text, bigint, bigint, text, jsonb)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_stage_has_index("
        "bigint, text[], boolean)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_stage_matches_parent("
        "text, text, bigint, text, boolean)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_stage_is_immutable("
        "text, text, bigint, bigint)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_stage_is_real("
        "text, text, bigint)"
    )
