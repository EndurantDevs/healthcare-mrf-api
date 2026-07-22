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
            "AND dataset_hash ~ '^[0-9a-f]{64}$' "
            "AND resource_profile_hash ~ '^[0-9a-f]{64}$' "
            "AND scope_contract_id <> ''"
        ),
        "pd_physical_projection_json_check": (
            "jsonb_typeof(selected_resources_json) = 'array' "
            "AND jsonb_array_length(selected_resources_json) > 0 "
            "AND jsonb_typeof(required_resources_json) = 'array' "
            "AND jsonb_typeof(transform_context_json) = 'object' "
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(proof_json) = 'object'"
        ),
        "pd_physical_projection_state_check": (
            "resource_count > 0 AND storage_schema <> '' "
            "AND storage_relation <> '' AND storage_relation_oid > 0 "
            "AND storage_trigger_oid > 0 "
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
            "AND resource_profile_hash ~ '^[0-9a-f]{64}$' "
            "AND scope_contract_id <> '' "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')"
        ),
        "pd_projection_recipe_json_check": (
            "jsonb_typeof(selected_resources_json) = 'array' "
            "AND jsonb_array_length(selected_resources_json) > 0 "
            "AND jsonb_typeof(required_resources_json) = 'array'"
            " AND jsonb_typeof(transform_context_json) = 'object'"
            " AND (prepared_proof_json IS NULL OR "
            "jsonb_typeof(prepared_proof_json) = 'object')"
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
            "AND block_proof_sha256 ~ '^[0-9a-f]{64}$'"
        ),
        "pd_projection_input_block_values_check": (
            "block_ordinal >= 0 "
            "AND record_start >= 0 AND record_count > 0 "
            "AND payload_bytes > 0 AND upstream_artifact_id <> '' "
            "AND source_object_id <> '' AND block_kind <> '' "
            "AND input_contract_id <> '' "
            "AND jsonb_typeof(summary_json) = 'object'"
        ),
    },
    "provider_directory_projection_admission": {
        "pd_projection_admission_digest_check": (
            "admission_id ~ '^[0-9a-f]{64}$' "
            "AND planned_recipe_id ~ '^[0-9a-f]{64}$' "
            "AND (recipe_id IS NULL OR recipe_id ~ '^[0-9a-f]{64}$') "
            "AND source_scope_hash ~ '^[0-9a-f]{64}$' "
            "AND completeness_manifest_hash ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_block_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND binding_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND stream_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')"
        ),
        "pd_projection_admission_json_check": (
            "jsonb_typeof(source_ids_json) = 'array' "
            "AND jsonb_array_length(source_ids_json) > 0 "
            "AND jsonb_typeof(completeness_manifest_json) = 'object' "
            "AND jsonb_typeof(completeness_manifest_json -> "
            "'selected_resources') = 'array' "
            "AND jsonb_typeof(completeness_manifest_json -> "
            "'required_resources') = 'array' "
            "AND jsonb_typeof(completeness_manifest_json -> "
            "'terminal_partitions') = 'array' "
            "AND completeness_manifest_json -> 'complete' = 'true'::jsonb"
        ),
        "pd_projection_admission_state_check": (
            "outcome_kind IN ('physical_workset', 'no_input') "
            "AND acquisition_adapter_id <> '' "
            "AND retained_consumer_recipe_id <> '' "
            "AND claim_generation > 0 AND input_block_count >= 0 "
            "AND binding_count > 0 AND stream_count >= 0 "
            "AND ((outcome_kind = 'physical_workset' "
            "AND recipe_id = planned_recipe_id AND input_block_count > 0) "
            "OR (outcome_kind = 'no_input' AND recipe_id IS NULL "
            "AND input_block_count = 0 AND stream_count > 0)) "
            "AND attempt > 0 AND status IN ('building', 'sealed', 'released') "
            "AND ((status = 'building' AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL "
            "AND lease_heartbeat_at IS NOT NULL "
            "AND sealed_at IS NULL AND released_at IS NULL) OR "
            "(status = 'sealed' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND sealed_at IS NOT NULL AND released_at IS NULL) OR "
            "(status = 'released' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND sealed_at IS NOT NULL AND released_at IS NOT NULL))"
        ),
    },
    "provider_directory_projection_admission_input_block": {
        "pd_projection_admission_block_digest_check": (
            "admission_id ~ '^[0-9a-f]{64}$' "
            "AND recipe_id ~ '^[0-9a-f]{64}$' "
            "AND binding_id ~ '^[0-9a-f]{64}$' "
            "AND block_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND retained_artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_layout_sha256 ~ '^[0-9a-f]{64}$' "
            "AND stream_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_key_hash ~ '^[0-9a-f]{64}$'"
        ),
        "pd_projection_admission_block_values_check": (
            "retained_consumer_recipe_id <> '' "
            "AND claim_generation > 0 "
            "AND sequence_ordinal >= 0 "
            "AND resource_type <> '' AND source_partition_ordinal >= 0 "
            "AND (retained_range_ordinal IS NULL "
            "OR retained_range_ordinal >= 0)"
        ),
    },
    "provider_directory_projection_admission_stream": {
        "pd_projection_admission_stream_digest_check": (
            "admission_id ~ '^[0-9a-f]{64}$' "
            "AND (recipe_id IS NULL OR recipe_id ~ '^[0-9a-f]{64}$') "
            "AND binding_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND stream_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND terminal_proof_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_key_hash ~ '^[0-9a-f]{64}$'"
        ),
        "pd_projection_admission_stream_values_check": (
            "claim_generation > 0 AND resource_type <> '' "
            "AND source_partition_ordinal >= 0 AND stream_ordinal >= 0 "
            "AND terminal_sequence_ordinal >= 0"
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
                   trigger_record.tgnewtable
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
    ) = tuple(trigger_record)
    if isinstance(enabled, bytes):
        enabled = enabled.decode("ascii")
    # PostgreSQL deparses visible relations and functions without their schema,
    # so pg_get_triggerdef() is not stable across connection search paths.  The
    # catalogs below are the canonical trigger contract and retain exact schema
    # identity without relying on presentation SQL.
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
            "AND jsonb_typeof(resource_counts_json) = 'object' "
            "AND jsonb_typeof(proof_json) = 'object'",
            name="pd_physical_projection_json_check",
        ),
        sa.CheckConstraint(
            "resource_count > 0 AND storage_schema <> '' "
            "AND storage_relation <> '' AND storage_relation_oid > 0 "
            "AND storage_trigger_oid > 0 "
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
        sa.Column("input_block_set_sha256", sa.String(length=64)),
        sa.Column("input_block_count", sa.Integer()),
        sa.Column("partition_set_sha256", sa.String(length=64)),
        sa.Column("partition_count", sa.Integer()),
        sa.Column("prepared_proof_json", postgresql.JSONB()),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        _timestamp("workset_registered_at", nullable=True),
        _timestamp("sealed_at", nullable=True),
        sa.CheckConstraint(
            "recipe_id ~ '^[0-9a-f]{64}$' "
            "AND input_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND transform_context_hash ~ '^[0-9a-f]{64}$' "
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
            "AND (prepared_proof_json IS NULL OR "
            "jsonb_typeof(prepared_proof_json) = 'object')",
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
        sa.Column("record_start", sa.BigInteger(), nullable=False),
        sa.Column("record_count", sa.BigInteger(), nullable=False),
        sa.Column("content_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload_bytes", sa.BigInteger(), nullable=False),
        sa.Column("summary_json", postgresql.JSONB(), nullable=False),
        sa.Column("block_proof_sha256", sa.String(length=64), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "block_id ~ '^[0-9a-f]{64}$' "
            "AND upstream_artifact_id ~ '^[0-9a-f]{64}$' "
            "AND source_object_id ~ '^[0-9a-f]{64}$' "
            "AND content_sha256 ~ '^[0-9a-f]{64}$' "
            "AND payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND block_proof_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_projection_input_block_digest_check",
        ),
        sa.CheckConstraint(
            "block_ordinal >= 0 "
            "AND record_start >= 0 AND record_count > 0 "
            "AND payload_bytes > 0 AND upstream_artifact_id <> '' "
            "AND source_object_id <> '' AND block_kind <> '' "
            "AND input_contract_id <> '' "
            "AND jsonb_typeof(summary_json) = 'object'",
            name="pd_projection_input_block_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id"],
            [f"{schema}.provider_directory_projection_recipe.recipe_id"],
            name="pd_projection_input_block_recipe_fkey",
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
        "provider_directory_projection_admission",
        sa.Column("admission_id", sa.String(length=64), nullable=False),
        sa.Column("planned_recipe_id", sa.String(length=64), nullable=False),
        sa.Column("recipe_id", sa.String(length=64), nullable=True),
        sa.Column("outcome_kind", sa.String(length=32), nullable=False),
        sa.Column("acquisition_adapter_id", sa.String(length=128), nullable=False),
        sa.Column("source_scope_hash", sa.String(length=64), nullable=False),
        sa.Column("source_ids_json", postgresql.JSONB(), nullable=False),
        sa.Column("completeness_manifest_hash", sa.String(length=64), nullable=False),
        sa.Column("completeness_manifest_json", postgresql.JSONB(), nullable=False),
        sa.Column("retained_campaign_id", sa.String(length=64), nullable=False),
        sa.Column("retained_campaign_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "retained_consumer_recipe_id",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column("claim_generation", sa.BigInteger(), nullable=False),
        sa.Column("input_block_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("input_block_count", sa.Integer(), nullable=False),
        sa.Column("binding_count", sa.Integer(), nullable=False),
        sa.Column("binding_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("stream_set_sha256", sa.String(length=64), nullable=False),
        sa.Column("stream_count", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("lease_token", sa.String(length=64), nullable=True),
        sa.Column("lease_expires_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("lease_heartbeat_at", sa.TIMESTAMP(timezone=True), nullable=True),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        _timestamp("sealed_at", nullable=True),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "admission_id ~ '^[0-9a-f]{64}$' "
            "AND planned_recipe_id ~ '^[0-9a-f]{64}$' "
            "AND (recipe_id IS NULL OR recipe_id ~ '^[0-9a-f]{64}$') "
            "AND source_scope_hash ~ '^[0-9a-f]{64}$' "
            "AND completeness_manifest_hash ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_block_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND binding_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND stream_set_sha256 ~ '^[0-9a-f]{64}$' "
            "AND (lease_token IS NULL OR lease_token ~ '^[0-9a-f]{64}$')",
            name="pd_projection_admission_digest_check",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(source_ids_json) = 'array' "
            "AND jsonb_array_length(source_ids_json) > 0 "
            "AND jsonb_typeof(completeness_manifest_json) = 'object' "
            "AND jsonb_typeof(completeness_manifest_json -> "
            "'selected_resources') = 'array' "
            "AND jsonb_typeof(completeness_manifest_json -> "
            "'required_resources') = 'array' "
            "AND jsonb_typeof(completeness_manifest_json -> "
            "'terminal_partitions') = 'array' "
            "AND completeness_manifest_json -> 'complete' = 'true'::jsonb",
            name="pd_projection_admission_json_check",
        ),
        sa.CheckConstraint(
            "outcome_kind IN ('physical_workset', 'no_input') "
            "AND acquisition_adapter_id <> '' "
            "AND retained_consumer_recipe_id <> '' "
            "AND claim_generation > 0 AND input_block_count >= 0 "
            "AND binding_count > 0 AND stream_count >= 0 "
            "AND ((outcome_kind = 'physical_workset' "
            "AND recipe_id = planned_recipe_id AND input_block_count > 0) "
            "OR (outcome_kind = 'no_input' AND recipe_id IS NULL "
            "AND input_block_count = 0 AND stream_count > 0)) "
            "AND attempt > 0 AND status IN ('building', 'sealed', 'released') "
            "AND ((status = 'building' AND lease_token IS NOT NULL "
            "AND lease_expires_at IS NOT NULL "
            "AND lease_heartbeat_at IS NOT NULL "
            "AND sealed_at IS NULL AND released_at IS NULL) OR "
            "(status = 'sealed' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND sealed_at IS NOT NULL AND released_at IS NULL) OR "
            "(status = 'released' AND lease_token IS NULL "
            "AND lease_expires_at IS NULL AND lease_heartbeat_at IS NULL "
            "AND sealed_at IS NOT NULL AND released_at IS NOT NULL))",
            name="pd_projection_admission_state_check",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id"],
            [f"{schema}.provider_directory_projection_recipe.recipe_id"],
            name="pd_projection_admission_recipe_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id"],
            [f"{schema}.provider_directory_retained_artifact_campaign.campaign_id"],
            name="pd_projection_admission_campaign_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id", "retained_consumer_recipe_id"],
            [
                f"{schema}.provider_directory_retained_artifact_consumer.campaign_id",
                f"{schema}.provider_directory_retained_artifact_consumer.consumer_recipe_id",
            ],
            name="pd_projection_admission_consumer_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "admission_id",
            name="provider_directory_projection_admission_pkey",
        ),
        sa.UniqueConstraint(
            "admission_id",
            "recipe_id",
            name="pd_projection_admission_recipe_key",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_admission_recipe_idx",
        "provider_directory_projection_admission",
        ["recipe_id", "status", "created_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_admission_planned_recipe_idx",
        "provider_directory_projection_admission",
        ["planned_recipe_id", "outcome_kind", "status"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_admission_lease_idx",
        "provider_directory_projection_admission",
        ["status", "lease_expires_at"],
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_projection_admission_input_block",
        sa.Column("admission_id", sa.String(length=64), nullable=False),
        sa.Column("recipe_id", sa.String(length=64), nullable=False),
        sa.Column("binding_id", sa.String(length=64), nullable=False),
        sa.Column("block_id", sa.String(length=64), nullable=False),
        sa.Column("retained_campaign_id", sa.String(length=64), nullable=False),
        sa.Column(
            "retained_consumer_recipe_id",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column("retained_source_item_id", sa.String(length=64), nullable=False),
        sa.Column("retained_artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("retained_layout_sha256", sa.String(length=64), nullable=False),
        sa.Column("retained_range_ordinal", sa.Integer(), nullable=True),
        sa.Column("stream_identity_sha256", sa.String(length=64), nullable=False),
        sa.Column("sequence_ordinal", sa.Integer(), nullable=False),
        sa.Column("claim_generation", sa.BigInteger(), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("partition_key_hash", sa.String(length=64), nullable=False),
        sa.Column("source_partition_ordinal", sa.Integer(), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "admission_id ~ '^[0-9a-f]{64}$' "
            "AND recipe_id ~ '^[0-9a-f]{64}$' "
            "AND binding_id ~ '^[0-9a-f]{64}$' "
            "AND block_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND retained_artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_layout_sha256 ~ '^[0-9a-f]{64}$' "
            "AND stream_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_key_hash ~ '^[0-9a-f]{64}$'",
            name="pd_projection_admission_block_digest_check",
        ),
        sa.CheckConstraint(
            "retained_consumer_recipe_id <> '' "
            "AND claim_generation > 0 "
            "AND sequence_ordinal >= 0 "
            "AND resource_type <> '' AND source_partition_ordinal >= 0 "
            "AND (retained_range_ordinal IS NULL "
            "OR retained_range_ordinal >= 0)",
            name="pd_projection_admission_block_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["admission_id", "recipe_id"],
            [
                f"{schema}.provider_directory_projection_admission.admission_id",
                f"{schema}.provider_directory_projection_admission.recipe_id",
            ],
            name="pd_projection_admission_block_admission_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id", "block_id"],
            [
                f"{schema}.provider_directory_projection_input_block.recipe_id",
                f"{schema}.provider_directory_projection_input_block.block_id",
            ],
            name="pd_projection_admission_block_core_fkey",
        ),
        sa.ForeignKeyConstraint(
            [
                "retained_campaign_id",
                "retained_consumer_recipe_id",
                "retained_source_item_id",
            ],
            [
                f"{schema}.provider_directory_retained_artifact_consumer_reference.campaign_id",
                f"{schema}.provider_directory_retained_artifact_consumer_reference.consumer_recipe_id",
                f"{schema}.provider_directory_retained_artifact_consumer_reference.source_item_id",
            ],
            name="pd_projection_admission_block_reference_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id", "retained_source_item_id"],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item.source_item_id",
            ],
            name="pd_projection_admission_block_item_fkey",
        ),
        sa.ForeignKeyConstraint(
            [
                "retained_campaign_id",
                "stream_identity_sha256",
                "sequence_ordinal",
            ],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item.stream_identity_sha256",
                f"{schema}.provider_directory_retained_artifact_campaign_item.sequence_ordinal",
            ],
            name="pd_projection_admission_block_stream_item_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_artifact_sha256"],
            [f"{schema}.provider_directory_retained_artifact.artifact_sha256"],
            name="pd_projection_admission_block_artifact_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_layout_sha256", "retained_artifact_sha256"],
            [
                f"{schema}.provider_directory_retained_artifact_layout.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_layout.artifact_sha256",
            ],
            name="pd_projection_admission_block_layout_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_layout_sha256", "retained_range_ordinal"],
            [
                f"{schema}.provider_directory_retained_artifact_range.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_range.range_ordinal",
            ],
            name="pd_projection_admission_block_range_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "admission_id",
            "binding_id",
            name="provider_directory_projection_admission_input_block_pkey",
        ),
        schema=schema,
    )

    create_table_or_validate(
        op,
        "provider_directory_projection_admission_stream",
        sa.Column("admission_id", sa.String(length=64), nullable=False),
        sa.Column("recipe_id", sa.String(length=64), nullable=True),
        sa.Column("binding_id", sa.String(length=64), nullable=False),
        sa.Column("retained_campaign_id", sa.String(length=64), nullable=False),
        sa.Column("retained_source_item_id", sa.String(length=64), nullable=False),
        sa.Column("claim_generation", sa.BigInteger(), nullable=False),
        sa.Column("resource_type", sa.String(length=64), nullable=False),
        sa.Column("partition_key_hash", sa.String(length=64), nullable=False),
        sa.Column("source_partition_ordinal", sa.Integer(), nullable=False),
        sa.Column("stream_identity_sha256", sa.String(length=64), nullable=False),
        sa.Column("stream_ordinal", sa.Integer(), nullable=False),
        sa.Column("terminal_sequence_ordinal", sa.Integer(), nullable=False),
        sa.Column("terminal_proof_sha256", sa.String(length=64), nullable=False),
        _timestamp("created_at"),
        sa.CheckConstraint(
            "admission_id ~ '^[0-9a-f]{64}$' "
            "AND (recipe_id IS NULL OR recipe_id ~ '^[0-9a-f]{64}$') "
            "AND binding_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND stream_identity_sha256 ~ '^[0-9a-f]{64}$' "
            "AND terminal_proof_sha256 ~ '^[0-9a-f]{64}$' "
            "AND partition_key_hash ~ '^[0-9a-f]{64}$'",
            name="pd_projection_admission_stream_digest_check",
        ),
        sa.CheckConstraint(
            "claim_generation > 0 AND resource_type <> '' "
            "AND source_partition_ordinal >= 0 AND stream_ordinal >= 0 "
            "AND terminal_sequence_ordinal >= 0",
            name="pd_projection_admission_stream_values_check",
        ),
        sa.ForeignKeyConstraint(
            ["admission_id"],
            [
                f"{schema}.provider_directory_projection_admission.admission_id",
            ],
            name="pd_projection_admission_stream_admission_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id", "retained_source_item_id"],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_item.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_item.source_item_id",
            ],
            name="pd_projection_admission_stream_item_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id", "stream_identity_sha256"],
            [
                f"{schema}.provider_directory_retained_artifact_campaign_stream.campaign_id",
                f"{schema}.provider_directory_retained_artifact_campaign_stream.stream_identity_sha256",
            ],
            name="pd_projection_admission_stream_retained_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "admission_id",
            "binding_id",
            name="provider_directory_projection_admission_stream_pkey",
        ),
        sa.UniqueConstraint(
            "admission_id",
            "stream_identity_sha256",
            name="pd_projection_admission_stream_identity_key",
        ),
        sa.UniqueConstraint(
            "admission_id",
            "stream_ordinal",
            name="pd_projection_admission_stream_ordinal_key",
        ),
        sa.UniqueConstraint(
            "admission_id",
            "retained_source_item_id",
            name="pd_projection_admission_stream_item_key",
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
            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_native_attestation_required'
                    USING ERRCODE = '55000';
            END IF;
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
                      AND recipe.resource_profile_hash =
                          NEW.resource_profile_hash
                      AND recipe.selected_resources_json =
                          NEW.selected_resources_json
                      AND recipe.required_resources_json =
                          NEW.required_resources_json
                      AND recipe.prepared_proof_json -> 'resource_counts' =
                          NEW.resource_counts_json
                      AND (recipe.prepared_proof_json ->>
                           'resource_count')::bigint = NEW.resource_count
                      AND recipe.stage_schema = NEW.storage_schema
                      AND recipe.stage_relation = NEW.storage_relation
                      AND recipe.stage_relation_oid = NEW.storage_relation_oid
                      AND {quoted_schema}.
                          provider_directory_projection_stage_matches_proof(
                              NEW.storage_schema,
                              NEW.storage_relation,
                              NEW.storage_relation_oid,
                              NEW.storage_trigger_oid,
                              NEW.physical_projection_id,
                              NEW.proof_json
                          )
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
                     WHERE recipe_id = OLD.physical_projection_id
                       AND attempt = recipe_attempt_setting::integer
                       AND status = 'sealed'
                       AND physical_projection_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_physical_projection_reference
                     WHERE physical_projection_id = OLD.physical_projection_id
                       AND released_at IS NULL
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
                      FROM {quoted_schema}.provider_directory_projection_admission
                     WHERE recipe_id = OLD.physical_projection_id
               )
               AND NOT EXISTS (
                    SELECT 1
                      FROM pg_class
                     WHERE oid = OLD.storage_relation_oid::oid
               )
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                     WHERE recipe_id = OLD.physical_projection_id
                       AND attempt = recipe_attempt_setting::integer
                       AND status = 'retired'
                       AND physical_projection_id IS NULL
               ) THEN
                RETURN OLD;
            END IF;
            RAISE EXCEPTION
                'provider_directory_physical_projection_immutable'
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
            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_native_attestation_required'
                    USING ERRCODE = '55000';
            END IF;
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
            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_native_attestation_required'
                    USING ERRCODE = '55000';
            END IF;
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
        {quoted_schema}.provider_directory_projection_stage_is_attached(
            candidate_relation_oid bigint,
            parent_relation text,
            physical_projection_id text
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT candidate_relation_oid IS NOT NULL
               AND parent_relation IS NOT NULL
               AND physical_projection_id ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
                    SELECT 1
                      FROM pg_class AS child_record
                      JOIN pg_namespace AS child_schema
                        ON child_schema.oid = child_record.relnamespace
                      JOIN pg_inherits AS inheritance_record
                        ON inheritance_record.inhrelid = child_record.oid
                      JOIN pg_class AS parent_record
                        ON parent_record.oid = inheritance_record.inhparent
                      JOIN pg_namespace AS parent_schema
                        ON parent_schema.oid = parent_record.relnamespace
                     WHERE child_record.oid = candidate_relation_oid::oid
                       AND child_schema.nspname = {_sql_literal(schema)}
                       AND child_record.relispartition
                       AND parent_schema.nspname = {_sql_literal(schema)}
                       AND parent_record.relname = parent_relation
                       AND parent_record.relkind = 'p'
                       AND pg_get_expr(
                               child_record.relpartbound,
                               child_record.oid
                           ) = format(
                               'FOR VALUES IN (%L)',
                               physical_projection_id
                           )
               );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_input_binding_id(
            candidate_block_id text,
            candidate_campaign_id text,
            candidate_source_item_id text,
            candidate_artifact_sha256 text,
            candidate_layout_sha256 text,
            candidate_range_ordinal integer,
            candidate_stream_identity_sha256 text,
            candidate_sequence_ordinal integer,
            candidate_resource_type text,
            candidate_partition_key_hash text,
            candidate_source_partition_ordinal integer
        ) RETURNS text LANGUAGE sql IMMUTABLE AS $$
            SELECT encode(
                sha256(
                    convert_to(
                        'provider-directory-projection-admission-input-binding-v2',
                        'UTF8'
                    )
                    || decode('00', 'hex')
                    || convert_to(
                        candidate_block_id || '|' ||
                        candidate_campaign_id || '|' ||
                        candidate_source_item_id || '|' ||
                        candidate_artifact_sha256 || '|' ||
                        candidate_layout_sha256 || '|' ||
                        COALESCE(candidate_range_ordinal::text, '~') || '|' ||
                        candidate_stream_identity_sha256 || '|' ||
                        candidate_sequence_ordinal::text || '|' ||
                        candidate_resource_type || '|' ||
                        candidate_partition_key_hash || '|' ||
                        candidate_source_partition_ordinal::text,
                        'UTF8'
                    )
                ),
                'hex'
            );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_terminal_binding_id(
            candidate_campaign_id text,
            candidate_source_item_id text,
            candidate_resource_type text,
            candidate_partition_key_hash text,
            candidate_source_partition_ordinal integer,
            candidate_stream_identity_sha256 text,
            candidate_stream_ordinal integer,
            candidate_terminal_sequence_ordinal integer,
            candidate_terminal_proof_sha256 text
        ) RETURNS text LANGUAGE sql IMMUTABLE STRICT AS $$
            SELECT encode(
                sha256(
                    convert_to(
                        'provider-directory-projection-admission-terminal-binding-v1',
                        'UTF8'
                    )
                    || decode('00', 'hex')
                    || convert_to(
                        candidate_campaign_id || '|' ||
                        candidate_source_item_id || '|' ||
                        candidate_resource_type || '|' ||
                        candidate_partition_key_hash || '|' ||
                        candidate_source_partition_ordinal::text || '|' ||
                        candidate_stream_identity_sha256 || '|' ||
                        candidate_stream_ordinal::text || '|' ||
                        candidate_terminal_sequence_ordinal::text || '|' ||
                        candidate_terminal_proof_sha256,
                        'UTF8'
                    )
                ),
                'hex'
            );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_binding_set_sha256(
            candidate_admission_id text
        ) RETURNS text LANGUAGE sql STABLE AS $$
            SELECT encode(
                sha256(
                    convert_to(
                        'provider-directory-projection-admission-binding-set-v4',
                        'UTF8'
                    )
                    || decode('00', 'hex')
                    || convert_to(
                        COALESCE(
                            string_agg(
                                binding.binding_id,
                                E'\n' ORDER BY binding.binding_id
                            ),
                            ''
                        ),
                        'UTF8'
                    )
                ),
                'hex'
            )
              FROM (
                    SELECT mapping.binding_id
                      FROM {quoted_schema}.
                           provider_directory_projection_admission_input_block
                           AS mapping
                     WHERE mapping.admission_id = candidate_admission_id
                    UNION ALL
                    SELECT stream.binding_id
                      FROM {quoted_schema}.
                           provider_directory_projection_admission_stream
                           AS stream
                     WHERE stream.admission_id = candidate_admission_id
              ) AS binding;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_stream_set_sha256(
            candidate_admission_id text
        ) RETURNS text LANGUAGE sql STABLE AS $$
            SELECT encode(
                sha256(
                    convert_to(
                        'provider-directory-projection-admission-stream-set-v1',
                        'UTF8'
                    )
                    || decode('00', 'hex')
                    || convert_to(
                        COALESCE(
                            string_agg(
                                stream.stream_identity_sha256 || '|' ||
                                stream.stream_ordinal::text || '|' ||
                                stream.terminal_sequence_ordinal::text || '|' ||
                                stream.terminal_proof_sha256,
                                E'\n' ORDER BY stream.stream_ordinal,
                                stream.stream_identity_sha256
                            ),
                            ''
                        ),
                        'UTF8'
                    )
                ),
                'hex'
            )
              FROM {quoted_schema}.
                   provider_directory_projection_admission_stream AS stream
             WHERE stream.admission_id = candidate_admission_id;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_mapping_is_exact(
            candidate_admission_id text,
            candidate_recipe_id text,
            candidate_binding_id text,
            candidate_block_id text,
            candidate_campaign_id text,
            candidate_consumer_recipe_id text,
            candidate_source_item_id text,
            candidate_artifact_sha256 text,
            candidate_layout_sha256 text,
            candidate_range_ordinal integer,
            candidate_stream_identity_sha256 text,
            candidate_sequence_ordinal integer,
            candidate_claim_generation bigint,
            candidate_resource_type text,
            candidate_partition_key_hash text,
            candidate_source_partition_ordinal integer
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT EXISTS (
                SELECT 1
                  FROM {quoted_schema}.provider_directory_projection_admission
                       AS admission
                  JOIN {quoted_schema}.provider_directory_projection_input_block
                       AS core_block
                    ON core_block.recipe_id = admission.recipe_id
                   AND core_block.block_id = candidate_block_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_campaign AS campaign
                    ON campaign.campaign_id = admission.retained_campaign_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_consumer AS consumer
                    ON consumer.campaign_id = campaign.campaign_id
                   AND consumer.consumer_recipe_id =
                       admission.retained_consumer_recipe_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_consumer_reference
                       AS retained_reference
                    ON retained_reference.campaign_id = consumer.campaign_id
                   AND retained_reference.consumer_recipe_id =
                       consumer.consumer_recipe_id
                   AND retained_reference.source_item_id =
                       candidate_source_item_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_campaign_item
                       AS campaign_item
                    ON campaign_item.campaign_id =
                       retained_reference.campaign_id
                   AND campaign_item.source_item_id =
                       retained_reference.source_item_id
                  JOIN {quoted_schema}.provider_directory_retained_artifact
                       AS artifact
                    ON artifact.artifact_sha256 =
                       retained_reference.artifact_sha256
                  JOIN {quoted_schema}.provider_directory_retained_artifact_layout
                       AS layout
                    ON layout.layout_sha256 = retained_reference.layout_sha256
                   AND layout.artifact_sha256 =
                       retained_reference.artifact_sha256
                  LEFT JOIN {quoted_schema}.
                       provider_directory_retained_artifact_range
                       AS retained_range
                    ON retained_range.layout_sha256 = layout.layout_sha256
                   AND retained_range.range_ordinal = candidate_range_ordinal
                 WHERE admission.admission_id = candidate_admission_id
                   AND admission.recipe_id IS NOT DISTINCT FROM
                       candidate_recipe_id
                   AND candidate_binding_id = {quoted_schema}.
                       provider_directory_projection_admission_input_binding_id(
                           candidate_block_id,
                           candidate_campaign_id,
                           candidate_source_item_id,
                           candidate_artifact_sha256,
                           candidate_layout_sha256,
                           candidate_range_ordinal,
                           candidate_stream_identity_sha256,
                           candidate_sequence_ordinal,
                           candidate_resource_type,
                           candidate_partition_key_hash,
                           candidate_source_partition_ordinal
                       )
                   AND admission.retained_campaign_id = candidate_campaign_id
                   AND admission.retained_consumer_recipe_id =
                       candidate_consumer_recipe_id
                   AND admission.claim_generation = candidate_claim_generation
                   AND EXISTS (
                        SELECT 1
                          FROM jsonb_array_elements(
                              admission.completeness_manifest_json ->
                                  'terminal_partitions'
                          ) AS terminal
                         WHERE terminal ->> 'resource_type' =
                               candidate_resource_type
                           AND terminal ->> 'partition_key_hash' =
                               candidate_partition_key_hash
                           AND (
                               terminal ->> 'source_partition_ordinal'
                           )::integer = candidate_source_partition_ordinal
                           AND terminal -> 'terminal' = 'true'::jsonb
                   )
                   AND campaign.state = 'complete'
                   AND campaign.complete
                   AND campaign.campaign_sha256 =
                       admission.retained_campaign_sha256
                   AND admission.completeness_manifest_json ->>
                       'endpoint_campaign_hash' =
                       admission.retained_campaign_sha256
                   AND campaign.released_at IS NULL
                   AND consumer.claimed_campaign_sha256 =
                       admission.retained_campaign_sha256
                   AND consumer.claim_generation = candidate_claim_generation
                   AND consumer.released_at IS NULL
                   AND retained_reference.claim_generation =
                       candidate_claim_generation
                   AND retained_reference.released_at IS NULL
                   AND retained_reference.artifact_sha256 =
                       candidate_artifact_sha256
                   AND retained_reference.layout_sha256 =
                       candidate_layout_sha256
                   AND campaign_item.item_role = 'payload'
                   AND campaign_item.status = 'admitted'
                   AND campaign_item.artifact_sha256 =
                       candidate_artifact_sha256
                   AND campaign_item.layout_sha256 = candidate_layout_sha256
                   AND campaign_item.family = candidate_resource_type
                   AND campaign_item.partition_metadata_sha256 =
                       candidate_partition_key_hash
                   AND campaign_item.stream_identity_sha256 =
                       candidate_stream_identity_sha256
                   AND campaign_item.sequence_ordinal =
                       candidate_sequence_ordinal
                   AND artifact.registry_status = 'verified'
                   AND artifact.released_at IS NULL
                   AND layout.registry_status = 'verified'
                   AND layout.released_at IS NULL
                   AND core_block.upstream_artifact_id =
                       candidate_artifact_sha256
                   AND core_block.source_object_id = candidate_layout_sha256
                   AND (
                        campaign.census_mode = 'fixed_catalog'
                        OR (
                            campaign.census_mode = 'ordered_streams'
                            AND EXISTS (
                                SELECT 1
                                  FROM {quoted_schema}.
                                       provider_directory_retained_artifact_campaign_stream
                                       AS payload_stream
                                 WHERE payload_stream.campaign_id =
                                       candidate_campaign_id
                                   AND payload_stream.stream_identity_sha256 =
                                       candidate_stream_identity_sha256
                                   AND payload_stream.complete
                                   AND payload_stream.completed_at IS NOT NULL
                                   AND candidate_sequence_ordinal <
                                       payload_stream.terminal_sequence_ordinal
                            )
                            AND EXISTS (
                                SELECT 1
                                  FROM jsonb_array_elements(
                                      admission.completeness_manifest_json ->
                                          'terminal_partitions'
                                  ) AS ordered_terminal
                                 WHERE ordered_terminal ->> 'resource_type' =
                                       candidate_resource_type
                                   AND ordered_terminal ->>
                                       'partition_key_hash' =
                                       candidate_partition_key_hash
                                   AND (
                                       ordered_terminal ->>
                                           'source_partition_ordinal'
                                   )::integer =
                                       candidate_source_partition_ordinal
                                   AND jsonb_typeof(
                                       ordered_terminal -> 'retained_stream'
                                   ) = 'object'
                                   AND ordered_terminal ->
                                       'retained_stream' ->>
                                       'identity_sha256' =
                                       candidate_stream_identity_sha256
                                   AND candidate_sequence_ordinal < (
                                       ordered_terminal ->
                                           'retained_stream' ->>
                                           'terminal_sequence_ordinal'
                                   )::integer
                            )
                        )
                   )
                   AND (
                        (
                            candidate_range_ordinal IS NULL
                            AND core_block.record_start = 0
                            AND core_block.record_count =
                                layout.artifact_record_count
                            AND core_block.payload_sha256 =
                                artifact.artifact_sha256
                            AND core_block.payload_bytes =
                                artifact.artifact_byte_count
                            AND core_block.content_sha256 =
                                layout.manifest_sha256
                        )
                        OR (
                            candidate_range_ordinal IS NOT NULL
                            AND retained_range.range_ordinal =
                                candidate_range_ordinal
                            AND retained_range.artifact_sha256 =
                                candidate_artifact_sha256
                            AND core_block.record_start =
                                retained_range.record_start
                            AND core_block.record_count =
                                retained_range.record_count
                            AND core_block.payload_sha256 =
                                retained_range.raw_sha256
                            AND core_block.payload_bytes =
                                retained_range.raw_byte_count
                            AND core_block.content_sha256 =
                                retained_range.canonical_sha256
                        )
                   )
            );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_stream_is_exact(
            candidate_admission_id text,
            candidate_recipe_id text,
            candidate_binding_id text,
            candidate_campaign_id text,
            candidate_source_item_id text,
            candidate_claim_generation bigint,
            candidate_resource_type text,
            candidate_partition_key_hash text,
            candidate_source_partition_ordinal integer,
            candidate_stream_identity_sha256 text,
            candidate_stream_ordinal integer,
            candidate_terminal_sequence_ordinal integer,
            candidate_terminal_proof_sha256 text
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            SELECT EXISTS (
                SELECT 1
                  FROM {quoted_schema}.provider_directory_projection_admission
                       AS admission
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_campaign AS campaign
                    ON campaign.campaign_id = admission.retained_campaign_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_consumer AS consumer
                    ON consumer.campaign_id = campaign.campaign_id
                   AND consumer.consumer_recipe_id =
                       admission.retained_consumer_recipe_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_campaign_stream
                       AS retained_stream
                    ON retained_stream.campaign_id = campaign.campaign_id
                   AND retained_stream.stream_identity_sha256 =
                       candidate_stream_identity_sha256
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_campaign_item
                       AS terminal_item
                    ON terminal_item.campaign_id = campaign.campaign_id
                   AND terminal_item.source_item_id = candidate_source_item_id
                 WHERE admission.admission_id = candidate_admission_id
                   AND admission.recipe_id IS NOT DISTINCT FROM
                       candidate_recipe_id
                   AND candidate_binding_id = {quoted_schema}.
                       provider_directory_projection_admission_terminal_binding_id(
                           candidate_campaign_id,
                           candidate_source_item_id,
                           candidate_resource_type,
                           candidate_partition_key_hash,
                           candidate_source_partition_ordinal,
                           candidate_stream_identity_sha256,
                           candidate_stream_ordinal,
                           candidate_terminal_sequence_ordinal,
                           candidate_terminal_proof_sha256
                       )
                   AND admission.retained_campaign_id = candidate_campaign_id
                   AND admission.claim_generation = candidate_claim_generation
                   AND campaign.state = 'complete'
                   AND campaign.complete
                   AND campaign.campaign_sha256 =
                       admission.retained_campaign_sha256
                   AND campaign.released_at IS NULL
                   AND consumer.claimed_campaign_sha256 =
                       admission.retained_campaign_sha256
                   AND consumer.claim_generation = candidate_claim_generation
                   AND consumer.released_at IS NULL
                   AND retained_stream.stream_ordinal = candidate_stream_ordinal
                   AND retained_stream.terminal_sequence_ordinal =
                       candidate_terminal_sequence_ordinal
                   AND retained_stream.terminal_proof_sha256 =
                       candidate_terminal_proof_sha256
                   AND retained_stream.complete
                   AND retained_stream.completed_at IS NOT NULL
                   AND terminal_item.item_role = 'terminal_zero'
                   AND terminal_item.status = 'terminal_zero'
                   AND terminal_item.artifact_sha256 IS NULL
                   AND terminal_item.layout_sha256 IS NULL
                   AND terminal_item.family = candidate_resource_type
                   AND terminal_item.partition_metadata_sha256 =
                       candidate_partition_key_hash
                   AND terminal_item.stream_identity_sha256 =
                       candidate_stream_identity_sha256
                   AND terminal_item.sequence_ordinal =
                       candidate_terminal_sequence_ordinal
                   AND terminal_item.terminal_proof_sha256 =
                       candidate_terminal_proof_sha256
                   AND EXISTS (
                        SELECT 1
                          FROM jsonb_array_elements(
                              admission.completeness_manifest_json ->
                                  'terminal_partitions'
                          ) AS terminal
                         WHERE terminal -> 'terminal' = 'true'::jsonb
                           AND terminal ->> 'resource_type' =
                               candidate_resource_type
                           AND terminal ->> 'partition_key_hash' =
                               candidate_partition_key_hash
                           AND (
                               terminal ->> 'source_partition_ordinal'
                           )::integer = candidate_source_partition_ordinal
                           AND jsonb_typeof(terminal -> 'retained_stream') =
                               'object'
                           AND terminal -> 'retained_stream' ->>
                               'identity_sha256' =
                               candidate_stream_identity_sha256
                           AND (
                               terminal -> 'retained_stream' ->>
                                   'stream_ordinal'
                           )::integer = candidate_stream_ordinal
                           AND (
                               terminal -> 'retained_stream' ->>
                                   'terminal_sequence_ordinal'
                           )::integer = candidate_terminal_sequence_ordinal
                           AND terminal -> 'retained_stream' ->>
                               'terminal_proof_sha256' =
                               candidate_terminal_proof_sha256
                           AND (
                               (terminal ->> 'row_count')::bigint <> 0
                               OR terminal ->> 'zero_row_proof_sha256' =
                                  candidate_terminal_proof_sha256
                           )
                   )
            );
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.provider_directory_projection_admission_scope_is_exact(
            candidate_admission_id text
        ) RETURNS boolean LANGUAGE sql STABLE AS $$
            WITH admission_scope AS (
                SELECT admission.*,
                       campaign.census_mode,
                       campaign.expected_item_count,
                       campaign.expected_stream_count
                  FROM {quoted_schema}.provider_directory_projection_admission
                       AS admission
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_campaign AS campaign
                    ON campaign.campaign_id = admission.retained_campaign_id
                  JOIN {quoted_schema}.
                       provider_directory_retained_artifact_consumer AS consumer
                    ON consumer.campaign_id = campaign.campaign_id
                   AND consumer.consumer_recipe_id =
                       admission.retained_consumer_recipe_id
                 WHERE admission.admission_id = candidate_admission_id
                   AND campaign.state = 'complete'
                   AND campaign.complete
                   AND campaign.campaign_sha256 =
                       admission.retained_campaign_sha256
                   AND campaign.released_at IS NULL
                   AND consumer.claimed_campaign_sha256 =
                       admission.retained_campaign_sha256
                   AND consumer.claim_generation = admission.claim_generation
                   AND consumer.released_at IS NULL
            ),
            binding_inventory AS (
                SELECT mapping.binding_id
                  FROM {quoted_schema}.
                       provider_directory_projection_admission_input_block
                       AS mapping
                 WHERE mapping.admission_id = candidate_admission_id
                UNION ALL
                SELECT stream.binding_id
                  FROM {quoted_schema}.
                       provider_directory_projection_admission_stream AS stream
                 WHERE stream.admission_id = candidate_admission_id
            ),
            actual_census_rows AS (
                SELECT mapping.resource_type,
                       mapping.partition_key_hash,
                       mapping.source_partition_ordinal,
                       1::bigint AS block_count,
                       core_block.record_count::bigint AS row_count,
                       core_block.payload_bytes::bigint AS byte_count
                  FROM {quoted_schema}.
                       provider_directory_projection_admission_input_block
                       AS mapping
                  JOIN {quoted_schema}.
                       provider_directory_projection_input_block AS core_block
                    ON core_block.recipe_id = mapping.recipe_id
                   AND core_block.block_id = mapping.block_id
                 WHERE mapping.admission_id = candidate_admission_id
                UNION ALL
                SELECT stream.resource_type,
                       stream.partition_key_hash,
                       stream.source_partition_ordinal,
                       0::bigint, 0::bigint, 0::bigint
                  FROM {quoted_schema}.
                       provider_directory_projection_admission_stream AS stream
                 WHERE stream.admission_id = candidate_admission_id
            ),
            actual_census AS (
                SELECT resource_type, partition_key_hash,
                       source_partition_ordinal,
                       sum(block_count)::bigint AS block_count,
                       sum(row_count)::bigint AS row_count,
                       sum(byte_count)::bigint AS byte_count
                  FROM actual_census_rows
                 GROUP BY resource_type, partition_key_hash,
                          source_partition_ordinal
            ),
            expected_census AS (
                SELECT terminal ->> 'resource_type' AS resource_type,
                       terminal ->> 'partition_key_hash'
                           AS partition_key_hash,
                       (terminal ->> 'source_partition_ordinal')::integer
                           AS source_partition_ordinal,
                       (terminal ->> 'block_count')::bigint AS block_count,
                       (terminal ->> 'row_count')::bigint AS row_count,
                       (terminal ->> 'byte_count')::bigint AS byte_count
                  FROM admission_scope AS admission,
                       LATERAL jsonb_array_elements(
                           admission.completeness_manifest_json ->
                               'terminal_partitions'
                       ) AS terminal
                 WHERE terminal -> 'terminal' = 'true'::jsonb
                   AND terminal ->> 'resource_type' <> ''
                   AND terminal ->> 'partition_key_hash' ~
                       '^[0-9a-f]{{64}}$'
                   AND terminal ? 'source_partition_ordinal'
                   AND terminal ? 'block_count'
                   AND terminal ? 'row_count'
                   AND terminal ? 'byte_count'
            )
            SELECT COALESCE(bool_and(scope_exact), false)
              FROM (
                SELECT
                    (
                        (
                            admission.outcome_kind = 'physical_workset'
                            AND admission.recipe_id =
                                admission.planned_recipe_id
                            AND admission.input_block_count > 0
                            AND EXISTS (
                                SELECT 1
                                  FROM {quoted_schema}.
                                       provider_directory_projection_recipe
                                       AS physical_recipe
                                 WHERE physical_recipe.recipe_id =
                                       admission.recipe_id
                                   AND physical_recipe.workset_registered_at
                                       IS NOT NULL
                                   AND physical_recipe.input_block_set_sha256 =
                                       admission.input_block_set_sha256
                                   AND physical_recipe.input_block_count =
                                       admission.input_block_count
                                   AND physical_recipe.selected_resources_json =
                                       admission.completeness_manifest_json ->
                                           'selected_resources'
                                   AND physical_recipe.required_resources_json =
                                       admission.completeness_manifest_json ->
                                           'required_resources'
                            )
                        )
                        OR (
                            admission.outcome_kind = 'no_input'
                            AND admission.recipe_id IS NULL
                            AND admission.input_block_count = 0
                            AND admission.census_mode = 'ordered_streams'
                            AND (
                                admission.completeness_manifest_json ->>
                                    'block_count'
                            )::bigint = 0
                            AND (
                                admission.completeness_manifest_json ->>
                                    'row_count'
                            )::bigint = 0
                            AND (
                                admission.completeness_manifest_json ->>
                                    'byte_count'
                            )::bigint = 0
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM {quoted_schema}.
                                       provider_directory_projection_recipe
                                 WHERE recipe_id = admission.planned_recipe_id
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM {quoted_schema}.
                                       provider_directory_projection_input_block
                                 WHERE recipe_id = admission.planned_recipe_id
                            )
                            AND NOT EXISTS (
                                SELECT 1
                                  FROM {quoted_schema}.
                                       provider_directory_projection_proof_shard
                                 WHERE recipe_id = admission.planned_recipe_id
                            )
                        )
                    )
                    AND admission.input_block_count = (
                        SELECT count(*)
                          FROM {quoted_schema}.
                               provider_directory_projection_input_block
                         WHERE recipe_id = admission.recipe_id
                    )
                    AND admission.binding_count = (
                        SELECT count(*) FROM binding_inventory
                    )
                    AND admission.binding_count = (
                        SELECT count(DISTINCT binding_id)
                          FROM binding_inventory
                    )
                    AND admission.stream_count = (
                        SELECT count(*)
                          FROM {quoted_schema}.
                               provider_directory_projection_admission_stream
                         WHERE admission_id = candidate_admission_id
                    )
                    AND admission.binding_set_sha256 = {quoted_schema}.
                        provider_directory_projection_admission_binding_set_sha256(
                            candidate_admission_id
                        )
                    AND admission.stream_set_sha256 = {quoted_schema}.
                        provider_directory_projection_admission_stream_set_sha256(
                            candidate_admission_id
                        )
                    AND (
                        (admission.census_mode = 'fixed_catalog'
                         AND admission.expected_stream_count = 0
                         AND admission.stream_count = 0)
                        OR
                        (admission.census_mode = 'ordered_streams'
                         AND admission.stream_count =
                             admission.expected_stream_count
                         AND admission.expected_stream_count = (
                             admission.completeness_manifest_json ->>
                                 'partition_count'
                         )::integer
                         AND NOT EXISTS (
                             SELECT 1
                               FROM jsonb_array_elements(
                                   admission.completeness_manifest_json ->
                                       'terminal_partitions'
                               ) AS ordered_terminal
                              WHERE jsonb_typeof(
                                  ordered_terminal -> 'retained_stream'
                              ) IS DISTINCT FROM 'object'
                         ))
                    )
                    AND admission.expected_item_count = (
                        SELECT count(*)
                          FROM {quoted_schema}.
                               provider_directory_retained_artifact_campaign_item
                         WHERE campaign_id = admission.retained_campaign_id
                    )
                    AND (
                        admission.completeness_manifest_json ->>
                            'partition_count'
                    )::integer = (
                        SELECT count(*) FROM expected_census
                    )
                    AND (
                        admission.completeness_manifest_json ->>
                            'partition_count'
                    )::integer = jsonb_array_length(
                        admission.completeness_manifest_json ->
                            'terminal_partitions'
                    )
                    AND (
                        SELECT count(*) FROM expected_census
                    ) = (
                        SELECT count(DISTINCT (
                            resource_type,
                            partition_key_hash,
                            source_partition_ordinal
                        )) FROM expected_census
                    )
                    AND admission.stream_count = (
                        SELECT count(*)
                          FROM jsonb_array_elements(
                              admission.completeness_manifest_json ->
                                  'terminal_partitions'
                          ) AS terminal
                         WHERE jsonb_typeof(
                             terminal -> 'retained_stream'
                         ) = 'object'
                    )
                    AND (
                        admission.completeness_manifest_json ->> 'block_count'
                    )::integer = (
                        SELECT count(*)
                          FROM {quoted_schema}.
                               provider_directory_projection_admission_input_block
                         WHERE admission_id = candidate_admission_id
                    )
                    AND (
                        admission.completeness_manifest_json ->> 'row_count'
                    )::bigint = COALESCE(
                        (SELECT sum(row_count) FROM actual_census_rows), 0
                    )
                    AND (
                        admission.completeness_manifest_json ->> 'byte_count'
                    )::bigint = COALESCE(
                        (SELECT sum(byte_count) FROM actual_census_rows), 0
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM actual_census AS actual
                          FULL JOIN expected_census AS expected
                            USING (
                                resource_type,
                                partition_key_hash,
                                source_partition_ordinal
                            )
                         WHERE actual.block_count IS DISTINCT FROM
                               expected.block_count
                            OR actual.row_count IS DISTINCT FROM
                               expected.row_count
                            OR actual.byte_count IS DISTINCT FROM
                               expected.byte_count
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_input_block
                               AS core_block
                         WHERE core_block.recipe_id = admission.recipe_id
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_projection_admission_input_block
                                      AS mapping
                                WHERE mapping.admission_id =
                                      candidate_admission_id
                                  AND mapping.recipe_id = core_block.recipe_id
                                  AND mapping.block_id = core_block.block_id
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_admission_input_block
                               AS mapping
                         WHERE mapping.admission_id = candidate_admission_id
                           AND NOT {quoted_schema}.
                               provider_directory_projection_admission_mapping_is_exact(
                                   mapping.admission_id,
                                   mapping.recipe_id,
                                   mapping.binding_id,
                                   mapping.block_id,
                                   mapping.retained_campaign_id,
                                   mapping.retained_consumer_recipe_id,
                                   mapping.retained_source_item_id,
                                   mapping.retained_artifact_sha256,
                                   mapping.retained_layout_sha256,
                                   mapping.retained_range_ordinal,
                                   mapping.stream_identity_sha256,
                                   mapping.sequence_ordinal,
                                   mapping.claim_generation,
                                   mapping.resource_type,
                                   mapping.partition_key_hash,
                                   mapping.source_partition_ordinal
                               )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_admission_stream
                               AS stream
                         WHERE stream.admission_id = candidate_admission_id
                           AND NOT {quoted_schema}.
                               provider_directory_projection_admission_stream_is_exact(
                                   stream.admission_id,
                                   stream.recipe_id,
                                   stream.binding_id,
                                   stream.retained_campaign_id,
                                   stream.retained_source_item_id,
                                   stream.claim_generation,
                                   stream.resource_type,
                                   stream.partition_key_hash,
                                   stream.source_partition_ordinal,
                                   stream.stream_identity_sha256,
                                   stream.stream_ordinal,
                                   stream.terminal_sequence_ordinal,
                                   stream.terminal_proof_sha256
                               )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM jsonb_array_elements(
                              admission.completeness_manifest_json ->
                                  'terminal_partitions'
                          ) AS terminal
                         WHERE jsonb_typeof(
                                   terminal -> 'retained_stream'
                               ) = 'object'
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_projection_admission_stream
                                      AS stream
                                WHERE stream.admission_id =
                                      candidate_admission_id
                                  AND stream.resource_type =
                                      terminal ->> 'resource_type'
                                  AND stream.partition_key_hash =
                                      terminal ->> 'partition_key_hash'
                                  AND stream.source_partition_ordinal = (
                                      terminal ->>
                                          'source_partition_ordinal'
                                  )::integer
                                  AND stream.stream_identity_sha256 =
                                      terminal -> 'retained_stream' ->>
                                          'identity_sha256'
                                  AND stream.stream_ordinal = (
                                      terminal -> 'retained_stream' ->>
                                          'stream_ordinal'
                                  )::integer
                                  AND stream.terminal_sequence_ordinal = (
                                      terminal -> 'retained_stream' ->>
                                          'terminal_sequence_ordinal'
                                  )::integer
                                  AND stream.terminal_proof_sha256 =
                                      terminal -> 'retained_stream' ->>
                                          'terminal_proof_sha256'
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_retained_artifact_campaign_item
                               AS item
                         WHERE item.campaign_id = admission.retained_campaign_id
                           AND (
                               (item.item_role = 'payload'
                                AND item.status = 'admitted'
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM {quoted_schema}.
                                           provider_directory_retained_artifact_consumer_reference
                                           AS retained_reference
                                      JOIN {quoted_schema}.
                                           provider_directory_projection_admission_input_block
                                           AS mapping
                                        ON mapping.admission_id =
                                           candidate_admission_id
                                       AND mapping.retained_campaign_id =
                                           retained_reference.campaign_id
                                       AND mapping.retained_consumer_recipe_id =
                                           retained_reference.consumer_recipe_id
                                       AND mapping.retained_source_item_id =
                                           retained_reference.source_item_id
                                       AND mapping.retained_artifact_sha256 =
                                           retained_reference.artifact_sha256
                                       AND mapping.retained_layout_sha256 =
                                           retained_reference.layout_sha256
                                       AND mapping.claim_generation =
                                           retained_reference.claim_generation
                                     WHERE retained_reference.campaign_id =
                                           admission.retained_campaign_id
                                       AND retained_reference.consumer_recipe_id =
                                           admission.retained_consumer_recipe_id
                                       AND retained_reference.source_item_id =
                                           item.source_item_id
                                       AND retained_reference.claim_generation =
                                           admission.claim_generation
                                       AND retained_reference.released_at IS NULL
                                ))
                               OR
                               (item.item_role = 'terminal_zero'
                                AND item.status = 'terminal_zero'
                                AND NOT EXISTS (
                                    SELECT 1
                                      FROM {quoted_schema}.
                                           provider_directory_projection_admission_stream
                                           AS stream
                                     WHERE stream.admission_id =
                                           candidate_admission_id
                                       AND stream.retained_campaign_id =
                                           item.campaign_id
                                       AND stream.retained_source_item_id =
                                           item.source_item_id
                                ))
                               OR item.item_role NOT IN (
                                   'payload', 'terminal_zero'
                               )
                               OR item.status NOT IN (
                                   'admitted', 'terminal_zero'
                               )
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_retained_artifact_consumer_reference
                               AS retained_reference
                         WHERE retained_reference.campaign_id =
                               admission.retained_campaign_id
                           AND retained_reference.consumer_recipe_id =
                               admission.retained_consumer_recipe_id
                           AND retained_reference.claim_generation =
                               admission.claim_generation
                           AND retained_reference.released_at IS NULL
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_projection_admission_input_block
                                      AS mapping
                                WHERE mapping.admission_id =
                                      candidate_admission_id
                                  AND mapping.retained_source_item_id =
                                      retained_reference.source_item_id
                                  AND mapping.retained_artifact_sha256 =
                                      retained_reference.artifact_sha256
                                  AND mapping.retained_layout_sha256 =
                                      retained_reference.layout_sha256
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_retained_artifact_campaign_stream
                               AS retained_stream
                         WHERE retained_stream.campaign_id =
                               admission.retained_campaign_id
                           AND (
                               NOT retained_stream.complete
                               OR NOT EXISTS (
                                   SELECT 1
                                     FROM {quoted_schema}.
                                          provider_directory_projection_admission_stream
                                          AS stream
                                    WHERE stream.admission_id =
                                          candidate_admission_id
                                      AND stream.retained_campaign_id =
                                          retained_stream.campaign_id
                                      AND stream.stream_identity_sha256 =
                                          retained_stream.stream_identity_sha256
                                      AND stream.stream_ordinal =
                                          retained_stream.stream_ordinal
                                      AND stream.terminal_sequence_ordinal =
                                          retained_stream.terminal_sequence_ordinal
                                      AND stream.terminal_proof_sha256 =
                                          retained_stream.terminal_proof_sha256
                               )
                           )
                    )
                    AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_retained_artifact_campaign_item
                               AS item
                          JOIN {quoted_schema}.
                               provider_directory_retained_artifact_layout
                               AS layout
                            ON layout.layout_sha256 = item.layout_sha256
                           AND layout.artifact_sha256 = item.artifact_sha256
                         WHERE item.campaign_id = admission.retained_campaign_id
                           AND item.item_role = 'payload'
                           AND item.status = 'admitted'
                           AND NOT (
                               (
                                   SELECT count(*) = 1
                                      FROM {quoted_schema}.
                                           provider_directory_projection_admission_input_block
                                           AS mapping
                                     WHERE mapping.admission_id =
                                           candidate_admission_id
                                       AND mapping.retained_source_item_id =
                                           item.source_item_id
                                       AND mapping.retained_range_ordinal IS NULL
                               )
                               AND NOT EXISTS (
                                   SELECT 1
                                     FROM {quoted_schema}.
                                          provider_directory_projection_admission_input_block
                                          AS mapping
                                    WHERE mapping.admission_id =
                                          candidate_admission_id
                                      AND mapping.retained_source_item_id =
                                          item.source_item_id
                                      AND mapping.retained_range_ordinal IS NOT NULL
                               )
                               OR
                               NOT EXISTS (
                                   SELECT 1
                                     FROM {quoted_schema}.
                                          provider_directory_projection_admission_input_block
                                          AS mapping
                                    WHERE mapping.admission_id =
                                          candidate_admission_id
                                      AND mapping.retained_source_item_id =
                                          item.source_item_id
                                      AND mapping.retained_range_ordinal IS NULL
                               )
                               AND (
                                   SELECT count(*) = layout.layout_range_count
                                      AND count(DISTINCT
                                          mapping.retained_range_ordinal
                                      ) = layout.layout_range_count
                                     FROM {quoted_schema}.
                                          provider_directory_projection_admission_input_block
                                          AS mapping
                                    WHERE mapping.admission_id =
                                          candidate_admission_id
                                      AND mapping.retained_source_item_id =
                                          item.source_item_id
                                      AND mapping.retained_range_ordinal IS NOT NULL
                               )
                           )
                    ) AS scope_exact
                  FROM admission_scope AS admission
              ) AS exact_scope;
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
            IF TG_OP = 'INSERT'
               AND action_setting = 'workset_register'
               AND recipe_id_setting = NEW.recipe_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND recipe_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
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
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.recipe_id
               AND physical_id_setting = OLD.recipe_id
               AND recipe_attempt_setting ~ '^[1-9][0-9]*$'
               AND COALESCE(recipe_lease_setting, '') = ''
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_projection_admission_input_block
                     WHERE recipe_id = OLD.recipe_id
                       AND block_id = OLD.block_id
               )
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                     WHERE recipe_id = OLD.recipe_id
                       AND attempt = recipe_attempt_setting::integer
                       AND status = 'retired'
               ) THEN
                RETURN OLD;
            END IF;
            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_workset_write_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            RAISE EXCEPTION 'provider_directory_projection_input_block_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_shard_admission()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            IF TG_OP = 'INSERT' THEN
                IF NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_input_block
                     WHERE recipe_id = NEW.recipe_id
                       AND block_id = NEW.input_block_id
                       AND content_sha256 = NEW.input_sha256
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_shard_core_mismatch'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF OLD.status = 'pending' AND NEW.status = 'building'
               AND NOT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_admission
                     WHERE recipe_id = NEW.recipe_id
                       AND status = 'sealed'
               ) THEN
                RAISE EXCEPTION
                    'provider_directory_projection_admission_required'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_admission_block()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            admission_id_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_id', true
            );
            admission_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_attempt',
                true
            );
            admission_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_lease_token',
                true
            );
        BEGIN
            IF TG_OP = 'INSERT'
               AND action_setting = 'admission_map'
               AND recipe_id_setting = (
                    SELECT planned_recipe_id
                      FROM {quoted_schema}.
                           provider_directory_projection_admission
                     WHERE admission_id = NEW.admission_id
               )
               AND admission_id_setting = NEW.admission_id
               AND admission_attempt_setting ~ '^[1-9][0-9]*$'
               AND admission_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
                    SELECT 1
                     FROM {quoted_schema}.provider_directory_projection_admission
                     WHERE admission_id = NEW.admission_id
                       AND recipe_id IS NOT DISTINCT FROM NEW.recipe_id
                       AND attempt = admission_attempt_setting::integer
                       AND status = 'building'
                       AND lease_token = admission_lease_setting
                       AND lease_expires_at > clock_timestamp()
                       AND retained_campaign_id = NEW.retained_campaign_id
                       AND retained_consumer_recipe_id =
                           NEW.retained_consumer_recipe_id
                       AND claim_generation = NEW.claim_generation
                     FOR SHARE
               )
               AND {quoted_schema}.
                   provider_directory_projection_admission_mapping_is_exact(
                       NEW.admission_id,
                       NEW.recipe_id,
                       NEW.binding_id,
                       NEW.block_id,
                       NEW.retained_campaign_id,
                       NEW.retained_consumer_recipe_id,
                       NEW.retained_source_item_id,
                       NEW.retained_artifact_sha256,
                       NEW.retained_layout_sha256,
                       NEW.retained_range_ordinal,
                       NEW.stream_identity_sha256,
                       NEW.sequence_ordinal,
                       NEW.claim_generation,
                       NEW.resource_type,
                       NEW.partition_key_hash,
                       NEW.source_partition_ordinal
                   ) THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.recipe_id
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_admission
                           AS admission
                      JOIN {quoted_schema}.provider_directory_projection_recipe
                           AS recipe ON recipe.recipe_id = admission.recipe_id
                     WHERE admission.admission_id = OLD.admission_id
                       AND admission.status = 'released'
                       AND recipe.status = 'retired'
               ) THEN
                RETURN OLD;
            END IF;
            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_admission_map_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            RAISE EXCEPTION
                'provider_directory_projection_admission_block_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_admission_stream()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            admission_id_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_id', true
            );
            admission_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_attempt',
                true
            );
            admission_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_lease_token',
                true
            );
        BEGIN
            IF TG_OP = 'INSERT'
               AND action_setting = 'admission_map'
               AND recipe_id_setting = (
                    SELECT planned_recipe_id
                      FROM {quoted_schema}.
                           provider_directory_projection_admission
                     WHERE admission_id = NEW.admission_id
               )
               AND admission_id_setting = NEW.admission_id
               AND admission_attempt_setting ~ '^[1-9][0-9]*$'
               AND admission_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_admission
                     WHERE admission_id = NEW.admission_id
                       AND recipe_id IS NOT DISTINCT FROM NEW.recipe_id
                       AND attempt = admission_attempt_setting::integer
                       AND status = 'building'
                       AND lease_token = admission_lease_setting
                       AND lease_expires_at > clock_timestamp()
                       AND retained_campaign_id = NEW.retained_campaign_id
                       AND claim_generation = NEW.claim_generation
                     FOR SHARE
               )
               AND {quoted_schema}.
                   provider_directory_projection_admission_stream_is_exact(
                       NEW.admission_id,
                       NEW.recipe_id,
                       NEW.binding_id,
                       NEW.retained_campaign_id,
                       NEW.retained_source_item_id,
                       NEW.claim_generation,
                       NEW.resource_type,
                       NEW.partition_key_hash,
                       NEW.source_partition_ordinal,
                       NEW.stream_identity_sha256,
                       NEW.stream_ordinal,
                       NEW.terminal_sequence_ordinal,
                       NEW.terminal_proof_sha256
                   ) THEN
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE'
               AND action_setting = 'gc'
               AND recipe_id_setting = OLD.recipe_id
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_admission
                           AS admission
                      JOIN {quoted_schema}.provider_directory_projection_recipe
                           AS recipe ON recipe.recipe_id = admission.recipe_id
                     WHERE admission.admission_id = OLD.admission_id
                       AND admission.status = 'released'
                       AND recipe.status = 'retired'
               ) THEN
                RETURN OLD;
            END IF;
            IF TG_OP = 'INSERT' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_admission_stream_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            RAISE EXCEPTION
                'provider_directory_projection_admission_stream_immutable'
                USING ERRCODE = '55000';
        END;
        $$;

        CREATE OR REPLACE FUNCTION
        {quoted_schema}.guard_provider_directory_projection_admission()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            action_setting text := current_setting(
                'healthporta.provider_directory_projection_action', true
            );
            recipe_id_setting text := current_setting(
                'healthporta.provider_directory_projection_recipe_id', true
            );
            admission_id_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_id', true
            );
            admission_attempt_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_attempt',
                true
            );
            admission_lease_setting text := current_setting(
                'healthporta.provider_directory_projection_admission_lease_token',
                true
            );
        BEGIN
            IF TG_OP = 'INSERT' THEN
                PERFORM pg_advisory_xact_lock(
                    hashtextextended(NEW.planned_recipe_id, 1732050807)
                );
                IF action_setting <> 'admission_insert'
                   OR recipe_id_setting <> NEW.planned_recipe_id
                   OR admission_id_setting <> NEW.admission_id
                   OR admission_attempt_setting <> NEW.attempt::text
                   OR admission_lease_setting <> NEW.lease_token
                   OR NEW.status <> 'building'
                   OR NEW.attempt <> 1
                   OR NEW.lease_token !~ '^[0-9a-f]{{64}}$'
                   OR NEW.lease_expires_at <= clock_timestamp()
                   OR NEW.lease_heartbeat_at IS NULL
                   OR NEW.sealed_at IS NOT NULL
                   OR NEW.released_at IS NOT NULL
                   OR NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_retained_artifact_campaign
                               AS campaign
                          JOIN {quoted_schema}.
                               provider_directory_retained_artifact_consumer
                               AS consumer
                            ON consumer.campaign_id = campaign.campaign_id
                           AND consumer.consumer_recipe_id =
                               NEW.retained_consumer_recipe_id
                         WHERE campaign.campaign_id =
                               NEW.retained_campaign_id
                           AND campaign.state = 'complete'
                           AND campaign.complete
                           AND campaign.campaign_sha256 =
                               NEW.retained_campaign_sha256
                           AND NEW.completeness_manifest_json ->>
                               'endpoint_campaign_hash' =
                               NEW.retained_campaign_sha256
                           AND campaign.released_at IS NULL
                           AND consumer.claimed_campaign_sha256 =
                               NEW.retained_campaign_sha256
                           AND consumer.claim_generation = NEW.claim_generation
                           AND consumer.released_at IS NULL
                           AND (
                               (
                                   NEW.outcome_kind = 'physical_workset'
                                   AND NEW.recipe_id = NEW.planned_recipe_id
                                   AND NEW.input_block_count > 0
                                   AND EXISTS (
                                       SELECT 1
                                         FROM {quoted_schema}.
                                              provider_directory_projection_recipe
                                              AS recipe
                                        WHERE recipe.recipe_id = NEW.recipe_id
                                          AND recipe.status IN (
                                              'building', 'proof_ready', 'sealed'
                                          )
                                          AND recipe.workset_registered_at
                                              IS NOT NULL
                                          AND recipe.input_block_set_sha256 =
                                              NEW.input_block_set_sha256
                                          AND recipe.input_block_count =
                                              NEW.input_block_count
                                          AND recipe.selected_resources_json =
                                              NEW.completeness_manifest_json ->
                                                  'selected_resources'
                                          AND recipe.required_resources_json =
                                              NEW.completeness_manifest_json ->
                                                  'required_resources'
                                   )
                               )
                               OR (
                                   NEW.outcome_kind = 'no_input'
                                   AND NEW.recipe_id IS NULL
                                   AND NEW.input_block_count = 0
                                   AND NEW.stream_count > 0
                                   AND NEW.binding_count = NEW.stream_count
                                   AND campaign.census_mode = 'ordered_streams'
                                   AND campaign.expected_stream_count =
                                       NEW.stream_count
                                   AND (
                                       NEW.completeness_manifest_json ->>
                                           'partition_count'
                                   )::integer = NEW.stream_count
                                   AND (
                                       NEW.completeness_manifest_json ->>
                                           'block_count'
                                   )::bigint = 0
                                   AND (
                                       NEW.completeness_manifest_json ->>
                                           'row_count'
                                   )::bigint = 0
                                   AND (
                                       NEW.completeness_manifest_json ->>
                                           'byte_count'
                                   )::bigint = 0
                                   AND NOT EXISTS (
                                       SELECT 1
                                         FROM jsonb_array_elements(
                                             NEW.completeness_manifest_json ->
                                                 'terminal_partitions'
                                         ) AS zero_terminal
                                        WHERE jsonb_typeof(
                                            zero_terminal -> 'retained_stream'
                                        ) IS DISTINCT FROM 'object'
                                           OR (
                                               zero_terminal ->> 'block_count'
                                           )::bigint <> 0
                                           OR (
                                               zero_terminal ->> 'row_count'
                                           )::bigint <> 0
                                           OR (
                                               zero_terminal ->> 'byte_count'
                                           )::bigint <> 0
                                   )
                                   AND NOT EXISTS (
                                       SELECT 1
                                         FROM {quoted_schema}.
                                              provider_directory_projection_recipe
                                        WHERE recipe_id = NEW.planned_recipe_id
                                   )
                                   AND NOT EXISTS (
                                       SELECT 1
                                         FROM {quoted_schema}.
                                              provider_directory_projection_input_block
                                        WHERE recipe_id = NEW.planned_recipe_id
                                   )
                                   AND NOT EXISTS (
                                       SELECT 1
                                         FROM {quoted_schema}.
                                              provider_directory_projection_proof_shard
                                        WHERE recipe_id = NEW.planned_recipe_id
                                   )
                               )
                           )
                         FOR SHARE OF campaign, consumer
                   ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_admission_insert_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' THEN
                IF action_setting = 'gc'
                   AND recipe_id_setting = OLD.recipe_id
                   AND OLD.status = 'released'
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_admission_input_block
                         WHERE admission_id = OLD.admission_id
                   )
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_admission_stream
                         WHERE admission_id = OLD.admission_id
                   )
                   AND EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_recipe
                         WHERE recipe_id = OLD.recipe_id
                           AND status = 'retired'
                   ) THEN
                    RETURN OLD;
                END IF;
                RAISE EXCEPTION
                    'provider_directory_projection_admission_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.admission_id IS DISTINCT FROM NEW.admission_id
               OR OLD.planned_recipe_id IS DISTINCT FROM NEW.planned_recipe_id
               OR OLD.recipe_id IS DISTINCT FROM NEW.recipe_id
               OR OLD.outcome_kind IS DISTINCT FROM NEW.outcome_kind
               OR OLD.acquisition_adapter_id IS DISTINCT FROM
                  NEW.acquisition_adapter_id
               OR OLD.source_scope_hash IS DISTINCT FROM NEW.source_scope_hash
               OR OLD.source_ids_json IS DISTINCT FROM NEW.source_ids_json
               OR OLD.completeness_manifest_hash IS DISTINCT FROM
                  NEW.completeness_manifest_hash
               OR OLD.completeness_manifest_json IS DISTINCT FROM
                  NEW.completeness_manifest_json
               OR OLD.retained_campaign_id IS DISTINCT FROM
                  NEW.retained_campaign_id
               OR OLD.retained_campaign_sha256 IS DISTINCT FROM
                  NEW.retained_campaign_sha256
               OR OLD.retained_consumer_recipe_id IS DISTINCT FROM
                  NEW.retained_consumer_recipe_id
               OR OLD.claim_generation IS DISTINCT FROM NEW.claim_generation
               OR OLD.input_block_set_sha256 IS DISTINCT FROM
                  NEW.input_block_set_sha256
               OR OLD.input_block_count IS DISTINCT FROM NEW.input_block_count
               OR OLD.binding_count IS DISTINCT FROM NEW.binding_count
               OR OLD.binding_set_sha256 IS DISTINCT FROM
                  NEW.binding_set_sha256
               OR OLD.stream_set_sha256 IS DISTINCT FROM
                  NEW.stream_set_sha256
               OR OLD.stream_count IS DISTINCT FROM NEW.stream_count
               OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
                RAISE EXCEPTION
                    'provider_directory_projection_admission_identity_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF action_setting <> 'gc' AND (
                recipe_id_setting <> OLD.planned_recipe_id
                OR admission_id_setting <> OLD.admission_id
                OR admission_attempt_setting <> OLD.attempt::text
            ) THEN
                RAISE EXCEPTION
                    'provider_directory_projection_admission_write_unfenced'
                    USING ERRCODE = '55000';
            END IF;
            IF action_setting = 'admission_heartbeat'
               AND OLD.status = 'building' AND NEW.status = 'building'
               AND OLD.lease_token = admission_lease_setting
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
            IF action_setting = 'admission_reclaim'
               AND OLD.status = 'building' AND NEW.status = 'building'
               AND OLD.lease_expires_at <= clock_timestamp()
               AND NEW.lease_token = admission_lease_setting
               AND admission_lease_setting ~ '^[0-9a-f]{{64}}$'
               AND NEW.lease_expires_at > clock_timestamp()
               AND to_jsonb(NEW) - ARRAY[
                   'lease_token', 'lease_expires_at', 'lease_heartbeat_at',
                   'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'lease_token', 'lease_expires_at', 'lease_heartbeat_at',
                   'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'admission_seal'
               AND OLD.status = 'building' AND NEW.status = 'sealed'
               AND OLD.lease_token = admission_lease_setting
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.lease_token IS NULL
               AND NEW.lease_expires_at IS NULL
               AND NEW.lease_heartbeat_at IS NULL
               AND NEW.sealed_at IS NOT NULL
               AND NEW.released_at IS NULL
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'lease_token', 'lease_expires_at',
                   'lease_heartbeat_at', 'sealed_at', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'lease_token', 'lease_expires_at',
                   'lease_heartbeat_at', 'sealed_at', 'updated_at'
               ]
               AND NEW.completeness_manifest_json ->> 'contract_id' =
                   'healthporta.provider-directory.acquisition-completeness.v1'
               AND {quoted_schema}.
                   provider_directory_projection_admission_scope_is_exact(
                       NEW.admission_id
                   ) THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'gc'
               AND OLD.status = 'sealed' AND NEW.status = 'released'
               AND recipe_id_setting = OLD.recipe_id
               AND NEW.released_at IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                     WHERE recipe_id = OLD.recipe_id
                       AND status = 'retired'
               )
               AND to_jsonb(NEW) - ARRAY['status', 'released_at', 'updated_at'] =
                   to_jsonb(OLD) - ARRAY['status', 'released_at', 'updated_at']
               THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION
                'provider_directory_projection_admission_transition_invalid'
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
                PERFORM pg_advisory_xact_lock(
                    hashtextextended(NEW.recipe_id, 1732050807)
                );
                IF EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_projection_admission
                     WHERE planned_recipe_id = NEW.recipe_id
                       AND outcome_kind = 'no_input'
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_no_input_recipe_conflict'
                        USING ERRCODE = '55000';
                END IF;
                IF action_setting <> 'recipe_insert'
                   OR recipe_id_setting <> NEW.recipe_id
                   OR recipe_attempt_setting <> NEW.attempt::text
                   OR recipe_lease_setting <> NEW.lease_token
                   OR NEW.status <> 'building'
                   OR NEW.attempt <> 1
                   OR NEW.lease_token !~ '^[0-9a-f]{{64}}$'
                   OR NEW.lease_expires_at <= clock_timestamp()
                   OR NEW.physical_projection_id IS NOT NULL
                   OR NEW.stage_schema IS NOT NULL
                   OR NEW.stage_relation IS NOT NULL
                   OR NEW.stage_relation_oid IS NOT NULL
                   OR NEW.workset_registered_at IS NOT NULL
                   OR NEW.prepared_proof_json IS NOT NULL
                   OR NEW.sealed_at IS NOT NULL THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_recipe_insert_state_invalid'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF NEW.status IN ('proof_ready', 'sealed')
               AND NEW.status IS DISTINCT FROM OLD.status THEN
                RAISE EXCEPTION
                    'provider_directory_projection_native_attestation_required'
                    USING ERRCODE = '55000';
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
                   )
                   AND NOT EXISTS (
                       SELECT 1
                         FROM {quoted_schema}.
                              provider_directory_projection_admission
                        WHERE recipe_id = OLD.recipe_id
                   ) THEN
                    RETURN OLD;
                END IF;
                RAISE EXCEPTION
                    'provider_directory_projection_recipe_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF OLD.recipe_id IS DISTINCT FROM NEW.recipe_id
               OR OLD.decoder_contract_id IS DISTINCT FROM NEW.decoder_contract_id
               OR OLD.input_set_sha256 IS DISTINCT FROM NEW.input_set_sha256
               OR OLD.transform_contract_id IS DISTINCT FROM
                  NEW.transform_contract_id
               OR OLD.scope_contract_id IS DISTINCT FROM NEW.scope_contract_id
               OR OLD.transform_context_hash IS DISTINCT FROM
                  NEW.transform_context_hash
               OR OLD.transform_context_json IS DISTINCT FROM
                  NEW.transform_context_json
               OR OLD.resource_profile_hash IS DISTINCT FROM
                  NEW.resource_profile_hash
               OR OLD.selected_resources_json IS DISTINCT FROM
                  NEW.selected_resources_json
               OR OLD.required_resources_json IS DISTINCT FROM
                  NEW.required_resources_json
               OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
                RAISE EXCEPTION
                    'provider_directory_projection_recipe_identity_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF recipe_id_setting <> OLD.recipe_id
               OR recipe_attempt_setting <> OLD.attempt::text THEN
                RAISE EXCEPTION
                    'provider_directory_projection_recipe_write_unfenced'
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
            IF OLD.status = 'failed' AND NEW.status = 'building' THEN
                RAISE EXCEPTION
                    'provider_directory_projection_failure_classification_required'
                    USING ERRCODE = '55000';
            END IF;
            IF action_setting = 'workset_register'
               AND OLD.status = 'building' AND NEW.status = 'building'
               AND OLD.lease_token = recipe_lease_setting
               AND NEW.lease_token = OLD.lease_token
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.lease_expires_at = OLD.lease_expires_at
               AND OLD.workset_registered_at IS NULL
               AND NEW.workset_registered_at IS NOT NULL
               AND NEW.input_block_set_sha256 = NEW.input_set_sha256
               AND NEW.input_block_count > 0
               AND NEW.partition_set_sha256 IS NOT NULL
               AND NEW.partition_count = NEW.input_block_count
               AND (
                   SELECT count(*)
                     FROM {quoted_schema}.
                          provider_directory_projection_input_block
                    WHERE recipe_id = NEW.recipe_id
               ) = NEW.input_block_count
               AND (
                   SELECT count(*)
                     FROM {quoted_schema}.
                          provider_directory_projection_proof_shard
                    WHERE recipe_id = NEW.recipe_id
                      AND attempt = NEW.attempt
               ) = NEW.partition_count
               AND NOT EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.
                          provider_directory_projection_input_block
                          AS core_block
                     LEFT JOIN {quoted_schema}.
                          provider_directory_projection_proof_shard
                          AS proof_shard
                       ON proof_shard.recipe_id = core_block.recipe_id
                      AND proof_shard.attempt = NEW.attempt
                      AND proof_shard.input_block_id = core_block.block_id
                    WHERE core_block.recipe_id = NEW.recipe_id
                    GROUP BY core_block.block_id, core_block.content_sha256
                   HAVING count(proof_shard.partition_id) <> 1
                       OR bool_or(
                           proof_shard.input_sha256 IS DISTINCT FROM
                           core_block.content_sha256
                       )
               )
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
               AND to_jsonb(NEW) - ARRAY[
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'updated_at'
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
                   ) = NEW.partition_count
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_admission
                     WHERE recipe_id = NEW.recipe_id
                       AND status = 'sealed'
               )
               AND (
                   SELECT count(*)
                     FROM {quoted_schema}.
                          provider_directory_projection_proof_shard
                    WHERE recipe_id = NEW.recipe_id
                      AND attempt = NEW.attempt
                      AND status = 'complete'
               ) = NEW.partition_count
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
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'prepared_proof_json', 'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'prepared_proof_json', 'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            IF action_setting = 'seal'
               AND OLD.status = 'proof_ready' AND NEW.status = 'sealed'
               AND OLD.lease_token = recipe_lease_setting
               AND OLD.lease_expires_at > clock_timestamp()
               AND NEW.physical_projection_id = physical_id_setting
               AND physical_id_setting = OLD.recipe_id
               AND NEW.lease_token IS NULL
               AND NEW.lease_expires_at IS NULL
               AND NEW.lease_heartbeat_at IS NULL
               AND NEW.sealed_at IS NOT NULL
               AND {quoted_schema}.
                   provider_directory_projection_stage_is_attached(
                       OLD.stage_relation_oid,
                       'provider_directory_physical_projection_resource',
                       physical_id_setting
                   )
               AND EXISTS (
                   SELECT 1
                     FROM {quoted_schema}.provider_directory_physical_projection
                          AS physical
                    WHERE physical.physical_projection_id = physical_id_setting
                      AND physical.proof_json = OLD.prepared_proof_json
                      AND physical.status = 'sealed'
                      AND physical.decoder_contract_id =
                          OLD.decoder_contract_id
                      AND physical.input_set_sha256 = OLD.input_set_sha256
                      AND physical.transform_contract_id =
                          OLD.transform_contract_id
                      AND physical.scope_contract_id = OLD.scope_contract_id
                      AND physical.transform_context_hash =
                          OLD.transform_context_hash
                      AND physical.transform_context_json =
                          OLD.transform_context_json
                      AND physical.resource_profile_hash =
                          OLD.resource_profile_hash
                      AND physical.selected_resources_json =
                          OLD.selected_resources_json
                      AND physical.required_resources_json =
                          OLD.required_resources_json
                      AND physical.storage_schema = OLD.stage_schema
                      AND physical.storage_relation = OLD.stage_relation
                      AND physical.storage_relation_oid =
                          OLD.stage_relation_oid
                      AND {quoted_schema}.
                          provider_directory_projection_stage_matches_proof(
                              physical.storage_schema,
                              physical.storage_relation,
                              physical.storage_relation_oid,
                              physical.storage_trigger_oid,
                              physical_id_setting,
                              OLD.prepared_proof_json
                          )
                      AND {quoted_schema}.
                          provider_directory_projection_stage_is_attached(
                              physical.storage_relation_oid,
                              'provider_directory_physical_projection_resource',
                              physical_id_setting
                          )
                      AND EXISTS (
                          SELECT 1
                            FROM {quoted_schema}.
                                 provider_directory_physical_projection_source_summary
                                 AS source_summary
                           WHERE source_summary.physical_projection_id =
                                 physical_id_setting
                             AND source_summary.proof_json =
                                 OLD.prepared_proof_json -> 'source_summary'
                      )
                      AND (
                          SELECT count(*)
                            FROM {quoted_schema}.
                                 provider_directory_physical_projection_partition
                           WHERE physical_projection_id = physical_id_setting
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
               AND physical_id_setting = OLD.physical_projection_id
               AND COALESCE(recipe_lease_setting, '') = ''
               AND NEW.physical_projection_id IS NULL
               AND NEW.stage_schema IS NULL
               AND NEW.stage_relation IS NULL
               AND NEW.stage_relation_oid IS NULL
               AND NEW.lease_token IS NULL
               AND NEW.lease_expires_at IS NULL
               AND EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_physical_projection
                     WHERE physical_projection_id = OLD.physical_projection_id
                       AND status = 'retiring'
                       AND retain_until <= clock_timestamp()
               )
               AND to_jsonb(NEW) - ARRAY[
                   'status', 'physical_projection_id',
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'updated_at'
               ] = to_jsonb(OLD) - ARRAY[
                   'status', 'physical_projection_id',
                   'stage_schema', 'stage_relation', 'stage_relation_oid',
                   'updated_at'
               ] THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION
                'provider_directory_projection_recipe_transition_invalid'
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
            "provider_directory_projection_shard_admission_guard",
            "provider_directory_projection_proof_shard",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_shard_admission",
        ),
        (
            "provider_directory_projection_admission_block_guard",
            "provider_directory_projection_admission_input_block",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_admission_block",
        ),
        (
            "provider_directory_projection_admission_stream_guard",
            "provider_directory_projection_admission_stream",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_admission_stream",
        ),
        (
            "provider_directory_projection_admission_guard",
            "provider_directory_projection_admission",
            "BEFORE INSERT OR UPDATE OR DELETE",
            "guard_provider_directory_projection_admission",
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
                SELECT 1 FROM
                    {quoted_schema}.provider_directory_projection_admission
            ) OR EXISTS (
                SELECT 1
                  FROM pg_inherits AS inheritance_record
                 WHERE inheritance_record.inhparent =
                       '{schema}.provider_directory_physical_projection_resource'
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
            "provider_directory_projection_shard_admission_guard",
            "provider_directory_projection_proof_shard",
        ),
        (
            "provider_directory_projection_admission_block_guard",
            "provider_directory_projection_admission_input_block",
        ),
        (
            "provider_directory_projection_admission_stream_guard",
            "provider_directory_projection_admission_stream",
        ),
        (
            "provider_directory_projection_admission_guard",
            "provider_directory_projection_admission",
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
        "provider_directory_physical_projection_resource",
        "provider_directory_projection_proof_shard",
        "provider_directory_projection_admission_stream",
        "provider_directory_projection_admission_input_block",
        "provider_directory_projection_admission",
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
        "guard_provider_directory_projection_shard_admission",
        "guard_provider_directory_projection_admission_block",
        "guard_provider_directory_projection_admission_stream",
        "guard_provider_directory_projection_admission",
        "guard_provider_directory_projection_input_block",
        "reject_provider_directory_projection_stage_mutation",
        "reject_provider_directory_projection_resource_mutation",
    ):
        op.execute(f"DROP FUNCTION {quoted_schema}.{function_name}()")
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_scope_is_exact("
        "text)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_stream_is_exact("
        "text, text, text, text, text, bigint, text, text, integer, text, "
        "integer, integer, text)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_mapping_is_exact("
        "text, text, text, text, text, text, text, text, text, integer, text, "
        "integer, bigint, text, text, integer)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_stream_set_sha256("
        "text)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_binding_set_sha256("
        "text)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_terminal_binding_id("
        "text, text, text, text, integer, text, integer, integer, text)"
    )
    op.execute(
        "DROP FUNCTION "
        f"{quoted_schema}.provider_directory_projection_admission_input_binding_id("
        "text, text, text, text, text, integer, text, integer, text, text, "
        "integer)"
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
        f"{quoted_schema}.provider_directory_projection_stage_is_attached("
        "bigint, text, text)"
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
