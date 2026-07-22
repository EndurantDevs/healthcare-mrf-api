"""Add retained child read leases for Provider Directory projection shards.

Revision ID: 20260722130000_provider_directory_projection_child_read_lease
Revises: 20260721170000_provider_directory_physical_projection
"""

from __future__ import annotations

import os

from alembic import op
import sqlalchemy as sa

from db.migration_adoption import create_table_or_validate
from db.migration_index_adoption import create_index_if_missing


revision = "20260722130000_provider_directory_projection_child_read_lease"
down_revision = "20260721170000_provider_directory_physical_projection"
branch_labels = None
depends_on = None


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _timestamp(name: str, *, nullable: bool = False) -> sa.Column:
    return sa.Column(
        name,
        sa.TIMESTAMP(timezone=True),
        nullable=nullable,
        server_default=None if nullable else sa.func.now(),
    )


def _quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _create_child_table(schema: str) -> None:
    create_table_or_validate(
        op,
        "provider_directory_projection_child_read_lease",
        sa.Column("recipe_id", sa.String(length=64), nullable=False),
        sa.Column("recipe_attempt", sa.Integer(), nullable=False),
        sa.Column("admission_id", sa.String(length=64), nullable=False),
        sa.Column("partition_id", sa.String(length=64), nullable=False),
        sa.Column("partition_attempt", sa.Integer(), nullable=False),
        sa.Column("recipe_lease_token", sa.String(length=64), nullable=False),
        sa.Column("shard_lease_token", sa.String(length=64), nullable=False),
        sa.Column("binding_id", sa.String(length=64), nullable=False),
        sa.Column("block_id", sa.String(length=64), nullable=False),
        sa.Column("retained_campaign_id", sa.String(length=64), nullable=False),
        sa.Column(
            "retained_campaign_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "retained_consumer_recipe_id",
            sa.String(length=128),
            nullable=False,
        ),
        sa.Column("retained_claim_generation", sa.BigInteger(), nullable=False),
        sa.Column(
            "retained_source_item_id",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "retained_artifact_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column(
            "retained_layout_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("retained_range_ordinal", sa.Integer(), nullable=True),
        sa.Column("artifact_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("raw_byte_start", sa.BigInteger(), nullable=False),
        sa.Column("expected_byte_count", sa.BigInteger(), nullable=False),
        sa.Column("expected_record_count", sa.BigInteger(), nullable=False),
        sa.Column("input_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "expected_payload_sha256",
            sa.String(length=64),
            nullable=False,
        ),
        sa.Column("child_generation", sa.BigInteger(), nullable=False),
        sa.Column("child_lease_token", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        _timestamp("lease_expires_at"),
        _timestamp("lease_heartbeat_at"),
        sa.Column("verified_byte_count", sa.BigInteger(), nullable=True),
        sa.Column("verified_record_count", sa.BigInteger(), nullable=True),
        sa.Column("verified_input_sha256", sa.String(length=64), nullable=True),
        sa.Column(
            "verified_payload_sha256",
            sa.String(length=64),
            nullable=True,
        ),
        _timestamp("created_at"),
        _timestamp("updated_at"),
        _timestamp("verified_at", nullable=True),
        _timestamp("released_at", nullable=True),
        sa.CheckConstraint(
            "recipe_id ~ '^[0-9a-f]{64}$' "
            "AND admission_id ~ '^[0-9a-f]{64}$' "
            "AND partition_id ~ '^[0-9a-f]{64}$' "
            "AND recipe_lease_token ~ '^[0-9a-f]{64}$' "
            "AND shard_lease_token ~ '^[0-9a-f]{64}$' "
            "AND binding_id ~ '^[0-9a-f]{64}$' "
            "AND block_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_id ~ '^[0-9a-f]{64}$' "
            "AND retained_campaign_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_source_item_id ~ '^[0-9a-f]{64}$' "
            "AND retained_artifact_sha256 ~ '^[0-9a-f]{64}$' "
            "AND retained_layout_sha256 ~ '^[0-9a-f]{64}$' "
            "AND input_sha256 ~ '^[0-9a-f]{64}$' "
            "AND expected_payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND child_lease_token ~ '^[0-9a-f]{64}$' "
            "AND (verified_payload_sha256 IS NULL OR "
            "verified_payload_sha256 ~ '^[0-9a-f]{64}$') "
            "AND (verified_input_sha256 IS NULL OR "
            "verified_input_sha256 ~ '^[0-9a-f]{64}$')",
            name="pd_projection_child_read_digest_check",
        ),
        sa.CheckConstraint(
            "recipe_attempt > 0 AND partition_attempt > 0 "
            "AND retained_consumer_recipe_id <> '' "
            "AND retained_claim_generation > 0 AND child_generation > 0 "
            "AND artifact_byte_count > 0 AND raw_byte_start >= 0 "
            "AND expected_byte_count > 0 AND expected_record_count > 0 "
            "AND raw_byte_start + expected_byte_count <= artifact_byte_count "
            "AND (retained_range_ordinal IS NOT NULL "
            "OR (raw_byte_start = 0 "
            "AND expected_byte_count = artifact_byte_count)) "
            "AND (retained_range_ordinal IS NULL "
            "OR retained_range_ordinal >= 0)",
            name="pd_projection_child_read_values_check",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'verified', 'released') "
            "AND lease_expires_at >= lease_heartbeat_at "
            "AND ((status = 'active' "
            "AND verified_byte_count IS NULL "
            "AND verified_record_count IS NULL "
            "AND verified_input_sha256 IS NULL "
            "AND verified_payload_sha256 IS NULL "
            "AND verified_at IS NULL AND released_at IS NULL) "
            "OR (status = 'verified' "
            "AND verified_byte_count IS NOT NULL "
            "AND verified_record_count IS NOT NULL "
            "AND verified_input_sha256 IS NOT NULL "
            "AND verified_payload_sha256 IS NOT NULL "
            "AND verified_byte_count = expected_byte_count "
            "AND verified_record_count = expected_record_count "
            "AND verified_input_sha256 = input_sha256 "
            "AND verified_payload_sha256 = expected_payload_sha256 "
            "AND verified_at IS NOT NULL AND released_at IS NULL) "
            "OR (status = 'released' AND released_at IS NOT NULL "
            "AND ((verified_byte_count IS NULL "
            "AND verified_record_count IS NULL "
            "AND verified_input_sha256 IS NULL "
            "AND verified_payload_sha256 IS NULL "
            "AND verified_at IS NULL) "
            "OR (verified_byte_count = expected_byte_count "
            "AND verified_byte_count IS NOT NULL "
            "AND verified_record_count IS NOT NULL "
            "AND verified_input_sha256 IS NOT NULL "
            "AND verified_payload_sha256 IS NOT NULL "
            "AND verified_record_count = expected_record_count "
            "AND verified_input_sha256 = input_sha256 "
            "AND verified_payload_sha256 = expected_payload_sha256 "
            "AND verified_at IS NOT NULL))))",
            name="pd_projection_child_read_state_check",
        ),
        sa.ForeignKeyConstraint(
            ["recipe_id", "recipe_attempt", "partition_id"],
            [
                f"{schema}.provider_directory_projection_proof_shard.recipe_id",
                f"{schema}.provider_directory_projection_proof_shard.attempt",
                f"{schema}.provider_directory_projection_proof_shard.partition_id",
            ],
            name="pd_projection_child_read_shard_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["admission_id", "recipe_id"],
            [
                f"{schema}.provider_directory_projection_admission.admission_id",
                f"{schema}.provider_directory_projection_admission.recipe_id",
            ],
            name="pd_projection_child_read_admission_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["admission_id", "binding_id"],
            [
                f"{schema}.provider_directory_projection_admission_input_block.admission_id",
                f"{schema}.provider_directory_projection_admission_input_block.binding_id",
            ],
            name="pd_projection_child_read_binding_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_campaign_id", "retained_consumer_recipe_id"],
            [
                f"{schema}.provider_directory_retained_artifact_consumer.campaign_id",
                f"{schema}.provider_directory_retained_artifact_consumer.consumer_recipe_id",
            ],
            name="pd_projection_child_read_consumer_fkey",
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
            name="pd_projection_child_read_reference_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_artifact_sha256"],
            [f"{schema}.provider_directory_retained_artifact.artifact_sha256"],
            name="pd_projection_child_read_artifact_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_layout_sha256", "retained_artifact_sha256"],
            [
                f"{schema}.provider_directory_retained_artifact_layout.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_layout.artifact_sha256",
            ],
            name="pd_projection_child_read_layout_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["retained_layout_sha256", "retained_range_ordinal"],
            [
                f"{schema}.provider_directory_retained_artifact_range.layout_sha256",
                f"{schema}.provider_directory_retained_artifact_range.range_ordinal",
            ],
            name="pd_projection_child_read_range_fkey",
        ),
        sa.PrimaryKeyConstraint(
            "recipe_id",
            "recipe_attempt",
            "partition_id",
            name="provider_directory_projection_child_read_lease_pkey",
        ),
        sa.UniqueConstraint(
            "child_lease_token",
            name="pd_projection_child_read_token_key",
        ),
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_child_read_lease_idx",
        "provider_directory_projection_child_read_lease",
        ["status", "lease_expires_at"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_admission_parent_release_idx",
        "provider_directory_projection_admission",
        [
            "retained_campaign_id",
            "retained_consumer_recipe_id",
            "status",
            "recipe_id",
            "admission_id",
        ],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_admission_block_lookup_idx",
        "provider_directory_projection_admission_input_block",
        ["admission_id", "recipe_id", "block_id", "binding_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_child_read_admission_idx",
        "provider_directory_projection_child_read_lease",
        ["admission_id", "status", "partition_id"],
        schema=schema,
    )
    create_index_if_missing(
        op,
        "pd_projection_child_read_parent_idx",
        "provider_directory_projection_child_read_lease",
        [
            "retained_campaign_id",
            "retained_consumer_recipe_id",
            "status",
        ],
        schema=schema,
    )


def _create_child_guard(quoted_schema: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION
            {quoted_schema}.guard_provider_directory_projection_child_read_lease()
            RETURNS trigger LANGUAGE plpgsql AS $$
            DECLARE
                action_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_action', true
                );
                recipe_id_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_recipe_id', true
                );
                recipe_attempt_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_recipe_attempt',
                    true
                );
                recipe_lease_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_recipe_lease_token',
                    true
                );
                partition_id_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_partition_id',
                    true
                );
                partition_attempt_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_partition_attempt',
                    true
                );
                shard_lease_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_shard_lease_token',
                    true
                );
                generation_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_generation', true
                );
                child_lease_setting text := current_setting(
                    'healthporta.provider_directory_projection_child_lease_token', true
                );
                dependency_exact boolean := false;
                completed_shard boolean := false;
            BEGIN
                IF TG_OP = 'DELETE' THEN
                    IF action_setting = 'gc'
                       AND OLD.status = 'released'
                       AND EXISTS (
                            SELECT 1
                              FROM {quoted_schema}.
                                   provider_directory_projection_recipe AS recipe
                             WHERE recipe.recipe_id = OLD.recipe_id
                               AND recipe.status = 'retired'
                       ) THEN
                        RETURN OLD;
                    END IF;
                    RAISE EXCEPTION
                        'provider_directory_projection_child_read_immutable'
                        USING ERRCODE = '55000';
                END IF;

                SELECT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.provider_directory_projection_recipe
                           AS recipe
                      JOIN {quoted_schema}.
                           provider_directory_projection_admission AS admission
                        ON admission.admission_id = NEW.admission_id
                       AND admission.recipe_id = recipe.recipe_id
                      JOIN {quoted_schema}.
                           provider_directory_projection_proof_shard AS shard
                        ON shard.recipe_id = recipe.recipe_id
                       AND shard.attempt = NEW.recipe_attempt
                       AND shard.partition_id = NEW.partition_id
                      JOIN {quoted_schema}.
                           provider_directory_projection_admission_input_block
                           AS binding
                        ON binding.admission_id = admission.admission_id
                       AND binding.binding_id = NEW.binding_id
                      JOIN {quoted_schema}.
                           provider_directory_projection_input_block AS block
                        ON block.recipe_id = recipe.recipe_id
                       AND block.block_id = binding.block_id
                      JOIN {quoted_schema}.
                           provider_directory_retained_artifact_campaign AS campaign
                        ON campaign.campaign_id = binding.retained_campaign_id
                      JOIN {quoted_schema}.
                           provider_directory_retained_artifact_consumer AS consumer
                        ON consumer.campaign_id = campaign.campaign_id
                       AND consumer.consumer_recipe_id =
                           binding.retained_consumer_recipe_id
                      JOIN {quoted_schema}.
                           provider_directory_retained_artifact_consumer_reference
                           AS retained_reference
                        ON retained_reference.campaign_id = consumer.campaign_id
                       AND retained_reference.consumer_recipe_id =
                           consumer.consumer_recipe_id
                       AND retained_reference.source_item_id =
                           binding.retained_source_item_id
                      JOIN {quoted_schema}.provider_directory_retained_artifact
                           AS artifact
                        ON artifact.artifact_sha256 =
                           retained_reference.artifact_sha256
                      JOIN {quoted_schema}.
                           provider_directory_retained_artifact_layout AS layout
                        ON layout.layout_sha256 =
                           retained_reference.layout_sha256
                       AND layout.artifact_sha256 =
                           retained_reference.artifact_sha256
                      LEFT JOIN {quoted_schema}.
                           provider_directory_retained_artifact_range
                           AS retained_range
                        ON retained_range.layout_sha256 = layout.layout_sha256
                       AND retained_range.range_ordinal =
                           binding.retained_range_ordinal
                     WHERE recipe.recipe_id = NEW.recipe_id
                       AND recipe.attempt = NEW.recipe_attempt
                       AND recipe.status = 'building'
                       AND recipe.lease_token = NEW.recipe_lease_token
                       AND recipe.lease_expires_at > clock_timestamp()
                       AND admission.status = 'sealed'
                       AND shard.status = 'building'
                       AND shard.partition_attempt = NEW.partition_attempt
                       AND shard.recipe_lease_token = NEW.recipe_lease_token
                       AND shard.lease_token = NEW.shard_lease_token
                       AND shard.lease_expires_at > clock_timestamp()
                       AND shard.input_block_id = NEW.block_id
                       AND shard.input_sha256 = NEW.input_sha256
                       AND binding.block_id = NEW.block_id
                       AND binding.retained_campaign_id =
                           NEW.retained_campaign_id
                       AND admission.retained_campaign_sha256 =
                           NEW.retained_campaign_sha256
                       AND binding.retained_consumer_recipe_id =
                           NEW.retained_consumer_recipe_id
                       AND binding.claim_generation =
                           NEW.retained_claim_generation
                       AND binding.retained_source_item_id =
                           NEW.retained_source_item_id
                       AND binding.retained_artifact_sha256 =
                           NEW.retained_artifact_sha256
                       AND binding.retained_layout_sha256 =
                           NEW.retained_layout_sha256
                       AND binding.retained_range_ordinal IS NOT DISTINCT FROM
                           NEW.retained_range_ordinal
                       AND campaign.state = 'complete' AND campaign.complete
                       AND campaign.campaign_sha256 =
                           NEW.retained_campaign_sha256
                       AND campaign.released_at IS NULL
                       AND consumer.claimed_campaign_sha256 =
                           NEW.retained_campaign_sha256
                       AND consumer.claim_generation =
                           NEW.retained_claim_generation
                       AND consumer.released_at IS NULL
                       AND retained_reference.claim_generation =
                           NEW.retained_claim_generation
                       AND retained_reference.released_at IS NULL
                       AND artifact.registry_status = 'verified'
                       AND artifact.released_at IS NULL
                       AND layout.registry_status = 'verified'
                       AND layout.released_at IS NULL
                       AND artifact.artifact_byte_count =
                           NEW.artifact_byte_count
                       AND block.record_count = NEW.expected_record_count
                       AND block.payload_bytes = NEW.expected_byte_count
                       AND block.content_sha256 = NEW.input_sha256
                       AND block.payload_sha256 =
                           NEW.expected_payload_sha256
                       AND NEW.raw_byte_start = CASE
                           WHEN binding.retained_range_ordinal IS NULL THEN 0
                           ELSE retained_range.raw_byte_start
                       END
                       AND NEW.expected_byte_count = CASE
                           WHEN binding.retained_range_ordinal IS NULL
                           THEN artifact.artifact_byte_count
                           ELSE retained_range.raw_byte_count
                       END
                       AND NEW.expected_record_count = CASE
                           WHEN binding.retained_range_ordinal IS NULL
                           THEN layout.artifact_record_count
                           ELSE retained_range.record_count
                       END
                       AND NEW.expected_payload_sha256 = CASE
                           WHEN binding.retained_range_ordinal IS NULL
                           THEN artifact.artifact_sha256
                           ELSE retained_range.raw_sha256
                       END
                       AND {quoted_schema}.
                           provider_directory_projection_admission_mapping_is_exact(
                               binding.admission_id,
                               binding.recipe_id,
                               binding.binding_id,
                               binding.block_id,
                               binding.retained_campaign_id,
                               binding.retained_consumer_recipe_id,
                               binding.retained_source_item_id,
                               binding.retained_artifact_sha256,
                               binding.retained_layout_sha256,
                               binding.retained_range_ordinal,
                               binding.stream_identity_sha256,
                               binding.sequence_ordinal,
                               binding.claim_generation,
                               binding.resource_type,
                               binding.partition_key_hash,
                               binding.source_partition_ordinal
                           )
                ) INTO dependency_exact;

                SELECT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_projection_proof_shard AS shard
                     WHERE shard.recipe_id = NEW.recipe_id
                       AND shard.attempt = NEW.recipe_attempt
                       AND shard.partition_id = NEW.partition_id
                       AND shard.partition_attempt = NEW.partition_attempt
                       AND shard.status = 'complete'
                ) INTO completed_shard;

                IF recipe_id_setting IS DISTINCT FROM NEW.recipe_id
                   OR recipe_attempt_setting IS DISTINCT FROM
                      NEW.recipe_attempt::text
                   OR recipe_lease_setting IS DISTINCT FROM
                      NEW.recipe_lease_token
                   OR partition_id_setting IS DISTINCT FROM NEW.partition_id
                   OR partition_attempt_setting IS DISTINCT FROM
                      NEW.partition_attempt::text
                   OR shard_lease_setting IS DISTINCT FROM
                      NEW.shard_lease_token
                   OR generation_setting IS DISTINCT FROM
                      NEW.child_generation::text
                   OR child_lease_setting IS DISTINCT FROM
                      NEW.child_lease_token THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_child_read_unfenced'
                        USING ERRCODE = '55000';
                END IF;

                IF TG_OP = 'INSERT' THEN
                    IF action_setting = 'claim'
                       AND NEW.child_generation = 1
                       AND NEW.status = 'active'
                       AND dependency_exact THEN
                        RETURN NEW;
                    END IF;
                    RAISE EXCEPTION
                        'provider_directory_projection_child_read_insert_invalid'
                        USING ERRCODE = '55000';
                END IF;

                IF OLD.recipe_id IS DISTINCT FROM NEW.recipe_id
                   OR OLD.recipe_attempt IS DISTINCT FROM NEW.recipe_attempt
                   OR OLD.partition_id IS DISTINCT FROM NEW.partition_id
                   OR OLD.created_at IS DISTINCT FROM NEW.created_at THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_child_read_identity_immutable'
                        USING ERRCODE = '55000';
                END IF;

                IF action_setting = 'claim'
                   AND NEW.child_generation = OLD.child_generation + 1
                   AND NEW.child_lease_token <> OLD.child_lease_token
                   AND NEW.status = 'active'
                   AND dependency_exact
                   AND (
                       OLD.status = 'released'
                       OR OLD.lease_expires_at <= clock_timestamp()
                       OR OLD.partition_attempt <> NEW.partition_attempt
                       OR OLD.recipe_lease_token <> NEW.recipe_lease_token
                       OR OLD.shard_lease_token <> NEW.shard_lease_token
                   ) THEN
                    RETURN NEW;
                END IF;

                IF action_setting = 'heartbeat'
                   AND OLD.status = 'active' AND NEW.status = 'active'
                   AND OLD.lease_expires_at > clock_timestamp()
                   AND NEW.lease_expires_at > OLD.lease_expires_at
                   AND dependency_exact
                   AND to_jsonb(NEW) - ARRAY[
                       'lease_expires_at', 'lease_heartbeat_at', 'updated_at'
                   ] = to_jsonb(OLD) - ARRAY[
                       'lease_expires_at', 'lease_heartbeat_at', 'updated_at'
                   ] THEN
                    RETURN NEW;
                END IF;

                IF action_setting = 'verify'
                   AND OLD.status = 'active' AND NEW.status = 'verified'
                   AND OLD.lease_expires_at > clock_timestamp()
                   AND dependency_exact
                   AND NEW.verified_byte_count = NEW.expected_byte_count
                   AND NEW.verified_record_count = NEW.expected_record_count
                   AND NEW.verified_input_sha256 = NEW.input_sha256
                   AND NEW.verified_payload_sha256 =
                       NEW.expected_payload_sha256
                   AND NEW.verified_at IS NOT NULL
                   AND to_jsonb(NEW) - ARRAY[
                       'status', 'verified_byte_count',
                       'verified_record_count', 'verified_input_sha256',
                       'verified_payload_sha256',
                       'verified_at', 'updated_at'
                   ] = to_jsonb(OLD) - ARRAY[
                       'status', 'verified_byte_count',
                       'verified_record_count', 'verified_input_sha256',
                       'verified_payload_sha256',
                       'verified_at', 'updated_at'
                   ] THEN
                    RETURN NEW;
                END IF;

                IF action_setting = 'release'
                   AND OLD.status IN ('active', 'verified')
                   AND NEW.status = 'released'
                   AND NEW.released_at IS NOT NULL
                   AND (dependency_exact OR completed_shard)
                   AND to_jsonb(NEW) - ARRAY[
                       'status', 'lease_expires_at', 'lease_heartbeat_at',
                       'updated_at', 'released_at'
                   ] = to_jsonb(OLD) - ARRAY[
                       'status', 'lease_expires_at', 'lease_heartbeat_at',
                       'updated_at', 'released_at'
                   ] THEN
                    RETURN NEW;
                END IF;

                RAISE EXCEPTION
                    'provider_directory_projection_child_read_transition_invalid'
                    USING ERRCODE = '55000';
            END;
            $$;
            """
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_projection_child_read_guard "
            f"ON {quoted_schema}.provider_directory_projection_child_read_lease;"
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER pd_projection_child_read_guard
            BEFORE INSERT OR UPDATE OR DELETE
            ON {quoted_schema}.provider_directory_projection_child_read_lease
            FOR EACH ROW EXECUTE FUNCTION
            {quoted_schema}.guard_provider_directory_projection_child_read_lease();
            """
        )
    )


def _create_completion_guard(quoted_schema: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION
            {quoted_schema}.require_verified_projection_child_read()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD.status = 'building' AND NEW.status = 'complete'
                   AND NOT EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_child_read_lease
                               AS child
                         WHERE child.recipe_id = OLD.recipe_id
                           AND child.recipe_attempt = OLD.attempt
                           AND child.partition_id = OLD.partition_id
                           AND child.partition_attempt = OLD.partition_attempt
                           AND child.recipe_lease_token =
                               OLD.recipe_lease_token
                           AND child.shard_lease_token = OLD.lease_token
                           AND child.status = 'verified'
                           AND child.released_at IS NULL
                           AND child.verified_byte_count =
                               child.expected_byte_count
                           AND child.verified_record_count =
                               child.expected_record_count
                           AND child.verified_input_sha256 = child.input_sha256
                           AND child.verified_payload_sha256 =
                               child.expected_payload_sha256
                   ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_child_read_verification_required'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END;
            $$;
            """
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_projection_shard_child_verified "
            f"ON {quoted_schema}.provider_directory_projection_proof_shard;"
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER pd_projection_shard_child_verified
            BEFORE UPDATE OF status
            ON {quoted_schema}.provider_directory_projection_proof_shard
            FOR EACH ROW EXECUTE FUNCTION
            {quoted_schema}.require_verified_projection_child_read();
            """
        )
    )


def _create_owner_terminal_guard(quoted_schema: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION
            {quoted_schema}.guard_projection_child_owner_terminal()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF TG_TABLE_NAME = 'provider_directory_projection_recipe'
                   AND NEW.status IN ('failed', 'retired')
                   AND OLD.status IS DISTINCT FROM NEW.status
                   AND EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_child_read_lease
                         WHERE recipe_id = OLD.recipe_id
                           AND status IN ('active', 'verified')
                   ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_child_owner_live'
                        USING ERRCODE = '55000';
                END IF;
                IF TG_TABLE_NAME = 'provider_directory_projection_admission'
                   AND OLD.status = 'sealed' AND NEW.status = 'released'
                   AND EXISTS (
                        SELECT 1
                          FROM {quoted_schema}.
                               provider_directory_projection_child_read_lease
                         WHERE admission_id = OLD.admission_id
                           AND status IN ('active', 'verified')
                   ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_child_owner_live'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END;
            $$;
            """
        )
    )
    for table_name, trigger_name in (
        (
            "provider_directory_projection_recipe",
            "pd_projection_recipe_child_owner_guard",
        ),
        (
            "provider_directory_projection_admission",
            "pd_projection_admission_child_owner_guard",
        ),
    ):
        op.execute(
            sa.text(
                f"DROP TRIGGER IF EXISTS {trigger_name} "
                f"ON {quoted_schema}.{table_name};"
            )
        )
        op.execute(
            sa.text(
                f"""
                CREATE TRIGGER {trigger_name}
                BEFORE UPDATE OF status ON {quoted_schema}.{table_name}
                FOR EACH ROW EXECUTE FUNCTION
                {quoted_schema}.guard_projection_child_owner_terminal();
                """
            )
        )


def _create_parent_release_guard(quoted_schema: str) -> None:
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION
            {quoted_schema}.projection_retained_parent_is_live(
                candidate_campaign_id text,
                candidate_consumer_recipe_id text
            ) RETURNS boolean LANGUAGE sql STABLE AS $$
                SELECT EXISTS (
                    SELECT 1
                      FROM {quoted_schema}.
                           provider_directory_projection_admission AS admission
                      LEFT JOIN {quoted_schema}.
                           provider_directory_projection_recipe AS recipe
                        ON recipe.recipe_id = admission.recipe_id
                     WHERE admission.retained_campaign_id =
                           candidate_campaign_id
                       AND admission.retained_consumer_recipe_id =
                           candidate_consumer_recipe_id
                       AND (
                           admission.status <> 'released'
                           OR recipe.status IN ('building', 'proof_ready')
                           OR EXISTS (
                               SELECT 1
                                 FROM {quoted_schema}.
                                      provider_directory_projection_child_read_lease
                                      AS child
                                WHERE child.admission_id =
                                      admission.admission_id
                                  AND child.status IN ('active', 'verified')
                           )
                       )
                );
            $$;
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION
            {quoted_schema}.guard_projection_retained_parent_release()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF OLD.released_at IS NOT NULL
                   OR NEW.released_at IS NULL THEN
                    RETURN NEW;
                END IF;
                IF {quoted_schema}.projection_retained_parent_is_live(
                       OLD.campaign_id,
                       OLD.consumer_recipe_id
                   ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_retained_parent_live'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END;
            $$;
            """
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE OR REPLACE FUNCTION
            {quoted_schema}.guard_projection_retained_reference_release()
            RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                      FROM (
                          SELECT DISTINCT old_reference.campaign_id,
                                 old_reference.consumer_recipe_id
                            FROM old_references AS old_reference
                            JOIN new_references AS new_reference
                              ON new_reference.campaign_id =
                                 old_reference.campaign_id
                             AND new_reference.consumer_recipe_id =
                                 old_reference.consumer_recipe_id
                             AND new_reference.source_item_id =
                                 old_reference.source_item_id
                           WHERE old_reference.released_at IS NULL
                             AND new_reference.released_at IS NOT NULL
                      ) AS released_parent
                     WHERE {quoted_schema}.projection_retained_parent_is_live(
                               released_parent.campaign_id,
                               released_parent.consumer_recipe_id
                           )
                ) THEN
                    RAISE EXCEPTION
                        'provider_directory_projection_retained_parent_live'
                        USING ERRCODE = '55000';
                END IF;
                RETURN NULL;
            END;
            $$;
            """
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_retained_consumer_projection_guard "
            f"ON {quoted_schema}.provider_directory_retained_artifact_consumer;"
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER pd_retained_consumer_projection_guard
            BEFORE UPDATE OF released_at
            ON {quoted_schema}.provider_directory_retained_artifact_consumer
            FOR EACH ROW EXECUTE FUNCTION
            {quoted_schema}.guard_projection_retained_parent_release();
            """
        )
    )
    op.execute(
        sa.text(
            "DROP TRIGGER IF EXISTS pd_retained_reference_projection_guard "
            f"ON {quoted_schema}."
            "provider_directory_retained_artifact_consumer_reference;"
        )
    )
    op.execute(
        sa.text(
            f"""
            CREATE TRIGGER pd_retained_reference_projection_guard
            AFTER UPDATE
            ON {quoted_schema}.
               provider_directory_retained_artifact_consumer_reference
            REFERENCING OLD TABLE AS old_references
                        NEW TABLE AS new_references
            FOR EACH STATEMENT EXECUTE FUNCTION
            {quoted_schema}.guard_projection_retained_reference_release();
            """
        )
    )


def upgrade() -> None:
    schema = _schema()
    quoted_schema = _quoted_identifier(schema)
    _create_child_table(schema)
    _create_child_guard(quoted_schema)
    _create_completion_guard(quoted_schema)
    _create_owner_terminal_guard(quoted_schema)
    _create_parent_release_guard(quoted_schema)


def downgrade() -> None:
    schema = _schema()
    quoted_schema = _quoted_identifier(schema)
    connection = op.get_bind()
    child_count = connection.execute(
        sa.text(
            f"SELECT count(*) FROM {quoted_schema}."
            "provider_directory_projection_child_read_lease;"
        )
    ).scalar_one()
    if child_count:
        raise RuntimeError(
            "provider_directory_projection_child_read_downgrade_refused"
        )
    for table_name, trigger_name in (
        (
            "provider_directory_retained_artifact_consumer",
            "pd_retained_consumer_projection_guard",
        ),
        (
            "provider_directory_retained_artifact_consumer_reference",
            "pd_retained_reference_projection_guard",
        ),
        (
            "provider_directory_projection_proof_shard",
            "pd_projection_shard_child_verified",
        ),
        (
            "provider_directory_projection_recipe",
            "pd_projection_recipe_child_owner_guard",
        ),
        (
            "provider_directory_projection_admission",
            "pd_projection_admission_child_owner_guard",
        ),
        (
            "provider_directory_projection_child_read_lease",
            "pd_projection_child_read_guard",
        ),
    ):
        op.execute(
            sa.text(
                f"DROP TRIGGER IF EXISTS {trigger_name} "
                f"ON {quoted_schema}.{table_name};"
            )
        )
    for function_name in (
        "guard_projection_retained_reference_release",
        "guard_projection_retained_parent_release",
        "guard_projection_child_owner_terminal",
        "require_verified_projection_child_read",
        "guard_provider_directory_projection_child_read_lease",
    ):
        op.execute(
            sa.text(
                f"DROP FUNCTION IF EXISTS {quoted_schema}.{function_name}();"
            )
        )
    op.execute(
        sa.text(
            "DROP FUNCTION IF EXISTS "
            f"{quoted_schema}.projection_retained_parent_is_live(text, text);"
        )
    )
    for index_name, table_name in (
        (
            "pd_projection_admission_parent_release_idx",
            "provider_directory_projection_admission",
        ),
        (
            "pd_projection_admission_block_lookup_idx",
            "provider_directory_projection_admission_input_block",
        ),
    ):
        op.drop_index(
            index_name,
            table_name=table_name,
            schema=schema,
            if_exists=True,
        )
    op.drop_table(
        "provider_directory_projection_child_read_lease",
        schema=schema,
    )
