"""Add durable, signed exact-wave admission records.

The migration deliberately creates no trigger that could publish work.  Only a
controller introduced in a later change may advance a nonterminal wave.
"""

from __future__ import annotations

import os

import sqlalchemy as sa
from alembic import op


revision = "20260806110000_ptg_import_wave_contract"
down_revision = "20260806100000_ptg2_tax_identity_source"
branch_labels = None
depends_on = None


def _schema() -> str:
    runtime, legacy = os.getenv("HLTHPRT_DB_SCHEMA"), os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    return runtime or legacy or "mrf"


def upgrade() -> None:
    schema = _schema()
    op.create_table(
        "ptg_import_wave",
        sa.Column("wave_id", sa.String(64), primary_key=True),
        sa.Column("idempotency_key", sa.String(160), nullable=False),
        sa.Column("request_digest", sa.String(64), nullable=False),
        sa.Column("cohort_attestation", sa.JSON(), nullable=False),
        sa.Column("cohort_attestation_digest", sa.String(64), nullable=False),
        sa.Column("cohort_signature_digest", sa.String(64), nullable=False),
        sa.Column("physical_coordinate_count", sa.Integer(), nullable=False),
        sa.Column("physical_coordinate_digest", sa.String(64), nullable=False),
        sa.Column("imported_coordinate_count", sa.Integer(), nullable=False),
        sa.Column("imported_coordinate_digest", sa.String(64), nullable=False),
        sa.Column("reused_coordinate_count", sa.Integer(), nullable=False),
        sa.Column("reused_coordinate_digest", sa.String(64), nullable=False),
        sa.Column("partition_digest", sa.String(64), nullable=False),
        sa.Column("intent_count", sa.Integer(), nullable=False),
        sa.Column("jobs_digest", sa.String(64), nullable=False),
        sa.Column("manifest_digest", sa.String(64), nullable=False),
        sa.Column("wave_digest", sa.String(64), nullable=False),
        sa.Column("queue", sa.String(64), nullable=False),
        sa.Column("release_queue", sa.String(160), nullable=False),
        sa.Column("worker_class", sa.String(64), nullable=False),
        sa.Column("resource_class", sa.String(32), nullable=False),
        sa.Column("worker_limit", sa.Integer(), nullable=False),
        sa.Column("protocol_identity", sa.String(96), nullable=False),
        sa.Column("serializer_identity", sa.String(96), nullable=False),
        sa.Column("enqueue_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("uncertainty_resume_state", sa.String(32)),
        sa.Column("created_at", sa.TIMESTAMP(), nullable=False),
        sa.Column("kubernetes_manifest", sa.JSON()),
        sa.Column("kubernetes_manifest_bytes", sa.LargeBinary()),
        sa.Column("kubernetes_manifest_sha256", sa.String(64)),
        sa.Column("kubernetes_manifest_identity", sa.String(64)),
        sa.Column("pinned_image_reference", sa.String(512)),
        sa.Column("pinned_image_digest", sa.String(64)),
        sa.Column("runtime_image_identity", sa.String(72)),
        sa.Column("kubernetes_config_identity", sa.String(64)),
        sa.Column("k8s_post_ticket", sa.String(128)),
        sa.Column("k8s_post_started_at", sa.TIMESTAMP()),
        sa.Column("kubernetes_job_uid", sa.String(128)),
        sa.Column("kubernetes_job_receipt", sa.JSON()),
        sa.Column("kubernetes_job_receipt_digest", sa.String(64)),
        sa.Column("kubernetes_ready_attestation", sa.JSON()),
        sa.Column("kubernetes_ready_attestation_digest", sa.String(64)),
        sa.Column("redis_release_ticket", sa.String(128)),
        sa.Column("redis_release_started_at", sa.TIMESTAMP()),
        sa.Column("redis_release_attestation", sa.JSON()),
        sa.Column("redis_release_attestation_digest", sa.String(64)),
        sa.Column("outcomes_digest", sa.String(64)),
        sa.Column("failure_receipt", sa.JSON()),
        sa.Column("failure_receipt_digest", sa.String(64)),
        sa.Column("linkage_ack", sa.JSON()),
        sa.Column("linkage_ack_digest", sa.String(64)),
        sa.Column("redis_cleanup_ticket", sa.String(128)),
        sa.Column("redis_cleanup_started_at", sa.TIMESTAMP()),
        sa.Column("redis_cleanup_evidence", sa.JSON()),
        sa.Column("redis_cleanup_evidence_digest", sa.String(64)),
        sa.Column("kubernetes_delete_ticket", sa.String(128)),
        sa.Column("kubernetes_delete_started_at", sa.TIMESTAMP()),
        sa.Column("kubernetes_delete_evidence", sa.JSON()),
        sa.Column("kubernetes_delete_evidence_digest", sa.String(64)),
        sa.Column("resolved_at", sa.TIMESTAMP()),
        sa.Column("terminal_evidence_digest", sa.String(64)),
        sa.Column("terminal_summary", sa.JSON()),
        sa.Column("cleanup_evidence_digest", sa.String(64)),
        sa.Column("cleanup_summary", sa.JSON()),
        sa.UniqueConstraint("idempotency_key", name="ptg_import_wave_idempotency_key"),
        sa.UniqueConstraint("wave_digest", name="ptg_import_wave_digest_key"),
        sa.CheckConstraint(
            "request_digest ~ '^[0-9a-f]{64}$' "
            "AND cohort_attestation_digest ~ '^[0-9a-f]{64}$' "
            "AND cohort_signature_digest ~ '^[0-9a-f]{64}$' "
            "AND physical_coordinate_digest ~ '^[0-9a-f]{64}$' "
            "AND imported_coordinate_digest ~ '^[0-9a-f]{64}$' "
            "AND reused_coordinate_digest ~ '^[0-9a-f]{64}$' "
            "AND partition_digest ~ '^[0-9a-f]{64}$' "
            "AND jobs_digest ~ '^[0-9a-f]{64}$' "
            "AND manifest_digest ~ '^[0-9a-f]{64}$' "
            "AND wave_digest ~ '^[0-9a-f]{64}$' "
            "AND physical_coordinate_count > 0 AND intent_count > 0 "
            "AND imported_coordinate_count = intent_count "
            "AND physical_coordinate_count = imported_coordinate_count + reused_coordinate_count "
            "AND worker_limit = 12 AND queue = 'arq:PTGSmall' "
            "AND release_queue = 'arq:PTGSmall:wave:' || wave_digest "
            "AND worker_class = 'process.PTGSmall' AND resource_class = 'small' "
            "AND protocol_identity = 'healthporta.ptg-small.exact-wave.v1' "
            "AND serializer_identity = 'arq-0.28.process-msgpack.v1' "
            "AND state IN ('admitted', 'materializing', 'slots_waiting', "
            "'redis_releasing', 'released', 'executing', 'awaiting_linkage', 'terminalizing', 'cleaning', "
            "'uncertain', 'succeeded', 'failed', 'canceled', 'dead_letter')",
            name="ptg_import_wave_contract_check",
        ),
        sa.CheckConstraint(
            "(state IN ('succeeded', 'failed', 'canceled', 'dead_letter') "
            "AND resolved_at IS NOT NULL "
            "AND terminal_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND cleanup_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND redis_cleanup_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND kubernetes_delete_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND linkage_ack_digest ~ '^[0-9a-f]{64}$' "
            "AND outcomes_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(terminal_summary) = 'object' "
            "AND json_typeof(cleanup_summary) = 'object' "
            "AND json_typeof(linkage_ack) = 'object') "
            "OR (state = 'cleaning' AND resolved_at IS NULL "
            "AND terminal_evidence_digest ~ '^[0-9a-f]{64}$' "
            "AND cleanup_evidence_digest IS NULL AND cleanup_summary IS NULL "
            "AND linkage_ack_digest ~ '^[0-9a-f]{64}$' "
            "AND outcomes_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(terminal_summary) = 'object' "
            "AND json_typeof(linkage_ack) = 'object') "
            "OR (state NOT IN ('succeeded', 'failed', 'canceled', 'dead_letter', 'cleaning') "
            "AND resolved_at IS NULL AND terminal_evidence_digest IS NULL "
            "AND terminal_summary IS NULL AND cleanup_evidence_digest IS NULL "
            "AND cleanup_summary IS NULL)",
            name="ptg_import_wave_terminal_evidence_check",
        ),
        sa.CheckConstraint(
            "(state = 'uncertain' AND uncertainty_resume_state IN "
            "('materializing', 'slots_waiting', 'redis_releasing', 'released', "
            "'executing', 'awaiting_linkage', 'terminalizing', 'cleaning')) "
            "OR (state <> 'uncertain' AND uncertainty_resume_state IS NULL)",
            name="ptg_import_wave_uncertainty_resume_check",
        ),
        sa.CheckConstraint(
            "(k8s_post_ticket IS NULL OR k8s_post_ticket ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (redis_release_ticket IS NULL OR redis_release_ticket ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (redis_cleanup_ticket IS NULL OR redis_cleanup_ticket ~ '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND (kubernetes_delete_ticket IS NULL OR kubernetes_delete_ticket ~ "
            "'^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') "
            "AND ((k8s_post_ticket IS NULL) = (k8s_post_started_at IS NULL)) "
            "AND ((redis_release_ticket IS NULL) = (redis_release_started_at IS NULL)) "
            "AND ((redis_cleanup_ticket IS NULL) = (redis_cleanup_started_at IS NULL)) "
            "AND ((kubernetes_delete_ticket IS NULL) = (kubernetes_delete_started_at IS NULL))",
            name="ptg_import_wave_operation_ticket_check",
        ),
        sa.CheckConstraint(
            "(failure_receipt IS NULL AND failure_receipt_digest IS NULL) "
            "OR (failure_receipt_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(failure_receipt) = 'object' "
            "AND state IN ('awaiting_linkage', 'terminalizing', 'cleaning', "
            "'succeeded', 'failed', 'canceled', 'dead_letter'))",
            name="ptg_import_wave_failure_receipt_check",
        ),
        sa.CheckConstraint(
            "((kubernetes_job_receipt IS NULL) = (kubernetes_job_receipt_digest IS NULL)) "
            "AND ((kubernetes_ready_attestation IS NULL) = (kubernetes_ready_attestation_digest IS NULL)) "
            "AND ((redis_release_attestation IS NULL) = (redis_release_attestation_digest IS NULL)) "
            "AND ((linkage_ack IS NULL) = (linkage_ack_digest IS NULL)) "
            "AND ((terminal_summary IS NULL) = (terminal_evidence_digest IS NULL)) "
            "AND ((redis_cleanup_evidence IS NULL) = (redis_cleanup_evidence_digest IS NULL)) "
            "AND ((kubernetes_delete_evidence IS NULL) = (kubernetes_delete_evidence_digest IS NULL)) "
            "AND ((cleanup_summary IS NULL) = (cleanup_evidence_digest IS NULL))",
            name="ptg_import_wave_receipt_pairs_check",
        ),
        schema=schema,
    )
    schema_sql = '"' + schema.replace('"', '""') + '"'
    op.execute(
        "CREATE UNIQUE INDEX ptg_import_wave_single_capacity_owner_idx "
        f"ON {schema_sql}.ptg_import_wave ((1)) "
        "WHERE state IN ('admitted', 'materializing', 'slots_waiting', "
        "'redis_releasing', 'released', 'executing', 'awaiting_linkage', 'terminalizing', 'cleaning', 'uncertain')"
    )
    op.create_table(
        "ptg_import_wave_intent",
        sa.Column("wave_id", sa.String(64), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("source_file_import_id", sa.String(64), nullable=False),
        sa.Column("content_version", sa.String(128), nullable=False),
        sa.Column("run_idempotency_key", sa.String(160), nullable=False),
        sa.Column("job_id", sa.String(96), nullable=False),
        sa.Column("params", sa.JSON(), nullable=False),
        sa.Column("job_payload", sa.JSON(), nullable=False),
        sa.Column("serialized_job", sa.LargeBinary(), nullable=False),
        sa.Column("serialized_job_digest", sa.String(64), nullable=False),
        sa.PrimaryKeyConstraint("wave_id", "ordinal"),
        sa.ForeignKeyConstraint(
            ("wave_id",), (f"{schema}.ptg_import_wave.wave_id",),
            name="ptg_import_wave_intent_wave_fkey", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("run_id", name="ptg_import_wave_intent_run_id_key"),
        sa.UniqueConstraint("job_id", name="ptg_import_wave_intent_job_id_key"),
        sa.UniqueConstraint(
            "wave_id", "ordinal", "run_id", "job_id",
            name="ptg_import_wave_intent_claim_identity_key",
        ),
        sa.UniqueConstraint(
            "wave_id", "source_file_import_id",
            name="ptg_import_wave_intent_source_per_wave_key",
        ),
        sa.CheckConstraint(
            "ordinal >= 0 AND length(run_id) > 0 "
            "AND length(source_file_import_id) > 0 "
            "AND length(content_version) > 0 "
            "AND length(run_idempotency_key) > 0 "
            "AND length(job_id) > 0 "
            "AND serialized_job_digest ~ '^[0-9a-f]{64}$' "
            "AND json_typeof(params) = 'object' AND json_typeof(job_payload) = 'object'",
            name="ptg_import_wave_intent_contract_check",
        ),
        schema=schema,
    )
    op.create_table(
        "ptg_import_wave_claim",
        sa.Column("wave_id", sa.String(64), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("job_id", sa.String(96), nullable=False),
        sa.Column("slot", sa.Integer(), nullable=False),
        sa.Column("pod_uid", sa.String(128), nullable=False),
        sa.Column("kubernetes_job_uid", sa.String(128), nullable=False),
        sa.Column("pinned_image_reference", sa.String(512), nullable=False),
        sa.Column("pinned_image_digest", sa.String(64), nullable=False),
        sa.Column("runtime_image_identity", sa.String(72), nullable=False),
        sa.Column("config_identity", sa.String(64), nullable=False),
        sa.Column("manifest_identity", sa.String(64), nullable=False),
        sa.Column("claim_status", sa.String(16), nullable=False),
        sa.Column("failure_code", sa.String(64)),
        sa.Column("claim_attempt_token", sa.String(32), nullable=False),
        sa.Column("claimed_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("wave_id", "ordinal"),
        sa.ForeignKeyConstraint(
            ("wave_id", "ordinal", "run_id", "job_id"),
            (
                f"{schema}.ptg_import_wave_intent.wave_id", f"{schema}.ptg_import_wave_intent.ordinal",
                f"{schema}.ptg_import_wave_intent.run_id", f"{schema}.ptg_import_wave_intent.job_id",
            ),
            name="ptg_import_wave_claim_intent_fkey", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("run_id", name="ptg_import_wave_claim_run_id_key"),
        sa.UniqueConstraint("job_id", name="ptg_import_wave_claim_job_id_key"),
        sa.CheckConstraint(
            "ordinal >= 0 AND slot >= 0 AND slot < 12 "
            "AND claim_status IN ('started', 'rejected') "
            "AND ((claim_status = 'started' AND failure_code IS NULL) "
            "OR (claim_status = 'rejected' "
            "AND failure_code IS NOT NULL "
            "AND failure_code ~ '^[a-z][a-z0-9_]{0,63}$')) "
            "AND claim_attempt_token ~ '^[0-9a-f]{32}$' "
            "AND manifest_identity ~ '^[0-9a-f]{64}$' "
            "AND length(pinned_image_reference) > 0 "
            "AND pinned_image_digest ~ '^[0-9a-f]{64}$' "
            "AND runtime_image_identity ~ '^sha256:[0-9a-f]{64}$' "
            "AND config_identity ~ '^[0-9a-f]{64}$' "
            "AND length(run_id) > 0 AND length(job_id) > 0 AND length(pod_uid) > 0",
            name="ptg_import_wave_claim_contract_check",
        ),
        schema=schema,
    )
    op.create_table(
        "ptg_import_wave_outcome",
        sa.Column("wave_id", sa.String(64), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(64), nullable=False),
        sa.Column("job_id", sa.String(96), nullable=False),
        sa.Column("source_file_import_id", sa.String(64), nullable=False),
        sa.Column("content_version", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("snapshot_id", sa.String(96)),
        sa.Column("import_id", sa.String(64)),
        sa.Column("outcome_digest", sa.String(64), nullable=False),
        sa.Column("recorded_at", sa.TIMESTAMP(), nullable=False),
        sa.PrimaryKeyConstraint("wave_id", "ordinal"),
        sa.ForeignKeyConstraint(
            ("wave_id", "ordinal", "run_id", "job_id"),
            (
                f"{schema}.ptg_import_wave_intent.wave_id", f"{schema}.ptg_import_wave_intent.ordinal",
                f"{schema}.ptg_import_wave_intent.run_id", f"{schema}.ptg_import_wave_intent.job_id",
            ),
            name="ptg_import_wave_outcome_intent_fkey", ondelete="CASCADE",
        ),
        sa.UniqueConstraint("run_id", name="ptg_import_wave_outcome_run_id_key"),
        sa.CheckConstraint(
            "ordinal >= 0 AND status IN ('succeeded', 'failed', 'canceled', 'dead_letter') "
            "AND length(job_id) > 0 AND outcome_digest ~ '^[0-9a-f]{64}$' "
            "AND (status <> 'succeeded' OR (snapshot_id IS NOT NULL "
            "AND length(snapshot_id) > 0 AND import_id = source_file_import_id))",
            name="ptg_import_wave_outcome_contract_check",
        ),
        schema=schema,
    )
    _install_immutable_identity_triggers(schema)


def downgrade() -> None:
    schema = _schema()
    op.drop_table("ptg_import_wave_outcome", schema=schema)
    op.drop_table("ptg_import_wave_claim", schema=schema)
    op.drop_table("ptg_import_wave_intent", schema=schema)
    op.drop_table("ptg_import_wave", schema=schema)
    quoted = '"' + schema.replace('"', '""') + '"'
    op.execute(f"DROP FUNCTION {quoted}.ptg_import_wave_immutable_identity()")


def _install_immutable_identity_triggers(schema: str) -> None:
    quoted = '"' + schema.replace('"', '""') + '"'
    op.execute(
        f"""
        CREATE FUNCTION {quoted}.ptg_import_wave_immutable_identity()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_TABLE_NAME IN ('ptg_import_wave_intent', 'ptg_import_wave_claim', 'ptg_import_wave_outcome') THEN
                RAISE EXCEPTION 'exact-wave immutable row cannot be updated';
            END IF;
            IF NEW.wave_id IS DISTINCT FROM OLD.wave_id
               OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
               OR NEW.request_digest IS DISTINCT FROM OLD.request_digest
               OR NEW.cohort_attestation::jsonb IS DISTINCT FROM OLD.cohort_attestation::jsonb
               OR NEW.cohort_attestation_digest IS DISTINCT FROM OLD.cohort_attestation_digest
               OR NEW.cohort_signature_digest IS DISTINCT FROM OLD.cohort_signature_digest
               OR NEW.physical_coordinate_count IS DISTINCT FROM OLD.physical_coordinate_count
               OR NEW.physical_coordinate_digest IS DISTINCT FROM OLD.physical_coordinate_digest
               OR NEW.imported_coordinate_count IS DISTINCT FROM OLD.imported_coordinate_count
               OR NEW.imported_coordinate_digest IS DISTINCT FROM OLD.imported_coordinate_digest
               OR NEW.reused_coordinate_count IS DISTINCT FROM OLD.reused_coordinate_count
               OR NEW.reused_coordinate_digest IS DISTINCT FROM OLD.reused_coordinate_digest
               OR NEW.partition_digest IS DISTINCT FROM OLD.partition_digest
               OR NEW.intent_count IS DISTINCT FROM OLD.intent_count
               OR NEW.jobs_digest IS DISTINCT FROM OLD.jobs_digest
               OR NEW.manifest_digest IS DISTINCT FROM OLD.manifest_digest
               OR NEW.wave_digest IS DISTINCT FROM OLD.wave_digest
               OR NEW.queue IS DISTINCT FROM OLD.queue
               OR NEW.release_queue IS DISTINCT FROM OLD.release_queue
               OR NEW.worker_class IS DISTINCT FROM OLD.worker_class
               OR NEW.resource_class IS DISTINCT FROM OLD.resource_class
               OR NEW.worker_limit IS DISTINCT FROM OLD.worker_limit
               OR NEW.protocol_identity IS DISTINCT FROM OLD.protocol_identity
               OR NEW.serializer_identity IS DISTINCT FROM OLD.serializer_identity
               OR NEW.enqueue_time_ms IS DISTINCT FROM OLD.enqueue_time_ms
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'exact-wave immutable identity cannot change';
            END IF;
            IF OLD.kubernetes_manifest_sha256 IS NOT NULL AND (
                NEW.kubernetes_manifest::jsonb IS DISTINCT FROM OLD.kubernetes_manifest::jsonb
                OR NEW.kubernetes_manifest_bytes IS DISTINCT FROM OLD.kubernetes_manifest_bytes
                OR NEW.kubernetes_manifest_sha256 IS DISTINCT FROM OLD.kubernetes_manifest_sha256
                OR NEW.kubernetes_manifest_identity IS DISTINCT FROM OLD.kubernetes_manifest_identity
                OR NEW.pinned_image_reference IS DISTINCT FROM OLD.pinned_image_reference
                OR NEW.pinned_image_digest IS DISTINCT FROM OLD.pinned_image_digest
                OR NEW.runtime_image_identity IS DISTINCT FROM OLD.runtime_image_identity
                OR NEW.kubernetes_config_identity IS DISTINCT FROM OLD.kubernetes_config_identity
            ) THEN
                RAISE EXCEPTION 'exact-wave desired manifest cannot change after persistence';
            END IF;
            IF OLD.failure_receipt_digest IS NOT NULL AND (
                NEW.failure_receipt::jsonb IS DISTINCT FROM OLD.failure_receipt::jsonb
                OR NEW.failure_receipt_digest IS DISTINCT FROM OLD.failure_receipt_digest
            ) THEN
                RAISE EXCEPTION 'exact-wave failure receipt cannot change';
            END IF;
            IF (OLD.kubernetes_job_receipt_digest IS NOT NULL AND (
                    NEW.kubernetes_job_receipt::jsonb IS DISTINCT FROM OLD.kubernetes_job_receipt::jsonb
                    OR NEW.kubernetes_job_receipt_digest IS DISTINCT FROM OLD.kubernetes_job_receipt_digest
                    OR NEW.kubernetes_job_uid IS DISTINCT FROM OLD.kubernetes_job_uid))
               OR (OLD.kubernetes_ready_attestation_digest IS NOT NULL AND (
                    NEW.kubernetes_ready_attestation::jsonb IS DISTINCT FROM OLD.kubernetes_ready_attestation::jsonb
                    OR NEW.kubernetes_ready_attestation_digest IS DISTINCT FROM OLD.kubernetes_ready_attestation_digest))
               OR (OLD.redis_release_attestation_digest IS NOT NULL AND (
                    NEW.redis_release_attestation::jsonb IS DISTINCT FROM OLD.redis_release_attestation::jsonb
                    OR NEW.redis_release_attestation_digest IS DISTINCT FROM OLD.redis_release_attestation_digest))
               OR (OLD.outcomes_digest IS NOT NULL
                    AND NEW.outcomes_digest IS DISTINCT FROM OLD.outcomes_digest)
               OR (OLD.linkage_ack_digest IS NOT NULL AND (
                    NEW.linkage_ack::jsonb IS DISTINCT FROM OLD.linkage_ack::jsonb
                    OR NEW.linkage_ack_digest IS DISTINCT FROM OLD.linkage_ack_digest))
               OR (OLD.terminal_evidence_digest IS NOT NULL AND (
                    NEW.terminal_summary::jsonb IS DISTINCT FROM OLD.terminal_summary::jsonb
                    OR NEW.terminal_evidence_digest IS DISTINCT FROM OLD.terminal_evidence_digest))
               OR (OLD.redis_cleanup_evidence_digest IS NOT NULL AND (
                    NEW.redis_cleanup_evidence::jsonb IS DISTINCT FROM OLD.redis_cleanup_evidence::jsonb
                    OR NEW.redis_cleanup_evidence_digest IS DISTINCT FROM OLD.redis_cleanup_evidence_digest))
               OR (OLD.kubernetes_delete_evidence_digest IS NOT NULL AND (
                    NEW.kubernetes_delete_evidence::jsonb IS DISTINCT FROM OLD.kubernetes_delete_evidence::jsonb
                    OR NEW.kubernetes_delete_evidence_digest IS DISTINCT FROM OLD.kubernetes_delete_evidence_digest))
               OR (OLD.cleanup_evidence_digest IS NOT NULL AND (
                    NEW.cleanup_summary::jsonb IS DISTINCT FROM OLD.cleanup_summary::jsonb
                    OR NEW.cleanup_evidence_digest IS DISTINCT FROM OLD.cleanup_evidence_digest
                    OR NEW.resolved_at IS DISTINCT FROM OLD.resolved_at)) THEN
                RAISE EXCEPTION 'exact-wave durable receipt cannot change after persistence';
            END IF;
            IF (OLD.k8s_post_ticket IS NOT NULL
                    AND (NEW.k8s_post_ticket IS DISTINCT FROM OLD.k8s_post_ticket
                         OR NEW.k8s_post_started_at IS DISTINCT FROM OLD.k8s_post_started_at))
               OR (OLD.redis_release_ticket IS NOT NULL
                    AND (NEW.redis_release_ticket IS DISTINCT FROM OLD.redis_release_ticket
                         OR NEW.redis_release_started_at IS DISTINCT FROM OLD.redis_release_started_at))
               OR (OLD.redis_cleanup_ticket IS NOT NULL
                    AND (NEW.redis_cleanup_ticket IS DISTINCT FROM OLD.redis_cleanup_ticket
                         OR NEW.redis_cleanup_started_at IS DISTINCT FROM OLD.redis_cleanup_started_at))
               OR (OLD.kubernetes_delete_ticket IS NOT NULL
                    AND (NEW.kubernetes_delete_ticket IS DISTINCT FROM OLD.kubernetes_delete_ticket
                         OR NEW.kubernetes_delete_started_at IS DISTINCT FROM OLD.kubernetes_delete_started_at)) THEN
                RAISE EXCEPTION 'exact-wave external-operation ticket cannot change';
            END IF;
            RETURN NEW;
        END;
        $$;
        """
    )
    for table in ("ptg_import_wave", "ptg_import_wave_intent", "ptg_import_wave_claim", "ptg_import_wave_outcome"):
        op.execute(
            f"CREATE TRIGGER {table}_immutable_identity BEFORE UPDATE ON {quoted}.{table} "
            f"FOR EACH ROW EXECUTE FUNCTION {quoted}.ptg_import_wave_immutable_identity()"
        )
