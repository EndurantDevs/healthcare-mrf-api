"""Add immutable V5 retirement for a materialized preclaim failure.

Revision ID: 20260808180000_ptg_import_wave_materialized_preclaim
Revises: 20260808170000_public_evidence_npi_enumeration_storage
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808180000_ptg_import_wave_materialized_preclaim"
down_revision = "20260808170000_public_evidence_npi_enumeration_storage"
branch_labels = None
depends_on = None

_BASIS = "materialized_preclaim_failure"
_LEGACY_BASIS = "logical_preclaim_failure"
_QUARANTINE_REASON = "materialized_preclaim_failure"
_LEGACY_QUARANTINE_REASON = "legacy_uncertain_slots_waiting_pre_receipt"
_ATTESTATION_VERSION = "healthporta.ptg-import-wave-attestation.v5"
_PROOF_VERSION = "healthporta.ptg-wave.materialized-preclaim-supersession.v1"
_GUARD_FUNCTION = "ptg_import_wave_materialized_preclaim_guard"
_GUARD_TRIGGER = "ptg_import_wave_materialized_preclaim_guard"
_BINDING_FUNCTION = "ptg_import_wave_materialized_preclaim_binding_guard"
_BINDING_TRIGGER = "ptg_import_wave_materialized_preclaim_binding_guard"
_OLD_GUARD_TRIGGER = "ptg_import_wave_supersession_preclaim_guard"
_OLD_GUARD_FUNCTION = "ptg_import_wave_supersession_preclaim_guard"
_OLD_BINDING_TRIGGER = "ptg_import_wave_supersession_successor_binding_guard"
_OLD_BINDING_FUNCTION = "ptg_import_wave_supersession_successor_binding_guard"
_CHILD_GUARD_FUNCTION = "ptg_import_wave_materialized_child_guard"
_RUN_GUARD_FUNCTION = "ptg_import_wave_materialized_run_guard"
_EVENT_GUARD_FUNCTION = "ptg_import_wave_materialized_event_guard"
_WRITE_ISOLATION_GUARD_FUNCTION = (
    "ptg_import_wave_materialized_write_isolation_guard"
)
_ADMISSION_LOCK = "import-run-admission:ptg-source-file"


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError(
            "DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema"
        )
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def upgrade() -> None:
    """Permit only the exact V5 materialized predecessor retirement."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    rollback = _qt(schema, "ptg_import_wave_admission_rollback")
    guard_function = _qt(schema, _GUARD_FUNCTION)
    binding_function = _qt(schema, _BINDING_FUNCTION)
    old_guard_function = _qt(schema, _OLD_GUARD_FUNCTION)
    old_binding_function = _qt(schema, _OLD_BINDING_FUNCTION)
    child_guard_function = _qt(schema, _CHILD_GUARD_FUNCTION)
    run_guard_function = _qt(schema, _RUN_GUARD_FUNCTION)
    event_guard_function = _qt(schema, _EVENT_GUARD_FUNCTION)
    write_isolation_guard_function = _qt(
        schema,
        _WRITE_ISOLATION_GUARD_FUNCTION,
    )

    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession}, {intent}, "
        f"{claim}, {outcome}, {run}, {event}, {rollback} "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')}"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')} CHECK "
        f"(reason IN ('{_LEGACY_QUARANTINE_REASON}', "
        f"'{_QUARANTINE_REASON}'))"
    )
    op.execute(
        f"ALTER TABLE {supersession} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_supersession_basis_check')}"
    )
    op.execute(
        f"ALTER TABLE {supersession} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_supersession_basis_check')} CHECK "
        f"(recovery_basis IN ('{_LEGACY_BASIS}', '{_BASIS}'))"
    )

    op.execute(f"DROP TRIGGER {_q(_OLD_GUARD_TRIGGER)} ON {supersession}")
    op.execute(
        f"CREATE TRIGGER {_q(_OLD_GUARD_TRIGGER)} BEFORE INSERT ON "
        f"{supersession} FOR EACH ROW WHEN "
        f"(NEW.recovery_basis = '{_LEGACY_BASIS}') EXECUTE FUNCTION "
        f"{old_guard_function}()"
    )
    op.execute(f"DROP TRIGGER {_q(_OLD_BINDING_TRIGGER)} ON {supersession}")
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {_q(_OLD_BINDING_TRIGGER)} AFTER INSERT "
        f"ON {supersession} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN "
        f"(NEW.recovery_basis = '{_LEGACY_BASIS}') EXECUTE FUNCTION "
        f"{old_binding_function}()"
    )

    op.execute(
        f"""
        CREATE FUNCTION {write_isolation_guard_function}(
            is_v5_retirement boolean,
            candidate_run_ids text[],
            candidate_wave_ids text[],
            candidate_wave_digests text[]
        )
        RETURNS void
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- A stronger transaction snapshot can remain older than a newly
            -- committed retirement even after the advisory lock is acquired.
            -- V5 retirement itself must use READ COMMITTED.  For other PTG
            -- writes, constrain only the exact materialized V4 boundary so
            -- unrelated legacy waves retain their existing isolation policy.
            IF current_setting('transaction_isolation') = 'read committed' THEN
                RETURN;
            END IF;
            IF is_v5_retirement OR EXISTS (
                SELECT 1
                  FROM {wave} AS candidate
                 WHERE (
                       candidate.wave_id = ANY(candidate_wave_ids)
                       OR candidate.wave_digest = ANY(candidate_wave_digests)
                       OR EXISTS (
                           SELECT 1 FROM {intent} AS member
                            WHERE member.wave_id = candidate.wave_id
                              AND member.run_id = ANY(candidate_run_ids)
                       )
                   )
                   AND (
                       EXISTS (
                           SELECT 1 FROM {supersession} AS retirement
                            WHERE retirement.recovery_basis = '{_BASIS}'
                              AND retirement.predecessor_wave_id =
                                    candidate.wave_id
                       )
                       OR (
                           candidate.cohort_attestation::jsonb
                                ->>'schema_version'
                                = 'healthporta.ptg-import-wave-attestation.v4'
                           AND candidate.cohort_attestation::jsonb->>'wave_id'
                                = candidate.wave_id
                           AND candidate.state = 'slots_waiting'
                           AND candidate.uncertainty_resume_state IS NULL
                           AND candidate.k8s_post_ticket IS NOT NULL
                           AND candidate.k8s_post_started_at IS NOT NULL
                           AND candidate.kubernetes_job_uid IS NOT NULL
                           AND candidate.kubernetes_job_receipt IS NOT NULL
                           AND candidate.kubernetes_job_receipt_digest IS NOT NULL
                           AND candidate.kubernetes_job_receipt::jsonb
                                = jsonb_build_object(
                                    'wave_digest', candidate.wave_digest,
                                    'job_uid', candidate.kubernetes_job_uid,
                                    'manifest_identity',
                                        candidate.kubernetes_manifest_identity,
                                    'config_identity',
                                        candidate.kubernetes_config_identity,
                                    'pinned_image_reference',
                                        candidate.pinned_image_reference,
                                    'pinned_image_digest',
                                        candidate.pinned_image_digest,
                                    'runtime_image_identity',
                                        candidate.runtime_image_identity
                                )
                           AND candidate.kubernetes_ready_attestation IS NULL
                           AND candidate.kubernetes_ready_attestation_digest IS NULL
                           AND candidate.redis_release_ticket IS NULL
                           AND candidate.redis_release_started_at IS NULL
                           AND candidate.redis_release_attestation IS NULL
                           AND candidate.redis_release_attestation_digest IS NULL
                           AND candidate.failure_receipt IS NULL
                           AND candidate.failure_receipt_digest IS NULL
                           AND candidate.outcomes_digest IS NULL
                           AND candidate.linkage_ack IS NULL
                           AND candidate.linkage_ack_digest IS NULL
                           AND candidate.terminal_evidence_digest IS NULL
                           AND candidate.terminal_summary IS NULL
                           AND candidate.redis_cleanup_ticket IS NULL
                           AND candidate.redis_cleanup_started_at IS NULL
                           AND candidate.redis_cleanup_evidence IS NULL
                           AND candidate.redis_cleanup_evidence_digest IS NULL
                           AND candidate.kubernetes_delete_ticket IS NULL
                           AND candidate.kubernetes_delete_started_at IS NULL
                           AND candidate.kubernetes_delete_evidence IS NULL
                           AND candidate.kubernetes_delete_evidence_digest IS NULL
                           AND candidate.cleanup_evidence_digest IS NULL
                           AND candidate.cleanup_summary IS NULL
                           AND candidate.resolved_at IS NULL
                           AND candidate.idempotency_key = candidate.wave_id
                           AND candidate.protocol_identity =
                                'healthporta.ptg-small.exact-wave.v1'
                           AND candidate.queue = 'arq:PTGSmall'
                           AND candidate.release_queue =
                                'arq:PTGSmall:wave:' || candidate.wave_digest
                           AND candidate.worker_class = 'process.PTGSmall'
                           AND candidate.resource_class = 'small'
                           AND candidate.worker_limit = 12
                           AND candidate.request_digest ~ '^[0-9a-f]{{64}}$'
                           AND candidate.cohort_attestation_digest
                                ~ '^[0-9a-f]{{64}}$'
                           AND candidate.wave_digest ~ '^[0-9a-f]{{64}}$'
                           AND candidate.wave_digest = encode(
                                sha256(
                                    convert_to(
                                        'healthporta.ptg-small.exact-wave.v1',
                                        'UTF8'
                                    )
                                    || decode('00', 'hex')
                                    || convert_to(
                                        candidate.request_digest,
                                        'UTF8'
                                    )
                                ),
                                'hex'
                           )
                           AND candidate.manifest_digest ~ '^[0-9a-f]{{64}}$'
                           AND candidate.jobs_digest ~ '^[0-9a-f]{{64}}$'
                           AND candidate.kubernetes_manifest_identity
                                ~ '^[0-9a-f]{{64}}$'
                           AND candidate.kubernetes_config_identity
                                ~ '^[0-9a-f]{{64}}$'
                           AND candidate.pinned_image_digest
                                ~ '^[0-9a-f]{{64}}$'
                           AND candidate.kubernetes_job_receipt_digest
                                ~ '^[0-9a-f]{{64}}$'
                           AND candidate.runtime_image_identity
                                ~ '^sha256:[0-9a-f]{{64}}$'
                           AND candidate.pinned_image_reference IS NOT NULL
                           AND candidate.pinned_image_reference =
                                btrim(candidate.pinned_image_reference)
                           AND candidate.pinned_image_reference <> ''
                           AND candidate.pinned_image_reference LIKE (
                                '%@sha256:' || candidate.pinned_image_digest
                           )
                           AND candidate.kubernetes_job_uid =
                                btrim(candidate.kubernetes_job_uid)
                           AND candidate.kubernetes_job_uid <> ''
                           AND (
                               SELECT count(*) FROM {intent} AS member
                                WHERE member.wave_id = candidate.wave_id
                           ) = candidate.intent_count
                           AND (
                               SELECT COALESCE(min(member.ordinal), -1)
                                 FROM {intent} AS member
                                WHERE member.wave_id = candidate.wave_id
                           ) = 0
                           AND (
                               SELECT COALESCE(max(member.ordinal), -1)
                                 FROM {intent} AS member
                                WHERE member.wave_id = candidate.wave_id
                           ) = candidate.intent_count - 1
                           AND (
                               SELECT count(*)
                                 FROM {intent} AS member
                                 JOIN {run} AS admitted
                                   ON admitted.run_id = member.run_id
                                WHERE member.wave_id = candidate.wave_id
                                  AND admitted.status = 'queued'
                                  AND admitted.importer = 'ptg'
                                  AND admitted.source_file_import_id =
                                        member.source_file_import_id
                                  AND admitted.import_id =
                                        member.source_file_import_id
                                  AND admitted.phase_detail =
                                        'wave admitted; controller materialization pending'
                                  AND admitted.started_at IS NULL
                                  AND admitted.finished_at IS NULL
                                  AND admitted.snapshot_id IS NULL
                                  AND (
                                      admitted.error IS NULL
                                      OR admitted.error::jsonb
                                           IS NOT DISTINCT FROM 'null'::jsonb
                                  )
                                  AND admitted.progress::jsonb =
                                      jsonb_build_object(
                                          'unit', 'run', 'total', 1,
                                          'done', 0, 'pct', 0, 'message',
                                          'wave admitted; controller materialization pending'
                                      )
                                  AND admitted.metrics::jsonb =
                                      jsonb_build_object(
                                          'wave_id', candidate.wave_id,
                                          'queue', candidate.release_queue,
                                          'base_queue', candidate.queue,
                                          'worker_class', candidate.worker_class,
                                          'resource_class',
                                              candidate.resource_class,
                                          'worker_limit', candidate.worker_limit,
                                          'job_id', member.job_id,
                                          'ordinal', member.ordinal,
                                          'wave_digest', candidate.wave_digest
                                      )
                           ) = candidate.intent_count
                           AND NOT EXISTS (
                               SELECT 1 FROM {claim} AS existing_claim
                                WHERE existing_claim.wave_id = candidate.wave_id
                           )
                           AND NOT EXISTS (
                               SELECT 1 FROM {outcome} AS existing_outcome
                                WHERE existing_outcome.wave_id = candidate.wave_id
                           )
                           AND NOT EXISTS (
                               SELECT 1
                                 FROM {event} AS attempt_event
                                 JOIN {intent} AS member
                                   ON member.run_id = attempt_event.outer_run_id
                                WHERE member.wave_id = candidate.wave_id
                                  AND attempt_event.event_kind =
                                        'worker_start_admitted'
                           )
                           AND EXISTS (
                               SELECT 1
                                 FROM {supersession} AS prior_logical
                                WHERE prior_logical.successor_wave_id =
                                        candidate.wave_id
                                  AND prior_logical.recovery_basis =
                                        '{_LEGACY_BASIS}'
                                  AND prior_logical.recovery_evidence_sha256 =
                                        prior_logical.recovery_evidence
                                            ->>'proof_digest'
                                  AND candidate.cohort_attestation::jsonb
                                        ->'supersession' =
                                        prior_logical.recovery_evidence
                           )
                           AND EXISTS (
                               SELECT 1 FROM {rollback} AS prior_rollback
                                WHERE prior_rollback.successor_wave_id =
                                        candidate.wave_id
                                  AND prior_rollback.recovery_basis =
                                        'admission_rollback_absent'
                                  AND prior_rollback.recovery_evidence_sha256 =
                                        prior_rollback.recovery_evidence
                                            ->>'proof_digest'
                                  AND candidate.cohort_attestation::jsonb
                                        ->'admission_rollback_supersession' =
                                        prior_rollback.recovery_evidence
                           )
                       )
                   )
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_WRITE_ISOLATION_UNSUPPORTED'
                    USING ERRCODE = 'P0001';
            END IF;
        END;
        $$
        """
    )

    op.execute(
        f"""
        CREATE FUNCTION {guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            predecessor record;
            quarantine_reason text;
            intent_count integer;
            first_ordinal integer;
            last_ordinal integer;
            pristine_run_count integer;
            claim_count integer;
            outcome_count integer;
            worker_start_event_count integer;
            prior_logical record;
            prior_rollback record;
        BEGIN
            PERFORM {write_isolation_guard_function}(
                TRUE,
                ARRAY[]::text[],
                ARRAY[]::text[],
                ARRAY[]::text[]
            );
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
            );
            LOCK TABLE {wave}, {quarantine}, {intent}, {claim}, {outcome},
                {run}, {event}, {supersession}, {rollback}
                IN SHARE ROW EXCLUSIVE MODE;
            SELECT * INTO predecessor FROM {wave}
             WHERE wave_id = NEW.predecessor_wave_id FOR KEY SHARE;
            SELECT reason INTO quarantine_reason FROM {quarantine}
             WHERE predecessor_wave_id = NEW.predecessor_wave_id FOR KEY SHARE;
            SELECT count(*), COALESCE(min(ordinal), -1),
                   COALESCE(max(ordinal), -1)
              INTO intent_count, first_ordinal, last_ordinal
              FROM {intent}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO claim_count FROM {claim}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO outcome_count FROM {outcome}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO pristine_run_count
              FROM {intent} AS member
              JOIN {run} AS admitted ON admitted.run_id = member.run_id
             WHERE member.wave_id = NEW.predecessor_wave_id
               AND admitted.status = 'queued'
               AND admitted.importer = 'ptg'
               AND admitted.source_file_import_id = member.source_file_import_id
               AND admitted.import_id = member.source_file_import_id
               AND admitted.phase_detail =
                    'wave admitted; controller materialization pending'
               AND admitted.started_at IS NULL
               AND admitted.finished_at IS NULL
               AND admitted.snapshot_id IS NULL
               AND (
                    admitted.error IS NULL
                    OR admitted.error::jsonb IS NOT DISTINCT FROM 'null'::jsonb
               )
               AND admitted.progress::jsonb = jsonb_build_object(
                    'unit', 'run', 'total', 1, 'done', 0, 'pct', 0,
                    'message',
                    'wave admitted; controller materialization pending'
               )
               AND admitted.metrics::jsonb = jsonb_build_object(
                    'wave_id', predecessor.wave_id,
                    'queue', predecessor.release_queue,
                    'base_queue', predecessor.queue,
                    'worker_class', predecessor.worker_class,
                    'resource_class', predecessor.resource_class,
                    'worker_limit', predecessor.worker_limit,
                    'job_id', member.job_id,
                    'ordinal', member.ordinal,
                    'wave_digest', predecessor.wave_digest
               );
            SELECT count(*) INTO worker_start_event_count
              FROM {event} AS attempt_event
              JOIN {intent} AS member
                ON member.run_id = attempt_event.outer_run_id
             WHERE member.wave_id = NEW.predecessor_wave_id
               AND attempt_event.event_kind = 'worker_start_admitted';
            SELECT * INTO prior_logical FROM {supersession}
             WHERE successor_wave_id = NEW.predecessor_wave_id
               AND recovery_basis = '{_LEGACY_BASIS}' FOR KEY SHARE;
            SELECT * INTO prior_rollback FROM {rollback}
             WHERE successor_wave_id = NEW.predecessor_wave_id FOR KEY SHARE;

            IF predecessor IS NULL
               OR quarantine_reason IS DISTINCT FROM '{_QUARANTINE_REASON}'
               OR predecessor.state IS DISTINCT FROM 'slots_waiting'
               OR predecessor.uncertainty_resume_state IS NOT NULL
               OR predecessor.k8s_post_ticket IS NULL
               OR predecessor.k8s_post_started_at IS NULL
               OR predecessor.kubernetes_job_uid IS NULL
               OR predecessor.kubernetes_job_receipt IS NULL
               OR predecessor.kubernetes_job_receipt_digest IS NULL
               OR predecessor.kubernetes_job_receipt::jsonb
                    IS DISTINCT FROM jsonb_build_object(
                        'wave_digest', predecessor.wave_digest,
                        'job_uid', predecessor.kubernetes_job_uid,
                        'manifest_identity',
                            predecessor.kubernetes_manifest_identity,
                        'config_identity',
                            predecessor.kubernetes_config_identity,
                        'pinned_image_reference',
                            predecessor.pinned_image_reference,
                        'pinned_image_digest',
                            predecessor.pinned_image_digest,
                        'runtime_image_identity',
                            predecessor.runtime_image_identity
                    )
               OR predecessor.kubernetes_ready_attestation IS NOT NULL
               OR predecessor.kubernetes_ready_attestation_digest IS NOT NULL
               OR predecessor.redis_release_ticket IS NOT NULL
               OR predecessor.redis_release_started_at IS NOT NULL
               OR predecessor.redis_release_attestation IS NOT NULL
               OR predecessor.redis_release_attestation_digest IS NOT NULL
               OR predecessor.outcomes_digest IS NOT NULL
               OR predecessor.failure_receipt IS NOT NULL
               OR predecessor.failure_receipt_digest IS NOT NULL
               OR predecessor.linkage_ack IS NOT NULL
               OR predecessor.linkage_ack_digest IS NOT NULL
               OR predecessor.terminal_evidence_digest IS NOT NULL
               OR predecessor.terminal_summary IS NOT NULL
               OR predecessor.redis_cleanup_ticket IS NOT NULL
               OR predecessor.redis_cleanup_started_at IS NOT NULL
               OR predecessor.redis_cleanup_evidence IS NOT NULL
               OR predecessor.redis_cleanup_evidence_digest IS NOT NULL
               OR predecessor.kubernetes_delete_ticket IS NOT NULL
               OR predecessor.kubernetes_delete_started_at IS NOT NULL
               OR predecessor.kubernetes_delete_evidence IS NOT NULL
               OR predecessor.kubernetes_delete_evidence_digest IS NOT NULL
               OR predecessor.cleanup_evidence_digest IS NOT NULL
               OR predecessor.cleanup_summary IS NOT NULL
               OR predecessor.resolved_at IS NOT NULL
               OR predecessor.idempotency_key
                    IS DISTINCT FROM predecessor.wave_id
               OR predecessor.protocol_identity
                    IS DISTINCT FROM 'healthporta.ptg-small.exact-wave.v1'
               OR predecessor.queue IS DISTINCT FROM 'arq:PTGSmall'
               OR predecessor.release_queue
                    IS DISTINCT FROM 'arq:PTGSmall:wave:'
                        || predecessor.wave_digest
               OR predecessor.worker_class IS DISTINCT FROM 'process.PTGSmall'
               OR predecessor.resource_class IS DISTINCT FROM 'small'
               OR predecessor.worker_limit IS DISTINCT FROM 12
               OR predecessor.request_digest !~ '^[0-9a-f]{{64}}$'
               OR predecessor.cohort_attestation_digest
                    !~ '^[0-9a-f]{{64}}$'
               OR predecessor.wave_digest !~ '^[0-9a-f]{{64}}$'
               OR predecessor.wave_digest IS DISTINCT FROM encode(
                    sha256(
                        convert_to(
                            'healthporta.ptg-small.exact-wave.v1',
                            'UTF8'
                        )
                        || decode('00', 'hex')
                        || convert_to(predecessor.request_digest, 'UTF8')
                    ),
                    'hex'
               )
               OR predecessor.manifest_digest !~ '^[0-9a-f]{{64}}$'
               OR predecessor.jobs_digest !~ '^[0-9a-f]{{64}}$'
               OR predecessor.kubernetes_manifest_identity
                    !~ '^[0-9a-f]{{64}}$'
               OR predecessor.kubernetes_config_identity
                    !~ '^[0-9a-f]{{64}}$'
               OR predecessor.pinned_image_digest !~ '^[0-9a-f]{{64}}$'
               OR predecessor.kubernetes_job_receipt_digest
                    !~ '^[0-9a-f]{{64}}$'
               OR predecessor.runtime_image_identity
                    !~ '^sha256:[0-9a-f]{{64}}$'
               OR predecessor.pinned_image_reference IS NULL
               OR predecessor.pinned_image_reference
                    IS DISTINCT FROM btrim(predecessor.pinned_image_reference)
               OR predecessor.pinned_image_reference = ''
               OR predecessor.pinned_image_reference NOT LIKE (
                    '%@sha256:' || predecessor.pinned_image_digest
               )
               OR predecessor.kubernetes_job_uid
                    IS DISTINCT FROM btrim(predecessor.kubernetes_job_uid)
               OR predecessor.kubernetes_job_uid = ''
               OR intent_count <> predecessor.intent_count
               OR first_ordinal <> 0
               OR last_ordinal <> predecessor.intent_count - 1
               OR pristine_run_count <> predecessor.intent_count
               OR claim_count <> 0
               OR outcome_count <> 0
               OR worker_start_event_count <> 0
               OR prior_logical IS NULL
               OR prior_rollback IS NULL
               OR predecessor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM
                        'healthporta.ptg-import-wave-attestation.v4'
               OR predecessor.cohort_attestation::jsonb->>'wave_id'
                    IS DISTINCT FROM predecessor.wave_id
               OR prior_logical.successor_wave_id
                    IS DISTINCT FROM predecessor.wave_id
               OR prior_logical.recovery_basis
                    IS DISTINCT FROM '{_LEGACY_BASIS}'
               OR prior_logical.recovery_evidence_sha256
                    IS DISTINCT FROM prior_logical.recovery_evidence
                        ->>'proof_digest'
               OR predecessor.cohort_attestation::jsonb->'supersession'
                    IS DISTINCT FROM prior_logical.recovery_evidence
               OR prior_rollback.successor_wave_id
                    IS DISTINCT FROM predecessor.wave_id
               OR prior_rollback.recovery_basis
                    IS DISTINCT FROM 'admission_rollback_absent'
               OR prior_rollback.recovery_evidence_sha256
                    IS DISTINCT FROM prior_rollback.recovery_evidence
                        ->>'proof_digest'
               OR predecessor.cohort_attestation::jsonb
                    ->'admission_rollback_supersession'
                    IS DISTINCT FROM prior_rollback.recovery_evidence THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_REQUIRED'
                    USING ERRCODE = 'P0001';
            END IF;

            IF NEW.recovery_basis IS DISTINCT FROM '{_BASIS}'
               OR jsonb_typeof(NEW.recovery_evidence)
                    IS DISTINCT FROM 'object'
               OR (SELECT count(*)
                     FROM jsonb_object_keys(NEW.recovery_evidence)) <> 9
               OR NEW.recovery_evidence - ARRAY[
                    'schema_version', 'recovery_basis', 'predecessor',
                    'successor_wave_id', 'prior_recovery', 'database',
                    'kubernetes', 'redis', 'proof_digest'
               ]::text[] <> '{{}}'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence->'schema_version')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'schema_version'
                    IS DISTINCT FROM '{_PROOF_VERSION}'
               OR jsonb_typeof(NEW.recovery_evidence->'recovery_basis')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'recovery_basis'
                    IS DISTINCT FROM '{_BASIS}'
               OR jsonb_typeof(NEW.recovery_evidence->'successor_wave_id')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'successor_wave_id'
                    IS DISTINCT FROM NEW.successor_wave_id
               OR jsonb_typeof(NEW.recovery_evidence->'predecessor')
                    IS DISTINCT FROM 'object'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{predecessor,intent_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{predecessor,intent_count}}'
                    !~ '^[1-9][0-9]*$'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{predecessor,worker_limit}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{predecessor,worker_limit}}'
                    !~ '^12$'
               OR NEW.recovery_evidence->'predecessor'
                    IS DISTINCT FROM jsonb_build_object(
                        'wave_id', predecessor.wave_id,
                        'idempotency_key', predecessor.idempotency_key,
                        'request_digest', predecessor.request_digest,
                        'cohort_attestation_digest',
                            predecessor.cohort_attestation_digest,
                        'wave_digest', predecessor.wave_digest,
                        'release_queue', predecessor.release_queue,
                        'manifest_digest', predecessor.manifest_digest,
                        'jobs_digest', predecessor.jobs_digest,
                        'intent_count', predecessor.intent_count,
                        'worker_limit', predecessor.worker_limit,
                        'kubernetes_manifest_identity',
                            predecessor.kubernetes_manifest_identity,
                        'kubernetes_config_identity',
                            predecessor.kubernetes_config_identity,
                        'pinned_image_reference',
                            predecessor.pinned_image_reference,
                        'pinned_image_digest',
                            predecessor.pinned_image_digest,
                        'runtime_image_identity',
                            predecessor.runtime_image_identity,
                        'kubernetes_job_uid',
                            predecessor.kubernetes_job_uid,
                        'kubernetes_job_receipt_digest',
                            predecessor.kubernetes_job_receipt_digest
                    )
               OR jsonb_typeof(NEW.recovery_evidence->'prior_recovery')
                    IS DISTINCT FROM 'object'
               OR NEW.recovery_evidence->'prior_recovery'
                    IS DISTINCT FROM jsonb_build_object(
                        'logical_preclaim_predecessor_wave_id',
                            prior_logical.predecessor_wave_id,
                        'logical_preclaim_proof_digest',
                            prior_logical.recovery_evidence_sha256,
                        'admission_rollback_predecessor_wave_id',
                            prior_rollback.predecessor_wave_id,
                        'admission_rollback_proof_digest',
                            prior_rollback.recovery_evidence_sha256
                    )
               OR jsonb_typeof(NEW.recovery_evidence->'database')
                    IS DISTINCT FROM 'object'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{database,pristine_run_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence
                    #>> '{{database,pristine_run_count}}'
                    !~ '^(0|[1-9][0-9]*)$'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{database,claim_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{database,claim_count}}'
                    !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{database,outcome_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{database,outcome_count}}'
                    !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{database,worker_start_event_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence
                    #>> '{{database,worker_start_event_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence->'database'
                    IS DISTINCT FROM jsonb_build_object(
                        'state', 'slots_waiting',
                        'pristine_run_count', predecessor.intent_count,
                        'claim_count', claim_count,
                        'outcome_count', outcome_count,
                        'worker_start_event_count', worker_start_event_count
                    )
               OR jsonb_typeof(NEW.recovery_evidence->'kubernetes')
                    IS DISTINCT FROM 'object'
               OR NEW.recovery_evidence #>> '{{kubernetes,completions}}'
                    !~ '^12$'
               OR NEW.recovery_evidence #>> '{{kubernetes,parallelism}}'
                    !~ '^12$'
               OR NEW.recovery_evidence #>> '{{kubernetes,backoff_limit}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{kubernetes,failed}}'
                    !~ '^12$'
               OR NEW.recovery_evidence #>> '{{kubernetes,active}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{kubernetes,succeeded}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{kubernetes,ready}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{kubernetes,terminating}}'
                    !~ '^0$'
               OR NEW.recovery_evidence->'kubernetes'
                    IS DISTINCT FROM jsonb_build_object(
                        'job_name', 'hpw-ptg-wave-'
                            || left(predecessor.wave_digest, 40),
                        'job_uid', predecessor.kubernetes_job_uid,
                        'job_receipt_digest',
                            predecessor.kubernetes_job_receipt_digest,
                        'completion_mode', 'Indexed',
                        'completions', 12,
                        'parallelism', 12,
                        'backoff_limit', 0,
                        'failed', 12,
                        'active', 0,
                        'succeeded', 0,
                        'ready', 0,
                        'terminating', 0,
                        'failed_condition', true,
                        'complete_condition', false
                    )
               OR jsonb_typeof(NEW.recovery_evidence->'redis')
                    IS DISTINCT FROM 'object'
               OR NEW.recovery_evidence #>> '{{redis,ready_slot_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{redis,queued_ordinal_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{redis,job_ordinal_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{redis,result_ordinal_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence #>> '{{redis,retry_ordinal_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence
                    #>> '{{redis,in_progress_ordinal_count}}'
                    !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence
                    #> '{{redis,unclaimed_attestation_digest}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence
                    #>> '{{redis,unclaimed_attestation_digest}}'
                    !~ '^[0-9a-f]{{64}}$'
               OR NEW.recovery_evidence->'redis'
                    IS DISTINCT FROM jsonb_build_object(
                        'unclaimed_attestation_digest',
                            NEW.recovery_evidence
                                #> '{{redis,unclaimed_attestation_digest}}',
                        'ready_slot_count', 0,
                        'release_present', false,
                        'queued_ordinal_count', 0,
                        'job_ordinal_count', 0,
                        'result_ordinal_count', 0,
                        'retry_ordinal_count', 0,
                        'in_progress_ordinal_count', 0,
                        'health_check_present', false
                    )
               OR jsonb_typeof(NEW.recovery_evidence->'proof_digest')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'proof_digest'
                    !~ '^[0-9a-f]{{64}}$'
               OR NEW.recovery_evidence->>'proof_digest'
                    IS DISTINCT FROM NEW.recovery_evidence_sha256 THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_EVIDENCE_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q(_GUARD_TRIGGER)} BEFORE INSERT ON {supersession} "
        f"FOR EACH ROW WHEN (NEW.recovery_basis = '{_BASIS}') "
        f"EXECUTE FUNCTION {guard_function}()"
    )
    op.execute(
        f"ALTER TABLE {supersession} ENABLE ALWAYS TRIGGER "
        f"{_q(_GUARD_TRIGGER)}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {binding_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            successor record;
        BEGIN
            SELECT candidate.*, candidate.xmin AS inserted_xid INTO successor
              FROM {wave} AS candidate
             WHERE wave_id = NEW.successor_wave_id FOR KEY SHARE;
            IF NOT FOUND
               OR successor.inserted_xid
                    IS DISTINCT FROM pg_current_xact_id()::xid
               OR successor.state IS DISTINCT FROM 'admitted'
               OR successor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_ATTESTATION_VERSION}'
               OR successor.cohort_attestation::jsonb->>'wave_id'
                    IS DISTINCT FROM NEW.successor_wave_id
               OR successor.cohort_attestation::jsonb
                    ->'materialized_preclaim_supersession'
                    IS DISTINCT FROM NEW.recovery_evidence
               OR EXISTS (
                   SELECT 1 FROM {supersession} AS retirement
                    WHERE retirement.predecessor_wave_id = NEW.successor_wave_id
               ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_BINDING_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {_q(_BINDING_TRIGGER)} AFTER INSERT ON "
        f"{supersession} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN "
        f"(NEW.recovery_basis = '{_BASIS}') EXECUTE FUNCTION "
        f"{binding_function}()"
    )
    op.execute(
        f"ALTER TABLE {supersession} ENABLE ALWAYS TRIGGER "
        f"{_q(_BINDING_TRIGGER)}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {child_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            old_wave_id text;
            new_wave_id text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                old_wave_id := OLD.wave_id;
            END IF;
            IF TG_OP <> 'DELETE' THEN
                new_wave_id := NEW.wave_id;
            END IF;
            PERFORM {write_isolation_guard_function}(
                FALSE,
                ARRAY[]::text[],
                ARRAY[old_wave_id, new_wave_id],
                ARRAY[]::text[]
            );
            IF NOT EXISTS (
                SELECT 1 FROM {supersession} AS retirement
                 WHERE retirement.recovery_basis = '{_BASIS}'
                   AND retirement.predecessor_wave_id IN (
                       old_wave_id,
                       new_wave_id
                   )
            ) THEN
                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
            );
            IF EXISTS (
                SELECT 1 FROM {supersession} AS retirement
                 WHERE retirement.recovery_basis = '{_BASIS}'
                   AND retirement.predecessor_wave_id IN (
                       old_wave_id,
                       new_wave_id
                   )
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_RETIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
    ):
        table = _qt(schema, table_name)
        trigger_name = _q(f"{table_name}_materialized_retirement_guard")
        op.execute(
            f"CREATE TRIGGER {trigger_name} BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {table} FOR EACH ROW EXECUTE FUNCTION "
            f"{child_guard_function}()"
        )
        op.execute(
            f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger_name}"
        )

    op.execute(
        f"""
        CREATE FUNCTION {run_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            candidate_run_ids text[] := ARRAY[]::text[];
            candidate_wave_ids text[] := ARRAY[]::text[];
            candidate_wave_digests text[] := ARRAY[]::text[];
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                candidate_run_ids := array_append(
                    candidate_run_ids,
                    OLD.run_id
                );
                candidate_wave_ids := candidate_wave_ids || ARRAY[
                    OLD.params::jsonb->>'_wave_id',
                    OLD.metrics::jsonb->>'wave_id'
                ];
                candidate_wave_digests := candidate_wave_digests || ARRAY[
                    OLD.params::jsonb->>'_wave_digest',
                    OLD.metrics::jsonb->>'wave_digest'
                ];
            END IF;
            IF TG_OP <> 'DELETE' THEN
                candidate_run_ids := array_append(
                    candidate_run_ids,
                    NEW.run_id
                );
                candidate_wave_ids := candidate_wave_ids || ARRAY[
                    NEW.params::jsonb->>'_wave_id',
                    NEW.metrics::jsonb->>'wave_id'
                ];
                candidate_wave_digests := candidate_wave_digests || ARRAY[
                    NEW.params::jsonb->>'_wave_digest',
                    NEW.metrics::jsonb->>'wave_digest'
                ];
            END IF;
            -- The retirement writer locks import_run in SHARE ROW EXCLUSIVE
            -- mode before inserting its evidence.  Probe first so unrelated
            -- importers never wait on the PTG admission lock; a matching row
            -- still takes the lock and repeats the authoritative check below.
            PERFORM {write_isolation_guard_function}(
                FALSE,
                candidate_run_ids,
                candidate_wave_ids,
                candidate_wave_digests
            );
            IF NOT EXISTS (
                SELECT 1
                  FROM {supersession} AS retirement
                  JOIN {wave} AS predecessor
                    ON predecessor.wave_id = retirement.predecessor_wave_id
                 WHERE retirement.recovery_basis = '{_BASIS}'
                   AND (
                       retirement.predecessor_wave_id = ANY(candidate_wave_ids)
                       OR predecessor.wave_digest = ANY(candidate_wave_digests)
                       OR EXISTS (
                           SELECT 1 FROM {intent} AS member
                            WHERE member.wave_id = retirement.predecessor_wave_id
                              AND member.run_id = ANY(candidate_run_ids)
                       )
                   )
            ) THEN
                IF TG_OP = 'DELETE' THEN
                    RETURN OLD;
                END IF;
                RETURN NEW;
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
            );
            IF EXISTS (
                SELECT 1
                  FROM {supersession} AS retirement
                  JOIN {wave} AS predecessor
                    ON predecessor.wave_id = retirement.predecessor_wave_id
                 WHERE retirement.recovery_basis = '{_BASIS}'
                   AND (
                       retirement.predecessor_wave_id = ANY(candidate_wave_ids)
                       OR predecessor.wave_digest = ANY(candidate_wave_digests)
                       OR EXISTS (
                           SELECT 1 FROM {intent} AS member
                            WHERE member.wave_id = retirement.predecessor_wave_id
                              AND member.run_id = ANY(candidate_run_ids)
                       )
                   )
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_RETIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    run_trigger = _q("ptg_import_wave_materialized_retired_run_guard")
    op.execute(
        f"CREATE TRIGGER {run_trigger} BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {run} FOR EACH ROW EXECUTE FUNCTION {run_guard_function}()"
    )
    op.execute(f"ALTER TABLE {run} ENABLE ALWAYS TRIGGER {run_trigger}")

    op.execute(
        f"""
        CREATE FUNCTION {event_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            PERFORM {write_isolation_guard_function}(
                FALSE,
                ARRAY[NEW.outer_run_id],
                ARRAY[]::text[],
                ARRAY[]::text[]
            );
            IF NOT EXISTS (
                SELECT 1
                  FROM {intent} AS member
                  JOIN {supersession} AS retirement
                    ON retirement.predecessor_wave_id = member.wave_id
                   AND retirement.recovery_basis = '{_BASIS}'
                 WHERE member.run_id = NEW.outer_run_id
            ) THEN
                RETURN NEW;
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
            );
            IF EXISTS (
                SELECT 1
                  FROM {intent} AS member
                  JOIN {supersession} AS retirement
                    ON retirement.predecessor_wave_id = member.wave_id
                   AND retirement.recovery_basis = '{_BASIS}'
                 WHERE member.run_id = NEW.outer_run_id
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_RETIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    event_trigger = _q("ptg_import_wave_materialized_retired_event_guard")
    op.execute(
        f"CREATE TRIGGER {event_trigger} BEFORE INSERT ON {event} "
        f"FOR EACH ROW EXECUTE FUNCTION {event_guard_function}()"
    )
    op.execute(f"ALTER TABLE {event} ENABLE ALWAYS TRIGGER {event_trigger}")


def downgrade() -> None:
    """Restore the V4-only constraints only when no V5 retirement exists."""

    schema = _schema()
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    guard_function = _qt(schema, _GUARD_FUNCTION)
    binding_function = _qt(schema, _BINDING_FUNCTION)
    old_guard_function = _qt(schema, _OLD_GUARD_FUNCTION)
    old_binding_function = _qt(schema, _OLD_BINDING_FUNCTION)
    child_guard_function = _qt(schema, _CHILD_GUARD_FUNCTION)
    run_guard_function = _qt(schema, _RUN_GUARD_FUNCTION)
    event_guard_function = _qt(schema, _EVENT_GUARD_FUNCTION)
    write_isolation_guard_function = _qt(
        schema,
        _WRITE_ISOLATION_GUARD_FUNCTION,
    )
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"""
        DO $downgrade$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {supersession}
                 WHERE recovery_basis = '{_BASIS}'
            ) OR EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE reason = '{_QUARANTINE_REASON}'
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_MATERIALIZED_PRECLAIM_DOWNGRADE_BLOCKED'
                    USING ERRCODE = 'P0001';
            END IF;
        END;
        $downgrade$
        """
    )
    event_trigger = _q("ptg_import_wave_materialized_retired_event_guard")
    op.execute(f"DROP TRIGGER {event_trigger} ON {event}")
    op.execute(f"DROP FUNCTION {event_guard_function}()")
    run_trigger = _q("ptg_import_wave_materialized_retired_run_guard")
    op.execute(f"DROP TRIGGER {run_trigger} ON {run}")
    op.execute(f"DROP FUNCTION {run_guard_function}()")
    for table_name, table in (
        ("ptg_import_wave_intent", intent),
        ("ptg_import_wave_claim", claim),
        ("ptg_import_wave_outcome", outcome),
    ):
        trigger_name = _q(f"{table_name}_materialized_retirement_guard")
        op.execute(f"DROP TRIGGER {trigger_name} ON {table}")
    op.execute(f"DROP FUNCTION {child_guard_function}()")
    op.execute(
        f"DROP FUNCTION {write_isolation_guard_function}("
        "boolean, text[], text[], text[])"
    )
    op.execute(f"DROP TRIGGER {_q(_BINDING_TRIGGER)} ON {supersession}")
    op.execute(f"DROP FUNCTION {binding_function}()")
    op.execute(f"DROP TRIGGER {_q(_GUARD_TRIGGER)} ON {supersession}")
    op.execute(f"DROP FUNCTION {guard_function}()")
    op.execute(f"DROP TRIGGER {_q(_OLD_BINDING_TRIGGER)} ON {supersession}")
    op.execute(
        f"CREATE CONSTRAINT TRIGGER {_q(_OLD_BINDING_TRIGGER)} AFTER INSERT "
        f"ON {supersession} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW "
        f"EXECUTE FUNCTION {old_binding_function}()"
    )
    op.execute(f"DROP TRIGGER {_q(_OLD_GUARD_TRIGGER)} ON {supersession}")
    op.execute(
        f"CREATE TRIGGER {_q(_OLD_GUARD_TRIGGER)} BEFORE INSERT ON "
        f"{supersession} FOR EACH ROW EXECUTE FUNCTION {old_guard_function}()"
    )
    op.execute(
        f"ALTER TABLE {supersession} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_supersession_basis_check')}"
    )
    op.execute(
        f"ALTER TABLE {supersession} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_supersession_basis_check')} CHECK "
        f"(recovery_basis = '{_LEGACY_BASIS}')"
    )
    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')}"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')} CHECK "
        f"(reason = '{_LEGACY_QUARANTINE_REASON}')"
    )
