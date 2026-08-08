"""Add immutable exact-wave quarantine and supersession records.

Revision ID: 20260807120000_ptg_import_wave_recovery_storage
Revises: 20260807110000_fhir_formulary_storage_foundation

This migration records a narrow, durable recovery boundary only.  It does not
admit, publish, or otherwise advance an exact wave.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260807120000_ptg_import_wave_recovery_storage"
down_revision = "20260807110000_fhir_formulary_storage_foundation"
branch_labels = None
depends_on = None

_QUARANTINE = "ptg_import_wave_quarantine"
_SUPERSESSION = "ptg_import_wave_supersession"
_IMMUTABLE_FUNCTION = "ptg_import_wave_recovery_immutable"
_EFFECTIVE_OWNER_FUNCTION = "ptg_import_wave_effective_owner_guard"
_PRECLAIM_GUARD_FUNCTION = "ptg_import_wave_supersession_preclaim_guard"
_SUCCESSOR_BINDING_FUNCTION = "ptg_import_wave_supersession_successor_binding_guard"
_QUARANTINED_WAVE_GUARD_FUNCTION = "ptg_import_wave_quarantine_update_guard"
_CAPACITY_OWNER_INDEX = "ptg_import_wave_single_capacity_owner_idx"
_QUARANTINE_REASON = "legacy_uncertain_slots_waiting_pre_receipt"
_RECOVERY_BASIS = "logical_preclaim_failure"
_CAPACITY_STATES = (
    "admitted", "materializing", "slots_waiting", "redis_releasing",
    "released", "executing", "awaiting_linkage", "terminalizing",
    "cleaning", "uncertain",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must identify the same schema")
    return runtime_schema or legacy_schema or "mrf"


def _q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _qt(schema: str, table: str) -> str:
    return f"{_q(schema)}.{_q(table)}"


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def upgrade() -> None:
    """Create append-only recovery records and fence the legacy waiting wedge."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, _QUARANTINE)
    supersession = _qt(schema, _SUPERSESSION)
    function = _qt(schema, _IMMUTABLE_FUNCTION)
    effective_owner_function = _qt(schema, _EFFECTIVE_OWNER_FUNCTION)
    preclaim_guard_function = _qt(schema, _PRECLAIM_GUARD_FUNCTION)
    successor_binding_function = _qt(schema, _SUCCESSOR_BINDING_FUNCTION)
    quarantined_wave_guard_function = _qt(schema, _QUARANTINED_WAVE_GUARD_FUNCTION)
    capacity_owner_index = _qt(schema, _CAPACITY_OWNER_INDEX)
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    import_run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")

    op.execute(
        f"""
        CREATE TABLE {quarantine} (
            predecessor_wave_id varchar(64) PRIMARY KEY,
            reason varchar(64) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('ptg_import_wave_quarantine_predecessor_wave_fkey')}
                FOREIGN KEY (predecessor_wave_id)
                REFERENCES {wave}(wave_id) ON DELETE RESTRICT,
            CONSTRAINT {_q('ptg_import_wave_quarantine_reason_check')}
                CHECK (reason = '{_QUARANTINE_REASON}')
        )
        """
    )
    op.execute(
        f"""
        CREATE TABLE {supersession} (
            predecessor_wave_id varchar(64) PRIMARY KEY,
            successor_wave_id varchar(64) NOT NULL,
            recovery_basis varchar(64) NOT NULL,
            recovery_evidence jsonb NOT NULL,
            recovery_evidence_canonical bytea NOT NULL,
            recovery_evidence_sha256 varchar(64) NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            CONSTRAINT {_q('ptg_import_wave_supersession_predecessor_wave_fkey')}
                FOREIGN KEY (predecessor_wave_id)
                REFERENCES {wave}(wave_id) ON DELETE RESTRICT,
            CONSTRAINT {_q('ptg_import_wave_supersession_successor_wave_fkey')}
                FOREIGN KEY (successor_wave_id)
                REFERENCES {wave}(wave_id) ON DELETE RESTRICT
                DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT {_q('ptg_import_wave_supersession_distinct_check')}
                CHECK (predecessor_wave_id <> successor_wave_id),
            CONSTRAINT {_q('ptg_import_wave_supersession_successor_wave_id_key')}
                UNIQUE (successor_wave_id),
            CONSTRAINT {_q('ptg_import_wave_supersession_basis_check')}
                CHECK (recovery_basis = '{_RECOVERY_BASIS}'),
            CONSTRAINT {_q('ptg_import_wave_supersession_evidence_check')}
                CHECK (
                    jsonb_typeof(recovery_evidence) = 'object'
                    AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                    AND octet_length(recovery_evidence_canonical) > 0
                    AND encode(sha256(recovery_evidence_canonical), 'hex')
                        = recovery_evidence_sha256
                    AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                        = recovery_evidence - 'proof_digest'
                )
        )
        """
    )

    # Hold the replacement fence before observing legacy rows. This makes the
    # immutable seed and the old-index replacement one migration transaction.
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession}, {intent}, {claim}, "
        f"{outcome}, {import_run}, {event} IN SHARE ROW EXCLUSIVE MODE"
    )

    # Only the historical post-ticket, pre-receipt shape is fenced.  The
    # source wave itself is never modified.
    op.execute(
        f"""
        INSERT INTO {quarantine} (predecessor_wave_id, reason)
        SELECT wave_id, '{_QUARANTINE_REASON}'
        FROM {wave}
        WHERE state = 'uncertain'
          AND uncertainty_resume_state = 'slots_waiting'
          AND k8s_post_ticket IS NOT NULL
          AND k8s_post_started_at IS NOT NULL
          AND kubernetes_job_uid IS NULL
          AND kubernetes_job_receipt IS NULL
          AND kubernetes_job_receipt_digest IS NULL
          AND kubernetes_ready_attestation IS NULL
          AND kubernetes_ready_attestation_digest IS NULL
          AND redis_release_ticket IS NULL
          AND redis_release_started_at IS NULL
          AND redis_release_attestation IS NULL
          AND redis_release_attestation_digest IS NULL
          AND failure_receipt IS NULL
          AND failure_receipt_digest IS NULL
          AND outcomes_digest IS NULL
          AND linkage_ack IS NULL
          AND linkage_ack_digest IS NULL
          AND terminal_evidence_digest IS NULL
          AND terminal_summary IS NULL
          AND redis_cleanup_ticket IS NULL
          AND redis_cleanup_started_at IS NULL
          AND redis_cleanup_evidence IS NULL
          AND redis_cleanup_evidence_digest IS NULL
          AND kubernetes_delete_ticket IS NULL
          AND kubernetes_delete_started_at IS NULL
          AND kubernetes_delete_evidence IS NULL
          AND kubernetes_delete_evidence_digest IS NULL
          AND cleanup_evidence_digest IS NULL
          AND cleanup_summary IS NULL
          AND resolved_at IS NULL
        ON CONFLICT (predecessor_wave_id) DO NOTHING
        """
    )
    # PostgreSQL partial-index predicates cannot consult the supersession
    # relation. Serialize the deferred cross-table check instead, so a
    # superseded predecessor no longer owns capacity while two effective
    # owners can never commit concurrently.
    capacity_states = ", ".join(_literal(state) for state in _CAPACITY_STATES)
    op.execute(f"DROP INDEX IF EXISTS {capacity_owner_index}")
    op.execute(
        f"""
        CREATE FUNCTION {effective_owner_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            effective_owner_count integer;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(
                    {_literal(f'ptg-import-wave-effective-owner:{schema}')}, 0
                )
            );
            SELECT count(*) INTO effective_owner_count
            FROM {wave} AS candidate
            WHERE candidate.state IN ({capacity_states})
              AND NOT EXISTS (
                  SELECT 1
                  FROM {supersession} AS retired
                  WHERE retired.predecessor_wave_id = candidate.wave_id
              );
            IF effective_owner_count > 1 THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_EFFECTIVE_OWNER_CONFLICT'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    for table_name in ("ptg_import_wave", _SUPERSESSION):
        table = _qt(schema, table_name)
        trigger = _q(f"{table_name}_effective_owner_guard")
        op.execute(
            f"""
            CREATE CONSTRAINT TRIGGER {trigger}
            AFTER INSERT OR UPDATE OR DELETE ON {table}
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION {effective_owner_function}()
            """
        )
    op.execute(
        f"""
        CREATE FUNCTION {preclaim_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            predecessor record;
            quarantine_reason text;
            actual_intent_count integer;
            first_ordinal integer;
            last_ordinal integer;
            claim_count integer;
            outcome_count integer;
            worker_start_event_count integer;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(
                    {_literal(f'ptg-import-wave-effective-owner:{schema}')}, 0
                )
            );
            LOCK TABLE {quarantine}, {intent}, {claim}, {outcome}, {import_run}, {event}
                IN SHARE ROW EXCLUSIVE MODE;
            SELECT reason INTO quarantine_reason
            FROM {quarantine}
            WHERE predecessor_wave_id = NEW.predecessor_wave_id
            FOR UPDATE;
            SELECT * INTO predecessor
            FROM {wave}
            WHERE wave_id = NEW.predecessor_wave_id
            FOR UPDATE;
            IF quarantine_reason IS DISTINCT FROM '{_QUARANTINE_REASON}'
               OR NOT FOUND
               OR predecessor.state IS DISTINCT FROM 'uncertain'
               OR predecessor.uncertainty_resume_state IS DISTINCT FROM 'slots_waiting'
               OR predecessor.k8s_post_ticket IS NULL
               OR predecessor.k8s_post_started_at IS NULL
               OR predecessor.kubernetes_job_uid IS NOT NULL
               OR predecessor.kubernetes_job_receipt IS NOT NULL
               OR predecessor.kubernetes_job_receipt_digest IS NOT NULL
               OR predecessor.kubernetes_ready_attestation IS NOT NULL
               OR predecessor.kubernetes_ready_attestation_digest IS NOT NULL
               OR predecessor.redis_release_ticket IS NOT NULL
               OR predecessor.redis_release_started_at IS NOT NULL
               OR predecessor.redis_release_attestation IS NOT NULL
               OR predecessor.redis_release_attestation_digest IS NOT NULL
               OR predecessor.failure_receipt IS NOT NULL
               OR predecessor.failure_receipt_digest IS NOT NULL
               OR predecessor.outcomes_digest IS NOT NULL
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
               OR predecessor.resolved_at IS NOT NULL THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_SUPERSESSION_PRECLAIM_REQUIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            PERFORM 1 FROM {intent}
            WHERE wave_id = NEW.predecessor_wave_id
            FOR UPDATE;
            PERFORM 1 FROM {import_run} AS run
            JOIN {intent} AS intent_row ON intent_row.run_id = run.run_id
            WHERE intent_row.wave_id = NEW.predecessor_wave_id
            FOR UPDATE OF run;
            SELECT count(*), COALESCE(min(ordinal), -1), COALESCE(max(ordinal), -1)
            INTO actual_intent_count, first_ordinal, last_ordinal
            FROM {intent}
            WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO claim_count
            FROM {claim}
            WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO outcome_count
            FROM {outcome}
            WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO worker_start_event_count
            FROM {event} AS attempt_event
            JOIN {intent} AS intent_row
              ON intent_row.run_id = attempt_event.outer_run_id
            WHERE intent_row.wave_id = NEW.predecessor_wave_id
              AND attempt_event.event_kind = 'worker_start_admitted';
            IF actual_intent_count <> predecessor.intent_count
               OR first_ordinal <> 0
               OR last_ordinal <> predecessor.intent_count - 1
               OR EXISTS (
                   SELECT 1
                   FROM {intent} AS intent_row
                   LEFT JOIN {import_run} AS run
                     ON run.run_id = intent_row.run_id
                   WHERE intent_row.wave_id = NEW.predecessor_wave_id
                     AND (
                         run.run_id IS NULL
                         OR run.importer IS DISTINCT FROM 'ptg'
                         OR run.status IS DISTINCT FROM 'queued'
                         OR run.source_file_import_id IS DISTINCT FROM intent_row.source_file_import_id
                         OR run.import_id IS DISTINCT FROM intent_row.source_file_import_id
                         OR run.phase_detail IS DISTINCT FROM 'wave admitted; controller materialization pending'
                         OR run.started_at IS NOT NULL
                         OR run.finished_at IS NOT NULL
                         OR run.snapshot_id IS NOT NULL
                         OR run.error IS NOT NULL
                         OR run.progress::jsonb IS DISTINCT FROM jsonb_build_object(
                             'unit', 'run', 'total', 1, 'done', 0, 'pct', 0,
                             'message', 'wave admitted; controller materialization pending'
                         )
                         OR run.metrics::jsonb IS DISTINCT FROM jsonb_build_object(
                             'wave_id', predecessor.wave_id,
                             'queue', predecessor.release_queue,
                             'base_queue', predecessor.queue,
                             'worker_class', predecessor.worker_class,
                             'resource_class', predecessor.resource_class,
                             'worker_limit', predecessor.worker_limit,
                             'job_id', intent_row.job_id,
                             'ordinal', intent_row.ordinal,
                             'wave_digest', predecessor.wave_digest
                         )
                     )
               )
               OR claim_count <> 0
               OR outcome_count <> 0
               OR worker_start_event_count <> 0 THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_SUPERSESSION_PRECLAIM_REQUIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            IF NEW.recovery_basis IS DISTINCT FROM '{_RECOVERY_BASIS}'
               OR (SELECT count(*) FROM jsonb_object_keys(NEW.recovery_evidence)) <> 8
               OR NEW.recovery_evidence - ARRAY[
                    'schema_version', 'recovery_basis', 'predecessor',
                    'successor_wave_id', 'database', 'kubernetes', 'redis',
                    'proof_digest'
               ]::text[] <> '{{}}'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence->'schema_version')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'schema_version'
                    IS DISTINCT FROM 'healthporta.ptg-wave.logical-preclaim-supersession.v1'
               OR jsonb_typeof(NEW.recovery_evidence->'recovery_basis')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'recovery_basis'
                    IS DISTINCT FROM '{_RECOVERY_BASIS}'
               OR jsonb_typeof(NEW.recovery_evidence->'predecessor')
                    IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'predecessor')) <> 5
               OR (NEW.recovery_evidence->'predecessor') - ARRAY[
                    'wave_id', 'wave_digest', 'manifest_digest', 'jobs_digest',
                    'intent_count'
               ]::text[] <> '{{}}'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence #> '{{predecessor,wave_id}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence #>> '{{predecessor,wave_id}}'
                    IS DISTINCT FROM predecessor.wave_id
               OR jsonb_typeof(NEW.recovery_evidence #> '{{predecessor,wave_digest}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence #>> '{{predecessor,wave_digest}}'
                    IS DISTINCT FROM predecessor.wave_digest
               OR jsonb_typeof(NEW.recovery_evidence #> '{{predecessor,manifest_digest}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence #>> '{{predecessor,manifest_digest}}'
                    IS DISTINCT FROM predecessor.manifest_digest
               OR jsonb_typeof(NEW.recovery_evidence #> '{{predecessor,jobs_digest}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence #>> '{{predecessor,jobs_digest}}'
                    IS DISTINCT FROM predecessor.jobs_digest
               OR jsonb_typeof(NEW.recovery_evidence #> '{{predecessor,intent_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{predecessor,intent_count}}'
                    !~ '^(0|[1-9][0-9]*)$'
               OR NEW.recovery_evidence #>> '{{predecessor,intent_count}}'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR jsonb_typeof(NEW.recovery_evidence->'successor_wave_id')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'successor_wave_id'
                    IS DISTINCT FROM NEW.successor_wave_id
               OR jsonb_typeof(NEW.recovery_evidence->'database')
                    IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'database')) <> 4
               OR (NEW.recovery_evidence->'database') - ARRAY[
                    'pristine_run_count', 'claim_count', 'outcome_count',
                    'worker_start_event_count'
               ]::text[] <> '{{}}'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence #> '{{database,pristine_run_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{database,pristine_run_count}}'
                    !~ '^(0|[1-9][0-9]*)$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{database,claim_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{database,claim_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{database,outcome_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{database,outcome_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{database,worker_start_event_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{database,worker_start_event_count}}'
                    !~ '^0$'
               OR NEW.recovery_evidence->'database' IS DISTINCT FROM jsonb_build_object(
                    'pristine_run_count', predecessor.intent_count,
                    'claim_count', claim_count,
                    'outcome_count', outcome_count,
                    'worker_start_event_count', worker_start_event_count
               )
               OR jsonb_typeof(NEW.recovery_evidence->'kubernetes')
                    IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'kubernetes')) <> 13
               OR (NEW.recovery_evidence->'kubernetes') - ARRAY[
                    'job_name', 'job_uid', 'completion_mode', 'completions',
                    'parallelism', 'backoff_limit', 'failed', 'active',
                    'succeeded', 'ready', 'terminating', 'failed_condition',
                    'complete_condition'
               ]::text[] <> '{{}}'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,job_name}}')
                    IS DISTINCT FROM 'string'
               OR coalesce(NEW.recovery_evidence #>> '{{kubernetes,job_name}}', '') = ''
               OR NEW.recovery_evidence #>> '{{kubernetes,job_name}}'
                    IS DISTINCT FROM 'hpw-ptg-wave-' || left(predecessor.wave_digest, 40)
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,job_uid}}')
                    IS DISTINCT FROM 'string'
               OR coalesce(NEW.recovery_evidence #>> '{{kubernetes,job_uid}}', '') = ''
               OR NEW.recovery_evidence #>> '{{kubernetes,job_uid}}'
                    IS DISTINCT FROM btrim(NEW.recovery_evidence #>> '{{kubernetes,job_uid}}')
               OR length(NEW.recovery_evidence #>> '{{kubernetes,job_uid}}') > 160
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,completion_mode}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence #>> '{{kubernetes,completion_mode}}'
                    IS DISTINCT FROM 'Indexed'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,completions}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,completions}}' !~ '^12$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,parallelism}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,parallelism}}' !~ '^12$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,failed}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,failed}}' !~ '^12$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,backoff_limit}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,backoff_limit}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,active}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,active}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,succeeded}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,succeeded}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,ready}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,ready}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{kubernetes,terminating}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{kubernetes,terminating}}' !~ '^0$'
               OR NEW.recovery_evidence #> '{{kubernetes,failed_condition}}'
                    IS DISTINCT FROM 'true'::jsonb
               OR NEW.recovery_evidence #> '{{kubernetes,complete_condition}}'
                    IS DISTINCT FROM 'false'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence->'redis')
                    IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(NEW.recovery_evidence->'redis')) <> 9
               OR (NEW.recovery_evidence->'redis') - ARRAY[
                    'unclaimed_attestation_digest', 'ready_slot_count',
                    'release_present', 'queued_ordinal_count',
                    'job_ordinal_count', 'result_ordinal_count',
                    'retry_ordinal_count', 'in_progress_ordinal_count',
                    'health_check_present'
               ]::text[] <> '{{}}'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,unclaimed_attestation_digest}}')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence #>> '{{redis,unclaimed_attestation_digest}}'
                    !~ '^[0-9a-f]{{64}}$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,ready_slot_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{redis,ready_slot_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,queued_ordinal_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{redis,queued_ordinal_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,job_ordinal_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{redis,job_ordinal_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,result_ordinal_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{redis,result_ordinal_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,retry_ordinal_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{redis,retry_ordinal_count}}' !~ '^0$'
               OR jsonb_typeof(NEW.recovery_evidence #> '{{redis,in_progress_ordinal_count}}')
                    IS DISTINCT FROM 'number'
               OR NEW.recovery_evidence #>> '{{redis,in_progress_ordinal_count}}' !~ '^0$'
               OR NEW.recovery_evidence #> '{{redis,release_present}}'
                    IS DISTINCT FROM 'false'::jsonb
               OR NEW.recovery_evidence #> '{{redis,health_check_present}}'
                    IS DISTINCT FROM 'false'::jsonb
               OR jsonb_typeof(NEW.recovery_evidence->'proof_digest')
                    IS DISTINCT FROM 'string'
               OR NEW.recovery_evidence->>'proof_digest'
                    IS DISTINCT FROM NEW.recovery_evidence_sha256 THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_SUPERSESSION_EVIDENCE_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_import_wave_supersession_preclaim_guard')}
        BEFORE INSERT ON {supersession}
        FOR EACH ROW EXECUTE FUNCTION {preclaim_guard_function}()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {quarantined_wave_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM {quarantine}
                WHERE predecessor_wave_id = OLD.wave_id
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_QUARANTINED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_import_wave_quarantine_update_guard')}
        BEFORE UPDATE ON {wave}
        FOR EACH ROW EXECUTE FUNCTION {quarantined_wave_guard_function}()
        """
    )
    op.execute(
        f"ALTER TABLE {wave} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_quarantine_update_guard')}"
    )
    # The application flushes the immutable supersession before it persists
    # the replacement wave. Bind that replacement at commit so the complete
    # atomic write remains valid, while no stale row can retire capacity.
    op.execute(
        f"""
        CREATE FUNCTION {successor_binding_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            successor record;
        BEGIN
            SELECT candidate.*, candidate.xmin AS inserted_xid INTO successor
            FROM {wave} AS candidate
            WHERE wave_id = NEW.successor_wave_id
            FOR KEY SHARE;
            IF NOT FOUND
               OR successor.inserted_xid IS DISTINCT FROM pg_current_xact_id()::xid
               OR successor.state IS DISTINCT FROM 'admitted'
               OR successor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM 'healthporta.ptg-import-wave-attestation.v3'
               OR successor.cohort_attestation::jsonb->>'wave_id'
                    IS DISTINCT FROM NEW.successor_wave_id
               OR successor.cohort_attestation::jsonb->'supersession'
                    IS DISTINCT FROM NEW.recovery_evidence
               OR EXISTS (
                   SELECT 1
                   FROM {supersession} AS successor_retirement
                   WHERE successor_retirement.predecessor_wave_id
                       = NEW.successor_wave_id
               ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_SUPERSESSION_SUCCESSOR_BINDING_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE CONSTRAINT TRIGGER {_q('ptg_import_wave_supersession_successor_binding_guard')}
        AFTER INSERT ON {supersession}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {successor_binding_function}()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'PTG_IMPORT_WAVE_RECOVERY_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END;
        $$
        """
    )
    for table_name in (_QUARANTINE, _SUPERSESSION):
        table = _qt(schema, table_name)
        row_trigger = _q(f"{table_name}_row_guard")
        truncate_trigger = _q(f"{table_name}_truncate_guard")
        op.execute(
            f"""
            CREATE TRIGGER {row_trigger}
            BEFORE UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION {function}()
            """
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {row_trigger}")
        op.execute(
            f"""
            CREATE TRIGGER {truncate_trigger}
            BEFORE TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION {function}()
            """
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {truncate_trigger}")


def downgrade() -> None:
    """Remove empty recovery storage only; recorded recovery evidence is retained."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, _QUARANTINE)
    supersession = _qt(schema, _SUPERSESSION)
    function = _qt(schema, _IMMUTABLE_FUNCTION)
    effective_owner_function = _qt(schema, _EFFECTIVE_OWNER_FUNCTION)
    preclaim_guard_function = _qt(schema, _PRECLAIM_GUARD_FUNCTION)
    successor_binding_function = _qt(schema, _SUCCESSOR_BINDING_FUNCTION)
    quarantined_wave_guard_function = _qt(schema, _QUARANTINED_WAVE_GUARD_FUNCTION)
    # PostgreSQL creates the index in the table's schema and does not accept a
    # schema-qualified index name in CREATE INDEX.
    capacity_owner_index = _q(_CAPACITY_OWNER_INDEX)
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession} "
        "IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {quarantine})
               OR EXISTS (SELECT 1 FROM {supersession}) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_RECOVERY_DOWNGRADE_REFUSED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_quarantine_update_guard')} ON {wave}"
    )
    op.execute(f"DROP FUNCTION {quarantined_wave_guard_function}()")
    op.execute(f"DROP TABLE {supersession}")
    op.execute(f"DROP TABLE {quarantine}")
    op.execute(f"DROP FUNCTION {successor_binding_function}()")
    op.execute(f"DROP FUNCTION {preclaim_guard_function}()")
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_effective_owner_guard')} ON {wave}"
    )
    op.execute(f"DROP FUNCTION {effective_owner_function}()")
    capacity_states = ", ".join(_literal(state) for state in _CAPACITY_STATES)
    op.execute(
        f"CREATE UNIQUE INDEX {capacity_owner_index} ON {wave} ((1)) "
        f"WHERE state IN ({capacity_states})"
    )
    op.execute(f"DROP FUNCTION {function}()")
