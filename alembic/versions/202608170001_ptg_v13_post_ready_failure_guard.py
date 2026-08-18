"""Fence V13 post-ready, unreleased failed PTG waves.

Revision ID: 202608170001_ptg_v13_post_ready_failure_guard
Revises: 20260818020000_provider_directory_terminal_publication_compact_guard

V13 is intentionally a healthcare-only Alembic revision.  It introduces no
storage shape: it admits one additional, signed quarantine evidence family,
then makes the predecessor's child, run, and source-event rows immutable.
"""

from __future__ import annotations

import os

from alembic import op


revision = "202608170001_ptg_v13_post_ready_failure_guard"
down_revision = (
    "20260818020000_provider_directory_terminal_publication_compact_guard"
)
branch_labels = None
depends_on = None


_V6 = "healthporta.ptg-import-wave-attestation.v6"
_NULL_BASIS_LEGACY_REASON = "legacy_uncertain_slots_waiting_pre_receipt"
_LEGACY_BASIS = "materialized_preclaim_failure"
_V12_BASIS = "v12_pristine_materialized_cutover"
_V13_BASIS = "v13_post_ready_unreleased_failure_cutover"
_ORDINARY_TERMINAL_GUARD = "ptg_wave_ordinary_terminal_receipt_guard"
_ORDINARY_TERMINAL_V12_PREDICATE = (
    f"OR retired.reason IS DISTINCT FROM '{_V12_BASIS}'\n"
    f"               OR retired.recovery_basis IS DISTINCT FROM '{_V12_BASIS}'"
)
_ORDINARY_TERMINAL_V13_PREDICATE = (
    "OR retired.reason IS DISTINCT FROM retired.recovery_basis\n"
    "               OR retired.recovery_basis IS NULL\n"
    "               OR retired.recovery_basis NOT IN (\n"
    f"                    '{_V12_BASIS}', '{_V13_BASIS}'\n"
    "               )"
)
_V12_PROOF = "healthporta.ptg-wave.v12-pristine-materialized-abandonment-proof.v1"
_V13_PROOF = (
    "healthporta.ptg-wave.v13-post-ready-unreleased-failure-"
    "abandonment-proof.v1"
)
_RETAINED_FAILURE = "healthporta.ptg-wave.kubernetes-retained-preclaim-failure.v1"
_REDIS_FAILURE = "healthporta.ptg-wave.redis-unclaimed-failure.v1"
_ABANDONMENT = "healthporta.ptg-wave-abandonment-receipt.v2"
_ADMISSION_LOCK = "import-run-admission:ptg-source-file"
_CAPACITY_STATES = (
    "admitted",
    "materializing",
    "slots_waiting",
    "redis_releasing",
    "released",
    "executing",
    "awaiting_linkage",
    "terminalizing",
    "cleaning",
    "uncertain",
)


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


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _replace_ordinary_terminal_receipt_guard(
    *,
    schema: str,
    old_predicate: str,
    new_predicate: str,
) -> None:
    """Patch the installed V12 guard only when its body is exact."""

    signature = f"{_qt(schema, _ORDINARY_TERMINAL_GUARD)}()"
    op.execute(
        f"""
        DO $migration$
        DECLARE
            definition text;
            old_fragment constant text := {_literal(old_predicate)};
            new_fragment constant text := {_literal(new_predicate)};
        BEGIN
            SELECT pg_catalog.pg_get_functiondef(
                pg_catalog.to_regprocedure({_literal(signature)})
            ) INTO definition;
            IF definition IS NULL
               OR pg_catalog.length(definition)
                    - pg_catalog.length(pg_catalog.replace(
                        definition, old_fragment, ''
                    ))
                    <> pg_catalog.length(old_fragment)
               OR pg_catalog.strpos(definition, new_fragment) <> 0 THEN
                RAISE EXCEPTION
                    'PTG_WAVE_ORDINARY_TERMINAL_GUARD_PATCH_PRECONDITION_FAILED'
                    USING ERRCODE = 'P0001';
            END IF;
            EXECUTE pg_catalog.replace(
                definition, old_fragment, new_fragment
            );
        END;
        $migration$
        """
    )


def _expected_admission_sql(alias: str) -> str:
    attestation = f"{alias}.cohort_attestation::jsonb"
    return f"""
        jsonb_build_object(
            'attestation_schema', '{_V6}',
            'receipt_key_id', {alias}.receipt_key_id,
            'receipt_public_modulus_hex',
                {alias}.receipt_public_modulus_hex,
            'receipt_public_exponent', {alias}.receipt_public_exponent,
            'wave_id', {alias}.wave_id,
            'wave_digest', {alias}.wave_digest,
            'request_digest', {alias}.request_digest,
            'cohort_attestation_digest',
                {alias}.cohort_attestation_digest,
            'cohort_signature_digest', {alias}.cohort_signature_digest,
            'authorization_digest',
                {attestation} #>> '{{snapshot,authorization_digest}}',
            'snapshot_digest',
                {attestation} #>> '{{snapshot,snapshot_digest}}',
            'membership_digest',
                {attestation} #>> '{{snapshot,membership_digest}}',
            'inventory_digest',
                {attestation} #>> '{{snapshot,inventory_digest}}',
            'subscription_coverage_digest',
                {attestation} #>> '{{snapshot,subscription_coverage_digest}}',
            'entitlement_coverage_digest',
                {attestation} #>> '{{snapshot,entitlement_coverage_digest}}',
            'entitlement_coverage_count',
                ({attestation} #>> '{{snapshot,entitlement_coverage_count}}')::integer,
            'catalog_generation',
                {attestation} #>> '{{snapshot,catalog_generation}}',
            'physical_coordinate_digest', {alias}.physical_coordinate_digest,
            'imported_coordinate_digest', {alias}.imported_coordinate_digest,
            'reused_coordinate_digest', {alias}.reused_coordinate_digest,
            'partition_digest', {alias}.partition_digest,
            'physical_coordinate_count', {alias}.physical_coordinate_count,
            'imported_coordinate_count', {alias}.imported_coordinate_count,
            'reused_coordinate_count', {alias}.reused_coordinate_count,
            'intent_count', {alias}.intent_count,
            'jobs_digest', {alias}.jobs_digest,
            'manifest_digest', {alias}.manifest_digest
        )
    """


def _replace_effective_owner_function(*, schema: str, include_v13: bool) -> None:
    """Make V13 quarantine free the same capacity as V12 quarantine."""

    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    function = _qt(schema, "ptg_import_wave_effective_owner_guard")
    states = ", ".join(_literal(state) for state in _CAPACITY_STATES)
    bases = (_LEGACY_BASIS, _V12_BASIS)
    if include_v13:
        bases += (_V13_BASIS,)
    basis_sql = ", ".join(_literal(basis) for basis in bases)
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION {function}()
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
             WHERE candidate.state IN ({states})
               AND NOT EXISTS (
                   SELECT 1 FROM {supersession} AS retired
                    WHERE retired.predecessor_wave_id = candidate.wave_id
               )
               AND NOT EXISTS (
                   SELECT 1 FROM {quarantine} AS abandoned
                    WHERE abandoned.predecessor_wave_id = candidate.wave_id
                      AND abandoned.recovery_basis IN ({basis_sql})
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


def _replace_quarantine_constraints(*, schema: str, include_v13: bool) -> None:
    """Extend, rather than replace, V12's three closed quarantine contracts."""

    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    for name in (
        "ptg_import_wave_quarantine_reason_check",
        "ptg_import_wave_quarantine_abandonment_evidence_check",
        "ptg_import_wave_quarantine_receipt_check",
    ):
        op.execute(f"ALTER TABLE {quarantine} DROP CONSTRAINT {_q(name)}")
    reason_values = (
        "'legacy_uncertain_slots_waiting_pre_receipt', "
        f"'{_LEGACY_BASIS}', '{_V12_BASIS}'"
    )
    if include_v13:
        reason_values += f", '{_V13_BASIS}'"
    op.execute(
        f"ALTER TABLE {quarantine} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_reason_check')} CHECK "
        f"(reason IN ({reason_values}))"
    )

    v13_evidence = ""
    v13_receipt = ""
    if include_v13:
        v13_evidence = f""" OR (
                reason = '{_V13_BASIS}'
                AND recovery_basis = '{_V13_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence->>'schema_version' = '{_V13_PROOF}'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(
                    sha256(
                        convert_to('{_V13_PROOF}', 'UTF8')
                        || decode('00', 'hex')
                        || recovery_evidence_canonical
                    ),
                    'hex'
                ) = recovery_evidence_sha256
                AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                    = recovery_evidence - 'proof_digest'
            )"""
        v13_receipt = f""" OR (
                reason = '{_V13_BASIS}'
                AND recovery_basis = '{_V13_BASIS}'
                AND recovery_evidence->>'schema_version' = '{_V13_PROOF}'
                AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$'
                AND jsonb_typeof(abandonment_receipt) = 'object'
                AND abandonment_receipt->>'schema' = '{_ABANDONMENT}'
                AND abandonment_receipt->>'key_id' = receipt_key_id
                AND abandonment_receipt->>'payload_digest'
                    = abandonment_receipt_payload_digest
                AND length(abandonment_receipt->>'signature') = 512
                AND abandonment_receipt->>'signature' ~ '^[0-9a-f]+$'
                AND abandonment_receipt#>>'{{payload,wave_id}}'
                    = predecessor_wave_id
                AND abandonment_receipt#>>'{{payload,cutover_id}}'
                    = successor_wave_id
                AND abandonment_receipt
                    #>>'{{payload,recovery_evidence_sha256}}'
                    = recovery_evidence_sha256
                AND abandonment_receipt_payload_digest ~ '^[0-9a-f]{{64}}$'
                AND abandonment_receipt_issued_at IS NOT NULL
            )"""
    op.execute(
        f"""
        ALTER TABLE {quarantine} ADD CONSTRAINT
            {_q('ptg_import_wave_quarantine_abandonment_evidence_check')}
        CHECK (
            (
                recovery_basis IS NULL
                AND reason IN (
                    '{_NULL_BASIS_LEGACY_REASON}', '{_LEGACY_BASIS}'
                )
                AND successor_wave_id IS NULL
                AND recovery_evidence IS NULL
                AND recovery_evidence_canonical IS NULL
                AND recovery_evidence_sha256 IS NULL
            ) OR (
                reason = '{_LEGACY_BASIS}'
                AND recovery_basis = '{_LEGACY_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(sha256(recovery_evidence_canonical), 'hex')
                    = recovery_evidence_sha256
                AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                    = recovery_evidence - 'proof_digest'
            ) OR (
                reason = '{_V12_BASIS}'
                AND recovery_basis = '{_V12_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence->>'schema_version' = '{_V12_PROOF}'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(
                    sha256(
                        convert_to('{_V12_PROOF}', 'UTF8')
                        || decode('00', 'hex')
                        || recovery_evidence_canonical
                    ),
                    'hex'
                ) = recovery_evidence_sha256
                AND convert_from(recovery_evidence_canonical, 'UTF8')::jsonb
                    = recovery_evidence - 'proof_digest'
            ){v13_evidence}
        )
        """
    )
    op.execute(
        f"""
        ALTER TABLE {quarantine} ADD CONSTRAINT
            {_q('ptg_import_wave_quarantine_receipt_check')}
        CHECK (
            (
                recovery_basis IS DISTINCT FROM '{_V12_BASIS}'
                {"AND recovery_basis IS DISTINCT FROM '" + _V13_BASIS + "'" if include_v13 else ""}
                AND abandonment_receipt IS NULL
                AND abandonment_receipt_payload_digest IS NULL
                AND abandonment_receipt_issued_at IS NULL
                AND receipt_key_id IS NULL
            ) OR (
                reason = '{_V12_BASIS}'
                AND recovery_basis = '{_V12_BASIS}'
                AND recovery_evidence->>'schema_version' = '{_V12_PROOF}'
                AND receipt_key_id ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,63}}$'
                AND jsonb_typeof(abandonment_receipt) = 'object'
                AND abandonment_receipt->>'schema' = '{_ABANDONMENT}'
                AND abandonment_receipt->>'key_id' = receipt_key_id
                AND abandonment_receipt->>'payload_digest'
                    = abandonment_receipt_payload_digest
                AND length(abandonment_receipt->>'signature') = 512
                AND abandonment_receipt->>'signature' ~ '^[0-9a-f]+$'
                AND abandonment_receipt#>>'{{payload,wave_id}}'
                    = predecessor_wave_id
                AND abandonment_receipt#>>'{{payload,cutover_id}}'
                    = successor_wave_id
                AND abandonment_receipt
                    #>>'{{payload,recovery_evidence_sha256}}'
                    = recovery_evidence_sha256
                AND abandonment_receipt_payload_digest ~ '^[0-9a-f]{{64}}$'
                AND abandonment_receipt_issued_at IS NOT NULL
            ){v13_receipt}
        )
        """
    )


def _install_v13_abandonment_guard(schema: str) -> None:
    """Verify all V13 evidence at the only database write boundary."""

    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    rollback = _qt(schema, "ptg_import_wave_admission_rollback")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    canonical = _qt(schema, "ptg_wave_canonical_json_ascii_v1")
    receipt_verifier = _qt(schema, "ptg_wave_is_valid_signed_receipt_v1")
    function = _qt(schema, "ptg_import_wave_v13_abandonment_guard")
    expected_admission = _expected_admission_sql("predecessor")
    op.execute(
        f"""
        CREATE FUNCTION {function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            predecessor record;
            proof jsonb := NEW.recovery_evidence;
            admission jsonb;
            database_proof jsonb;
            kubernetes_proof jsonb;
            job_receipt jsonb;
            failure jsonb;
            retained_slots jsonb;
            redis_proof jsonb;
            redis_slots jsonb;
            receipt jsonb := NEW.abandonment_receipt;
            intent_count integer;
            run_count integer;
            pristine_run_count integer;
            claim_count integer;
            outcome_count integer;
            worker_start_event_count integer;
            expected_cutover_id text;
            expected_receipt_payload jsonb;
            expected_runtime_identity_digest text;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
            );
            LOCK TABLE {wave}, {intent}, {claim}, {outcome}, {run}, {event},
                {supersession}, {rollback} IN SHARE ROW EXCLUSIVE MODE;
            SELECT * INTO predecessor FROM {wave}
             WHERE wave_id = NEW.predecessor_wave_id FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONMENT_WAVE_MISSING'
                    USING ERRCODE = 'P0001';
            END IF;
            SELECT count(*), count(admitted.run_id), count(*) FILTER (
                WHERE admitted.engine = 'healthcare-mrf-api'
                  AND admitted.node_id IS NULL
                  AND admitted.importer = 'ptg'
                  AND admitted.family = 'pricing'
                  AND admitted.status = 'queued'
                  AND admitted.phase_detail =
                        'wave admitted; controller materialization pending'
                  AND admitted.params::jsonb = member.params::jsonb
                  AND admitted.idempotency_key = member.run_idempotency_key
                  AND admitted.triggered_by = 'api'
                  AND admitted.schedule_id IS NULL
                  AND admitted.subscription_id IS NULL
                  AND admitted.source_file_import_id
                        = member.source_file_import_id
                  AND admitted.created_at = predecessor.created_at
                  AND admitted.started_at IS NULL
                  AND admitted.finished_at IS NULL
                  AND admitted.heartbeat_at = predecessor.created_at
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
                  )
                  AND admitted.error IS NULL
                  AND admitted.snapshot_id IS NULL
                  AND admitted.import_id = member.source_file_import_id
                  AND admitted.retry_of_run_id IS NULL
            ) INTO intent_count, run_count, pristine_run_count
              FROM {intent} AS member
              LEFT JOIN {run} AS admitted ON admitted.run_id = member.run_id
             WHERE member.wave_id = predecessor.wave_id;
            SELECT count(*) INTO claim_count FROM {claim}
             WHERE wave_id = predecessor.wave_id;
            SELECT count(*) INTO outcome_count FROM {outcome}
             WHERE wave_id = predecessor.wave_id;
            SELECT count(*) INTO worker_start_event_count
              FROM {event} AS started
              JOIN {intent} AS member ON member.run_id = started.outer_run_id
             WHERE member.wave_id = predecessor.wave_id
               AND started.event_kind = 'worker_start_admitted';
            expected_cutover_id := encode(
                sha256(convert_to(
                    'ptg-ordinary-cutover-id-v1:' || predecessor.wave_id,
                    'UTF8'
                )),
                'hex'
            );
            admission := proof->'admission';
            database_proof := proof->'database';
            kubernetes_proof := proof->'kubernetes';
            redis_proof := proof->'redis';

            IF NEW.reason IS DISTINCT FROM '{_V13_BASIS}'
               OR NEW.recovery_basis IS DISTINCT FROM '{_V13_BASIS}'
               OR NEW.successor_wave_id IS DISTINCT FROM expected_cutover_id
               OR predecessor.state IS DISTINCT FROM 'slots_waiting'
               OR predecessor.uncertainty_resume_state IS NOT NULL
               OR predecessor.worker_limit IS DISTINCT FROM 12
               OR predecessor.intent_count NOT BETWEEN 1 AND 4096
               OR predecessor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_V6}'
               OR predecessor.receipt_key_id IS DISTINCT FROM NEW.receipt_key_id
               OR predecessor.k8s_post_ticket IS NULL
               OR predecessor.k8s_post_started_at IS NULL
               OR predecessor.kubernetes_job_uid IS NULL
               OR btrim(predecessor.kubernetes_job_uid) = ''
               OR btrim(predecessor.kubernetes_job_uid)
                    IS DISTINCT FROM predecessor.kubernetes_job_uid
               OR length(predecessor.kubernetes_job_uid) > 128
               OR predecessor.kubernetes_job_receipt_digest
                    !~ '^[0-9a-f]{{64}}$'
               OR predecessor.kubernetes_manifest IS NULL
               OR jsonb_typeof(predecessor.kubernetes_manifest::jsonb)
                    IS DISTINCT FROM 'object'
               OR predecessor.kubernetes_manifest_bytes IS NULL
               OR predecessor.kubernetes_manifest_sha256
                    !~ '^[0-9a-f]{{64}}$'
               OR encode(sha256(predecessor.kubernetes_manifest_bytes), 'hex')
                    IS DISTINCT FROM predecessor.kubernetes_manifest_sha256
               OR convert_from(predecessor.kubernetes_manifest_bytes, 'UTF8')::jsonb
                    IS DISTINCT FROM predecessor.kubernetes_manifest::jsonb
               OR (predecessor.kubernetes_manifest_identity
                    ~ '^[0-9a-f]{{64}}$') IS DISTINCT FROM TRUE
               OR (predecessor.kubernetes_config_identity
                    ~ '^[0-9a-f]{{64}}$') IS DISTINCT FROM TRUE
               OR (predecessor.pinned_image_reference
                    ~ '^[^[:space:]]+@sha256:[0-9a-f]{{64}}$')
                    IS DISTINCT FROM TRUE
               OR (predecessor.pinned_image_digest
                    ~ '^[0-9a-f]{{64}}$') IS DISTINCT FROM TRUE
               OR right(predecessor.pinned_image_reference, 64)
                    IS DISTINCT FROM predecessor.pinned_image_digest
               OR (predecessor.runtime_image_identity
                    ~ '^sha256:[0-9a-f]{{64}}$') IS DISTINCT FROM TRUE
               OR predecessor.kubernetes_job_receipt::jsonb
                    IS DISTINCT FROM jsonb_build_object(
                        'wave_digest', predecessor.wave_digest,
                        'job_uid', predecessor.kubernetes_job_uid,
                        'manifest_identity', predecessor.kubernetes_manifest_identity,
                        'config_identity', predecessor.kubernetes_config_identity,
                        'pinned_image_reference', predecessor.pinned_image_reference,
                        'pinned_image_digest', predecessor.pinned_image_digest,
                        'runtime_image_identity', predecessor.runtime_image_identity
                    )
               OR predecessor.kubernetes_job_receipt_digest IS DISTINCT FROM encode(
                    sha256(convert_to(
                        {canonical}(predecessor.kubernetes_job_receipt::jsonb),
                        'UTF8'
                    )),
                    'hex'
               )
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
               OR predecessor.linkage_receipt IS NOT NULL
               OR predecessor.linkage_receipt_payload_digest IS NOT NULL
               OR predecessor.linkage_receipt_issued_at IS NOT NULL
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
               OR intent_count IS DISTINCT FROM predecessor.intent_count
               OR run_count IS DISTINCT FROM predecessor.intent_count
               OR pristine_run_count IS DISTINCT FROM predecessor.intent_count
               OR claim_count <> 0
               OR outcome_count <> 0
               OR worker_start_event_count <> 0
               OR EXISTS (
                    SELECT 1 FROM {supersession}
                     WHERE predecessor_wave_id = predecessor.wave_id
                        OR successor_wave_id = predecessor.wave_id
               )
               OR EXISTS (
                    SELECT 1 FROM {rollback}
                     WHERE predecessor_wave_id = predecessor.wave_id
                        OR successor_wave_id = predecessor.wave_id
               ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONMENT_NOT_PRISTINE'
                    USING ERRCODE = 'P0001';
            END IF;

            IF jsonb_typeof(proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(proof)) <> 9
               OR proof - ARRAY[
                    'schema_version', 'recovery_basis', 'operation_id',
                    'cutover_id', 'admission', 'database', 'kubernetes',
                    'redis', 'proof_digest'
               ]::text[] <> '{{}}'::jsonb
               OR proof->>'schema_version' IS DISTINCT FROM '{_V13_PROOF}'
               OR proof->>'recovery_basis' IS DISTINCT FROM '{_V13_BASIS}'
               OR proof->>'operation_id' IS DISTINCT FROM predecessor.wave_id
               OR proof->>'cutover_id' IS DISTINCT FROM expected_cutover_id
               OR proof->>'proof_digest'
                    IS DISTINCT FROM NEW.recovery_evidence_sha256
               OR proof->>'proof_digest' !~ '^[0-9a-f]{{64}}$'
               OR proof->>'proof_digest' IS DISTINCT FROM encode(
                    sha256(
                        convert_to('{_V13_PROOF}', 'UTF8')
                        || decode('00', 'hex')
                        || convert_to({canonical}(proof - 'proof_digest'), 'UTF8')
                    ),
                    'hex'
               )
               OR admission IS DISTINCT FROM ({expected_admission}) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONMENT_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            IF jsonb_typeof(database_proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(database_proof)) <> 11
               OR database_proof - ARRAY[
                    'state', 'intent_count', 'run_count',
                    'pristine_run_count', 'unassigned_run_count',
                    'claim_count', 'outcome_count',
                    'worker_start_event_count', 'member_rows_digest',
                    'intent_rows_digest', 'run_rows_digest'
               ]::text[] <> '{{}}'::jsonb
               OR database_proof->>'state' IS DISTINCT FROM 'slots_waiting'
               OR database_proof->>'intent_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'run_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'pristine_run_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'unassigned_run_count'
                    IS DISTINCT FROM predecessor.intent_count::text
               OR database_proof->>'claim_count' <> '0'
               OR database_proof->>'outcome_count' <> '0'
               OR database_proof->>'worker_start_event_count' <> '0'
               OR database_proof->>'member_rows_digest' !~ '^[0-9a-f]{{64}}$'
               OR database_proof->>'intent_rows_digest' !~ '^[0-9a-f]{{64}}$'
               OR database_proof->>'run_rows_digest' !~ '^[0-9a-f]{{64}}$' THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_DATABASE_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            job_receipt := kubernetes_proof->'job_receipt';
            IF jsonb_typeof(kubernetes_proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(kubernetes_proof)) <> 5
               OR kubernetes_proof - ARRAY[
                    'job_receipt', 'job_receipt_digest', 'ready_attestation',
                    'ready_attestation_digest', 'failure'
               ]::text[] <> '{{}}'::jsonb
               OR job_receipt IS DISTINCT FROM predecessor.kubernetes_job_receipt::jsonb
               OR kubernetes_proof->>'job_receipt_digest'
                    IS DISTINCT FROM predecessor.kubernetes_job_receipt_digest
               OR kubernetes_proof->>'job_receipt_digest' IS DISTINCT FROM encode(
                    sha256(convert_to({canonical}(job_receipt), 'UTF8')), 'hex'
               )
               OR kubernetes_proof->'ready_attestation'
                    IS DISTINCT FROM 'null'::jsonb
               OR kubernetes_proof->'ready_attestation_digest'
                    IS DISTINCT FROM 'null'::jsonb THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_KUBERNETES_RECEIPT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            failure := kubernetes_proof->'failure';
            retained_slots := failure->'retained_failed_slots';
            IF jsonb_typeof(failure) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(failure)) <> 26
               OR failure - ARRAY[
                    'schema_version', 'wave_digest', 'queue', 'manifest_digest',
                    'jobs_digest', 'job_count', 'config_identity',
                    'manifest_identity', 'image_identity',
                    'runtime_image_identity', 'job_name', 'job_uid',
                    'backoff_limit', 'job_active', 'job_failed',
                    'job_succeeded', 'job_ready', 'job_terminating',
                    'completed_indexes', 'failed_indexes', 'completion_time',
                    'start_time', 'uncounted_terminated_pods',
                    'job_conditions', 'retained_failed_slots',
                    'attestation_digest'
               ]::text[] <> '{{}}'::jsonb
               OR failure->>'schema_version' IS DISTINCT FROM '{_RETAINED_FAILURE}'
               OR failure->'wave_digest' IS DISTINCT FROM job_receipt->'wave_digest'
               OR failure->>'queue' IS DISTINCT FROM predecessor.release_queue
               OR failure->'manifest_digest' IS DISTINCT FROM admission->'manifest_digest'
               OR failure->'jobs_digest' IS DISTINCT FROM admission->'jobs_digest'
               OR failure->'job_count'
                    IS DISTINCT FROM to_jsonb(predecessor.intent_count)
               OR failure->'config_identity'
                    IS DISTINCT FROM job_receipt->'config_identity'
               OR failure->'manifest_identity'
                    IS DISTINCT FROM job_receipt->'manifest_identity'
               OR failure->'image_identity'
                    IS DISTINCT FROM job_receipt->'pinned_image_reference'
               OR failure->'runtime_image_identity'
                    IS DISTINCT FROM job_receipt->'runtime_image_identity'
               OR failure->>'job_name' IS DISTINCT FROM
                    'hpw-ptg-wave-' || left(predecessor.wave_digest, 40)
               OR failure->'job_uid' IS DISTINCT FROM job_receipt->'job_uid'
               OR failure->'backoff_limit' IS DISTINCT FROM '0'::jsonb
               OR failure->'job_active' IS DISTINCT FROM 'null'::jsonb
               OR failure->'job_failed' IS DISTINCT FROM '12'::jsonb
               OR failure->'job_succeeded' IS DISTINCT FROM 'null'::jsonb
               OR failure->'job_ready' IS DISTINCT FROM '0'::jsonb
               OR failure->'job_terminating' IS DISTINCT FROM '0'::jsonb
               OR failure->'completed_indexes' IS DISTINCT FROM 'null'::jsonb
               OR failure->'failed_indexes' IS DISTINCT FROM 'null'::jsonb
               OR failure->'completion_time' IS DISTINCT FROM 'null'::jsonb
               OR failure->'uncounted_terminated_pods'
                    IS DISTINCT FROM jsonb_build_object()
               OR jsonb_typeof(failure->'start_time') IS DISTINCT FROM 'string'
               OR failure->>'start_time' !~
                    '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$'
               OR failure->>'attestation_digest' IS DISTINCT FROM encode(
                    sha256(convert_to(
                        {canonical}(failure - 'attestation_digest'), 'UTF8'
                    )),
                    'hex'
               ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_FAILURE_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            IF jsonb_typeof(failure->'job_conditions') IS DISTINCT FROM 'array'
               OR jsonb_array_length(failure->'job_conditions') <> 2
               OR failure->'job_conditions'->0->>'type' IS DISTINCT FROM 'Failed'
               OR failure->'job_conditions'->1->>'type'
                    IS DISTINCT FROM 'FailureTarget'
               OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements(failure->'job_conditions') AS condition(value)
                     WHERE jsonb_typeof(condition.value) IS DISTINCT FROM 'object'
                        OR (SELECT count(*) FROM jsonb_object_keys(condition.value)) <> 6
                        OR condition.value - ARRAY[
                            'type', 'status', 'reason', 'message',
                            'last_probe_time', 'last_transition_time'
                        ]::text[] <> '{{}}'::jsonb
                        OR condition.value->>'status' IS DISTINCT FROM 'True'
                        OR condition.value->>'reason'
                            IS DISTINCT FROM 'BackoffLimitExceeded'
                        OR condition.value->>'message'
                            IS DISTINCT FROM 'Job has reached the specified backoff limit'
                        OR jsonb_typeof(condition.value->'last_probe_time')
                            IS DISTINCT FROM 'string'
                        OR jsonb_typeof(condition.value->'last_transition_time')
                            IS DISTINCT FROM 'string'
                        OR condition.value->>'last_probe_time' !~
                            '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$'
                        OR condition.value->>'last_transition_time' !~
                            '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$'
               ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_JOB_CONDITIONS_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            IF jsonb_typeof(retained_slots) IS DISTINCT FROM 'array'
               OR jsonb_array_length(retained_slots) NOT BETWEEN 1 AND 12
               OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements(retained_slots) AS retained(value)
                     WHERE jsonb_typeof(retained.value) IS DISTINCT FROM 'object'
                        OR (SELECT count(*) FROM jsonb_object_keys(retained.value)) <> 5
                        OR retained.value - ARRAY[
                            'slot', 'pod_uid', 'phase',
                            'runtime_image_identity', 'termination'
                        ]::text[] <> '{{}}'::jsonb
                        OR jsonb_typeof(retained.value->'slot')
                            IS DISTINCT FROM 'number'
                        OR retained.value->>'slot' NOT IN (
                            '0', '1', '2', '3', '4', '5',
                            '6', '7', '8', '9', '10', '11'
                        )
                        OR jsonb_typeof(retained.value->'pod_uid')
                            IS DISTINCT FROM 'string'
                        OR btrim(retained.value->>'pod_uid') = ''
                        OR btrim(retained.value->>'pod_uid')
                            IS DISTINCT FROM retained.value->>'pod_uid'
                        OR length(retained.value->>'pod_uid') > 512
                        OR retained.value->>'phase' IS DISTINCT FROM 'Failed'
                        OR retained.value->'runtime_image_identity'
                            IS DISTINCT FROM failure->'runtime_image_identity'
                        OR jsonb_typeof(retained.value->'termination')
                            IS DISTINCT FROM 'object'
                        OR (SELECT count(*) FROM jsonb_object_keys(
                                retained.value->'termination'
                           )) <> 5
                        OR (retained.value->'termination') - ARRAY[
                            'container_id', 'reason', 'exit_code',
                            'started_at', 'finished_at'
                        ]::text[] <> '{{}}'::jsonb
                        OR jsonb_typeof(retained.value->'termination'->'container_id')
                            IS DISTINCT FROM 'string'
                        OR btrim(retained.value->'termination'->>'container_id') = ''
                        OR btrim(retained.value->'termination'->>'container_id')
                            IS DISTINCT FROM retained.value->'termination'->>'container_id'
                        OR retained.value->'termination'->>'reason'
                            IS DISTINCT FROM 'Error'
                        OR retained.value->'termination'->'exit_code'
                            IS DISTINCT FROM '1'::jsonb
                        OR jsonb_typeof(retained.value->'termination'->'started_at')
                            IS DISTINCT FROM 'string'
                        OR jsonb_typeof(retained.value->'termination'->'finished_at')
                            IS DISTINCT FROM 'string'
                        OR retained.value->'termination'->>'started_at' !~
                            '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$'
                        OR retained.value->'termination'->>'finished_at' !~
                            '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}Z$'
               )
               OR (SELECT count(DISTINCT retained.value->>'slot')
                     FROM jsonb_array_elements(retained_slots) AS retained(value))
                    <> jsonb_array_length(retained_slots) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_RETAINED_PODS_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            redis_slots := redis_proof->'ready_slots';
            expected_runtime_identity_digest := encode(
                sha256(convert_to(
                    {canonical}(jsonb_build_object(
                        'schema_version', 1,
                        'config_identity', job_receipt->'config_identity',
                        'manifest_identity', job_receipt->'manifest_identity',
                        'image_identity', job_receipt->'pinned_image_reference',
                        'runtime_image_identity', job_receipt->'runtime_image_identity'
                    )),
                    'UTF8'
                )),
                'hex'
            );
            IF jsonb_typeof(redis_proof) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(redis_proof)) <> 19
               OR redis_proof - ARRAY[
                    'schema_version', 'wave_id', 'queue_name', 'manifest_digest',
                    'jobs_digest', 'job_count', 'target_key_count', 'ready_slots',
                    'ready_slots_digest', 'release_present', 'release_digest',
                    'release_receipt', 'queued_ordinals', 'job_ordinals',
                    'result_ordinals', 'retry_ordinals', 'in_progress_ordinals',
                    'health_check_present', 'attestation_digest'
               ]::text[] <> '{{}}'::jsonb
               OR redis_proof->>'schema_version' IS DISTINCT FROM '{_REDIS_FAILURE}'
               OR redis_proof->'wave_id' IS DISTINCT FROM job_receipt->'wave_digest'
               OR redis_proof->'queue_name' IS DISTINCT FROM failure->'queue'
               OR redis_proof->'manifest_digest' IS DISTINCT FROM admission->'manifest_digest'
               OR redis_proof->'jobs_digest' IS DISTINCT FROM admission->'jobs_digest'
               OR redis_proof->'job_count'
                    IS DISTINCT FROM to_jsonb(predecessor.intent_count)
               OR redis_proof->'target_key_count'
                    IS DISTINCT FROM to_jsonb(4 + 4 * predecessor.intent_count)
               OR redis_proof->'release_present' IS DISTINCT FROM 'false'::jsonb
               OR redis_proof->'release_digest' IS DISTINCT FROM 'null'::jsonb
               OR redis_proof->'release_receipt' IS DISTINCT FROM 'null'::jsonb
               OR redis_proof->'queued_ordinals' IS DISTINCT FROM jsonb_build_array()
               OR redis_proof->'job_ordinals' IS DISTINCT FROM jsonb_build_array()
               OR redis_proof->'result_ordinals' IS DISTINCT FROM jsonb_build_array()
               OR redis_proof->'retry_ordinals' IS DISTINCT FROM jsonb_build_array()
               OR redis_proof->'in_progress_ordinals'
                    IS DISTINCT FROM jsonb_build_array()
               OR redis_proof->'health_check_present' IS DISTINCT FROM 'false'::jsonb
               OR jsonb_typeof(redis_slots) IS DISTINCT FROM 'array'
               OR jsonb_array_length(redis_slots) <> 12
               OR redis_proof->>'ready_slots_digest' IS DISTINCT FROM encode(
                    sha256(convert_to({canonical}(redis_slots), 'UTF8')), 'hex'
               )
               OR redis_proof->>'attestation_digest' IS DISTINCT FROM encode(
                    sha256(convert_to(
                        {canonical}(redis_proof - 'attestation_digest'), 'UTF8'
                    )),
                    'hex'
               ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_REDIS_PROOF_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            IF EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements(redis_slots) AS slot(value)
                     WHERE jsonb_typeof(slot.value) IS DISTINCT FROM 'object'
                        OR (SELECT count(*) FROM jsonb_object_keys(slot.value)) <> 14
                        OR slot.value - ARRAY[
                            'config_identity', 'kubernetes_manifest_identity',
                            'image_identity', 'runtime_image_identity',
                            'runtime_identity_digest', 'manifest_digest',
                            'pod_uid', 'queue_name', 'slot', 'wave_id',
                            'jobs_digest', 'job_count', 'protocol_identity',
                            'serializer_identity'
                        ]::text[] <> '{{}}'::jsonb
                        OR slot.value->'config_identity'
                            IS DISTINCT FROM job_receipt->'config_identity'
                        OR slot.value->'kubernetes_manifest_identity'
                            IS DISTINCT FROM job_receipt->'manifest_identity'
                        OR slot.value->'image_identity'
                            IS DISTINCT FROM job_receipt->'pinned_image_reference'
                        OR slot.value->'runtime_image_identity'
                            IS DISTINCT FROM job_receipt->'runtime_image_identity'
                        OR slot.value->>'runtime_identity_digest'
                            IS DISTINCT FROM expected_runtime_identity_digest
                        OR slot.value->'manifest_digest'
                            IS DISTINCT FROM admission->'manifest_digest'
                        OR jsonb_typeof(slot.value->'pod_uid')
                            IS DISTINCT FROM 'string'
                        OR btrim(slot.value->>'pod_uid') = ''
                        OR btrim(slot.value->>'pod_uid')
                            IS DISTINCT FROM slot.value->>'pod_uid'
                        OR length(slot.value->>'pod_uid') > 512
                        OR slot.value->'queue_name' IS DISTINCT FROM failure->'queue'
                        OR jsonb_typeof(slot.value->'slot')
                            IS DISTINCT FROM 'number'
                        OR slot.value->>'slot' NOT IN (
                            '0', '1', '2', '3', '4', '5',
                            '6', '7', '8', '9', '10', '11'
                        )
                        OR slot.value->'wave_id' IS DISTINCT FROM job_receipt->'wave_digest'
                        OR slot.value->'jobs_digest' IS DISTINCT FROM admission->'jobs_digest'
                        OR slot.value->>'job_count'
                            IS DISTINCT FROM lpad(predecessor.intent_count::text, 4, '0')
                        OR slot.value->>'protocol_identity'
                            IS DISTINCT FROM 'healthporta.ptg-small.exact-wave.v1'
                        OR slot.value->>'serializer_identity'
                            IS DISTINCT FROM 'arq-0.28.process-msgpack.v1'
               )
               OR (SELECT count(DISTINCT slot.value->>'slot')
                     FROM jsonb_array_elements(redis_slots) AS slot(value)) <> 12
               OR EXISTS (
                    SELECT 1
                      FROM jsonb_array_elements(retained_slots) AS retained(value)
                      LEFT JOIN jsonb_array_elements(redis_slots) AS slot(value)
                        ON slot.value->>'slot' = retained.value->>'slot'
                     WHERE slot.value->>'pod_uid'
                            IS DISTINCT FROM retained.value->>'pod_uid'
               ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_REDIS_SLOTS_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;

            expected_receipt_payload := jsonb_build_object(
                'operation_id', predecessor.wave_id,
                'cutover_id', expected_cutover_id,
                'wave_id', predecessor.wave_id,
                'wave_digest', predecessor.wave_digest,
                'state', 'abandoned',
                'quarantine_reason', '{_V13_BASIS}',
                'recovery_schema', '{_V13_PROOF}',
                'recovery_evidence_sha256', NEW.recovery_evidence_sha256,
                'admission', admission,
                'database', database_proof,
                'kubernetes', kubernetes_proof,
                'redis', redis_proof
            );
            IF jsonb_typeof(receipt) IS DISTINCT FROM 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(receipt)) <> 6
               OR receipt - ARRAY[
                    'schema', 'key_id', 'issued_at', 'payload',
                    'payload_digest', 'signature'
               ]::text[] <> '{{}}'::jsonb
               OR receipt->>'schema' IS DISTINCT FROM '{_ABANDONMENT}'
               OR receipt->>'key_id' IS DISTINCT FROM NEW.receipt_key_id
               OR receipt->'payload' IS DISTINCT FROM expected_receipt_payload
               OR {receipt_verifier}(
                    receipt,
                    '{_ABANDONMENT}',
                    expected_receipt_payload,
                    predecessor.receipt_key_id,
                    predecessor.receipt_public_modulus_hex,
                    predecessor.receipt_public_exponent
               ) IS DISTINCT FROM TRUE
               OR receipt->>'payload_digest'
                    IS DISTINCT FROM NEW.abandonment_receipt_payload_digest
               OR receipt->>'payload_digest' !~ '^[0-9a-f]{{64}}$'
               OR length(receipt->>'signature') IS DISTINCT FROM 512
               OR receipt->>'signature' !~ '^[0-9a-f]+$'
               OR receipt->>'issued_at'
                    !~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}T[0-9]{{2}}:[0-9]{{2}}:[0-9]{{2}}\\.[0-9]{{6}}Z$'
               OR to_char(
                    NEW.abandonment_receipt_issued_at AT TIME ZONE 'UTC',
                    'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'
               ) IS DISTINCT FROM receipt->>'issued_at'
               OR NEW.created_at
                    IS DISTINCT FROM NEW.abandonment_receipt_issued_at THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_RECEIPT_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_v13_abandonment_guard')} "
        f"BEFORE INSERT ON {quarantine} FOR EACH ROW WHEN "
        f"(NEW.recovery_basis = '{_V13_BASIS}') EXECUTE FUNCTION {function}()"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_v13_abandonment_guard')}"
    )


def _install_v13_immutability_guards(schema: str) -> None:
    """Freeze V13's exact predecessor rows after its only allowed write."""

    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    intent = _qt(schema, "ptg_import_wave_intent")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    wave = _qt(schema, "ptg_import_wave")
    child_function = _qt(schema, "ptg_import_wave_v13_abandoned_child_guard")
    run_function = _qt(schema, "ptg_import_wave_v13_abandoned_run_guard")
    event_function = _qt(schema, "ptg_import_wave_v13_abandoned_event_guard")
    truncate_function = _qt(schema, "ptg_import_wave_v13_abandoned_truncate_guard")
    op.execute(
        f"""
        CREATE FUNCTION {child_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            old_wave_id text;
            new_wave_id text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN old_wave_id := OLD.wave_id; END IF;
            IF TG_OP <> 'DELETE' THEN new_wave_id := NEW.wave_id; END IF;
            IF EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_V13_BASIS}'
                   AND predecessor_wave_id IN (old_wave_id, new_wave_id)
            ) THEN
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
                );
                IF EXISTS (
                    SELECT 1 FROM {quarantine}
                     WHERE recovery_basis = '{_V13_BASIS}'
                       AND predecessor_wave_id IN (old_wave_id, new_wave_id)
                ) THEN
                    RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONED_IMMUTABLE'
                        USING ERRCODE = 'P0001';
                END IF;
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
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
        trigger = _q(f"{table_name}_v13_abandoned_guard")
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {table} FOR EACH ROW EXECUTE FUNCTION {child_function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")

    op.execute(
        f"""
        CREATE FUNCTION {run_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            candidate_run_ids text[] := ARRAY[]::text[];
            candidate_wave_ids text[] := ARRAY[]::text[];
            candidate_wave_digests text[] := ARRAY[]::text[];
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                candidate_run_ids := array_append(candidate_run_ids, OLD.run_id);
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
                candidate_run_ids := array_append(candidate_run_ids, NEW.run_id);
                candidate_wave_ids := candidate_wave_ids || ARRAY[
                    NEW.params::jsonb->>'_wave_id',
                    NEW.metrics::jsonb->>'wave_id'
                ];
                candidate_wave_digests := candidate_wave_digests || ARRAY[
                    NEW.params::jsonb->>'_wave_digest',
                    NEW.metrics::jsonb->>'wave_digest'
                ];
            END IF;
            IF EXISTS (
                SELECT 1 FROM {quarantine} AS retired
                JOIN {wave} AS predecessor
                  ON predecessor.wave_id = retired.predecessor_wave_id
               WHERE retired.recovery_basis = '{_V13_BASIS}'
                 AND (
                    retired.predecessor_wave_id = ANY(candidate_wave_ids)
                    OR predecessor.wave_digest = ANY(candidate_wave_digests)
                    OR EXISTS (
                        SELECT 1 FROM {intent} AS member
                         WHERE member.wave_id = retired.predecessor_wave_id
                           AND member.run_id = ANY(candidate_run_ids)
                    )
                 )
            ) THEN
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
                );
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_v13_abandoned_run_guard')} "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {run} FOR EACH ROW "
        f"EXECUTE FUNCTION {run_function}()"
    )
    op.execute(
        f"ALTER TABLE {run} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_v13_abandoned_run_guard')}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {event_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE
            old_run_id text;
            new_run_id text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN old_run_id := OLD.outer_run_id; END IF;
            IF TG_OP <> 'DELETE' THEN new_run_id := NEW.outer_run_id; END IF;
            IF EXISTS (
                SELECT 1 FROM {intent} AS member
                JOIN {quarantine} AS retired
                  ON retired.predecessor_wave_id = member.wave_id
                 AND retired.recovery_basis = '{_V13_BASIS}'
               WHERE member.run_id IN (old_run_id, new_run_id)
            ) THEN
                PERFORM pg_catalog.pg_advisory_xact_lock(
                    pg_catalog.hashtextextended('{_ADMISSION_LOCK}', 0)
                );
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q('ptg_import_wave_v13_abandoned_event_guard')} "
        f"BEFORE INSERT OR UPDATE OR DELETE ON {event} FOR EACH ROW "
        f"EXECUTE FUNCTION {event_function}()"
    )
    op.execute(
        f"ALTER TABLE {event} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_import_wave_v13_abandoned_event_guard')}"
    )

    op.execute(
        f"""
        CREATE FUNCTION {truncate_function}()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_V13_BASIS}'
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_ABANDONED_IMMUTABLE'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    ):
        table = _qt(schema, table_name)
        trigger = _q(f"{table_name}_v13_abandoned_truncate_guard")
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE TRUNCATE ON {table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {truncate_function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")


def upgrade() -> None:
    """Install the closed V13 proof family without changing stored shape."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    rollback = _qt(schema, "ptg_import_wave_admission_rollback")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        f"pg_catalog.hashtextextended({_literal(_ADMISSION_LOCK)}, 0))"
    )
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession}, {rollback}, "
        f"{intent}, {claim}, {outcome}, {run}, {event} "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    _replace_ordinary_terminal_receipt_guard(
        schema=schema,
        old_predicate=_ORDINARY_TERMINAL_V12_PREDICATE,
        new_predicate=_ORDINARY_TERMINAL_V13_PREDICATE,
    )
    _replace_quarantine_constraints(schema=schema, include_v13=True)
    _install_v13_abandonment_guard(schema)
    _install_v13_immutability_guards(schema)
    _replace_effective_owner_function(schema=schema, include_v13=True)


def downgrade() -> None:
    """Remove V13 only before a V13 quarantine has been persisted."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        "SELECT pg_catalog.pg_advisory_xact_lock("
        f"pg_catalog.hashtextextended({_literal(_ADMISSION_LOCK)}, 0))"
    )
    op.execute(
        f"LOCK TABLE {wave}, {quarantine} IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_V13_BASIS}'
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_V13_DOWNGRADE_BLOCKED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    _replace_ordinary_terminal_receipt_guard(
        schema=schema,
        old_predicate=_ORDINARY_TERMINAL_V13_PREDICATE,
        new_predicate=_ORDINARY_TERMINAL_V12_PREDICATE,
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    ):
        op.execute(
            f"DROP TRIGGER {_q(f'{table_name}_v13_abandoned_truncate_guard')} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP FUNCTION "
        f"{_qt(schema, 'ptg_import_wave_v13_abandoned_truncate_guard')}()"
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
    ):
        op.execute(
            f"DROP TRIGGER {_q(f'{table_name}_v13_abandoned_guard')} "
            f"ON {_qt(schema, table_name)}"
        )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_v13_abandoned_run_guard')} "
        f"ON {_qt(schema, 'import_run')}"
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_v13_abandoned_event_guard')} "
        f"ON {_qt(schema, 'ptg_source_attempt_event')}"
    )
    for function_name in (
        "ptg_import_wave_v13_abandoned_child_guard",
        "ptg_import_wave_v13_abandoned_run_guard",
        "ptg_import_wave_v13_abandoned_event_guard",
    ):
        op.execute(f"DROP FUNCTION {_qt(schema, function_name)}()")
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_v13_abandonment_guard')} "
        f"ON {quarantine}"
    )
    op.execute(
        f"DROP FUNCTION {_qt(schema, 'ptg_import_wave_v13_abandonment_guard')}()"
    )
    _replace_quarantine_constraints(schema=schema, include_v13=False)
    _replace_effective_owner_function(schema=schema, include_v13=False)
