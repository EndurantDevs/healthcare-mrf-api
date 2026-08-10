"""Add audited materialized-wave abandonment for ordinary PTG admission.

Revision ID: 20260809040000_ptg_import_wave_ordinary_cutover
Revises: 20260810090000_provider_directory_terminal_root_retirement

The cutover preserves the wave, intents, and queued runs.  It adds one
append-only quarantine record whose proof is validated by the existing
materialized-preclaim guard; no successor wave or supersession row is made.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260809040000_ptg_import_wave_ordinary_cutover"
down_revision = (
    "20260810090000_provider_directory_terminal_root_retirement"
)
branch_labels = None
depends_on = None

_BASIS = "materialized_preclaim_failure"
_ADMISSION_LOCK = "import-run-admission:ptg-source-file"
_EFFECTIVE_OWNER_FUNCTION = "ptg_import_wave_effective_owner_guard"
_EXISTING_PROOF_GUARD_FUNCTION = "ptg_import_wave_materialized_preclaim_guard"
_PROOF_TRIGGER = "ptg_import_wave_abandonment_proof_guard"
_UNASSIGNED_FUNCTION = "ptg_import_wave_abandonment_unassigned_guard"
_UNASSIGNED_TRIGGER = "ptg_import_wave_abandonment_unassigned_guard"
_CHILD_FUNCTION = "ptg_import_wave_abandonment_child_guard"
_RUN_FUNCTION = "ptg_import_wave_abandonment_run_guard"
_EVENT_FUNCTION = "ptg_import_wave_abandonment_event_guard"
_TRUNCATE_FUNCTION = "ptg_import_wave_abandonment_truncate_guard"
_CAPACITY_STATES = (
    "admitted", "materializing", "slots_waiting", "redis_releasing",
    "released", "executing", "awaiting_linkage", "terminalizing",
    "cleaning", "uncertain",
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


def _replace_effective_owner_function(
    *,
    function: str,
    wave: str,
    quarantine: str,
    supersession: str,
    schema: str,
    include_abandonment: bool,
) -> None:
    capacity_states = ", ".join(
        _literal(state) for state in _CAPACITY_STATES
    )
    abandonment_exclusion = ""
    if include_abandonment:
        abandonment_exclusion = f"""
              AND NOT EXISTS (
                  SELECT 1
                    FROM {quarantine} AS abandoned
                   WHERE abandoned.predecessor_wave_id = candidate.wave_id
                     AND abandoned.recovery_basis = '{_BASIS}'
              )
        """
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
             WHERE candidate.state IN ({capacity_states})
               AND NOT EXISTS (
                   SELECT 1
                     FROM {supersession} AS retired
                    WHERE retired.predecessor_wave_id = candidate.wave_id
               )
               {abandonment_exclusion};
            IF effective_owner_count > 1 THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_EFFECTIVE_OWNER_CONFLICT'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )


def upgrade() -> None:
    """Install one immutable, proof-bound non-wave cutover."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    proof_guard_function = _qt(schema, _EXISTING_PROOF_GUARD_FUNCTION)
    unassigned_function = _qt(schema, _UNASSIGNED_FUNCTION)
    child_function = _qt(schema, _CHILD_FUNCTION)
    run_function = _qt(schema, _RUN_FUNCTION)
    event_function = _qt(schema, _EVENT_FUNCTION)
    truncate_function = _qt(schema, _TRUNCATE_FUNCTION)
    effective_owner_function = _qt(schema, _EFFECTIVE_OWNER_FUNCTION)

    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession}, {intent}, "
        f"{claim}, {outcome}, {run}, {event} IN SHARE ROW EXCLUSIVE MODE"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD COLUMN successor_wave_id varchar(64)"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD COLUMN recovery_basis varchar(64)"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD COLUMN recovery_evidence jsonb"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD COLUMN recovery_evidence_canonical bytea"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD COLUMN recovery_evidence_sha256 varchar(64)"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ADD CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_cutover_id_key')} "
        "UNIQUE (successor_wave_id)"
    )
    op.execute(
        f"""
        ALTER TABLE {quarantine} ADD CONSTRAINT
            {_q('ptg_import_wave_quarantine_abandonment_evidence_check')}
        CHECK (
            (
                recovery_basis IS NULL
                AND successor_wave_id IS NULL
                AND recovery_evidence IS NULL
                AND recovery_evidence_canonical IS NULL
                AND recovery_evidence_sha256 IS NULL
            ) OR (
                reason = '{_BASIS}'
                AND recovery_basis = '{_BASIS}'
                AND successor_wave_id IS NOT NULL
                AND successor_wave_id <> predecessor_wave_id
                AND jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(sha256(recovery_evidence_canonical), 'hex')
                    = recovery_evidence_sha256
                AND convert_from(
                    recovery_evidence_canonical,
                    'UTF8'
                )::jsonb = recovery_evidence - 'proof_digest'
            )
        )
        """
    )

    # Reuse the deployed full materialized-preclaim proof guard.  The legacy
    # proof field named successor_wave_id contains the stable cutover identity,
    # but this quarantine column deliberately has no FK to ptg_import_wave.
    op.execute(
        f"CREATE TRIGGER {_q(_PROOF_TRIGGER)} AFTER INSERT ON {quarantine} "
        f"FOR EACH ROW WHEN (NEW.recovery_basis = '{_BASIS}') "
        f"EXECUTE FUNCTION {proof_guard_function}()"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ENABLE ALWAYS TRIGGER {_q(_PROOF_TRIGGER)}"
    )

    # Exact proof validation predates node assignment.  Ordinary cutover adds
    # the stricter requirement that every retained wave run is unassigned.
    op.execute(
        f"""
        CREATE FUNCTION {unassigned_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF EXISTS (
                SELECT 1
                  FROM {intent} AS member
                  JOIN {run} AS admitted ON admitted.run_id = member.run_id
                 WHERE member.wave_id = NEW.predecessor_wave_id
                   AND admitted.node_id IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ABANDONMENT_REQUIRES_UNASSIGNED_RUNS'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"CREATE TRIGGER {_q(_UNASSIGNED_TRIGGER)} AFTER INSERT ON "
        f"{quarantine} FOR EACH ROW WHEN "
        f"(NEW.recovery_basis = '{_BASIS}') EXECUTE FUNCTION "
        f"{unassigned_function}()"
    )
    op.execute(
        f"ALTER TABLE {quarantine} ENABLE ALWAYS TRIGGER "
        f"{_q(_UNASSIGNED_TRIGGER)}"
    )

    # The wave row was already immutable for every quarantine.  Extend that
    # preservation boundary to the exact intents, runs, and attempt markers.
    op.execute(
        f"""
        CREATE FUNCTION {child_function}()
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
            IF NOT EXISTS (
                SELECT 1 FROM {quarantine} AS abandoned
                 WHERE abandoned.recovery_basis = '{_BASIS}'
                   AND abandoned.predecessor_wave_id IN (
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
                SELECT 1 FROM {quarantine} AS abandoned
                 WHERE abandoned.recovery_basis = '{_BASIS}'
                   AND abandoned.predecessor_wave_id IN (
                       old_wave_id,
                       new_wave_id
                   )
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_ABANDONED_IMMUTABLE'
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
        trigger = _q(f"{table_name}_abandonment_guard")
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE OR DELETE "
            f"ON {table} FOR EACH ROW EXECUTE FUNCTION {child_function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")

    op.execute(
        f"""
        CREATE FUNCTION {run_function}()
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
            IF NOT EXISTS (
                SELECT 1
                  FROM {quarantine} AS abandoned
                  JOIN {wave} AS predecessor
                    ON predecessor.wave_id = abandoned.predecessor_wave_id
                 WHERE abandoned.recovery_basis = '{_BASIS}'
                   AND (
                       abandoned.predecessor_wave_id = ANY(candidate_wave_ids)
                       OR predecessor.wave_digest = ANY(candidate_wave_digests)
                       OR EXISTS (
                           SELECT 1 FROM {intent} AS member
                            WHERE member.wave_id = abandoned.predecessor_wave_id
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
                  FROM {quarantine} AS abandoned
                  JOIN {wave} AS predecessor
                    ON predecessor.wave_id = abandoned.predecessor_wave_id
                 WHERE abandoned.recovery_basis = '{_BASIS}'
                   AND (
                       abandoned.predecessor_wave_id = ANY(candidate_wave_ids)
                       OR predecessor.wave_digest = ANY(candidate_wave_digests)
                       OR EXISTS (
                           SELECT 1 FROM {intent} AS member
                            WHERE member.wave_id = abandoned.predecessor_wave_id
                              AND member.run_id = ANY(candidate_run_ids)
                       )
                   )
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_ABANDONED_IMMUTABLE'
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
    run_trigger = _q("ptg_import_wave_abandonment_run_guard")
    op.execute(
        f"CREATE TRIGGER {run_trigger} BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {run} FOR EACH ROW EXECUTE FUNCTION {run_function}()"
    )
    op.execute(f"ALTER TABLE {run} ENABLE ALWAYS TRIGGER {run_trigger}")

    op.execute(
        f"""
        CREATE FUNCTION {event_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            old_run_id text;
            new_run_id text;
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                old_run_id := OLD.outer_run_id;
            END IF;
            IF TG_OP <> 'DELETE' THEN
                new_run_id := NEW.outer_run_id;
            END IF;
            IF NOT EXISTS (
                SELECT 1
                  FROM {intent} AS member
                  JOIN {quarantine} AS abandoned
                    ON abandoned.predecessor_wave_id = member.wave_id
                   AND abandoned.recovery_basis = '{_BASIS}'
                 WHERE member.run_id IN (old_run_id, new_run_id)
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
                  FROM {intent} AS member
                  JOIN {quarantine} AS abandoned
                    ON abandoned.predecessor_wave_id = member.wave_id
                   AND abandoned.recovery_basis = '{_BASIS}'
                 WHERE member.run_id IN (old_run_id, new_run_id)
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_ABANDONED_IMMUTABLE'
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
    event_trigger = _q("ptg_import_wave_abandonment_event_guard")
    op.execute(
        f"CREATE TRIGGER {event_trigger} BEFORE INSERT OR UPDATE OR DELETE "
        f"ON {event} FOR EACH ROW EXECUTE FUNCTION {event_function}()"
    )
    op.execute(f"ALTER TABLE {event} ENABLE ALWAYS TRIGGER {event_trigger}")

    # Row triggers do not fire for TRUNCATE.  Keep unrelated tables usable
    # when they contain no abandoned-wave evidence, but fail the whole
    # statement before any retained child, run, or event row can be erased.
    op.execute(
        f"""
        CREATE FUNCTION {truncate_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            protected_evidence_present boolean;
        BEGIN
            IF TG_TABLE_NAME = 'ptg_import_wave_intent' THEN
                SELECT EXISTS (
                    SELECT 1
                      FROM {intent} AS member
                      JOIN {quarantine} AS abandoned
                        ON abandoned.predecessor_wave_id = member.wave_id
                       AND abandoned.recovery_basis = '{_BASIS}'
                ) INTO protected_evidence_present;
            ELSIF TG_TABLE_NAME = 'ptg_import_wave_claim' THEN
                SELECT EXISTS (
                    SELECT 1
                      FROM {claim} AS claimed
                      JOIN {quarantine} AS abandoned
                        ON abandoned.predecessor_wave_id = claimed.wave_id
                       AND abandoned.recovery_basis = '{_BASIS}'
                ) INTO protected_evidence_present;
            ELSIF TG_TABLE_NAME = 'ptg_import_wave_outcome' THEN
                SELECT EXISTS (
                    SELECT 1
                      FROM {outcome} AS completed
                      JOIN {quarantine} AS abandoned
                        ON abandoned.predecessor_wave_id = completed.wave_id
                       AND abandoned.recovery_basis = '{_BASIS}'
                ) INTO protected_evidence_present;
            ELSIF TG_TABLE_NAME = 'import_run' THEN
                SELECT EXISTS (
                    SELECT 1
                      FROM {run} AS admitted
                      JOIN {intent} AS member
                        ON member.run_id = admitted.run_id
                      JOIN {quarantine} AS abandoned
                        ON abandoned.predecessor_wave_id = member.wave_id
                       AND abandoned.recovery_basis = '{_BASIS}'
                ) INTO protected_evidence_present;
            ELSIF TG_TABLE_NAME = 'ptg_source_attempt_event' THEN
                SELECT EXISTS (
                    SELECT 1
                      FROM {event} AS attempt_event
                      JOIN {intent} AS member
                        ON member.run_id = attempt_event.outer_run_id
                      JOIN {quarantine} AS abandoned
                        ON abandoned.predecessor_wave_id = member.wave_id
                       AND abandoned.recovery_basis = '{_BASIS}'
                ) INTO protected_evidence_present;
            ELSE
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ABANDONMENT_TRUNCATE_TARGET_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            IF protected_evidence_present THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ABANDONED_IMMUTABLE'
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
        trigger = _q(f"{table_name}_abandonment_truncate_guard")
        op.execute(
            f"CREATE TRIGGER {trigger} BEFORE TRUNCATE ON {table} "
            f"FOR EACH STATEMENT EXECUTE FUNCTION {truncate_function}()"
        )
        op.execute(f"ALTER TABLE {table} ENABLE ALWAYS TRIGGER {trigger}")

    _replace_effective_owner_function(
        function=effective_owner_function,
        wave=wave,
        quarantine=quarantine,
        supersession=supersession,
        schema=schema,
        include_abandonment=True,
    )


def downgrade() -> None:
    """Remove the cutover shape only when no abandonment was recorded."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    quarantine = _qt(schema, "ptg_import_wave_quarantine")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    effective_owner_function = _qt(schema, _EFFECTIVE_OWNER_FUNCTION)

    op.execute("SET LOCAL lock_timeout = '5s'")
    op.execute(
        f"LOCK TABLE {wave}, {quarantine}, {supersession} "
        "IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {quarantine}
                 WHERE recovery_basis = '{_BASIS}'
            ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ABANDONMENT_DOWNGRADE_BLOCKED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    _replace_effective_owner_function(
        function=effective_owner_function,
        wave=wave,
        quarantine=quarantine,
        supersession=supersession,
        schema=schema,
        include_abandonment=False,
    )
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
        "import_run",
        "ptg_source_attempt_event",
    ):
        table = _qt(schema, table_name)
        trigger = _q(f"{table_name}_abandonment_truncate_guard")
        op.execute(f"DROP TRIGGER {trigger} ON {table}")
    op.execute(f"DROP FUNCTION {_qt(schema, _TRUNCATE_FUNCTION)}()")
    for table_name in (
        "ptg_import_wave_intent",
        "ptg_import_wave_claim",
        "ptg_import_wave_outcome",
    ):
        table = _qt(schema, table_name)
        trigger = _q(f"{table_name}_abandonment_guard")
        op.execute(f"DROP TRIGGER {trigger} ON {table}")
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_abandonment_run_guard')} "
        f"ON {_qt(schema, 'import_run')}"
    )
    op.execute(
        f"DROP TRIGGER {_q('ptg_import_wave_abandonment_event_guard')} "
        f"ON {_qt(schema, 'ptg_source_attempt_event')}"
    )
    op.execute(f"DROP FUNCTION {_qt(schema, _CHILD_FUNCTION)}()")
    op.execute(f"DROP FUNCTION {_qt(schema, _RUN_FUNCTION)}()")
    op.execute(f"DROP FUNCTION {_qt(schema, _EVENT_FUNCTION)}()")
    op.execute(f"DROP TRIGGER {_q(_UNASSIGNED_TRIGGER)} ON {quarantine}")
    op.execute(f"DROP FUNCTION {_qt(schema, _UNASSIGNED_FUNCTION)}()")
    op.execute(f"DROP TRIGGER {_q(_PROOF_TRIGGER)} ON {quarantine}")
    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_abandonment_evidence_check')}"
    )
    op.execute(
        f"ALTER TABLE {quarantine} DROP CONSTRAINT "
        f"{_q('ptg_import_wave_quarantine_cutover_id_key')}"
    )
    for column_name in (
        "recovery_evidence_sha256",
        "recovery_evidence_canonical",
        "recovery_evidence",
        "recovery_basis",
        "successor_wave_id",
    ):
        op.execute(
            f"ALTER TABLE {quarantine} DROP COLUMN {_q(column_name)}"
        )
