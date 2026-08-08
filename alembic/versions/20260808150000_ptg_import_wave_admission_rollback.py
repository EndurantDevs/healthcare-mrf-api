"""Add immutable retirement for one proved-absent wave admission.

Revision ID: 20260808150000_ptg_import_wave_admission_rollback
Revises: 20260808140000_ptg_import_wave_json_null_preclaim

The new record is written in the same transaction as its V4 successor.  It
permanently blocks the absent predecessor identities without advancing or
reconciling that predecessor.
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808150000_ptg_import_wave_admission_rollback"
down_revision = "20260808140000_ptg_import_wave_json_null_preclaim"
branch_labels = None
depends_on = None

_TABLE = "ptg_import_wave_admission_rollback"
_INSERT_GUARD_FUNCTION = "ptg_import_wave_admission_rollback_insert_guard"
_BINDING_FUNCTION = "ptg_import_wave_admission_rollback_binding_guard"
_V4_BINDING_FUNCTION = "ptg_import_wave_v4_dual_retirement_binding_guard"
_WAVE_GUARD_FUNCTION = "ptg_import_wave_retired_admission_guard"
_RUN_GUARD_FUNCTION = "ptg_import_wave_retired_run_guard"
_IMMUTABLE_FUNCTION = "ptg_import_wave_admission_rollback_immutable"
_SUPERSESSION_BINDING_FUNCTION = (
    "ptg_import_wave_supersession_successor_binding_guard"
)
_ADMISSION_LOCK = "import-run-admission:ptg-source-file"
_SCHEMA_VERSION = (
    "healthporta.ptg-wave.admission-rollback-supersession.v1"
)
_ATTESTATION_VERSION = "healthporta.ptg-import-wave-attestation.v4"
_RECOVERY_BASIS = "admission_rollback_absent"
_PROTOCOL_IDENTITY = "healthporta.ptg-small.exact-wave.v1"
_OLD_SUPERSESSION_VERSION_PREDICATE = (
    "OR successor.cohort_attestation::jsonb->>'schema_version'\n"
    "                    IS DISTINCT FROM "
    "'healthporta.ptg-import-wave-attestation.v3'"
)
_NEW_SUPERSESSION_VERSION_PREDICATE = (
    "OR (\n"
    "                   successor.cohort_attestation::jsonb->>'schema_version'\n"
    "                       IS DISTINCT FROM "
    "'healthporta.ptg-import-wave-attestation.v3'\n"
    "                   AND successor.cohort_attestation::jsonb->>'schema_version'\n"
    "                       IS DISTINCT FROM "
    "'healthporta.ptg-import-wave-attestation.v4'\n"
    "               )"
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


def _patch_supersession_binding(
    old_predicate: str,
    new_predicate: str,
) -> str:
    schema = _schema()
    signature = (
        f"{_q(schema)}.{_q(_SUPERSESSION_BINDING_FUNCTION)}()"
    )
    return f"""
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
                'PTG_IMPORT_WAVE_ROLLBACK_BINDING_PATCH_PRECONDITION_FAILED'
                USING ERRCODE = 'P0001';
        END IF;
        EXECUTE pg_catalog.replace(definition, old_fragment, new_fragment);
    END;
    $migration$
    """


def upgrade() -> None:
    """Create the append-only rollback record and permanent identity guards."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    intent = _qt(schema, "ptg_import_wave_intent")
    claim = _qt(schema, "ptg_import_wave_claim")
    outcome = _qt(schema, "ptg_import_wave_outcome")
    import_run = _qt(schema, "import_run")
    event = _qt(schema, "ptg_source_attempt_event")
    supersession = _qt(schema, "ptg_import_wave_supersession")
    rollback = _qt(schema, _TABLE)
    insert_guard_function = _qt(schema, _INSERT_GUARD_FUNCTION)
    binding_function = _qt(schema, _BINDING_FUNCTION)
    v4_binding_function = _qt(schema, _V4_BINDING_FUNCTION)
    wave_guard_function = _qt(schema, _WAVE_GUARD_FUNCTION)
    run_guard_function = _qt(schema, _RUN_GUARD_FUNCTION)
    immutable_function = _qt(schema, _IMMUTABLE_FUNCTION)

    op.execute(
        f"""
        CREATE TABLE {rollback} (
            predecessor_wave_id varchar(64) PRIMARY KEY,
            predecessor_idempotency_key varchar(160) NOT NULL,
            predecessor_request_digest varchar(64) NOT NULL,
            predecessor_wave_digest varchar(64) NOT NULL,
            predecessor_release_queue varchar(160) NOT NULL,
            predecessor_intent_count integer NOT NULL,
            successor_wave_id varchar(64) NOT NULL,
            recovery_basis varchar(64) NOT NULL,
            recovery_evidence jsonb NOT NULL,
            recovery_evidence_canonical bytea NOT NULL,
            recovery_evidence_sha256 varchar(64) NOT NULL,
            created_at timestamptz NOT NULL,
            CONSTRAINT {_q('ptg_wave_rollback_successor_wave_fkey')}
                FOREIGN KEY (successor_wave_id)
                REFERENCES {wave}(wave_id) ON DELETE RESTRICT
                DEFERRABLE INITIALLY DEFERRED,
            CONSTRAINT {_q('ptg_wave_rollback_predecessor_idempotency_key')}
                UNIQUE (predecessor_idempotency_key),
            CONSTRAINT {_q('ptg_wave_rollback_predecessor_request_digest_key')}
                UNIQUE (predecessor_request_digest),
            CONSTRAINT {_q('ptg_wave_rollback_predecessor_wave_digest_key')}
                UNIQUE (predecessor_wave_digest),
            CONSTRAINT {_q('ptg_wave_rollback_successor_wave_id_key')}
                UNIQUE (successor_wave_id),
            CONSTRAINT {_q('ptg_wave_rollback_distinct_check')}
                CHECK (predecessor_wave_id <> successor_wave_id),
            CONSTRAINT {_q('ptg_wave_rollback_basis_check')}
                CHECK (recovery_basis = '{_RECOVERY_BASIS}'),
            CONSTRAINT {_q('ptg_wave_rollback_predecessor_check')} CHECK (
                predecessor_request_digest ~ '^[0-9a-f]{{64}}$'
                AND predecessor_wave_digest ~ '^[0-9a-f]{{64}}$'
                AND predecessor_release_queue
                    = 'arq:PTGSmall:wave:' || predecessor_wave_digest
                AND predecessor_intent_count BETWEEN 1 AND 4096
            ),
            CONSTRAINT {_q('ptg_wave_rollback_evidence_check')} CHECK (
                jsonb_typeof(recovery_evidence) = 'object'
                AND recovery_evidence_sha256 ~ '^[0-9a-f]{{64}}$'
                AND octet_length(recovery_evidence_canonical) > 0
                AND encode(sha256(recovery_evidence_canonical), 'hex')
                    = recovery_evidence_sha256
                AND convert_from(
                    recovery_evidence_canonical, 'UTF8'
                )::jsonb = recovery_evidence - 'proof_digest'
            )
        )
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {insert_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            wave_id_count integer;
            idempotency_key_count integer;
            request_digest_count integer;
            wave_digest_count integer;
            intent_count integer;
            claim_count integer;
            outcome_count integer;
            tagged_run_count integer;
            worker_event_count integer;
            supersession_predecessor_count integer;
            supersession_successor_count integer;
            retirement_count integer;
            expected_wave_digest text;
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended({_literal(_ADMISSION_LOCK)}, 0)
            );
            LOCK TABLE {wave}, {intent}, {claim}, {outcome}, {import_run},
                {event}, {supersession}, {rollback}
                IN SHARE ROW EXCLUSIVE MODE;
            SELECT count(*) INTO wave_id_count FROM {wave}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO idempotency_key_count FROM {wave}
             WHERE idempotency_key = NEW.predecessor_idempotency_key;
            SELECT count(*) INTO request_digest_count FROM {wave}
             WHERE request_digest = NEW.predecessor_request_digest;
            SELECT count(*) INTO wave_digest_count FROM {wave}
             WHERE wave_digest = NEW.predecessor_wave_digest;
            SELECT count(*) INTO intent_count FROM {intent}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO claim_count FROM {claim}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO outcome_count FROM {outcome}
             WHERE wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO tagged_run_count FROM {import_run} AS run
             WHERE run.params::jsonb->>'_wave_id' = NEW.predecessor_wave_id
                OR run.metrics::jsonb->>'wave_id' = NEW.predecessor_wave_id
                OR run.params::jsonb->>'_wave_digest'
                    = NEW.predecessor_wave_digest
                OR run.metrics::jsonb->>'wave_digest'
                    = NEW.predecessor_wave_digest;
            SELECT count(*) INTO worker_event_count
              FROM {event} AS attempt_event
              JOIN {import_run} AS run
                ON run.run_id = attempt_event.outer_run_id
             WHERE attempt_event.event_kind = 'worker_start_admitted'
               AND (
                   run.params::jsonb->>'_wave_id'
                       = NEW.predecessor_wave_id
                   OR run.metrics::jsonb->>'wave_id'
                       = NEW.predecessor_wave_id
                   OR run.params::jsonb->>'_wave_digest'
                       = NEW.predecessor_wave_digest
                   OR run.metrics::jsonb->>'wave_digest'
                       = NEW.predecessor_wave_digest
               );
            SELECT count(*) INTO supersession_predecessor_count
              FROM {supersession}
             WHERE predecessor_wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO supersession_successor_count
              FROM {supersession}
             WHERE successor_wave_id = NEW.predecessor_wave_id;
            SELECT count(*) INTO retirement_count FROM {rollback}
             WHERE predecessor_wave_id = NEW.predecessor_wave_id
                OR predecessor_idempotency_key
                    = NEW.predecessor_idempotency_key
                OR predecessor_request_digest
                    = NEW.predecessor_request_digest
                OR predecessor_wave_digest = NEW.predecessor_wave_digest;
            expected_wave_digest := encode(
                sha256(
                    convert_to({_literal(_PROTOCOL_IDENTITY)}, 'UTF8')
                    || decode('00', 'hex')
                    || convert_to(NEW.predecessor_request_digest, 'UTF8')
                ),
                'hex'
            );
            IF wave_id_count <> 0
               OR idempotency_key_count <> 0
               OR request_digest_count <> 0
               OR wave_digest_count <> 0
               OR intent_count <> 0
               OR claim_count <> 0
               OR outcome_count <> 0
               OR tagged_run_count <> 0
               OR worker_event_count <> 0
               OR supersession_predecessor_count <> 0
               OR supersession_successor_count <> 0
               OR retirement_count <> 0 THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ADMISSION_ROLLBACK_ABSENCE_REQUIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            IF NEW.predecessor_idempotency_key
                    IS DISTINCT FROM NEW.predecessor_wave_id
               OR NEW.predecessor_wave_digest
                    IS DISTINCT FROM expected_wave_digest
               OR NEW.recovery_evidence IS DISTINCT FROM jsonb_build_object(
                    'schema_version', '{_SCHEMA_VERSION}',
                    'recovery_basis', '{_RECOVERY_BASIS}',
                    'predecessor', jsonb_build_object(
                        'wave_id', NEW.predecessor_wave_id,
                        'idempotency_key', NEW.predecessor_idempotency_key,
                        'request_digest', NEW.predecessor_request_digest,
                        'wave_digest', NEW.predecessor_wave_digest,
                        'release_queue', NEW.predecessor_release_queue,
                        'intent_count', NEW.predecessor_intent_count
                    ),
                    'successor_wave_id', NEW.successor_wave_id,
                    'database', jsonb_build_object(
                        'wave_id_count', wave_id_count,
                        'idempotency_key_count', idempotency_key_count,
                        'request_digest_count', request_digest_count,
                        'wave_digest_count', wave_digest_count,
                        'intent_count', intent_count,
                        'claim_count', claim_count,
                        'outcome_count', outcome_count,
                        'wave_tagged_import_run_count', tagged_run_count,
                        'wave_tagged_worker_start_event_count',
                            worker_event_count,
                        'supersession_predecessor_count',
                            supersession_predecessor_count,
                        'supersession_successor_count',
                            supersession_successor_count,
                        'retirement_count', retirement_count
                    ),
                    'kubernetes', jsonb_build_object(
                        'job_name', 'hpw-ptg-wave-'
                            || left(NEW.predecessor_wave_digest, 40),
                        'job_present', false,
                        'pod_count', 0
                    ),
                    'redis', jsonb_build_object(
                        'queue_name', NEW.predecessor_release_queue,
                        'queued_entry_count', 0,
                        'ready_slot_count', 0,
                        'release_present', false,
                        'health_check_present', false
                    ),
                    'proof_digest', NEW.recovery_evidence_sha256
               ) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ADMISSION_ROLLBACK_EVIDENCE_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_wave_rollback_insert_guard')}
        BEFORE INSERT ON {rollback}
        FOR EACH ROW EXECUTE FUNCTION {insert_guard_function}()
        """
    )
    op.execute(
        f"ALTER TABLE {rollback} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_rollback_insert_guard')}"
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
            SELECT candidate.*, candidate.xmin AS inserted_xid
              INTO successor
              FROM {wave} AS candidate
             WHERE wave_id = NEW.successor_wave_id
             FOR KEY SHARE;
            IF NOT FOUND
               OR successor.inserted_xid
                    IS DISTINCT FROM pg_current_xact_id()::xid
               OR successor.state IS DISTINCT FROM 'admitted'
               OR successor.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_ATTESTATION_VERSION}'
               OR successor.cohort_attestation::jsonb->>'wave_id'
                    IS DISTINCT FROM NEW.successor_wave_id
               OR successor.cohort_attestation::jsonb
                    ->'admission_rollback_supersession'
                    IS DISTINCT FROM NEW.recovery_evidence THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ADMISSION_ROLLBACK_BINDING_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE CONSTRAINT TRIGGER {_q('ptg_wave_rollback_successor_binding_guard')}
        AFTER INSERT ON {rollback}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {binding_function}()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {v4_binding_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.cohort_attestation::jsonb->>'schema_version'
                    IS DISTINCT FROM '{_ATTESTATION_VERSION}' THEN
                RETURN NULL;
            END IF;
            IF (
                SELECT count(*) FROM {supersession}
                 WHERE successor_wave_id = NEW.wave_id
                   AND recovery_evidence =
                       NEW.cohort_attestation::jsonb->'supersession'
            ) <> 1
               OR (
                SELECT count(*) FROM {rollback}
                 WHERE successor_wave_id = NEW.wave_id
                   AND recovery_evidence = NEW.cohort_attestation::jsonb
                       ->'admission_rollback_supersession'
            ) <> 1 THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_V4_DUAL_RETIREMENT_BINDING_INVALID'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE CONSTRAINT TRIGGER {_q('ptg_wave_v4_dual_retirement_binding_guard')}
        AFTER INSERT ON {wave}
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION {v4_binding_function}()
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION {wave_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended({_literal(_ADMISSION_LOCK)}, 0)
            );
            IF EXISTS (
                SELECT 1 FROM {rollback}
                 WHERE predecessor_wave_id = NEW.wave_id
                    OR predecessor_idempotency_key = NEW.idempotency_key
                    OR predecessor_request_digest = NEW.request_digest
                    OR predecessor_wave_digest = NEW.wave_digest
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_ADMISSION_RETIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_wave_retired_admission_guard')}
        BEFORE INSERT ON {wave}
        FOR EACH ROW EXECUTE FUNCTION {wave_guard_function}()
        """
    )
    op.execute(
        f"ALTER TABLE {wave} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_retired_admission_guard')}"
    )
    op.execute(
        f"""
        CREATE FUNCTION {run_guard_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            IF NEW.importer IS DISTINCT FROM 'ptg' THEN
                RETURN NEW;
            END IF;
            IF NEW.params::jsonb->>'_wave_id' IS NULL
               AND NEW.metrics::jsonb->>'wave_id' IS NULL
               AND NEW.params::jsonb->>'_wave_digest' IS NULL
               AND NEW.metrics::jsonb->>'wave_digest' IS NULL THEN
                RETURN NEW;
            END IF;
            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended({_literal(_ADMISSION_LOCK)}, 0)
            );
            IF EXISTS (
                SELECT 1 FROM {rollback}
                 WHERE predecessor_wave_id IN (
                           NEW.params::jsonb->>'_wave_id',
                           NEW.metrics::jsonb->>'wave_id'
                       )
                    OR predecessor_idempotency_key IN (
                           NEW.params::jsonb->>'_wave_id',
                           NEW.metrics::jsonb->>'wave_id'
                       )
                    OR predecessor_wave_digest IN (
                           NEW.params::jsonb->>'_wave_digest',
                           NEW.metrics::jsonb->>'wave_digest'
                       )
            ) THEN
                RAISE EXCEPTION 'PTG_IMPORT_WAVE_ADMISSION_RETIRED'
                    USING ERRCODE = 'P0001';
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_wave_retired_run_guard')}
        BEFORE INSERT OR UPDATE ON {import_run}
        FOR EACH ROW EXECUTE FUNCTION {run_guard_function}()
        """
    )
    op.execute(
        f"ALTER TABLE {import_run} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_retired_run_guard')}"
    )
    op.execute(
        f"""
        CREATE FUNCTION {immutable_function}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'PTG_IMPORT_WAVE_ADMISSION_ROLLBACK_IMMUTABLE'
                USING ERRCODE = 'P0001';
        END;
        $$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_wave_rollback_row_guard')}
        BEFORE UPDATE OR DELETE ON {rollback}
        FOR EACH ROW EXECUTE FUNCTION {immutable_function}()
        """
    )
    op.execute(
        f"ALTER TABLE {rollback} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_rollback_row_guard')}"
    )
    op.execute(
        f"""
        CREATE TRIGGER {_q('ptg_wave_rollback_truncate_guard')}
        BEFORE TRUNCATE ON {rollback}
        FOR EACH STATEMENT EXECUTE FUNCTION {immutable_function}()
        """
    )
    op.execute(
        f"ALTER TABLE {rollback} ENABLE ALWAYS TRIGGER "
        f"{_q('ptg_wave_rollback_truncate_guard')}"
    )
    op.execute(_patch_supersession_binding(
        _OLD_SUPERSESSION_VERSION_PREDICATE,
        _NEW_SUPERSESSION_VERSION_PREDICATE,
    ))


def downgrade() -> None:
    """Remove only empty rollback storage and restore V3-only binding."""

    schema = _schema()
    wave = _qt(schema, "ptg_import_wave")
    import_run = _qt(schema, "import_run")
    rollback = _qt(schema, _TABLE)
    insert_guard_function = _qt(schema, _INSERT_GUARD_FUNCTION)
    binding_function = _qt(schema, _BINDING_FUNCTION)
    v4_binding_function = _qt(schema, _V4_BINDING_FUNCTION)
    wave_guard_function = _qt(schema, _WAVE_GUARD_FUNCTION)
    run_guard_function = _qt(schema, _RUN_GUARD_FUNCTION)
    immutable_function = _qt(schema, _IMMUTABLE_FUNCTION)

    op.execute(
        f"LOCK TABLE {wave}, {import_run}, {rollback} "
        "IN ACCESS EXCLUSIVE MODE"
    )
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM {rollback}) THEN
                RAISE EXCEPTION
                    'PTG_IMPORT_WAVE_ADMISSION_ROLLBACK_DOWNGRADE_REFUSED'
                    USING ERRCODE = '55000';
            END IF;
        END;
        $$
        """
    )
    op.execute(_patch_supersession_binding(
        _NEW_SUPERSESSION_VERSION_PREDICATE,
        _OLD_SUPERSESSION_VERSION_PREDICATE,
    ))
    op.execute(
        f"DROP TRIGGER {_q('ptg_wave_retired_run_guard')} ON {import_run}"
    )
    op.execute(f"DROP FUNCTION {run_guard_function}()")
    op.execute(
        f"DROP TRIGGER {_q('ptg_wave_retired_admission_guard')} ON {wave}"
    )
    op.execute(f"DROP FUNCTION {wave_guard_function}()")
    op.execute(
        f"DROP TRIGGER {_q('ptg_wave_v4_dual_retirement_binding_guard')} "
        f"ON {wave}"
    )
    op.execute(f"DROP FUNCTION {v4_binding_function}()")
    op.execute(f"DROP TABLE {rollback}")
    op.execute(f"DROP FUNCTION {binding_function}()")
    op.execute(f"DROP FUNCTION {insert_guard_function}()")
    op.execute(f"DROP FUNCTION {immutable_function}()")
