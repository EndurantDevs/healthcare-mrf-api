"""Guard FHIR formulary publication and immutable content.

Revision ID: 20260808130000_fhir_formulary_publication_guards
Revises: 20260808120000_fhir_formulary_twin_admission
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260808130000_fhir_formulary_publication_guards"
down_revision = "20260808120000_fhir_formulary_twin_admission"
branch_labels = None
depends_on = None

_ADMISSION = "fhir_formulary_twin_admission"
_ATTEMPT = "fhir_formulary_twin_attempt"
_CURRENT_GUARD = "guard_fhir_formulary_current_twin_admission"
_CURRENT_COMMIT_GUARD = "assert_fhir_formulary_current_published"
_DATASET_GUARD = "guard_fhir_formulary_twin_dataset"
_COW_GUARD = "guard_fhir_formulary_cow_immutable"
_OWNER_INSERT_GUARD = "guard_fhir_formulary_build_owner_insert"
_CONTENT_INSERT_GUARD = "guard_fhir_formulary_alias_content_insert"
_SOURCE_GUARD = "guard_fhir_formulary_current_source"
_COW_TABLES = (
    "fhir_formulary_coverage_plan",
    "fhir_formulary_coverage_plan_version",
    "fhir_formulary_dataset_coverage_plan",
    "fhir_formulary_drug_plan_alias",
    "fhir_formulary_drug_plan_alias_version",
    "fhir_formulary_dataset_alias",
    "fhir_formulary_medication",
    "fhir_formulary_alias_membership",
    "fhir_formulary_alternative",
)
_OWNER_TABLES = (
    "fhir_formulary_dataset_coverage_plan",
    "fhir_formulary_dataset_alias",
    "fhir_formulary_checkpoint",
)
_CONTENT_TABLES = (
    "fhir_formulary_alias_membership",
    "fhir_formulary_alternative",
)


def _schema() -> str:
    runtime_schema = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy_schema = os.getenv("DB_SCHEMA")
    if runtime_schema and legacy_schema and runtime_schema != legacy_schema:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime_schema or legacy_schema or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _table(schema: str, table_name: str) -> str:
    return f"{_quote(schema)}.{_quote(table_name)}"


def upgrade() -> None:
    """Install fail-closed pointer, dataset, and content-graph guards."""

    schema = _schema()
    _preflight_existing_current(schema)
    _install_current_guard(schema)
    _install_publication_dataset_guard(schema)
    _install_current_source_guard(schema)
    _install_content_graph_guards(schema)


def _preflight_existing_current(schema: str) -> None:
    current = _table(schema, "fhir_formulary_current")
    dataset = _table(schema, "fhir_formulary_dataset")
    source = _table(schema, "fhir_formulary_source")
    op.execute(
        f"""DO $block$ BEGIN
            IF EXISTS (
                SELECT 1 FROM {current} AS pointer LEFT JOIN {dataset} AS candidate
                  ON candidate.source_id = pointer.source_id
                 AND candidate.dataset_id = pointer.dataset_id
                 LEFT JOIN {source} AS owner ON owner.source_id = pointer.source_id
                 WHERE candidate.dataset_id IS NULL OR owner.source_id IS NULL
                    OR owner.metadata_json -> 'synthetic' IS DISTINCT FROM 'true'::jsonb
                    OR candidate.status <> 'published'
                    OR candidate.publish_requested IS DISTINCT FROM false
                    OR candidate.seed_eligible IS DISTINCT FROM true
                    OR candidate.previous_dataset_id IS NOT NULL
                    OR candidate.verified_at IS NULL OR candidate.failed_at IS NOT NULL
                    OR candidate.error_json IS NOT NULL OR candidate.published_at IS NULL
                    OR candidate.published_at IS DISTINCT FROM pointer.published_at
                    OR candidate.list_count <= 0 OR candidate.alias_count <= 0
                    OR candidate.medication_count <= 0
                    OR candidate.coverage_hash !~ '^[0-9a-f]{{64}}$'
                    OR candidate.membership_hash !~ '^[0-9a-f]{{64}}$'
                    OR pointer.generation <> 1
            ) THEN
                RAISE EXCEPTION 'fhir_formulary_preexisting_current_invalid'
                    USING ERRCODE = '55000';
            END IF;
        END; $block$;"""
    )


def _install_current_guard(schema: str) -> None:
    admission = _table(schema, _ADMISSION)
    dataset = _table(schema, "fhir_formulary_dataset")
    current = _table(schema, "fhir_formulary_current")
    source = _table(schema, "fhir_formulary_source")
    guard = _table(schema, _CURRENT_GUARD)
    commit_guard = _table(schema, _CURRENT_COMMIT_GUARD)
    op.execute(
        f"""CREATE FUNCTION {guard}()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE candidate_row record; source_metadata jsonb; source_enabled boolean;
            pointer_predecessor varchar(64); exact_admission_exists boolean;
        BEGIN
            IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
                RAISE EXCEPTION 'fhir_formulary_current_immutable' USING ERRCODE = '55000';
            END IF;
            SELECT metadata_json, enabled INTO source_metadata, source_enabled FROM {source}
             WHERE source_id = NEW.source_id FOR SHARE;
            IF NOT FOUND OR source_enabled IS DISTINCT FROM true THEN
                RAISE EXCEPTION 'fhir_formulary_current_source_invalid' USING ERRCODE = '55000';
            END IF;
            SELECT * INTO candidate_row FROM {dataset}
             WHERE source_id = NEW.source_id AND dataset_id = NEW.dataset_id FOR SHARE;
            IF NOT FOUND OR candidate_row.status IS DISTINCT FROM 'verified'
               OR candidate_row.verified_at IS NULL OR candidate_row.failed_at IS NOT NULL
               OR candidate_row.error_json IS NOT NULL OR candidate_row.published_at IS NOT NULL
               OR candidate_row.list_count <= 0 OR candidate_row.alias_count <= 0
               OR candidate_row.medication_count <= 0
               OR candidate_row.coverage_hash !~ '^[0-9a-f]{{64}}$'
               OR candidate_row.membership_hash !~ '^[0-9a-f]{{64}}$'
               OR NEW.published_at IS DISTINCT FROM transaction_timestamp() THEN
                RAISE EXCEPTION 'fhir_formulary_current_candidate_invalid'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'INSERT' THEN
                pointer_predecessor := NULL;
                IF NEW.generation <> 1 THEN
                    RAISE EXCEPTION 'fhir_formulary_current_generation_invalid'
                        USING ERRCODE = '55000';
                END IF;
            ELSE
                pointer_predecessor := OLD.dataset_id;
                IF NEW.source_id IS DISTINCT FROM OLD.source_id
                   OR NEW.generation <> OLD.generation + 1 THEN
                    RAISE EXCEPTION 'fhir_formulary_current_generation_invalid'
                        USING ERRCODE = '55000';
                END IF;
            END IF;
            IF candidate_row.publish_requested = false AND candidate_row.seed_eligible = true THEN
                IF TG_OP <> 'INSERT' OR candidate_row.previous_dataset_id IS NOT NULL
                   OR source_metadata -> 'synthetic' IS DISTINCT FROM 'true'::jsonb THEN
                    RAISE EXCEPTION 'fhir_formulary_seed_pointer_invalid' USING ERRCODE = '55000';
                END IF;
                RETURN NEW;
            END IF;
            IF candidate_row.publish_requested IS DISTINCT FROM true
               OR candidate_row.seed_eligible IS DISTINCT FROM false
               OR candidate_row.previous_dataset_id IS DISTINCT FROM pointer_predecessor THEN
                RAISE EXCEPTION 'fhir_formulary_current_intent_invalid' USING ERRCODE = '55000';
            END IF;
            SELECT EXISTS (
                SELECT 1 FROM {admission} AS evidence JOIN {dataset} AS baseline
                  ON baseline.source_id = evidence.source_id
                 AND baseline.dataset_id = evidence.baseline_dataset_id
                 AND baseline.run_id = evidence.baseline_run_id
                 WHERE evidence.source_id = NEW.source_id
                   AND evidence.candidate_dataset_id = NEW.dataset_id
                   AND evidence.candidate_run_id = candidate_row.run_id
                   AND evidence.predecessor_dataset_id IS NOT DISTINCT FROM pointer_predecessor
                   AND ROW(evidence.cutoff_at, evidence.acquisition_contract_hash,
                       evidence.list_count, evidence.alias_count, evidence.medication_count,
                       evidence.coverage_hash, evidence.membership_hash,
                       evidence.candidate_verified_at) = ROW(candidate_row.cutoff_at,
                       candidate_row.summary_json ->> 'acquisition_contract_hash',
                       candidate_row.list_count, candidate_row.alias_count,
                       candidate_row.medication_count, candidate_row.coverage_hash,
                       candidate_row.membership_hash, candidate_row.verified_at)
                   AND ROW(baseline.status, baseline.publish_requested,
                       baseline.seed_eligible, baseline.failed_at, baseline.error_json,
                       baseline.published_at) IS NOT DISTINCT FROM
                       ROW('verified', false, false, NULL, NULL, NULL)
                   AND ROW(baseline.previous_dataset_id, baseline.cutoff_at,
                       baseline.summary_json ->> 'acquisition_contract_hash',
                       baseline.list_count, baseline.alias_count, baseline.medication_count,
                       baseline.coverage_hash, baseline.membership_hash, baseline.verified_at)
                       IS NOT DISTINCT FROM ROW(evidence.predecessor_dataset_id,
                       evidence.cutoff_at, evidence.acquisition_contract_hash,
                       evidence.list_count, evidence.alias_count, evidence.medication_count,
                       evidence.coverage_hash, evidence.membership_hash,
                       evidence.baseline_verified_at)
            ) INTO exact_admission_exists;
            IF NOT exact_admission_exists THEN
                RAISE EXCEPTION 'fhir_formulary_twin_admission_required' USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END; $function$;"""
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_current_twin_admission_guard BEFORE INSERT OR UPDATE "
        f"OR DELETE ON {current} FOR EACH ROW EXECUTE FUNCTION {guard}();"
    )
    op.execute(
        f"CREATE TRIGGER fhir_formulary_current_twin_admission_truncate_guard BEFORE TRUNCATE "
        f"ON {current} FOR EACH STATEMENT EXECUTE FUNCTION {guard}();"
    )
    op.execute(
        f"""CREATE FUNCTION {commit_guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM {dataset} WHERE source_id = NEW.source_id
                AND dataset_id = NEW.dataset_id AND status = 'published'
                AND published_at IS NOT DISTINCT FROM NEW.published_at) THEN
                RAISE EXCEPTION 'fhir_formulary_current_not_published' USING ERRCODE = '55000';
            END IF;
            RETURN NULL;
        END; $function$;"""
    )
    op.execute(f"REVOKE ALL ON FUNCTION {commit_guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE CONSTRAINT TRIGGER fhir_formulary_current_published_guard AFTER INSERT OR UPDATE "
        f"ON {current} DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION {commit_guard}();"
    )


def _install_current_source_guard(schema: str) -> None:
    source = _table(schema, "fhir_formulary_source")
    guard = _table(schema, _SOURCE_GUARD)
    op.execute(
        f"""CREATE FUNCTION {guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$ BEGIN
            IF TG_OP = 'DELETE' OR
               (to_jsonb(NEW) - 'enabled' - 'updated_at') IS DISTINCT FROM
               (to_jsonb(OLD) - 'enabled' - 'updated_at') THEN
                RAISE EXCEPTION 'fhir_formulary_current_source_immutable'
                    USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END; $function$;"""
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_current_source_guard BEFORE UPDATE OR DELETE ON "
        f"{source} FOR EACH ROW EXECUTE FUNCTION {guard}();"
    )


def _install_publication_dataset_guard(schema: str) -> None:
    attempt = _table(schema, _ATTEMPT)
    admission = _table(schema, _ADMISSION)
    current = _table(schema, "fhir_formulary_current")
    guard = _table(schema, _DATASET_GUARD)
    op.execute(
        f"""CREATE OR REPLACE FUNCTION {guard}()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE attempt_row record; attempt_exists boolean; admission_exists boolean;
            pointer_published_at timestamptz; current_exists boolean;
        BEGIN
            IF OLD.status = 'published' THEN
                RAISE EXCEPTION 'fhir_formulary_twin_dataset_immutable' USING ERRCODE = '55000';
            END IF;
            SELECT * INTO attempt_row FROM {attempt} WHERE source_id = OLD.source_id
             AND (baseline_dataset_id = OLD.dataset_id OR candidate_dataset_id = OLD.dataset_id)
             FOR SHARE;
            attempt_exists := FOUND;
            SELECT published_at INTO pointer_published_at FROM {current}
             WHERE source_id = OLD.source_id AND dataset_id = OLD.dataset_id FOR SHARE;
            current_exists := FOUND;
            IF NOT attempt_exists AND NOT current_exists THEN
                IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
                RETURN NEW;
            END IF;
            IF TG_OP = 'DELETE' OR (attempt_exists AND
               (OLD.dataset_id = attempt_row.baseline_dataset_id
                OR attempt_row.matched IS DISTINCT FROM true)) THEN
                RAISE EXCEPTION 'fhir_formulary_twin_dataset_immutable' USING ERRCODE = '55000';
            END IF;
            admission_exists := false;
            IF attempt_exists THEN
                SELECT EXISTS (SELECT 1 FROM {admission} WHERE source_id = OLD.source_id
                    AND baseline_dataset_id = attempt_row.baseline_dataset_id
                    AND candidate_dataset_id = OLD.dataset_id) INTO admission_exists;
            END IF;
            IF current_exists AND OLD.status = 'verified' AND NEW.status = 'published'
               AND OLD.published_at IS NULL
               AND NEW.published_at IS NOT DISTINCT FROM pointer_published_at
               AND (to_jsonb(NEW) - 'status' - 'published_at') IS NOT DISTINCT FROM
                   (to_jsonb(OLD) - 'status' - 'published_at')
               AND ((NOT attempt_exists AND OLD.publish_requested = false
                     AND OLD.seed_eligible = true AND OLD.previous_dataset_id IS NULL)
                    OR (attempt_exists AND admission_exists)) THEN
                RETURN NEW;
            END IF;
            RAISE EXCEPTION 'fhir_formulary_twin_dataset_immutable' USING ERRCODE = '55000';
        END; $function$;"""
    )
    op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")


def _install_content_graph_guards(schema: str) -> None:
    dataset = _table(schema, "fhir_formulary_dataset")
    dataset_alias = _table(schema, "fhir_formulary_dataset_alias")
    attempt = _table(schema, _ATTEMPT)
    current = _table(schema, "fhir_formulary_current")
    cow_guard = _table(schema, _COW_GUARD)
    owner_guard = _table(schema, _OWNER_INSERT_GUARD)
    content_guard = _table(schema, _CONTENT_INSERT_GUARD)
    op.execute(
        f"""CREATE FUNCTION {cow_guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$ BEGIN
            RAISE EXCEPTION 'fhir_formulary_cow_immutable' USING ERRCODE = '55000';
        END; $function$;"""
    )
    for table_name in _COW_TABLES:
        op.execute(
            f"CREATE TRIGGER fhir_formulary_cow_immutable_guard BEFORE UPDATE OR DELETE OR "
            f"TRUNCATE ON {_table(schema, table_name)} FOR EACH STATEMENT "
            f"EXECUTE FUNCTION {cow_guard}();"
        )
    checkpoint = _table(schema, "fhir_formulary_checkpoint")
    op.execute(
        f"CREATE TRIGGER fhir_formulary_checkpoint_truncate_guard BEFORE TRUNCATE ON "
        f"{checkpoint} FOR EACH STATEMENT EXECUTE FUNCTION {cow_guard}();"
    )
    op.execute(
        f"""CREATE FUNCTION {owner_guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$ DECLARE owner_status varchar(32); BEGIN
            SELECT status INTO owner_status FROM {dataset} WHERE source_id = NEW.source_id
             AND dataset_id = NEW.dataset_id FOR SHARE;
            IF NOT FOUND OR owner_status IS DISTINCT FROM 'building' THEN
                RAISE EXCEPTION 'fhir_formulary_late_dataset_content' USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END; $function$;"""
    )
    for table_name in _OWNER_TABLES:
        op.execute(
            f"CREATE TRIGGER fhir_formulary_build_owner_insert_guard BEFORE INSERT ON "
            f"{_table(schema, table_name)} FOR EACH ROW EXECUTE FUNCTION {owner_guard}();"
        )
    op.execute(
        f"""CREATE FUNCTION {content_guard}() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $function$ BEGIN
            PERFORM 1 FROM {dataset_alias} AS link JOIN {dataset} AS owner
              ON owner.source_id = link.source_id AND owner.dataset_id = link.dataset_id
             WHERE link.alias_version_id = NEW.alias_version_id
             ORDER BY owner.source_id, owner.dataset_id FOR SHARE OF owner;
            IF EXISTS (
                SELECT 1 FROM {dataset_alias} AS link JOIN {dataset} AS owner
                  ON owner.source_id = link.source_id AND owner.dataset_id = link.dataset_id
                 WHERE link.alias_version_id = NEW.alias_version_id AND (
                    owner.status IN ('verified', 'published')
                    OR EXISTS (SELECT 1 FROM {attempt} WHERE source_id = link.source_id
                        AND (baseline_dataset_id = link.dataset_id
                             OR candidate_dataset_id = link.dataset_id))
                    OR EXISTS (SELECT 1 FROM {current} WHERE source_id = link.source_id
                        AND dataset_id = link.dataset_id)
                 )
            ) THEN
                RAISE EXCEPTION 'fhir_formulary_late_alias_content' USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END; $function$;"""
    )
    for table_name in _CONTENT_TABLES:
        op.execute(
            f"CREATE TRIGGER fhir_formulary_alias_content_insert_guard AFTER INSERT ON "
            f"{_table(schema, table_name)} FOR EACH ROW EXECUTE FUNCTION {content_guard}();"
        )
    for guard in (cow_guard, owner_guard, content_guard):
        op.execute(f"REVOKE ALL ON FUNCTION {guard}() FROM PUBLIC;")


def _restore_strict_dataset_guard(schema: str) -> None:
    attempt = _table(schema, _ATTEMPT)
    guard = _table(schema, _DATASET_GUARD)
    op.execute(
        f"""CREATE OR REPLACE FUNCTION {guard}()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$ BEGIN
            IF EXISTS (SELECT 1 FROM {attempt} WHERE source_id = OLD.source_id
                AND (baseline_dataset_id = OLD.dataset_id
                     OR candidate_dataset_id = OLD.dataset_id)) THEN
                RAISE EXCEPTION 'fhir_formulary_twin_dataset_immutable' USING ERRCODE = '55000';
            END IF;
            IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END; $function$;"""
    )


def downgrade() -> None:
    """Remove publication guards only when no twin evidence can be exposed."""

    schema = _schema()
    attempt = _table(schema, _ATTEMPT)
    admission = _table(schema, _ADMISSION)
    current = _table(schema, "fhir_formulary_current")
    dataset = _table(schema, "fhir_formulary_dataset")
    op.execute(
        f"""DO $block$ BEGIN
            IF EXISTS (SELECT 1 FROM {attempt}) OR EXISTS (SELECT 1 FROM {admission})
               OR EXISTS (SELECT 1 FROM {current} AS pointer JOIN {dataset} AS candidate
                  ON candidate.source_id = pointer.source_id
                 AND candidate.dataset_id = pointer.dataset_id
                 WHERE candidate.publish_requested = true AND candidate.seed_eligible = false) THEN
                RAISE EXCEPTION 'fhir_formulary_publication_guard_downgrade_forbidden'
                    USING ERRCODE = '55000';
            END IF;
        END; $block$;"""
    )
    for trigger_name in (
        "fhir_formulary_current_twin_admission_guard",
        "fhir_formulary_current_twin_admission_truncate_guard",
        "fhir_formulary_current_published_guard",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {current};")
    op.execute(
        f"DROP TRIGGER IF EXISTS fhir_formulary_current_source_guard "
        f"ON {_table(schema, 'fhir_formulary_source')};"
    )
    for table_name in _COW_TABLES:
        op.execute(
            f"DROP TRIGGER IF EXISTS fhir_formulary_cow_immutable_guard "
            f"ON {_table(schema, table_name)};"
        )
    op.execute(
        "DROP TRIGGER IF EXISTS fhir_formulary_checkpoint_truncate_guard "
        f"ON {_table(schema, 'fhir_formulary_checkpoint')};"
    )
    for table_name in _OWNER_TABLES:
        op.execute(
            f"DROP TRIGGER IF EXISTS fhir_formulary_build_owner_insert_guard "
            f"ON {_table(schema, table_name)};"
        )
    for table_name in _CONTENT_TABLES:
        op.execute(
            f"DROP TRIGGER IF EXISTS fhir_formulary_alias_content_insert_guard "
            f"ON {_table(schema, table_name)};"
        )
    _restore_strict_dataset_guard(schema)
    for function_name in (
        _CONTENT_INSERT_GUARD, _OWNER_INSERT_GUARD, _COW_GUARD,
        _SOURCE_GUARD, _CURRENT_COMMIT_GUARD, _CURRENT_GUARD,
    ):
        op.execute(f"DROP FUNCTION IF EXISTS {_table(schema, function_name)}();")
