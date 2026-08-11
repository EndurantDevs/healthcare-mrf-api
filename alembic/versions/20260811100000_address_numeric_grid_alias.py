"""Add reviewed numeric-grid address aliases.

Revision ID: 20260811100000_address_numeric_grid_alias
Revises: 20260810130000_provider_directory_reviewed_subset_terminal_window
"""

from __future__ import annotations

import os

from alembic import op


revision = "20260811100000_address_numeric_grid_alias"
down_revision = "20260811030000_fhir_formulary_source_acquisition_lease"
branch_labels = None
depends_on = None


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _numeric_grid_function_sql(schema: str) -> str:
    qschema = _quote_ident(schema)
    return f"""
    CREATE OR REPLACE FUNCTION {qschema}.addr_numeric_grid_parts_v1(
        first_line text,
        second_line text
    )
    RETURNS text[]
    LANGUAGE plpgsql
    IMMUTABLE
    PARALLEL SAFE
    AS $$
    DECLARE
        street_text text;
        tokens text[];
        token_count integer;
        token_index integer := 2;
        house_number text;
        pre_direction text := '';
        grid_number text;
        post_direction text := '';
    BEGIN
        street_text := lower({qschema}.addr_street_text_v1(first_line, second_line));
        IF street_text ~ '[0-9][[:space:]]*[^a-z0-9[:space:]][[:space:]]*[0-9]' THEN
            RETURN NULL;
        END IF;
        street_text := trim(regexp_replace(street_text, '[^a-z0-9]+', ' ', 'g'));
        IF street_text = '' THEN
            RETURN NULL;
        END IF;
        tokens := regexp_split_to_array(street_text, '\\s+');
        token_count := cardinality(tokens);
        IF token_count < 2 OR token_count > 4 OR tokens[1] !~ '^[0-9]+$' THEN
            RETURN NULL;
        END IF;
        house_number := tokens[1];
        IF token_index <= token_count
           AND {qschema}.addr_street_token_is_directional_v1(tokens[token_index])
        THEN
            pre_direction := {qschema}.addr_street_token_norm_v1(tokens[token_index]);
            token_index := token_index + 1;
        END IF;
        IF token_index > token_count
           OR tokens[token_index] !~ '^[0-9]+(st|nd|rd|th)?$'
        THEN
            RETURN NULL;
        END IF;
        grid_number := {qschema}.addr_street_token_norm_v1(tokens[token_index]);
        IF grid_number !~ '^[0-9]+$' THEN
            RETURN NULL;
        END IF;
        token_index := token_index + 1;
        IF token_index <= token_count
           AND {qschema}.addr_street_token_is_directional_v1(tokens[token_index])
        THEN
            post_direction := {qschema}.addr_street_token_norm_v1(tokens[token_index]);
            token_index := token_index + 1;
        END IF;
        IF token_index <= token_count THEN
            RETURN NULL;
        END IF;
        RETURN ARRAY[house_number, pre_direction, grid_number, post_direction];
    END;
    $$;
    """


def _alias_schema_sql(schema: str) -> str:
    qschema = _quote_ident(schema)
    archive = f"{qschema}.{_quote_ident('address_archive_v2')}"
    state_table = f"{qschema}.{_quote_ident('address_alias_state_v1')}"
    run_table = f"{qschema}.{_quote_ident('address_alias_run_v1')}"
    candidate_table = f"{qschema}.{_quote_ident('address_alias_candidate_v1')}"
    alias_table = f"{qschema}.{_quote_ident('address_alias_v1')}"
    return f"""
    ALTER TABLE {archive}
        ADD COLUMN strict_source_bits integer NOT NULL DEFAULT 0;
    ALTER TABLE {archive}
        ADD CONSTRAINT address_archive_v2_strict_source_bits_ck
        CHECK (
            strict_source_bits >= 0
            AND (strict_source_bits & source_bits) = strict_source_bits
        ) NOT VALID;

    ALTER TABLE IF EXISTS {qschema}.{_quote_ident('partd_pharmacy_activity_v2')}
        ADD COLUMN IF NOT EXISTS address_observed_in_source boolean
        NOT NULL DEFAULT false;
    ALTER TABLE IF EXISTS {qschema}.{_quote_ident('partd_pharmacy_activity_stage_v2')}
        ADD COLUMN IF NOT EXISTS address_observed_in_source boolean
        NOT NULL DEFAULT false;

    CREATE TABLE {state_table} (
        singleton boolean PRIMARY KEY DEFAULT true,
        schema_version smallint NOT NULL DEFAULT 1,
        active_ruleset_version smallint NOT NULL DEFAULT 1,
        generation bigint NOT NULL DEFAULT 0,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT address_alias_state_v1_singleton_ck CHECK (singleton),
        CONSTRAINT address_alias_state_v1_schema_ck CHECK (schema_version = 1),
        CONSTRAINT address_alias_state_v1_ruleset_ck
            CHECK (active_ruleset_version = 1),
        CONSTRAINT address_alias_state_v1_generation_ck CHECK (generation >= 0)
    );
    INSERT INTO {state_table} (singleton) VALUES (true);

    CREATE TABLE {qschema}.{_quote_ident('address_alias_artifact_state_v1')} (
        artifact_name varchar(128) PRIMARY KEY,
        generation bigint NOT NULL DEFAULT 0,
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT address_alias_artifact_state_v1_generation_ck
            CHECK (generation >= 0)
    );
    INSERT INTO {qschema}.{_quote_ident('address_alias_artifact_state_v1')} (
        artifact_name,
        generation
    ) VALUES
        ('provider_directory_address_overlay', 0),
        ('provider_directory_address_corroboration', 0);

    CREATE TABLE {run_table} (
        run_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
        alias_kind varchar(64) NOT NULL,
        ruleset_version smallint NOT NULL,
        mode varchar(16) NOT NULL,
        status varchar(16) NOT NULL,
        reviewed_shadow_run_id uuid REFERENCES {run_table} (run_id),
        reviewed_candidate_digest varchar(64),
        reviewed_by varchar(256),
        reviewed_at timestamptz,
        candidate_digest varchar(64),
        evidence_digest varchar(64),
        scope_state_code varchar(2),
        scope_zip_prefix varchar(5),
        last_source_address_key uuid,
        archive_row_count bigint NOT NULL DEFAULT 0,
        source_count bigint NOT NULL DEFAULT 0,
        candidate_source_count bigint NOT NULL DEFAULT 0,
        candidate_row_count bigint NOT NULL DEFAULT 0,
        no_candidate_count bigint NOT NULL DEFAULT 0,
        active_skipped_count bigint NOT NULL DEFAULT 0,
        eligible_count bigint NOT NULL DEFAULT 0,
        ambiguous_count bigint NOT NULL DEFAULT 0,
        insufficient_provenance_count bigint NOT NULL DEFAULT 0,
        backfill_target_count bigint NOT NULL DEFAULT 0,
        evidence_target_count bigint NOT NULL DEFAULT 0,
        evidence_pair_count bigint NOT NULL DEFAULT 0,
        provenance_update_count bigint NOT NULL DEFAULT 0,
        reason_buckets jsonb NOT NULL DEFAULT '{{}}'::jsonb,
        sample_rows jsonb NOT NULL DEFAULT '[]'::jsonb,
        error_text text,
        started_at timestamptz NOT NULL DEFAULT now(),
        completed_at timestamptz,
        CONSTRAINT address_alias_run_v1_kind_ck
            CHECK (alias_kind = 'numeric_grid_direction_v1'),
        CONSTRAINT address_alias_run_v1_ruleset_ck CHECK (ruleset_version = 1),
        CONSTRAINT address_alias_run_v1_mode_ck
            CHECK (mode IN ('shadow', 'backfill', 'apply', 'revoke')),
        CONSTRAINT address_alias_run_v1_status_ck CHECK (
            status IN (
                'running', 'interrupted', 'sealed', 'backfilled', 'applied',
                'revoked', 'failed'
            )
        ),
        CONSTRAINT address_alias_run_v1_scope_state_ck CHECK (
            scope_state_code IS NULL OR scope_state_code ~ '^[A-Z]{{2}}$'
        ),
        CONSTRAINT address_alias_run_v1_scope_zip_ck CHECK (
            scope_zip_prefix IS NULL OR scope_zip_prefix ~ '^[0-9]{{1,5}}$'
        ),
        CONSTRAINT address_alias_run_v1_digest_ck CHECK (
            candidate_digest IS NULL OR candidate_digest ~ '^[0-9a-f]{{64}}$'
        ),
        CONSTRAINT address_alias_run_v1_evidence_digest_ck CHECK (
            evidence_digest IS NULL OR evidence_digest ~ '^[0-9a-f]{{64}}$'
        ),
        CONSTRAINT address_alias_run_v1_reviewed_digest_ck CHECK (
            reviewed_candidate_digest IS NULL
            OR reviewed_candidate_digest ~ '^[0-9a-f]{{64}}$'
        )
    );

    CREATE FUNCTION {qschema}.addr_alias_run_status_guard_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'address alias run audit rows are immutable'
                USING ERRCODE = '23514';
        END IF;
        IF OLD.status <> 'running' THEN
            IF (to_jsonb(OLD) - 'reviewed_by' - 'reviewed_at')
               IS DISTINCT FROM
               (to_jsonb(NEW) - 'reviewed_by' - 'reviewed_at') THEN
                RAISE EXCEPTION 'terminal address alias run evidence is immutable'
                    USING ERRCODE = '23514';
            END IF;
            IF ROW(OLD.reviewed_by, OLD.reviewed_at)
               IS DISTINCT FROM ROW(NEW.reviewed_by, NEW.reviewed_at)
               AND NOT (
                    OLD.reviewed_by IS NULL
                    AND OLD.reviewed_at IS NULL
                    AND NULLIF(trim(NEW.reviewed_by), '') IS NOT NULL
                    AND NEW.reviewed_at IS NOT NULL
               ) THEN
                RAISE EXCEPTION 'terminal address alias run review is write-once'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
        RETURN NEW;
    END;
    $$;
    CREATE TRIGGER address_alias_run_v1_status_guard_trg
    BEFORE UPDATE OR DELETE ON {run_table}
    FOR EACH ROW
    EXECUTE FUNCTION {qschema}.addr_alias_run_status_guard_v1();

    CREATE TABLE {candidate_table} (
        run_id uuid NOT NULL REFERENCES {run_table} (run_id) ON DELETE CASCADE,
        source_address_key uuid NOT NULL,
        source_identity_key text NOT NULL,
        target_address_key uuid NOT NULL REFERENCES {archive} (address_key),
        target_identity_key text NOT NULL,
        candidate_count integer NOT NULL,
        target_strict_source_bits integer NOT NULL,
        target_strict_source_count smallint NOT NULL,
        decision varchar(32) NOT NULL,
        review_status varchar(24) NOT NULL DEFAULT 'not_applicable',
        reviewed_by varchar(256),
        reviewed_at timestamptz,
        review_reason text,
        PRIMARY KEY (run_id, source_address_key, target_address_key),
        CONSTRAINT address_alias_candidate_v1_distinct_keys_ck
            CHECK (source_address_key <> target_address_key),
        CONSTRAINT address_alias_candidate_v1_count_ck CHECK (candidate_count >= 1),
        CONSTRAINT address_alias_candidate_v1_source_count_ck
            CHECK (target_strict_source_count >= 0),
        CONSTRAINT address_alias_candidate_v1_decision_ck CHECK (
            decision IN ('eligible', 'ambiguous', 'insufficient_provenance')
        ),
        CONSTRAINT address_alias_candidate_v1_review_ck CHECK (
            (decision = 'eligible' AND review_status IN ('pending', 'approved', 'rejected'))
            OR (decision <> 'eligible' AND review_status = 'not_applicable')
        ),
        CONSTRAINT address_alias_candidate_v1_reviewer_ck CHECK (
            (review_status IN ('approved', 'rejected')
                AND NULLIF(trim(reviewed_by), '') IS NOT NULL
                AND reviewed_at IS NOT NULL)
            OR (review_status IN ('pending', 'not_applicable')
                AND reviewed_by IS NULL
                AND reviewed_at IS NULL)
        )
    );
    CREATE INDEX address_alias_candidate_v1_source_idx
        ON {candidate_table} (source_address_key, run_id);

    CREATE FUNCTION {qschema}.addr_alias_candidate_guard_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        parent_status varchar(16);
    BEGIN
        SELECT status
          INTO parent_status
          FROM {run_table}
         WHERE run_id = CASE
             WHEN TG_OP = 'DELETE' THEN OLD.run_id
             ELSE NEW.run_id
         END
         FOR SHARE;
        IF parent_status IS NULL THEN
            RAISE EXCEPTION 'address alias candidate parent run is missing'
                USING ERRCODE = '23514';
        END IF;
        IF TG_OP = 'INSERT' THEN
            IF parent_status <> 'running' THEN
                RAISE EXCEPTION 'address alias candidates may only be inserted into a running run'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF TG_OP = 'DELETE' THEN
            IF parent_status <> 'running' THEN
                RAISE EXCEPTION 'sealed address alias candidate evidence is immutable'
                    USING ERRCODE = '23514';
            END IF;
            RETURN OLD;
        END IF;
        IF ROW(
                OLD.run_id,
                OLD.source_address_key,
                OLD.source_identity_key,
                OLD.target_address_key,
                OLD.target_identity_key,
                OLD.candidate_count,
                OLD.target_strict_source_bits,
                OLD.target_strict_source_count,
                OLD.decision
            ) IS DISTINCT FROM ROW(
                NEW.run_id,
                NEW.source_address_key,
                NEW.source_identity_key,
                NEW.target_address_key,
                NEW.target_identity_key,
                NEW.candidate_count,
                NEW.target_strict_source_bits,
                NEW.target_strict_source_count,
                NEW.decision
            ) THEN
            IF parent_status <> 'running' THEN
                RAISE EXCEPTION 'sealed address alias candidate evidence is immutable'
                    USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
        END IF;
        IF parent_status = 'running' THEN
            RETURN NEW;
        END IF;
        IF OLD.decision = 'eligible'
           AND OLD.review_status = 'pending'
           AND NEW.review_status IN ('approved', 'rejected') THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'address alias candidate review is terminal and immutable'
            USING ERRCODE = '23514';
    END;
    $$;
    CREATE TRIGGER address_alias_candidate_v1_guard_trg
    BEFORE INSERT OR UPDATE OR DELETE ON {candidate_table}
    FOR EACH ROW
    EXECUTE FUNCTION {qschema}.addr_alias_candidate_guard_v1();

    CREATE TABLE {alias_table} (
        alias_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        source_address_key uuid NOT NULL,
        source_identity_key text NOT NULL,
        target_address_key uuid NOT NULL REFERENCES {archive} (address_key),
        target_identity_key text NOT NULL,
        alias_kind varchar(64) NOT NULL,
        ruleset_version smallint NOT NULL,
        target_strict_source_bits integer NOT NULL,
        target_strict_source_count smallint NOT NULL,
        candidate_count integer NOT NULL,
        shadow_run_id uuid NOT NULL REFERENCES {run_table} (run_id),
        apply_run_id uuid NOT NULL REFERENCES {run_table} (run_id),
        reviewed_candidate_digest varchar(64) NOT NULL,
        applied_at timestamptz NOT NULL DEFAULT now(),
        revoked_at timestamptz,
        revoked_reason text,
        revoked_by varchar(256),
        revoke_run_id uuid REFERENCES {run_table} (run_id),
        created_at timestamptz NOT NULL DEFAULT now(),
        updated_at timestamptz NOT NULL DEFAULT now(),
        CONSTRAINT address_alias_v1_distinct_keys_ck
            CHECK (source_address_key <> target_address_key),
        CONSTRAINT address_alias_v1_kind_ck
            CHECK (alias_kind = 'numeric_grid_direction_v1'),
        CONSTRAINT address_alias_v1_ruleset_ck CHECK (ruleset_version = 1),
        CONSTRAINT address_alias_v1_evidence_ck CHECK (
            target_strict_source_count >= 2
            AND candidate_count = 1
        ),
        CONSTRAINT address_alias_v1_reviewed_digest_ck CHECK (
            reviewed_candidate_digest ~ '^[0-9a-f]{{64}}$'
        ),
        CONSTRAINT address_alias_v1_revoke_ck CHECK (
            (
                revoked_at IS NULL
                AND revoked_reason IS NULL
                AND revoked_by IS NULL
                AND revoke_run_id IS NULL
            )
            OR (
                revoked_at IS NOT NULL
                AND NULLIF(trim(revoked_reason), '') IS NOT NULL
                AND NULLIF(trim(revoked_by), '') IS NOT NULL
                AND revoke_run_id IS NOT NULL
            )
        )
    );
    CREATE UNIQUE INDEX address_alias_v1_active_source_idx
        ON {alias_table} (source_address_key)
        WHERE revoked_at IS NULL;
    CREATE INDEX address_alias_v1_target_idx
        ON {alias_table} (target_address_key)
        WHERE revoked_at IS NULL;
    CREATE INDEX address_alias_v1_shadow_run_idx
        ON {alias_table} (shadow_run_id);

    CREATE FUNCTION {qschema}.addr_alias_immutable_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'address_alias_v1 rows must be revoked, not deleted'
                USING ERRCODE = '23514';
        END IF;
        IF ROW(
                OLD.alias_id,
                OLD.source_address_key,
                OLD.source_identity_key,
                OLD.target_address_key,
                OLD.target_identity_key,
                OLD.alias_kind,
                OLD.ruleset_version,
                OLD.target_strict_source_bits,
                OLD.target_strict_source_count,
                OLD.candidate_count,
                OLD.shadow_run_id,
                OLD.apply_run_id,
                OLD.reviewed_candidate_digest,
                OLD.applied_at,
                OLD.created_at
            ) IS DISTINCT FROM ROW(
                NEW.alias_id,
                NEW.source_address_key,
                NEW.source_identity_key,
                NEW.target_address_key,
                NEW.target_identity_key,
                NEW.alias_kind,
                NEW.ruleset_version,
                NEW.target_strict_source_bits,
                NEW.target_strict_source_count,
                NEW.candidate_count,
                NEW.shadow_run_id,
                NEW.apply_run_id,
                NEW.reviewed_candidate_digest,
                NEW.applied_at,
                NEW.created_at
            ) THEN
            RAISE EXCEPTION 'address_alias_v1 identity and review evidence are immutable'
                USING ERRCODE = '23514';
        END IF;
        IF OLD.revoked_at IS NOT NULL AND ROW(
                NEW.revoked_at,
                NEW.revoked_reason,
                NEW.revoked_by,
                NEW.revoke_run_id
            ) IS DISTINCT FROM ROW(
                OLD.revoked_at,
                OLD.revoked_reason,
                OLD.revoked_by,
                OLD.revoke_run_id
            ) THEN
            RAISE EXCEPTION 'address_alias_v1 revocation is immutable'
                USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END;
    $$;
    CREATE TRIGGER address_alias_v1_immutable_trg
    BEFORE UPDATE OR DELETE ON {alias_table}
    FOR EACH ROW
    EXECUTE FUNCTION {qschema}.addr_alias_immutable_v1();

    CREATE FUNCTION {qschema}.addr_alias_generation_after_insert_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF EXISTS (SELECT 1 FROM new_alias_rows WHERE revoked_at IS NULL) THEN
            PERFORM pg_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'));
            UPDATE {state_table}
               SET generation = generation + 1,
                   updated_at = now()
             WHERE singleton = true;
        END IF;
        RETURN NULL;
    END;
    $$;
    CREATE TRIGGER address_alias_v1_generation_insert_trg
    AFTER INSERT ON {alias_table}
    REFERENCING NEW TABLE AS new_alias_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION {qschema}.addr_alias_generation_after_insert_v1();

    CREATE FUNCTION {qschema}.addr_alias_generation_after_update_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF EXISTS (
            SELECT 1
            FROM old_alias_rows AS old_row
            JOIN new_alias_rows AS new_row USING (alias_id)
            WHERE (old_row.revoked_at IS NULL OR new_row.revoked_at IS NULL)
              AND ROW(
                    old_row.source_address_key,
                    old_row.source_identity_key,
                    old_row.target_address_key,
                    old_row.target_identity_key,
                    old_row.alias_kind,
                    old_row.ruleset_version,
                    old_row.revoked_at
                  ) IS DISTINCT FROM ROW(
                    new_row.source_address_key,
                    new_row.source_identity_key,
                    new_row.target_address_key,
                    new_row.target_identity_key,
                    new_row.alias_kind,
                    new_row.ruleset_version,
                    new_row.revoked_at
                  )
        ) THEN
            PERFORM pg_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'));
            UPDATE {state_table}
               SET generation = generation + 1,
                   updated_at = now()
             WHERE singleton = true;
        END IF;
        RETURN NULL;
    END;
    $$;
    CREATE TRIGGER address_alias_v1_generation_update_trg
    AFTER UPDATE ON {alias_table}
    REFERENCING OLD TABLE AS old_alias_rows NEW TABLE AS new_alias_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION {qschema}.addr_alias_generation_after_update_v1();

    CREATE FUNCTION {qschema}.addr_alias_generation_after_delete_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    BEGIN
        IF EXISTS (SELECT 1 FROM old_alias_rows WHERE revoked_at IS NULL) THEN
            PERFORM pg_advisory_xact_lock(hashtext('address_numeric_grid_alias_v1'));
            UPDATE {state_table}
               SET generation = generation + 1,
                   updated_at = now()
             WHERE singleton = true;
        END IF;
        RETURN NULL;
    END;
    $$;
    CREATE TRIGGER address_alias_v1_generation_delete_trg
    AFTER DELETE ON {alias_table}
    REFERENCING OLD TABLE AS old_alias_rows
    FOR EACH STATEMENT
    EXECUTE FUNCTION {qschema}.addr_alias_generation_after_delete_v1();
    """


def _split_sql_statements(script: str) -> tuple[str, ...]:
    """Split controlled migration DDL without splitting quoted function bodies."""
    statements: list[str] = []
    statement_start = 0
    index = 0
    quote: str | None = None
    dollar_tag: str | None = None
    line_comment = False
    block_comment = False
    while index < len(script):
        if line_comment:
            if script[index] == "\n":
                line_comment = False
            index += 1
            continue
        if block_comment:
            if script.startswith("*/", index):
                block_comment = False
                index += 2
            else:
                index += 1
            continue
        if dollar_tag is not None:
            if script.startswith(dollar_tag, index):
                index += len(dollar_tag)
                dollar_tag = None
            else:
                index += 1
            continue
        if quote is not None:
            if script[index] == quote:
                if index + 1 < len(script) and script[index + 1] == quote:
                    index += 2
                    continue
                quote = None
            index += 1
            continue
        if script.startswith("--", index):
            line_comment = True
            index += 2
            continue
        if script.startswith("/*", index):
            block_comment = True
            index += 2
            continue
        if script[index] in {"'", '"'}:
            quote = script[index]
            index += 1
            continue
        if script[index] == "$":
            tag_end = script.find("$", index + 1)
            if tag_end >= 0:
                candidate = script[index : tag_end + 1]
                tag_body = candidate[1:-1]
                if not tag_body or tag_body.replace("_", "a").isalnum():
                    dollar_tag = candidate
                    index = tag_end + 1
                    continue
        if script[index] == ";":
            statement = script[statement_start : index + 1].strip()
            if statement:
                statements.append(statement)
            statement_start = index + 1
        index += 1
    trailing = script[statement_start:].strip()
    if trailing:
        statements.append(trailing)
    return tuple(statements)


def upgrade() -> None:
    schema = _schema()
    op.execute(_numeric_grid_function_sql(schema))
    for statement in _split_sql_statements(_alias_schema_sql(schema)):
        op.execute(statement)


def _downgrade_statements(schema: str) -> tuple[str, ...]:
    qschema = _quote_ident(schema)
    return (
        f"DROP TABLE {qschema}.{_quote_ident('address_alias_v1')};",
        f"DROP TABLE {qschema}.{_quote_ident('address_alias_candidate_v1')};",
        f"DROP FUNCTION {qschema}.addr_alias_candidate_guard_v1();",
        f"DROP TRIGGER address_alias_run_v1_status_guard_trg "
        f"ON {qschema}.{_quote_ident('address_alias_run_v1')};",
        f"DROP FUNCTION {qschema}.addr_alias_run_status_guard_v1();",
        f"DROP TABLE {qschema}.{_quote_ident('address_alias_run_v1')};",
        f"DROP FUNCTION {qschema}.addr_alias_generation_after_delete_v1();",
        f"DROP FUNCTION {qschema}.addr_alias_generation_after_update_v1();",
        f"DROP FUNCTION {qschema}.addr_alias_generation_after_insert_v1();",
        f"DROP FUNCTION {qschema}.addr_alias_immutable_v1();",
        f"DROP TABLE {qschema}.{_quote_ident('address_alias_artifact_state_v1')};",
        f"DROP TABLE {qschema}.{_quote_ident('address_alias_state_v1')};",
        f"ALTER TABLE IF EXISTS {qschema}.{_quote_ident('partd_pharmacy_activity_v2')} "
        "DROP COLUMN IF EXISTS address_observed_in_source;",
        f"ALTER TABLE IF EXISTS {qschema}.{_quote_ident('partd_pharmacy_activity_stage_v2')} "
        "DROP COLUMN IF EXISTS address_observed_in_source;",
        f"ALTER TABLE {qschema}.{_quote_ident('address_archive_v2')} "
        "DROP COLUMN strict_source_bits;",
        f"DROP FUNCTION {qschema}.addr_numeric_grid_parts_v1(text, text);",
    )


def downgrade() -> None:
    for statement in _downgrade_statements(_schema()):
        op.execute(statement)
