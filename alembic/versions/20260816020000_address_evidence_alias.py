# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Add reviewed evidence-gated address aliases.

Revision ID: 20260816020000_address_evidence_alias
Revises: 20260816010000_provider_directory_terminal_publication_guard
"""

from __future__ import annotations

import os
import re

from alembic import op

from process.ext.address_pub28 import (
    PUB28_STREET_SUFFIX_MAP,
    PUB28_UNIT_DESIGNATOR_MAP,
)


revision = "20260816020000_address_evidence_alias"
down_revision = "20260816010000_provider_directory_terminal_publication_guard"
branch_labels = None
depends_on = None


_EVIDENCE_KIND = "evidence_gated_address_match_v1"
_NUMERIC_KIND = "numeric_grid_direction_v1"
_MATCH_RULES = (
    "candidate_confirmed_bare_unit",
    "unit_designator_punctuation",
    "candidate_confirmed_spaced_unit",
    "direction_relocation",
    "terminal_suffix_omission",
)


def _schema() -> str:
    return os.getenv("DB_SCHEMA") or os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


def _q(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _ql(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _evidence_match_function_sql(schema: str) -> str:
    qschema = _q(schema)
    unit_prefixes = ", ".join(
        _ql(value)
        for value in sorted(
            set(PUB28_UNIT_DESIGNATOR_MAP.values()),
            key=lambda value: (-len(value), value),
        )
    )
    unit_designators = "|".join(
        re.escape(value)
        for value in sorted(PUB28_UNIT_DESIGNATOR_MAP, key=len, reverse=True)
    ).replace("'", "''")
    route_markers = sorted(
        {
            token
            for token, normalized in PUB28_STREET_SUFFIX_MAP.items()
            if normalized == "hwy"
        }
        | {"route", "rte", "interstate", "us", "sr", "sh", "cr", "i", "fm"}
    )
    route_marker_values = ", ".join(_ql(value) for value in route_markers)
    return rf"""
    CREATE OR REPLACE FUNCTION {qschema}.addr_evidence_alias_match_v1(
        source_first_line text,
        source_second_line text,
        source_city text,
        source_state text,
        source_postal text,
        source_country text,
        target_first_line text,
        target_second_line text,
        target_city text,
        target_state text,
        target_postal text,
        target_country text,
        target_address_key uuid
    )
    RETURNS TABLE(
        match_rule text,
        effective_source_first_line text,
        street_relation text
    )
    LANGUAGE plpgsql
    IMMUTABLE
    PARALLEL SAFE
    AS $function$
    DECLARE
        source_unit text := {qschema}.addr_unit_norm_v1(
            source_first_line, source_second_line
        );
        target_unit text := {qschema}.addr_unit_norm_v1(
            target_first_line, target_second_line
        );
        source_street text := {qschema}.addr_street_norm_v1(
            source_first_line, source_second_line
        );
        target_street text := {qschema}.addr_street_norm_v1(
            target_first_line, target_second_line
        );
        source_direction text := {qschema}.addr_street_direction_token_v1(
            source_first_line, source_second_line
        );
        target_direction text := {qschema}.addr_street_direction_token_v1(
            target_first_line, target_second_line
        );
        source_suffix text := {qschema}.addr_street_suffix_token_v1(
            source_first_line, source_second_line
        );
        target_suffix text := {qschema}.addr_street_suffix_token_v1(
            target_first_line, target_second_line
        );
        relation text;
        source_tokens text[];
        second_tokens text[];
        tail_tokens text[];
        tail_size integer;
        base_end integer;
        base_first text;
        bare_value text;
        target_prefix text;
        candidate_prefix text;
        target_value text;
        target_text text := lower(
            COALESCE(target_first_line, '') || ' ' ||
            COALESCE(target_second_line, '')
        );
        alternate_first text;
        alternate_second text;
        prefixes constant text[] := ARRAY[{unit_prefixes}];
        route_markers constant text[] := ARRAY[{route_marker_values}];
        punctuation_pattern constant text :=
            '\m({unit_designators})\M\s*:\s*';
    BEGIN
        IF source_street = target_street THEN
            relation := 'same_street';
        ELSIF source_direction IS NOT NULL
          AND source_direction = target_direction
          AND {qschema}.addr_street_directionless_norm_v1(
                source_first_line, source_second_line
              ) = {qschema}.addr_street_directionless_norm_v1(
                target_first_line, target_second_line
              ) THEN
            relation := 'direction_relocation';
        ELSIF (source_suffix IS NULL) <> (target_suffix IS NULL)
          AND source_direction IS NOT DISTINCT FROM target_direction
          AND {qschema}.addr_street_suffixless_norm_v1(
                source_first_line, source_second_line
              ) = {qschema}.addr_street_suffixless_norm_v1(
                target_first_line, target_second_line
              ) THEN
            relation := 'terminal_suffix_omission';
        END IF;

        IF COALESCE(trim(source_second_line), '') = ''
          AND source_unit = ''
          AND target_unit <> ''
          AND (
                strpos(target_text, '#') > 0
                OR EXISTS (
                    SELECT 1
                    FROM regexp_split_to_table(target_text, '[^a-z0-9#]+') AS token(value)
                    WHERE {qschema}.addr_unit_prefix_v1(token.value) IS NOT NULL
                )
          ) THEN
            SELECT array_agg(lower(hit.part[1]) ORDER BY hit.ord)
              INTO source_tokens
              FROM regexp_matches(
                    COALESCE(source_first_line, ''),
                    '([a-z0-9]+)',
                    'gi'
                   ) WITH ORDINALITY AS hit(part, ord);
            target_prefix := NULL;
            FOREACH candidate_prefix IN ARRAY prefixes LOOP
                IF length(target_unit) > length(candidate_prefix)
                  AND left(target_unit, length(candidate_prefix)) = candidate_prefix THEN
                    target_prefix := candidate_prefix;
                    EXIT;
                END IF;
            END LOOP;
            IF target_prefix IS NOT NULL THEN
                FOR tail_size IN 1..2 LOOP
                    base_end := cardinality(source_tokens) - tail_size;
                    CONTINUE WHEN base_end < 2;
                    tail_tokens := source_tokens[base_end + 1:cardinality(source_tokens)];
                    CONTINUE WHEN EXISTS (
                        SELECT 1
                        FROM unnest(tail_tokens) AS token(value)
                        WHERE {qschema}.addr_street_token_is_directional_v1(token.value)
                           OR {qschema}.addr_street_token_is_suffix_v1(token.value)
                    );
                    CONTINUE WHEN source_tokens[base_end] = ANY(route_markers)
                        OR (
                            source_tokens[base_end] = 's'
                            AND source_tokens[base_end - 1] = 'u'
                        )
                        OR (
                            source_tokens[base_end] IN ('road', 'rd')
                            AND source_tokens[base_end - 1] IN ('county', 'state')
                        )
                        OR (
                            source_tokens[base_end] IN ('no', 'number')
                            AND source_tokens[base_end - 1] = ANY(route_markers)
                        )
                        OR (
                            source_tokens[base_end] = 'loop'
                            AND (
                                base_end = 2
                                OR {qschema}.addr_street_token_is_directional_v1(
                                    source_tokens[base_end - 1]
                                )
                                OR source_tokens[base_end - 1] IN ('business', 'state')
                            )
                        );
                    base_first := rtrim(
                        left(
                            COALESCE(source_first_line, ''),
                            regexp_instr(
                                COALESCE(source_first_line, ''),
                                '[a-z0-9]+',
                                1,
                                base_end + 1,
                                0,
                                'i'
                            ) - 1
                        ),
                        ' ,'
                    );
                    bare_value := array_to_string(tail_tokens, '');
                    CONTINUE WHEN {qschema}.addr_unit_norm_v1(
                        base_first,
                        target_prefix || ' ' || bare_value
                    ) <> target_unit;
                    IF {qschema}.addr_street_norm_v1(base_first, '') = target_street THEN
                        relation := 'same_street';
                    ELSIF {qschema}.addr_street_direction_token_v1(base_first, '')
                            IS NOT NULL
                      AND {qschema}.addr_street_direction_token_v1(base_first, '')
                            = target_direction
                      AND {qschema}.addr_street_directionless_norm_v1(base_first, '')
                            = {qschema}.addr_street_directionless_norm_v1(
                                target_first_line, target_second_line
                              ) THEN
                        relation := 'direction_relocation';
                    ELSIF (
                            {qschema}.addr_street_suffix_token_v1(base_first, '') IS NULL
                          ) <> (target_suffix IS NULL)
                      AND {qschema}.addr_street_direction_token_v1(base_first, '')
                            IS NOT DISTINCT FROM target_direction
                      AND {qschema}.addr_street_suffixless_norm_v1(base_first, '')
                            = {qschema}.addr_street_suffixless_norm_v1(
                                target_first_line, target_second_line
                              ) THEN
                        relation := 'terminal_suffix_omission';
                    ELSE
                        relation := NULL;
                    END IF;
                    CONTINUE WHEN relation IS NULL;
                    CONTINUE WHEN relation = 'same_street'
                      AND {qschema}.addr_key_v1(
                            base_first,
                            target_prefix || ' ' || bare_value,
                            source_city,
                            source_state,
                            source_postal,
                            source_country
                          ) IS DISTINCT FROM target_address_key;
                    match_rule := 'candidate_confirmed_bare_unit';
                    effective_source_first_line := base_first;
                    street_relation := relation;
                    RETURN NEXT;
                    RETURN;
                END LOOP;
            END IF;
        END IF;

        IF source_unit = '' THEN
            alternate_first := regexp_replace(
                COALESCE(source_first_line, ''),
                punctuation_pattern,
                '\1 ',
                'gi'
            );
            alternate_second := regexp_replace(
                COALESCE(source_second_line, ''),
                punctuation_pattern,
                '\1 ',
                'gi'
            );
            IF (alternate_first, alternate_second) IS DISTINCT FROM
               (COALESCE(source_first_line, ''), COALESCE(source_second_line, ''))
              AND {qschema}.addr_unit_norm_v1(
                    alternate_first, alternate_second
                  ) <> ''
              AND {qschema}.addr_unit_norm_v1(
                    alternate_first, alternate_second
                  ) = target_unit
              AND {qschema}.addr_key_v1(
                    alternate_first,
                    alternate_second,
                    source_city,
                    source_state,
                    source_postal,
                    source_country
                  ) = target_address_key THEN
                match_rule := 'unit_designator_punctuation';
                effective_source_first_line := alternate_first;
                street_relation := 'same_street';
                RETURN NEXT;
                RETURN;
            END IF;
        END IF;

        IF source_unit = ''
          AND target_unit <> ''
          AND COALESCE(trim(source_second_line), '') <> '' THEN
            SELECT array_agg(lower(hit.part[1]) ORDER BY hit.ord)
              INTO second_tokens
              FROM regexp_matches(
                    COALESCE(source_second_line, ''),
                    '([a-z0-9]+)',
                    'gi'
                   ) WITH ORDINALITY AS hit(part, ord);
            target_prefix := NULL;
            FOREACH candidate_prefix IN ARRAY prefixes LOOP
                IF length(target_unit) > length(candidate_prefix)
                  AND left(target_unit, length(candidate_prefix)) = candidate_prefix THEN
                    target_prefix := candidate_prefix;
                    EXIT;
                END IF;
            END LOOP;
            target_value := substring(target_unit FROM length(target_prefix) + 1);
            IF target_prefix IS NOT NULL
              AND cardinality(second_tokens) BETWEEN 1 AND 2
              AND target_value = array_to_string(second_tokens, '')
              AND NOT EXISTS (
                    SELECT 1
                    FROM unnest(second_tokens) AS token(value)
                    WHERE {qschema}.addr_street_token_is_directional_v1(token.value)
                       OR {qschema}.addr_street_token_is_suffix_v1(token.value)
              ) THEN
                IF {qschema}.addr_street_norm_v1(source_first_line, '') = target_street THEN
                    relation := 'same_street';
                ELSIF {qschema}.addr_street_direction_token_v1(source_first_line, '')
                        IS NOT NULL
                  AND {qschema}.addr_street_direction_token_v1(source_first_line, '')
                        = target_direction
                  AND {qschema}.addr_street_directionless_norm_v1(source_first_line, '')
                        = {qschema}.addr_street_directionless_norm_v1(
                            target_first_line, target_second_line
                          ) THEN
                    relation := 'direction_relocation';
                ELSIF (
                        {qschema}.addr_street_suffix_token_v1(source_first_line, '') IS NULL
                      ) <> (target_suffix IS NULL)
                  AND {qschema}.addr_street_direction_token_v1(source_first_line, '')
                        IS NOT DISTINCT FROM target_direction
                  AND {qschema}.addr_street_suffixless_norm_v1(source_first_line, '')
                        = {qschema}.addr_street_suffixless_norm_v1(
                            target_first_line, target_second_line
                          ) THEN
                    relation := 'terminal_suffix_omission';
                ELSE
                    relation := NULL;
                END IF;
                IF relation IS NOT NULL THEN
                    match_rule := 'candidate_confirmed_spaced_unit';
                    effective_source_first_line := source_first_line;
                    street_relation := relation;
                    RETURN NEXT;
                    RETURN;
                END IF;
            END IF;
        END IF;

        IF source_unit = target_unit
          AND source_suffix IS NULL
          AND target_suffix IS NOT NULL
          AND relation = 'terminal_suffix_omission' THEN
            match_rule := 'terminal_suffix_omission';
            effective_source_first_line := source_first_line;
            street_relation := relation;
            RETURN NEXT;
            RETURN;
        END IF;
        IF source_unit = target_unit
          AND source_direction IS NOT NULL
          AND source_direction = target_direction
          AND relation = 'direction_relocation' THEN
            match_rule := 'direction_relocation';
            effective_source_first_line := source_first_line;
            street_relation := relation;
            RETURN NEXT;
            RETURN;
        END IF;
    END;
    $function$;
    """


def _candidate_guard_sql(schema: str, *, include_evidence: bool) -> str:
    qschema = _q(schema)
    run_table = f"{qschema}.{_q('address_alias_run_v1')}"
    candidate_table = f"{qschema}.{_q('address_alias_candidate_v1')}"
    evidence_validation = ""
    evidence_old = ""
    evidence_new = ""
    if include_evidence:
        evidence_validation = rf"""
        IF TG_OP <> 'DELETE' THEN
            IF parent_kind = '{_EVIDENCE_KIND}' AND num_nonnulls(
                NEW.match_rule,
                NEW.match_classification,
                NEW.evidence_npi,
                NEW.evidence_npi_count
            ) <> 4 THEN
                RAISE EXCEPTION 'evidence address alias candidates require exact match evidence'
                    USING ERRCODE = '23514';
            END IF;
            IF parent_kind = '{_NUMERIC_KIND}' AND num_nonnulls(
                NEW.match_rule,
                NEW.match_classification,
                NEW.evidence_npi,
                NEW.evidence_npi_count
            ) <> 0 THEN
                RAISE EXCEPTION 'numeric-grid candidates cannot carry address match evidence'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
        """
        evidence_old = """,
                OLD.match_rule,
                OLD.match_classification,
                OLD.evidence_npi,
                OLD.evidence_npi_count"""
        evidence_new = """,
                NEW.match_rule,
                NEW.match_classification,
                NEW.evidence_npi,
                NEW.evidence_npi_count"""
    return rf"""
    CREATE OR REPLACE FUNCTION {qschema}.addr_alias_candidate_guard_v1()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $function$
    DECLARE
        parent_status varchar(16);
        parent_kind varchar(64);
    BEGIN
        SELECT status, alias_kind
          INTO parent_status, parent_kind
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
        {evidence_validation}
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
                OLD.decision{evidence_old}
            ) IS DISTINCT FROM ROW(
                NEW.run_id,
                NEW.source_address_key,
                NEW.source_identity_key,
                NEW.target_address_key,
                NEW.target_identity_key,
                NEW.candidate_count,
                NEW.target_strict_source_bits,
                NEW.target_strict_source_count,
                NEW.decision{evidence_new}
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
    $function$;
    """


def _upgrade_statements(schema: str) -> tuple[str, ...]:
    qschema = _q(schema)
    state = f"{qschema}.{_q('address_alias_state_v1')}"
    runs = f"{qschema}.{_q('address_alias_run_v1')}"
    candidates = f"{qschema}.{_q('address_alias_candidate_v1')}"
    aliases = f"{qschema}.{_q('address_alias_v1')}"
    match_rules = ", ".join(_ql(value) for value in _MATCH_RULES)
    return (
        f"ALTER TABLE {state} DROP CONSTRAINT address_alias_state_v1_schema_ck;",
        f"ALTER TABLE {state} ALTER COLUMN schema_version SET DEFAULT 2;",
        f"UPDATE {state} SET schema_version = 2, generation = generation + 1, "
        "updated_at = now() "
        "WHERE singleton = true;",
        f"ALTER TABLE {state} ADD CONSTRAINT address_alias_state_v1_schema_ck "
        "CHECK (schema_version = 2);",
        f"""
        ALTER TABLE {runs}
            DROP CONSTRAINT address_alias_run_v1_kind_ck,
            ADD CONSTRAINT address_alias_run_v1_kind_ck
                CHECK (alias_kind IN ('{_NUMERIC_KIND}', '{_EVIDENCE_KIND}'));
        """,
        f"""
        ALTER TABLE {aliases}
            DROP CONSTRAINT address_alias_v1_kind_ck,
            ADD CONSTRAINT address_alias_v1_kind_ck
                CHECK (alias_kind IN ('{_NUMERIC_KIND}', '{_EVIDENCE_KIND}'));
        """,
        f"""
        ALTER TABLE {candidates}
            ADD COLUMN match_rule varchar(64),
            ADD COLUMN match_classification varchar(16),
            ADD COLUMN evidence_npi bigint,
            ADD COLUMN evidence_npi_count integer,
            ADD CONSTRAINT address_alias_candidate_v1_match_evidence_ck CHECK (
                num_nonnulls(
                    match_rule, match_classification,
                    evidence_npi, evidence_npi_count
                ) = 0
                OR (
                    num_nonnulls(
                        match_rule, match_classification,
                        evidence_npi, evidence_npi_count
                    ) = 4
                    AND
                    match_rule IN ({match_rules})
                    AND match_classification = 'exact'
                    AND evidence_npi BETWEEN 1000000000 AND 2999999999
                    AND {qschema}.public_evidence_npi_valid(evidence_npi::text)
                    AND evidence_npi_count >= 1
                )
            );
        """,
        _candidate_guard_sql(schema, include_evidence=True),
        _evidence_match_function_sql(schema),
    )


def _downgrade_statements(schema: str) -> tuple[str, ...]:
    qschema = _q(schema)
    state = f"{qschema}.{_q('address_alias_state_v1')}"
    runs = f"{qschema}.{_q('address_alias_run_v1')}"
    candidates = f"{qschema}.{_q('address_alias_candidate_v1')}"
    aliases = f"{qschema}.{_q('address_alias_v1')}"
    return (
        f"""
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM {runs} WHERE alias_kind = '{_EVIDENCE_KIND}')
               OR EXISTS (SELECT 1 FROM {aliases} WHERE alias_kind = '{_EVIDENCE_KIND}') THEN
                RAISE EXCEPTION 'cannot downgrade with evidence-gated alias history';
            END IF;
        END;
        $block$;
        """,
        f"DROP FUNCTION {qschema}.addr_evidence_alias_match_v1(text, text, text, text, text, text, text, text, text, text, text, text, uuid);",
        _candidate_guard_sql(schema, include_evidence=False),
        f"""
        ALTER TABLE {candidates}
            DROP CONSTRAINT address_alias_candidate_v1_match_evidence_ck,
            DROP COLUMN evidence_npi_count,
            DROP COLUMN evidence_npi,
            DROP COLUMN match_classification,
            DROP COLUMN match_rule;
        """,
        f"""
        ALTER TABLE {aliases}
            DROP CONSTRAINT address_alias_v1_kind_ck,
            ADD CONSTRAINT address_alias_v1_kind_ck
                CHECK (alias_kind = '{_NUMERIC_KIND}');
        """,
        f"""
        ALTER TABLE {runs}
            DROP CONSTRAINT address_alias_run_v1_kind_ck,
            ADD CONSTRAINT address_alias_run_v1_kind_ck
                CHECK (alias_kind = '{_NUMERIC_KIND}');
        """,
        f"ALTER TABLE {state} DROP CONSTRAINT address_alias_state_v1_schema_ck;",
        f"ALTER TABLE {state} ALTER COLUMN schema_version SET DEFAULT 1;",
        f"UPDATE {state} SET schema_version = 1, updated_at = now() "
        "WHERE singleton = true;",
        f"ALTER TABLE {state} ADD CONSTRAINT address_alias_state_v1_schema_ck "
        "CHECK (schema_version = 1);",
    )


def upgrade() -> None:
    for statement in _upgrade_statements(_schema()):
        op.execute(statement.strip())


def downgrade() -> None:
    for statement in _downgrade_statements(_schema()):
        op.execute(statement.strip())
