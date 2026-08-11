# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SQL builders for reviewed, offline address aliases."""

from __future__ import annotations

import re


ADDRESS_ALIAS_TABLE = "address_alias_v1"
ADDRESS_ALIAS_STATE_TABLE = "address_alias_state_v1"
ADDRESS_ALIAS_RUN_TABLE = "address_alias_run_v1"
ADDRESS_ALIAS_CANDIDATE_TABLE = "address_alias_candidate_v1"
ADDRESS_ALIAS_ARTIFACT_STATE_TABLE = "address_alias_artifact_state_v1"
ADDRESS_ALIAS_SCHEMA_VERSION = 1
NUMERIC_GRID_ALIAS_KIND = "numeric_grid_direction_v1"
NUMERIC_GRID_ALIAS_RULESET_VERSION = 1
NUMERIC_GRID_ALIAS_MODES = frozenset({"off", "shadow", "apply"})
ADDRESS_ALIAS_ADVISORY_LOCK_KEY = "address_numeric_grid_alias_v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def numeric_grid_alias_mode(value: str | None) -> str:
    """Validate and normalize one offline alias operation mode."""
    normalized = str(value or "off").strip().lower()
    if normalized not in NUMERIC_GRID_ALIAS_MODES:
        choices = ", ".join(sorted(NUMERIC_GRID_ALIAS_MODES))
        raise ValueError(f"mode must be one of: {choices}")
    return normalized


def _quote_ident(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid SQL identifier: {value!r}")
    return f'"{value}"'


def _relation(schema: str, table: str) -> str:
    return f"{_quote_ident(schema)}.{_quote_ident(table)}"


def active_alias_generation_sql(*, schema: str) -> str:
    """Read and validate the singleton alias generation."""
    state = _relation(schema, ADDRESS_ALIAS_STATE_TABLE)
    return f"""
        SELECT schema_version, active_ruleset_version, generation
        FROM {state}
        WHERE singleton = true;
    """


def alias_advisory_xact_lock_sql() -> str:
    """Serialize alias activation with derived-artifact cutovers."""
    return (
        "SELECT pg_advisory_xact_lock("
        f"hashtext('{ADDRESS_ALIAS_ADVISORY_LOCK_KEY}')"
        ");"
    )


def numeric_grid_source_count_sql(*, schema: str, archive: str) -> str:
    """Count structurally incomplete grid sources in one optional pilot scope."""
    qschema = _quote_ident(schema)
    return f"""
        SELECT count(*)::bigint
        FROM {archive} AS source
        CROSS JOIN LATERAL (
            SELECT {qschema}.addr_numeric_grid_parts_v1(
                source.first_line,
                source.second_line
            ) AS parts
        ) AS parsed
        WHERE source.address_key IS NOT NULL
          AND source.identity_key IS NOT NULL
          AND COALESCE(source.precision, split_part(source.identity_key, '|', 8)) = 'street'
          AND source.merged_into IS NULL
          AND parsed.parts IS NOT NULL
          AND (
                (parsed.parts[2] = '' AND parsed.parts[4] <> '')
             OR (parsed.parts[2] <> '' AND parsed.parts[4] = '')
          )
          AND (
                CAST(:scope_state_code AS varchar) IS NULL
                OR source.state_code = CAST(:scope_state_code AS varchar)
          )
          AND (
                CAST(:scope_zip_prefix AS varchar) IS NULL
                OR source.zip5 LIKE CAST(:scope_zip_prefix AS varchar) || '%'
          );
    """


_NUMERIC_GRID_CANDIDATE_INSERT_TEMPLATE = """
        WITH parsed_archive AS MATERIALIZED (
            SELECT
                archived.address_key,
                archived.identity_key,
                COALESCE(archived.unit_norm, '') AS unit_norm,
                archived.state_code,
                archived.zip5,
                COALESCE(archived.country_code, 'US') AS country_code,
                COALESCE(archived.strict_source_bits, 0) AS strict_source_bits,
                bit_count(
                    (COALESCE(archived.strict_source_bits, 0)::bigint)::bit(64)
                )::smallint AS strict_source_count,
                {qschema}.addr_numeric_grid_parts_v1(
                    archived.first_line,
                    archived.second_line
                ) AS parts
            FROM {archive} AS archived
            WHERE archived.address_key IS NOT NULL
              AND archived.identity_key IS NOT NULL
              AND COALESCE(
                    archived.precision,
                    split_part(archived.identity_key, '|', 8)
                  ) = 'street'
              AND archived.merged_into IS NULL
        ), sources AS MATERIALIZED (
            SELECT parsed.*
            FROM parsed_archive AS parsed
            WHERE parsed.parts IS NOT NULL
              AND (
                    (parsed.parts[2] = '' AND parsed.parts[4] <> '')
                 OR (parsed.parts[2] <> '' AND parsed.parts[4] = '')
              )
              AND (
                    CAST(:scope_state_code AS varchar) IS NULL
                    OR parsed.state_code = CAST(:scope_state_code AS varchar)
              )
              AND (
                    CAST(:scope_zip_prefix AS varchar) IS NULL
                    OR parsed.zip5 LIKE CAST(:scope_zip_prefix AS varchar) || '%'
              )
              AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM {aliases} AS active
                        WHERE active.source_address_key = parsed.address_key
                          AND active.revoked_at IS NULL
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM {aliases} AS retried
                        WHERE retried.source_address_key = parsed.address_key
                          AND retried.shadow_run_id = CAST(:retry_shadow_run_id AS uuid)
                          AND retried.revoked_at IS NULL
                    )
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM {aliases} AS upstream
                    WHERE upstream.target_address_key = parsed.address_key
                      AND upstream.revoked_at IS NULL
              )
        ), targets AS MATERIALIZED (
            SELECT parsed.*
            FROM parsed_archive AS parsed
            WHERE parsed.parts IS NOT NULL
              AND parsed.parts[2] <> ''
              AND parsed.parts[4] <> ''
              AND NOT EXISTS (
                    SELECT 1
                    FROM {aliases} AS downstream
                    WHERE downstream.source_address_key = parsed.address_key
                      AND downstream.revoked_at IS NULL
              )
        ), structural_candidates AS MATERIALIZED (
            SELECT
                source.address_key AS source_address_key,
                source.identity_key AS source_identity_key,
                target.address_key AS target_address_key,
                target.identity_key AS target_identity_key,
                target.strict_source_bits AS target_strict_source_bits,
                target.strict_source_count AS target_strict_source_count
            FROM sources AS source
            JOIN targets AS target
              ON target.parts[1] = source.parts[1]
             AND target.parts[3] = source.parts[3]
             AND target.unit_norm = source.unit_norm
             AND target.state_code = source.state_code
             AND target.zip5 = source.zip5
             AND target.country_code = source.country_code
             AND target.address_key <> source.address_key
             AND (
                    (
                        source.parts[2] = ''
                        AND target.parts[2] <> ''
                        AND target.parts[4] = source.parts[4]
                    )
                 OR (
                        source.parts[4] = ''
                        AND target.parts[4] <> ''
                        AND target.parts[2] = source.parts[2]
                    )
             )
        ), target_counts AS MATERIALIZED (
            SELECT
                source_address_key,
                count(DISTINCT target_address_key)::integer AS candidate_count
            FROM structural_candidates
            GROUP BY source_address_key
        ), classified AS (
            SELECT
                candidate.*,
                counts.candidate_count,
                CASE
                    WHEN counts.candidate_count <> 1 THEN 'ambiguous'
                    WHEN candidate.target_strict_source_count < 2
                        THEN 'insufficient_provenance'
                    ELSE 'eligible'
                END::varchar AS decision
            FROM structural_candidates AS candidate
            JOIN target_counts AS counts USING (source_address_key)
        ), inserted AS (
            INSERT INTO {candidates} (
                run_id,
                source_address_key,
                source_identity_key,
                target_address_key,
                target_identity_key,
                candidate_count,
                target_strict_source_bits,
                target_strict_source_count,
                decision,
                review_status
            )
            SELECT
                CAST(:run_id AS uuid),
                source_address_key,
                source_identity_key,
                target_address_key,
                target_identity_key,
                candidate_count,
                target_strict_source_bits,
                target_strict_source_count,
                decision,
                CASE WHEN decision = 'eligible' THEN 'pending'
                     ELSE 'not_applicable' END
            FROM classified
            RETURNING 1
        )
        SELECT count(*)::bigint FROM inserted;
    """


def numeric_grid_candidate_insert_sql(
    *,
    schema: str,
    archive: str,
) -> str:
    """Persist all strict structural candidates for one repeatable-read shadow run."""
    return _NUMERIC_GRID_CANDIDATE_INSERT_TEMPLATE.format(
        qschema=_quote_ident(schema),
        archive=archive,
        aliases=_relation(schema, ADDRESS_ALIAS_TABLE),
        candidates=_relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE),
    )


def candidate_metrics_sql(*, schema: str) -> str:
    """Return source-level candidate counts for one durable run."""
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        SELECT jsonb_build_object(
            'candidate_rows', count(*),
            'candidate_sources', count(DISTINCT source_address_key),
            'eligible', count(DISTINCT source_address_key)
                FILTER (WHERE decision = 'eligible'),
            'ambiguous', count(DISTINCT source_address_key)
                FILTER (WHERE decision = 'ambiguous'),
            'insufficient_provenance', count(DISTINCT source_address_key)
                FILTER (WHERE decision = 'insufficient_provenance')
        )
        FROM {candidates}
        WHERE run_id = CAST(:run_id AS uuid);
    """


def candidate_rows_sql(*, schema: str) -> str:
    """Stream deterministic candidate rows for digesting or review."""
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        SELECT
            source_address_key::text AS source_address_key,
            source_identity_key,
            target_address_key::text AS target_address_key,
            target_identity_key,
            candidate_count,
            target_strict_source_bits,
            target_strict_source_count,
            decision
        FROM {candidates}
        WHERE run_id = CAST(:run_id AS uuid)
        ORDER BY source_address_key, target_address_key;
    """


def existing_numeric_grid_aliases_sql(
    *,
    schema: str,
    keyed_table: str,
    archive: str,
) -> str:
    """Materialize active aliases matching strict staged keys."""
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    return f"""
        CREATE TEMP TABLE address_numeric_grid_existing_aliases ON COMMIT DROP AS
        SELECT
            keyed.rn,
            keyed.computed_address_key AS source_address_key,
            keyed.identity_key AS staged_source_identity_key,
            active.source_identity_key,
            target.address_key AS target_address_key,
            active.target_identity_key AS recorded_target_identity_key,
            target.identity_key AS target_identity_key,
            target.premise_key AS target_premise_key,
            target.line1_norm AS target_line1_norm,
            COALESCE(target.unit_norm, '') AS target_unit_norm,
            target.city_norm AS target_city_norm,
            target.state_code AS target_state_code,
            target.zip5 AS target_zip5,
            target.zip4 AS target_zip4,
            COALESCE(target.country_code, 'US') AS target_country_code,
            target.first_line AS target_first_line,
            target.second_line AS target_second_line,
            target.city_name AS target_city_name,
            target.state_name AS target_state_name,
            target.postal_code AS target_postal_code
        FROM {keyed_table} AS keyed
        JOIN {aliases} AS active
          ON active.source_address_key = keyed.computed_address_key
         AND active.alias_kind = '{NUMERIC_GRID_ALIAS_KIND}'
         AND active.ruleset_version = {NUMERIC_GRID_ALIAS_RULESET_VERSION}
         AND active.revoked_at IS NULL
        LEFT JOIN {archive} AS target
          ON target.address_key = active.target_address_key
         AND target.merged_into IS NULL;
    """


def existing_numeric_grid_alias_violation_sql(*, schema: str) -> str:
    """Reject identity drift, missing targets, and multi-hop active aliases."""
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    return f"""
        WITH violations AS (
            SELECT
                CASE
                    WHEN existing.source_identity_key IS DISTINCT FROM
                         existing.staged_source_identity_key
                        THEN 'source_identity_mismatch'
                    WHEN existing.target_address_key IS NULL
                        THEN 'missing_or_merged_target'
                    WHEN existing.recorded_target_identity_key IS DISTINCT FROM
                         existing.target_identity_key
                        THEN 'target_identity_mismatch'
                    ELSE NULL
                END::text AS violation_kind,
                existing.source_address_key,
                existing.target_address_key
            FROM address_numeric_grid_existing_aliases AS existing
            UNION ALL
            SELECT
                'multi_hop_alias'::text,
                existing.source_address_key,
                existing.target_address_key
            FROM address_numeric_grid_existing_aliases AS existing
            JOIN {aliases} AS downstream
              ON downstream.source_address_key = existing.target_address_key
             AND downstream.revoked_at IS NULL
        )
        SELECT *
        FROM violations
        WHERE violation_kind IS NOT NULL
        ORDER BY violation_kind, source_address_key
        LIMIT 1;
    """


def update_existing_alias_keyed_rows_sql(*, keyed_table: str) -> str:
    """Change only effective keyed fields while retaining the strict computed key."""
    return f"""
        UPDATE {keyed_table} AS keyed
           SET address_key = aliases.target_address_key,
               identity_key = aliases.target_identity_key,
               premise_key = aliases.target_premise_key,
               line1_norm = aliases.target_line1_norm,
               unit_norm = aliases.target_unit_norm,
               city_norm = aliases.target_city_norm,
               state_code = aliases.target_state_code,
               zip5 = aliases.target_zip5,
               zip4 = aliases.target_zip4,
               country_code = aliases.target_country_code,
               first_line = aliases.target_first_line,
               second_line = aliases.target_second_line,
               city_name = aliases.target_city_name,
               state_name = aliases.target_state_name,
               postal_code = aliases.target_postal_code
          FROM address_numeric_grid_existing_aliases AS aliases
         WHERE keyed.rn = aliases.rn;
    """


def active_alias_conflict_sql(*, schema: str) -> str:
    """Find an active mapping that differs from an approved candidate."""
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        SELECT
            candidate.source_address_key,
            active.target_address_key AS active_target_address_key,
            candidate.target_address_key AS candidate_target_address_key
        FROM {candidates} AS candidate
        JOIN {aliases} AS active
          ON active.source_address_key = candidate.source_address_key
         AND active.revoked_at IS NULL
        WHERE candidate.run_id = CAST(:apply_run_id AS uuid)
          AND candidate.decision = 'eligible'
          AND active.target_address_key <> candidate.target_address_key
        ORDER BY candidate.source_address_key
        LIMIT 1;
    """


def revoked_shadow_alias_sql(*, schema: str) -> str:
    """Find history that prevents a reviewed shadow from being resurrected."""
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    return f"""
        SELECT source_address_key, target_address_key
        FROM {aliases}
        WHERE shadow_run_id = CAST(:shadow_run_id AS uuid)
          AND revoked_at IS NOT NULL
        ORDER BY source_address_key
        LIMIT 1;
    """


def promote_reviewed_aliases_sql(*, schema: str) -> str:
    """Insert newly approved aliases and advance generation once if it changed."""
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    candidates = _relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE)
    return f"""
        WITH promoted AS (
            INSERT INTO {aliases} (
                source_address_key,
                source_identity_key,
                target_address_key,
                target_identity_key,
                alias_kind,
                ruleset_version,
                target_strict_source_bits,
                target_strict_source_count,
                candidate_count,
                shadow_run_id,
                apply_run_id,
                reviewed_candidate_digest
            )
            SELECT
                candidate.source_address_key,
                candidate.source_identity_key,
                candidate.target_address_key,
                candidate.target_identity_key,
                '{NUMERIC_GRID_ALIAS_KIND}',
                {NUMERIC_GRID_ALIAS_RULESET_VERSION},
                candidate.target_strict_source_bits,
                candidate.target_strict_source_count,
                candidate.candidate_count,
                CAST(:shadow_run_id AS uuid),
                CAST(:apply_run_id AS uuid),
                CAST(:candidate_digest AS varchar)
            FROM {candidates} AS candidate
            JOIN {candidates} AS reviewed
              ON reviewed.run_id = CAST(:shadow_run_id AS uuid)
             AND reviewed.source_address_key = candidate.source_address_key
             AND reviewed.target_address_key = candidate.target_address_key
             AND reviewed.source_identity_key = candidate.source_identity_key
             AND reviewed.target_identity_key = candidate.target_identity_key
             AND reviewed.review_status = 'approved'
            WHERE candidate.run_id = CAST(:apply_run_id AS uuid)
              AND candidate.decision = 'eligible'
              AND NOT EXISTS (
                    SELECT 1
                    FROM {aliases} AS active
                    WHERE active.source_address_key = candidate.source_address_key
                      AND active.target_address_key = candidate.target_address_key
                      AND active.revoked_at IS NULL
            )
            RETURNING 1
        )
        SELECT count(*)::integer AS promoted_count FROM promoted;
    """
