# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SQL builders for reviewed evidence-gated address aliases."""

from __future__ import annotations

from process.ext.address_alias_sql import (
    ADDRESS_ALIAS_CANDIDATE_TABLE,
    ADDRESS_ALIAS_TABLE,
    _quote_ident,
    _relation,
)


def evidence_source_count_sql(*, schema: str, archive: str) -> str:
    """Count endpoint-visible canonical keys in one optional review scope."""
    qschema = _quote_ident(schema)
    unified = _relation(schema, "entity_address_unified")
    return f"""
        SELECT count(DISTINCT visible.address_key)::bigint
        FROM {unified} AS visible
        JOIN {archive} AS archived
          ON archived.address_key = visible.address_key
         AND archived.merged_into IS NULL
        WHERE visible.type IN ('primary', 'secondary', 'practice', 'site')
          AND visible.address_key IS NOT NULL
          AND {qschema}.public_evidence_npi_valid(
                COALESCE(visible.npi, visible.inferred_npi)::text
              )
          AND COALESCE(
                archived.precision,
                split_part(archived.identity_key, '|', 8)
              ) = 'street'
          AND (
                CAST(:scope_state_code AS varchar) IS NULL
                OR archived.state_code = CAST(:scope_state_code AS varchar)
          )
          AND (
                CAST(:scope_zip_prefix AS varchar) IS NULL
                OR archived.zip5 LIKE CAST(:scope_zip_prefix AS varchar) || '%'
          );
    """

def evidence_input_stale_count_sql(*, schema: str) -> str:
    """Count serving rows built against a different active alias generation."""
    unified = _relation(schema, "entity_address_unified")
    return f"""
        SELECT count(*)::bigint
        FROM {unified}
        WHERE base_address_version IS NULL
           OR base_address_version !~ (
                '\\+alias-v1:g' || CAST(:alias_generation AS bigint)::text || '$'
           );
    """


_EVIDENCE_CANDIDATE_INSERT_TEMPLATE = """
        WITH memberships AS MATERIALIZED (
            SELECT
                COALESCE(visible.npi, visible.inferred_npi)::bigint AS npi,
                visible.address_key
            FROM {unified} AS visible
            WHERE visible.type IN ('primary', 'secondary', 'practice', 'site')
              AND visible.address_key IS NOT NULL
              AND {qschema}.public_evidence_npi_valid(
                    COALESCE(visible.npi, visible.inferred_npi)::text
                  )
            GROUP BY
                COALESCE(visible.npi, visible.inferred_npi)::bigint,
                visible.address_key
        ), visible_addresses AS MATERIALIZED (
            SELECT
                membership.npi,
                archived.address_key,
                archived.identity_key,
                archived.first_line,
                archived.second_line,
                archived.city_name,
                archived.state_name,
                archived.postal_code,
                archived.state_code,
                archived.zip5,
                COALESCE(archived.country_code, 'US') AS country_code,
                COALESCE(archived.strict_source_bits, 0) AS strict_source_bits,
                bit_count(
                    (COALESCE(archived.strict_source_bits, 0)::bigint)::bit(64)
                )::smallint AS strict_source_count
            FROM memberships AS membership
            JOIN {archive} AS archived
              ON archived.address_key = membership.address_key
            WHERE archived.address_key IS NOT NULL
              AND archived.identity_key IS NOT NULL
              AND archived.merged_into IS NULL
              AND COALESCE(
                    archived.precision,
                    split_part(archived.identity_key, '|', 8)
                  ) = 'street'
              AND {qschema}.addr_key_from_identity_v1(archived.identity_key)
                    = archived.address_key
              AND {qschema}.addr_identity_key_v1(
                    archived.first_line,
                    archived.second_line,
                    archived.city_name,
                    archived.state_name,
                    archived.postal_code,
                    COALESCE(archived.country_code, 'US')
                  ) = archived.identity_key
              AND archived.state_code
                    = {qschema}.addr_state_code_v1(archived.state_name)
              AND archived.zip5
                    = {qschema}.addr_zip5_norm_v1(archived.postal_code)
              AND COALESCE(archived.country_code, 'US')
                    = {qschema}.addr_country_code_v1(archived.country_code)
              AND archived.state_code IS NOT NULL
              AND archived.zip5 IS NOT NULL
              AND {qschema}.addr_street_norm_v1(
                    archived.first_line, archived.second_line
                  ) IS NOT NULL
              AND (
                    CAST(:scope_state_code AS varchar) IS NULL
                    OR archived.state_code = CAST(:scope_state_code AS varchar)
              )
              AND (
                    CAST(:scope_zip_prefix AS varchar) IS NULL
                    OR archived.zip5 LIKE CAST(:scope_zip_prefix AS varchar) || '%'
              )
        ), pair_matches AS MATERIALIZED (
            SELECT
                source.npi,
                source.address_key AS source_address_key,
                source.identity_key AS source_identity_key,
                target.address_key AS target_address_key,
                target.identity_key AS target_identity_key,
                target.strict_source_bits AS target_strict_source_bits,
                target.strict_source_count AS target_strict_source_count,
                source.state_code,
                source.zip5,
                source.country_code,
                matched.match_rule,
                matched.effective_source_first_line,
                matched.street_relation,
                CASE matched.match_rule
                    WHEN 'candidate_confirmed_bare_unit' THEN 10
                    WHEN 'unit_designator_punctuation' THEN 20
                    WHEN 'candidate_confirmed_spaced_unit' THEN 30
                    WHEN 'direction_relocation' THEN 40
                    WHEN 'terminal_suffix_omission' THEN 50
                    ELSE 1000
                END AS rule_priority
            FROM visible_addresses AS source
            JOIN visible_addresses AS target
              ON target.npi = source.npi
             AND target.state_code = source.state_code
             AND target.zip5 = source.zip5
             AND target.country_code = source.country_code
             AND target.address_key <> source.address_key
            CROSS JOIN LATERAL {qschema}.addr_evidence_alias_match_v1(
                source.first_line,
                source.second_line,
                source.city_name,
                source.state_name,
                source.postal_code,
                source.country_code,
                target.first_line,
                target.second_line,
                target.city_name,
                target.state_name,
                target.postal_code,
                target.country_code,
                target.address_key
            ) AS matched
            WHERE (
                    matched.match_rule <> 'direction_relocation'
                    OR target.strict_source_count > source.strict_source_count
                  )
              AND (
                    NOT EXISTS (
                        SELECT 1
                        FROM {aliases} AS active
                        WHERE active.source_address_key = source.address_key
                          AND active.revoked_at IS NULL
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM {aliases} AS retried
                        WHERE retried.source_address_key = source.address_key
                          AND retried.shadow_run_id = CAST(:retry_shadow_run_id AS uuid)
                          AND retried.revoked_at IS NULL
                    )
                  )
              AND NOT EXISTS (
                    SELECT 1
                    FROM {aliases} AS upstream
                    WHERE upstream.target_address_key = source.address_key
                      AND upstream.revoked_at IS NULL
                  )
              AND NOT EXISTS (
                    SELECT 1
                    FROM {aliases} AS downstream
                    WHERE downstream.source_address_key = target.address_key
                      AND downstream.revoked_at IS NULL
                  )
        ), candidate_sources AS MATERIALIZED (
            SELECT DISTINCT
                source_address_key,
                state_code,
                zip5,
                country_code,
                effective_source_first_line
            FROM pair_matches
        ), marker_sets AS MATERIALIZED (
            SELECT
                source.source_address_key,
                source.effective_source_first_line,
                count(DISTINCT {qschema}.addr_street_direction_token_v1(
                    archived.first_line, ''
                )) FILTER (WHERE {qschema}.addr_street_direction_token_v1(
                    archived.first_line, ''
                ) IS NOT NULL)::integer AS direction_count,
                min({qschema}.addr_street_direction_token_v1(
                    archived.first_line, ''
                )) FILTER (WHERE {qschema}.addr_street_direction_token_v1(
                    archived.first_line, ''
                ) IS NOT NULL) AS only_direction,
                count(DISTINCT {qschema}.addr_street_suffix_token_v1(
                    archived.first_line, ''
                )) FILTER (WHERE {qschema}.addr_street_suffix_token_v1(
                    archived.first_line, ''
                ) IS NOT NULL)::integer AS suffix_count,
                min({qschema}.addr_street_suffix_token_v1(
                    archived.first_line, ''
                )) FILTER (WHERE {qschema}.addr_street_suffix_token_v1(
                    archived.first_line, ''
                ) IS NOT NULL) AS only_suffix
            FROM candidate_sources AS source
            JOIN {archive} AS archived
              ON {qschema}.addr_state_code_v1(archived.state_name)
                    = source.state_code
             AND {qschema}.addr_zip5_norm_v1(archived.postal_code)
                    = source.zip5
             AND {qschema}.addr_country_code_v1(archived.country_code)
                    = source.country_code
             AND archived.merged_into IS NULL
             AND {qschema}.addr_street_completion_norm_v1(
                    archived.first_line, ''
                 ) = {qschema}.addr_street_completion_norm_v1(
                    source.effective_source_first_line, ''
                 )
            GROUP BY source.source_address_key,
                     source.effective_source_first_line
        ), global_related_targets AS MATERIALIZED (
            SELECT DISTINCT
                candidate.source_address_key,
                target.address_key AS target_address_key
            FROM candidate_sources AS candidate
            JOIN {archive} AS source
              ON source.address_key = candidate.source_address_key
             AND source.merged_into IS NULL
             AND source.identity_key IS NOT NULL
             AND {qschema}.addr_key_from_identity_v1(source.identity_key)
                    = source.address_key
             AND {qschema}.addr_identity_key_v1(
                    source.first_line,
                    source.second_line,
                    source.city_name,
                    source.state_name,
                    source.postal_code,
                    COALESCE(source.country_code, 'US')
                 ) = source.identity_key
             AND source.state_code
                    = {qschema}.addr_state_code_v1(source.state_name)
             AND source.zip5
                    = {qschema}.addr_zip5_norm_v1(source.postal_code)
             AND COALESCE(source.country_code, 'US')
                    = {qschema}.addr_country_code_v1(source.country_code)
            JOIN {archive} AS target
              ON {qschema}.addr_state_code_v1(target.state_name)
                    = candidate.state_code
             AND {qschema}.addr_zip5_norm_v1(target.postal_code)
                    = candidate.zip5
             AND {qschema}.addr_country_code_v1(target.country_code)
                    = candidate.country_code
             AND target.address_key <> candidate.source_address_key
             AND target.merged_into IS NULL
             AND target.identity_key IS NOT NULL
             AND COALESCE(
                    target.precision,
                    split_part(target.identity_key, '|', 8)
                 ) = 'street'
             AND {qschema}.addr_key_from_identity_v1(target.identity_key)
                    = target.address_key
             AND {qschema}.addr_identity_key_v1(
                    target.first_line,
                    target.second_line,
                    target.city_name,
                    target.state_name,
                    target.postal_code,
                    COALESCE(target.country_code, 'US')
                 ) = target.identity_key
            CROSS JOIN LATERAL {qschema}.addr_evidence_alias_match_v1(
                source.first_line,
                source.second_line,
                source.city_name,
                source.state_name,
                source.postal_code,
                COALESCE(source.country_code, 'US'),
                target.first_line,
                target.second_line,
                target.city_name,
                target.state_name,
                target.postal_code,
                COALESCE(target.country_code, 'US'),
                target.address_key
            ) AS matched
        ), assessed_matches AS MATERIALIZED (
            SELECT
                matched.*,
                NOT (
                    COALESCE(markers.direction_count, 0) <= 1
                    AND COALESCE(markers.suffix_count, 0) <= 1
                    AND (
                        {qschema}.addr_street_direction_token_v1(
                            matched.effective_source_first_line, ''
                        ) IS NULL
                        OR markers.only_direction IS NULL
                        OR {qschema}.addr_street_direction_token_v1(
                            matched.effective_source_first_line, ''
                        ) = markers.only_direction
                    )
                    AND (
                        {qschema}.addr_street_suffix_token_v1(
                            matched.effective_source_first_line, ''
                        ) IS NULL
                        OR markers.only_suffix IS NULL
                        OR {qschema}.addr_street_suffix_token_v1(
                            matched.effective_source_first_line, ''
                        ) = markers.only_suffix
                    )
                ) AS marker_conflict
            FROM pair_matches AS matched
            LEFT JOIN marker_sets AS markers
              ON markers.source_address_key = matched.source_address_key
             AND markers.effective_source_first_line
                    = matched.effective_source_first_line
        ), evidence_counts AS (
            SELECT
                source_address_key,
                target_address_key,
                count(DISTINCT npi)::integer AS evidence_npi_count
            FROM assessed_matches
            GROUP BY source_address_key, target_address_key
        ), preferred_pairs AS MATERIALIZED (
            SELECT DISTINCT ON (source_address_key, target_address_key)
                source_address_key,
                source_identity_key,
                target_address_key,
                target_identity_key,
                target_strict_source_bits,
                target_strict_source_count,
                match_rule,
                npi AS evidence_npi,
                CASE
                    WHEN street_relation = 'same_street' THEN false
                    ELSE marker_conflict
                END AS marker_conflict,
                rule_priority
            FROM assessed_matches
            ORDER BY
                source_address_key,
                target_address_key,
                rule_priority,
                match_rule,
                npi
        ), target_counts AS MATERIALIZED (
            SELECT
                source_address_key,
                count(DISTINCT target_address_key)::integer AS candidate_count
            FROM (
                SELECT source_address_key, target_address_key
                FROM global_related_targets
                UNION
                SELECT source_address_key, target_address_key
                FROM preferred_pairs
            ) AS global_candidates
            GROUP BY source_address_key
        ), classified AS (
            SELECT
                candidate.*,
                evidence.evidence_npi_count,
                counts.candidate_count,
                CASE
                    WHEN counts.candidate_count <> 1 THEN 'ambiguous'
                    WHEN candidate.marker_conflict THEN 'ambiguous'
                    WHEN EXISTS (
                        SELECT 1
                        FROM preferred_pairs AS downstream
                        WHERE downstream.source_address_key = candidate.target_address_key
                    ) THEN 'ambiguous'
                    WHEN candidate.target_strict_source_count < 2
                        THEN 'insufficient_provenance'
                    ELSE 'eligible'
                END::varchar AS decision
            FROM preferred_pairs AS candidate
            JOIN evidence_counts AS evidence
              USING (source_address_key, target_address_key)
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
                review_status,
                match_rule,
                match_classification,
                evidence_npi,
                evidence_npi_count
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
                     ELSE 'not_applicable' END,
                match_rule,
                'exact',
                evidence_npi,
                evidence_npi_count
            FROM classified
            RETURNING 1
        )
        SELECT count(*)::bigint FROM inserted;
    """


def evidence_candidate_insert_sql(*, schema: str, archive: str) -> str:
    """Persist reviewed exact candidates witnessed by one valid visible NPI."""
    return _EVIDENCE_CANDIDATE_INSERT_TEMPLATE.format(
        qschema=_quote_ident(schema),
        unified=_relation(schema, "entity_address_unified"),
        archive=archive,
        aliases=_relation(schema, ADDRESS_ALIAS_TABLE),
        candidates=_relation(schema, ADDRESS_ALIAS_CANDIDATE_TABLE),
    )


def evidence_skipped_source_count_sql(*, schema: str, archive: str) -> str:
    """Count scoped visible keys already covered by an active alias."""
    qschema = _quote_ident(schema)
    unified = _relation(schema, "entity_address_unified")
    aliases = _relation(schema, ADDRESS_ALIAS_TABLE)
    return f"""
        SELECT count(DISTINCT visible.address_key)::bigint
        FROM {unified} AS visible
        JOIN {archive} AS archived
          ON archived.address_key = visible.address_key
         AND archived.merged_into IS NULL
        JOIN {aliases} AS active
          ON active.source_address_key = visible.address_key
         AND active.revoked_at IS NULL
        WHERE visible.type IN ('primary', 'secondary', 'practice', 'site')
          AND {qschema}.public_evidence_npi_valid(
                COALESCE(visible.npi, visible.inferred_npi)::text
              )
          AND (
                CAST(:scope_state_code AS varchar) IS NULL
                OR archived.state_code = CAST(:scope_state_code AS varchar)
          )
          AND (
                CAST(:scope_zip_prefix AS varchar) IS NULL
                OR archived.zip5 LIKE CAST(:scope_zip_prefix AS varchar) || '%'
          );
    """
