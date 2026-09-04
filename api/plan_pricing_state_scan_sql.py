# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded SQL for release-bound statewide pricing traversal."""

from __future__ import annotations

from api.plan_pricing_projection_contract import table


def state_scan_provider_page_sql() -> str:
    """Return the bounded provider-state witness keyset query."""

    return f"""
        SELECT npi, provider_fragment
          FROM {table('plan_pricing_provider_state')}
         WHERE projection_id = :projection_id
           AND state = :state AND npi > :after_npi
         ORDER BY npi
         LIMIT :npi_sentinel_limit
    """


def _membership_candidates_sql() -> str:
    """Return the bounded provider-membership candidate CTEs."""

    return f"""
        selected AS MATERIALIZED (
            SELECT npi, npi_ordinal
              FROM UNNEST(CAST(:selected_npis AS bigint[]))
                   WITH ORDINALITY selected(npi, npi_ordinal)
        ), membership_candidates AS MATERIALIZED (
            SELECT selected.npi, selected.npi_ordinal,
                   membership.binding_ordinal,
                   membership.provider_set_key
              FROM selected
              CROSS JOIN LATERAL (
                  SELECT candidate.binding_ordinal,
                         candidate.provider_set_key
                    FROM {table('plan_pricing_provider_membership')} candidate
                   WHERE candidate.projection_id = :projection_id
                     AND candidate.npi = selected.npi
                   LIMIT :membership_probe_limit
              ) membership
             LIMIT :membership_sentinel_limit
        ), membership_budget AS MATERIALIZED (
            SELECT EXISTS (
                SELECT 1 FROM membership_candidates
                 OFFSET :membership_limit
            ) AS exceeded
        )
    """


def _occurrence_candidates_sql() -> str:
    """Return the bounded rate-occurrence candidate CTE."""

    return f"""
        occurrence_candidates AS MATERIALIZED (
            SELECT membership.npi, membership.npi_ordinal,
                   membership.binding_ordinal,
                   occurrence.occurrence_ordinal,
                   membership.provider_set_key,
                   occurrence.provider_set_ref,
                   occurrence.price_key,
                   occurrence.price_set_ref,
                   occurrence.rate_pack_ref,
                   occurrence.source_artifact_key,
                   occurrence.provider_count,
                   occurrence.group_fragment,
                   occurrence.occurrence_multiplicity
              FROM membership_candidates membership
              CROSS JOIN LATERAL (
                  SELECT candidate.occurrence_ordinal,
                         candidate.provider_set_ref,
                         candidate.price_key,
                         candidate.price_set_ref,
                         candidate.rate_pack_ref,
                         candidate.source_artifact_key,
                         candidate.provider_count,
                         candidate.group_fragment,
                         candidate.occurrence_multiplicity
                    FROM {table('plan_pricing_rate_occurrence')} candidate
                   WHERE candidate.projection_id = :projection_id
                     AND candidate.binding_ordinal = membership.binding_ordinal
                     AND candidate.provider_set_key = membership.provider_set_key
                     AND candidate.code_system = :code_system
                     AND candidate.code = :code
                   LIMIT :occurrence_probe_limit
              ) occurrence
              CROSS JOIN membership_budget
             WHERE NOT membership_budget.exceeded
             LIMIT :occurrence_sentinel_limit
        )
    """


def _page_result_sql() -> str:
    """Return the bounded page CTE and its overflow sentinel union."""

    return """
        page AS MATERIALIZED (
            SELECT selected.npi,
                   occurrence.binding_ordinal,
                   occurrence.occurrence_ordinal,
                   occurrence.provider_set_key,
                   occurrence.provider_set_ref,
                   occurrence.price_key,
                   occurrence.price_set_ref,
                   occurrence.rate_pack_ref,
                   occurrence.source_artifact_key,
                   occurrence.provider_count,
                   occurrence.group_fragment,
                   occurrence.occurrence_multiplicity,
                   FALSE AS membership_budget_exceeded
              FROM selected
              LEFT JOIN occurrence_candidates occurrence
                ON occurrence.npi = selected.npi
              CROSS JOIN membership_budget
             WHERE NOT membership_budget.exceeded
             LIMIT :page_row_limit
        )
        SELECT * FROM page
        UNION ALL
        SELECT NULL::bigint AS npi,
               NULL::integer AS binding_ordinal,
               NULL::integer AS occurrence_ordinal,
               NULL::bigint AS provider_set_key,
               NULL::varchar AS provider_set_ref,
               NULL::bigint AS price_key,
               NULL::varchar AS price_set_ref,
               NULL::varchar AS rate_pack_ref,
               NULL::bigint AS source_artifact_key,
               NULL::integer AS provider_count,
               NULL::jsonb AS group_fragment,
               NULL::bigint AS occurrence_multiplicity,
               TRUE AS membership_budget_exceeded
          FROM membership_budget
         WHERE membership_budget.exceeded
    """


def state_scan_page_sql() -> str:
    """Probe indexed children before applying any canonical ordering."""

    return f"""
        WITH {_membership_candidates_sql()},
             {_occurrence_candidates_sql()},
             {_page_result_sql()}
    """


__all__ = ["state_scan_page_sql", "state_scan_provider_page_sql"]
