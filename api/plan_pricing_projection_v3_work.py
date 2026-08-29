# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Per-code PostgreSQL work admission for factorized pricing projections."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from sqlalchemy import text

from api.plan_pricing_projection_v3_aggregate import (
    MAX_CODE_AGGREGATE_WORK_ROWS,
    MAX_PROJECTION_AGGREGATE_WORK_ROWS,
)
from api.plan_pricing_projection_v3_code import (
    MAX_CODE_RATE_PROFILE_WORK_ROWS,
    MAX_PROJECTION_RATE_PROFILE_WORK_ROWS,
    MAX_RATE_PROFILE_RATES,
)
from api.plan_pricing_projection_v3_types import _BuildState


# These fail closed until an exact release census and PostgreSQL resource proof
# establish bounds with retained headroom.
MAX_CODE_MEMBERSHIP_PROBES: int | None = None
MAX_PROJECTION_MEMBERSHIP_PROBES: int | None = None
MAX_CODE_MEMBER_CELL_WORK_ROWS: int | None = None
MAX_PROJECTION_MEMBER_CELL_WORK_ROWS: int | None = None
MAX_BIGINT = (1 << 63) - 1


_RESET_CODE_WORK_SQL = """
    TRUNCATE plan_pricing_eligible_member_cell_stage,
             plan_pricing_set_cell_stage,
             plan_pricing_rate_frequency_stage
"""

_MEMBERSHIP_PROBE_SQL = """
    WITH rate_sets AS MATERIALIZED (
        SELECT DISTINCT binding_ordinal, provider_set_key
          FROM plan_pricing_code_occurrence_stage
    )
    SELECT COALESCE(SUM(provider_set.membership_count), 0)::bigint
      FROM rate_sets
      JOIN plan_pricing_provider_set_stage provider_set
        USING (binding_ordinal, provider_set_key)
"""

_SET_CELL_INSERT_SQL = """
    INSERT INTO plan_pricing_set_cell_stage (
        binding_ordinal, provider_set_key, geo_cell
    )
    SELECT DISTINCT binding_ordinal, provider_set_key, geo_cell
      FROM plan_pricing_eligible_member_cell_stage
"""

_RATE_FREQUENCY_INSERT_SQL = """
    INSERT INTO plan_pricing_rate_frequency_stage (
        binding_ordinal, provider_set_key, negotiated_rate,
        join_row_count, multiplicity
    )
    SELECT occurrence.binding_ordinal, occurrence.provider_set_key,
           price.negotiated_rate, COUNT(*)::bigint,
           SUM(
               occurrence.occurrence_count * price.rate_multiplicity
           )::bigint
      FROM plan_pricing_code_occurrence_stage occurrence
      JOIN plan_pricing_price_rate_stage price
        ON price.binding_ordinal = occurrence.binding_ordinal
       AND price.price_set_id = occurrence.price_set_id
      JOIN plan_pricing_provider_set_stage membership
        ON membership.binding_ordinal = occurrence.binding_ordinal
       AND membership.provider_set_key = occurrence.provider_set_key
     WHERE membership.membership_count > 0
     GROUP BY occurrence.binding_ordinal, occurrence.provider_set_key,
              price.negotiated_rate
"""

_WORK_METRICS_SQL = """
    WITH profile AS MATERIALIZED (
        SELECT rate.binding_ordinal, rate.provider_set_key,
               SUM(rate.join_row_count)::numeric AS join_rows,
               COUNT(*)::numeric
                   AS distinct_rate_count,
               SUM(rate.multiplicity)::numeric AS rate_count
          FROM plan_pricing_rate_frequency_stage rate
         GROUP BY rate.binding_ordinal, rate.provider_set_key
    ), aggregate_by_cell AS MATERIALIZED (
        SELECT cell.geo_cell,
               SUM(profile.join_rows)::numeric AS join_rows,
               SUM(profile.rate_count)::numeric AS rate_count
          FROM plan_pricing_set_cell_stage cell
          JOIN profile USING (binding_ordinal, provider_set_key)
         GROUP BY cell.geo_cell
    )
    SELECT
        (SELECT COUNT(*)::numeric FROM plan_pricing_set_cell_stage)
            AS set_cell_rows,
        COALESCE((SELECT SUM(join_rows) FROM profile), 0::numeric)
            AS profile_join_rows,
        COALESCE((SELECT SUM(join_rows) FROM aggregate_by_cell), 0::numeric)
            AS aggregate_join_rows,
        COALESCE((SELECT SUM(rate_count) FROM profile), 0::numeric)
            AS profile_rate_count_sum,
        COALESCE((SELECT MAX(rate_count) FROM profile), 0::numeric)
            AS profile_rate_count_max,
        COALESCE((SELECT MAX(distinct_rate_count) FROM profile), 0::numeric)
            AS profile_distinct_rate_count_max,
        COALESCE((SELECT SUM(rate_count) FROM aggregate_by_cell), 0::numeric)
            AS aggregate_rate_count_sum,
        COALESCE((SELECT MAX(rate_count) FROM aggregate_by_cell), 0::numeric)
            AS aggregate_rate_count_max
"""


@dataclass(frozen=True)
class _CodeWork:
    membership_probe_rows: int
    member_cell_rows: int
    set_cell_rows: int
    profile_join_rows: int
    aggregate_join_rows: int
    profile_rate_count_sum: int
    profile_rate_count_max: int
    profile_distinct_rate_count_max: int
    aggregate_rate_count_sum: int
    aggregate_rate_count_max: int


def _taxonomy_filter(has_taxonomy_rule: bool) -> str:
    return (
        "AND provider.entity_type_code = 1 "
        "AND EXISTS (SELECT 1 FROM unnest(provider.taxonomy_codes) taxonomy_code "
        "WHERE upper(btrim(taxonomy_code)) "
        "= ANY(CAST(:taxonomy_codes AS varchar[])))"
        if has_taxonomy_rule
        else ""
    )


_MEMBER_CELL_PROBE_SQL = """
        INSERT INTO plan_pricing_eligible_member_cell_stage (
            binding_ordinal, provider_set_key, geo_cell, npi
        )
        SELECT member.binding_ordinal, member.provider_set_key,
               provider.geo_cell, member.npi
          FROM (
              SELECT DISTINCT binding_ordinal, provider_set_key
                FROM plan_pricing_code_occurrence_stage
          ) rate_sets
          JOIN plan_pricing_provider_member_stage member
            USING (binding_ordinal, provider_set_key)
          JOIN plan_pricing_provider_cell_stage provider
            ON provider.projection_id = :projection_id
           AND provider.npi = member.npi
         LIMIT :member_cell_limit
"""


def _delete_ineligible_member_cells_sql(has_taxonomy_rule: bool) -> str | None:
    if not has_taxonomy_rule:
        return None
    return f"""
        DELETE FROM plan_pricing_eligible_member_cell_stage member
         WHERE NOT EXISTS (
               SELECT 1
                 FROM plan_pricing_provider_cell_stage provider
                WHERE provider.projection_id = :projection_id
                  AND provider.npi = member.npi
                  AND provider.geo_cell = member.geo_cell
                  {_taxonomy_filter(True)}
         )
    """


@dataclass(frozen=True)
class _WorkLimits:
    code_membership_probes: int
    projection_membership_probes: int
    code_member_cells: int
    projection_member_cells: int


def _work_limits(state: _BuildState) -> _WorkLimits:
    raw_limits = (
        MAX_CODE_MEMBERSHIP_PROBES,
        MAX_PROJECTION_MEMBERSHIP_PROBES,
        MAX_CODE_MEMBER_CELL_WORK_ROWS,
        MAX_PROJECTION_MEMBER_CELL_WORK_ROWS,
    )
    if (
        any(type(limit) is not int or limit <= 0 for limit in raw_limits)
        or state.membership_probe_work_rows > raw_limits[1]
        or state.member_cell_work_rows > raw_limits[3]
    ):
        raise ValueError("pricing projection join work bound is not calibrated")
    return _WorkLimits(*raw_limits)


def _normalized_taxonomy_codes(
    code_identity: tuple[str, str],
) -> tuple[bool, list[str] | None]:
    from api import ptg2_serving as serving

    taxonomy_rule = serving._inferred_provider_taxonomy_rule(
        {"code_system": code_identity[0], "code": code_identity[1]}
    )
    if taxonomy_rule is None:
        return False, None
    return True, sorted(
        {
            str(taxonomy_code).strip().upper()
            for taxonomy_code in taxonomy_rule.taxonomy_codes
            if str(taxonomy_code).strip()
        }
    )


def _code_work_from_row(
    membership_probe_rows: int,
    member_cell_rows: int,
    raw_work_by_field: Mapping[str, Any],
) -> _CodeWork:
    values = (
        membership_probe_rows,
        member_cell_rows,
        int(raw_work_by_field["set_cell_rows"]),
        int(raw_work_by_field["profile_join_rows"]),
        int(raw_work_by_field["aggregate_join_rows"]),
        int(raw_work_by_field["profile_rate_count_sum"]),
        int(raw_work_by_field["profile_rate_count_max"]),
        int(raw_work_by_field["profile_distinct_rate_count_max"]),
        int(raw_work_by_field["aggregate_rate_count_sum"]),
        int(raw_work_by_field["aggregate_rate_count_max"]),
    )
    if any(value < 0 for value in values):
        raise ValueError("pricing projection code work is invalid")
    return _CodeWork(*values)


def _record_code_work(
    state: _BuildState,
    work: _CodeWork,
    limits: _WorkLimits,
) -> None:
    if (
        work.membership_probe_rows > limits.code_membership_probes
        or state.membership_probe_work_rows + work.membership_probe_rows
        > limits.projection_membership_probes
    ):
        raise ValueError("pricing projection membership-probe work bound exceeded")
    if (
        work.member_cell_rows > limits.code_member_cells
        or state.member_cell_work_rows + work.member_cell_rows
        > limits.projection_member_cells
    ):
        raise ValueError("pricing projection member-cell work bound exceeded")
    if (
        work.profile_join_rows > MAX_CODE_RATE_PROFILE_WORK_ROWS
        or state.rate_profile_work_rows + work.profile_join_rows
        > MAX_PROJECTION_RATE_PROFILE_WORK_ROWS
    ):
        raise ValueError("pricing projection rate-profile work bound exceeded")
    if (
        work.aggregate_join_rows > MAX_CODE_AGGREGATE_WORK_ROWS
        or state.aggregate_work_rows + work.aggregate_join_rows
        > MAX_PROJECTION_AGGREGATE_WORK_ROWS
    ):
        raise ValueError("pricing projection aggregate work bound exceeded")
    if (
        work.profile_distinct_rate_count_max > MAX_RATE_PROFILE_RATES
    ):
        raise ValueError("pricing projection rate profile is too large")
    if (
        work.profile_rate_count_max > MAX_BIGINT
        or work.aggregate_rate_count_max > MAX_BIGINT
    ):
        raise ValueError("pricing projection rate count exceeds bigint")
    state.membership_probe_work_rows += work.membership_probe_rows
    state.member_cell_work_rows += work.member_cell_rows
    state.rate_profile_work_rows += work.profile_join_rows
    state.aggregate_work_rows += work.aggregate_join_rows


async def _stage_code_work(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    membership_probe_limit: int,
    member_cell_limit: int,
) -> _CodeWork:
    """Stage and measure one code's exact provider/rate join work."""

    has_taxonomy_rule, taxonomy_codes = _normalized_taxonomy_codes(code_identity)
    await session.execute(text(_RESET_CODE_WORK_SQL))
    membership_result = await session.execute(text(_MEMBERSHIP_PROBE_SQL))
    membership_probe_rows = int(membership_result.scalar_one())
    if membership_probe_rows > membership_probe_limit:
        raise ValueError("pricing projection membership-probe work bound exceeded")
    await session.execute(
        text(_MEMBER_CELL_PROBE_SQL),
        {
            "projection_id": projection_id,
            "member_cell_limit": member_cell_limit + 1,
        },
    )
    count_result = await session.execute(
        text("SELECT COUNT(*) FROM plan_pricing_eligible_member_cell_stage")
    )
    member_cell_rows = int(count_result.scalar_one())
    if member_cell_rows > member_cell_limit:
        raise ValueError("pricing projection member-cell work bound exceeded")
    taxonomy_delete_sql = _delete_ineligible_member_cells_sql(has_taxonomy_rule)
    if taxonomy_delete_sql is not None:
        await session.execute(
            text(taxonomy_delete_sql),
            {
                "projection_id": projection_id,
                "taxonomy_codes": taxonomy_codes,
            },
        )
    await session.execute(text(_SET_CELL_INSERT_SQL))
    await session.execute(text(_RATE_FREQUENCY_INSERT_SQL))
    metrics_result = await session.execute(text(_WORK_METRICS_SQL))
    work = _code_work_from_row(
        membership_probe_rows,
        member_cell_rows,
        metrics_result.mappings().one(),
    )
    return work


async def _prepare_code_work(
    session: Any,
    projection_id: str,
    code_identity: tuple[str, str],
    state: _BuildState,
) -> _CodeWork:
    """Stage and admit one code's exact provider/rate join work."""

    limits = _work_limits(state)
    remaining_membership_limit = min(
        limits.code_membership_probes,
        limits.projection_membership_probes - state.membership_probe_work_rows,
    )
    remaining_cell_limit = min(
        limits.code_member_cells,
        limits.projection_member_cells - state.member_cell_work_rows,
    )
    work = await _stage_code_work(
        session,
        projection_id,
        code_identity,
        remaining_membership_limit,
        remaining_cell_limit,
    )
    _record_code_work(state, work, limits)
    return work
