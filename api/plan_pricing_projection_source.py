# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Sealed rate-code and provider inputs for plan-pricing projections."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping

from sqlalchemy import text

from api.plan_pricing_projection_contract import (
    SCHEMA,
    ZIP5,
    projection_code_identity,
    row_mapping,
    table,
)
from api.ptg2_tables import snapshot_serving_tables


PROVIDER_BATCH_SIZE = 5_000


@dataclass(frozen=True)
class BindingProjection:
    binding: dict[str, Any]
    serving_tables: Any
    code_rows_by_identity: dict[tuple[str, str], list[dict[str, Any]]]


def _group_code_rows(code_result: Any, serving: Any) -> dict[
    tuple[str, str], list[dict[str, Any]]
]:
    code_rows_by_identity: dict[
        tuple[str, str], list[dict[str, Any]]
    ] = defaultdict(list)
    for raw_code_row in code_result:
        code_by_field = serving._canonical_code_metadata_row(raw_code_row)
        code_identity = projection_code_identity(
            code_by_field.get("reported_code_system"),
            code_by_field.get("reported_code"),
        )
        if code_identity is not None:
            code_rows_by_identity[code_identity].append(code_by_field)
    return dict(code_rows_by_identity)


async def binding_projection(
    session: Any,
    binding: dict[str, Any],
) -> BindingProjection:
    """Read the sealed code scope for one release binding."""

    from api import ptg2_serving as serving

    serving_tables = await snapshot_serving_tables(
        session,
        str(binding["snapshot_id"]),
    )
    serving._require_strict_shared_v3(serving_tables)
    scope_join_sql, filters, parameters_by_name, plan_order = (
        serving._shared_v3_code_scope_sql(
            serving_tables,
            requested_plan=str(binding["plan_id"]),
            plan_market_type=str(
                binding.get("market_type")
                or binding.get("plan_market_type")
                or ""
            ),
        )
    )
    filters.append("code_metadata.snapshot_key = :shared_snapshot_key")
    parameters_by_name["shared_snapshot_key"] = (
        serving._required_shared_snapshot_key(serving_tables)
    )
    code_result = await session.execute(
        text(
            f"""
            SELECT code_metadata.code_key,
                   logical_scope.plan_id,
                   logical_scope.plan_market_type,
                   code_metadata.reported_code_system,
                   code_metadata.reported_code,
                   code_metadata.negotiation_arrangement,
                   code_metadata.billing_code_type_version,
                   code_metadata.source_name,
                   code_metadata.source_description,
                   code_metadata.rate_count
              FROM {serving._shared_v3_code_table()} code_metadata
              {scope_join_sql}
             WHERE {' AND '.join(filters)}
             ORDER BY {plan_order}, code_metadata.code_key
            """
        ),
        parameters_by_name,
    )
    return BindingProjection(
        binding,
        serving_tables,
        _group_code_rows(code_result, serving),
    )


def _provider_rows_sql() -> str:
    from api import ptg2_serving as serving

    assurance_sql = serving._ptg2_geo_assured_address_sql("addr")
    taxonomy_sql = serving._provider_taxonomy_summary_lateral_sql(
        "source_npis.npi"
    )
    return f"""
        WITH source_npis AS MATERIALIZED (
            SELECT UNNEST(CAST(:npis AS bigint[])) AS npi
        ), ranked_addresses AS MATERIALIZED (
            SELECT addr.*,
                   COALESCE(addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5))
                       AS projected_zip5,
                   ROW_NUMBER() OVER (
                       PARTITION BY addr.npi, COALESCE(
                           addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5)
                       )
                       ORDER BY CASE addr.type
                                    WHEN 'practice' THEN 0
                                    WHEN 'primary' THEN 1
                                    WHEN 'secondary' THEN 2
                                    WHEN 'site' THEN 3
                                    ELSE 4
                                END,
                                addr.checksum,
                                addr.location_key
                   ) AS address_rank
              FROM {table('entity_address_unified')} addr
              JOIN source_npis ON source_npis.npi = addr.npi
             WHERE addr.type IN ('practice', 'primary', 'secondary', 'site')
               AND {assurance_sql}
               AND COALESCE(
                       addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5)
                   ) ~ '^[0-9]{{5}}$'
        )
        SELECT source_npis.npi,
               {serving._ptg2_provider_name_sql('n')} AS provider_name,
               n.entity_type_code,
               n.provider_credential_text AS credential,
               COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
               COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications,
               tax.primary_specialty,
               addr.city_name AS city,
               addr.state_name AS state,
               addr.projected_zip5 AS zip5
          FROM source_npis
          LEFT JOIN {table('npi')} n ON n.npi = source_npis.npi
          JOIN ranked_addresses addr
            ON addr.npi = source_npis.npi
           AND addr.address_rank = 1
          {taxonomy_sql}
         ORDER BY source_npis.npi, addr.projected_zip5
    """


def _append_provider_rows(
    provider_rows_by_npi: dict[int, list[dict[str, Any]]],
    provider_result: Any,
) -> None:
    for raw_provider_row in provider_result:
        provider_by_field = row_mapping(raw_provider_row)
        npi = int(provider_by_field["npi"])
        zip5 = str(provider_by_field.get("zip5") or "")[:5]
        if not ZIP5.fullmatch(zip5):
            continue
        provider_by_field["zip5"] = zip5
        provider_by_field["state"] = (
            str(provider_by_field.get("state") or "").strip().upper() or None
        )
        provider_rows_by_npi[npi].append(provider_by_field)


async def projection_provider_rows_for_npis(
    session: Any,
    npis: Iterable[int],
) -> dict[int, tuple[dict[str, Any], ...]]:
    """Freeze one assured service-location card per NPI and ZIP cell."""

    normalized_npis = sorted({int(npi) for npi in npis if int(npi) > 0})
    provider_rows_by_npi: dict[int, list[dict[str, Any]]] = defaultdict(list)
    provider_statement = text(_provider_rows_sql())
    for start in range(0, len(normalized_npis), PROVIDER_BATCH_SIZE):
        npi_batch = normalized_npis[start : start + PROVIDER_BATCH_SIZE]
        provider_result = await session.execute(
            provider_statement,
            {"npis": npi_batch},
        )
        _append_provider_rows(provider_rows_by_npi, provider_result)
    return {
        npi: tuple(provider_rows)
        for npi, provider_rows in provider_rows_by_npi.items()
    }


def numeric_rates(
    prices: Iterable[Mapping[str, Any]],
) -> tuple[Decimal, ...]:
    """Retain finite non-negative negotiated rates as exact decimals."""

    numeric_rates_list: list[Decimal] = []
    for price_by_field in prices:
        raw_rate = price_by_field.get("negotiated_rate")
        try:
            rate = Decimal(str(raw_rate).strip())
        except (InvalidOperation, TypeError, ValueError):
            continue
        if rate.is_finite() and rate >= 0:
            numeric_rates_list.append(rate)
    return tuple(numeric_rates_list)


def eligible_projection_providers(
    providers: Iterable[dict[str, Any]],
    code_identity: tuple[str, str],
) -> list[dict[str, Any]]:
    """Apply the serving reader's inferred-taxonomy rule before projection."""

    from api import ptg2_serving as serving

    code_system, code = code_identity
    rule = serving._inferred_provider_taxonomy_rule(
        {"code_system": code_system, "code": code}
    )
    provider_rows_list = list(providers)
    if rule is None:
        return provider_rows_list
    eligible_taxonomy_codes = frozenset(rule.taxonomy_codes)
    return [
        provider_by_field
        for provider_by_field in provider_rows_list
        if provider_by_field.get("entity_type_code") == 1
        and eligible_taxonomy_codes.intersection(
            str(taxonomy_code or "").strip().upper()
            for taxonomy_code in provider_by_field.get("taxonomy_codes") or ()
        )
    ]
