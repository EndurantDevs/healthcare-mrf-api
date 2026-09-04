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
MAX_PROVIDER_ROWS_PER_BATCH = 100_000


@dataclass(frozen=True)
class BindingProjection:
    binding: dict[str, Any]
    serving_tables: Any
    code_rows_by_identity: dict[tuple[str, str], list[dict[str, Any]]]
    raw_code_row_count: int


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
    *,
    maximum_code_rows: int | None = None,
) -> BindingProjection:
    """Read the sealed code scope for one release binding."""

    from api import ptg2_serving as serving

    serving_tables = await snapshot_serving_tables(
        session,
        str(binding["snapshot_id"]),
    )
    serving._require_strict_shared_v3(serving_tables)
    code_statement, parameters_by_name = _binding_code_query(
        serving,
        serving_tables,
        binding,
        maximum_code_rows,
    )
    code_result = await session.execute(code_statement, parameters_by_name)
    raw_code_rows = list(code_result)
    if (
        maximum_code_rows is not None
        and len(raw_code_rows) > maximum_code_rows
    ):
        raise ValueError("pricing projection code-row bound exceeded")
    return BindingProjection(
        binding,
        serving_tables,
        _group_code_rows(raw_code_rows, serving),
        len(raw_code_rows),
    )


def _binding_code_query(
    serving: Any,
    serving_tables: Any,
    binding: Mapping[str, Any],
    maximum_code_rows: int | None,
) -> tuple[Any, dict[str, Any]]:
    """Build the plan-scoped sealed-code query and its bound parameters."""

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
    limit_sql = ""
    if maximum_code_rows is not None:
        if maximum_code_rows <= 0:
            raise ValueError("pricing projection code-row bound is invalid")
        parameters_by_name["projection_code_row_limit"] = maximum_code_rows + 1
        limit_sql = "LIMIT :projection_code_row_limit"
    return (
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
             {limit_sql}
            """
        ),
        parameters_by_name,
    )


def _assured_addresses_sql(serving: Any) -> str:
    assurance_sql = serving._ptg2_geo_assured_address_sql("addr")
    evidence_level_sql = serving._ptg2_geo_evidence_level_sql("addr")
    return f"""
        WITH source_npis AS MATERIALIZED (
            SELECT UNNEST(CAST(:npis AS bigint[])) AS npi
        ), assured_addresses AS MATERIALIZED (
            SELECT addr.*,
                   COALESCE(addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5))
                       AS projected_zip5,
                   CASE
                     WHEN UPPER(BTRIM(COALESCE(addr.state_name, '')))
                          ~ '^[A-Z]{{2}}$'
                     THEN UPPER(BTRIM(addr.state_name))
                     WHEN UPPER(BTRIM(COALESCE(addr.state_code, '')))
                          ~ '^[A-Z]{{2}}$'
                     THEN UPPER(BTRIM(addr.state_code))
                   END AS projected_state,
                   {evidence_level_sql} AS geo_evidence_level
              FROM {table('entity_address_unified')} addr
              JOIN source_npis ON source_npis.npi = addr.npi
             WHERE addr.type IN ('practice', 'primary', 'secondary', 'site')
               AND {assurance_sql}
               AND COALESCE(
                       addr.zip5, LEFT(COALESCE(addr.postal_code, ''), 5)
                   ) ~ '^[0-9]{{5}}$'
        )
    """


def _ranked_addresses_sql(serving: Any) -> str:
    display_rank_sql = serving.address_display_rank_sql("addr")
    type_rank_sql = """CASE addr.type
        WHEN 'practice' THEN 0
        WHEN 'primary' THEN 1
        WHEN 'secondary' THEN 2
        WHEN 'site' THEN 3
        ELSE 4
    END"""
    return f"""
        , zip_ranked_addresses AS MATERIALIZED (
            SELECT addr.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY addr.npi, addr.projected_zip5
                       ORDER BY {display_rank_sql},
                                {type_rank_sql},
                                addr.checksum,
                                addr.location_key
                   ) AS zip_address_rank
              FROM assured_addresses addr
        ), ranked_addresses AS MATERIALIZED (
            SELECT addr.*,
                   CASE WHEN addr.projected_state IS NOT NULL THEN
                       ROW_NUMBER() OVER (
                           PARTITION BY addr.npi, addr.projected_state
                           ORDER BY {display_rank_sql},
                                    {type_rank_sql},
                                    addr.checksum,
                                    addr.location_key
                       )
                   END AS state_address_rank
              FROM zip_ranked_addresses addr
             WHERE addr.zip_address_rank = 1
        )
    """


def _address_payload_sql() -> str:
    return """jsonb_build_object(
        'npi', source_npis.npi,
        'type', addr.type,
        'checksum', addr.checksum,
        'first_line', addr.first_line,
        'second_line', addr.second_line,
        'city', addr.city_name,
        'state', addr.projected_state,
        'postal_code', COALESCE(
            NULLIF(BTRIM(addr.postal_code), ''), addr.projected_zip5
        ),
        'country_code', addr.country_code,
        'telephone_number', addr.telephone_number,
        'fax_number', addr.fax_number,
        'phone_number', addr.phone_number,
        'phone_extension', addr.phone_extension,
        'fax_number_digits', addr.fax_number_digits,
        'fax_extension', addr.fax_extension,
        'address_key', addr.address_key::text,
        'address_site_key', addr.premise_key::text,
        'premise_key', addr.premise_key::text,
        'location_key', addr.location_key,
        'address_precision', addr.address_precision,
        'county_fips', addr.county_fips,
        'address_sources', addr.address_sources,
        'source_record_ids', addr.source_record_ids,
        'source_count', addr.source_count,
        'multi_source_confirmed', addr.multi_source_confirmed,
        'source_mask', addr.source_mask,
        'address_source_mask', addr.address_source_mask,
        'location_confidence_id', addr.location_confidence_id,
        'lat', addr.lat,
        'long', addr.long
    )::text"""


def _provider_selection_sql(serving: Any) -> str:
    taxonomy_sql = serving._provider_taxonomy_summary_lateral_sql(
        "source_npis.npi"
    )
    address_payload_sql = _address_payload_sql()
    return f"""
        SELECT source_npis.npi,
               {serving._ptg2_provider_name_sql('n')} AS provider_name,
               n.entity_type_code,
               n.provider_credential_text AS credential,
               n.provider_sex_code,
               COALESCE(tax.taxonomy_codes, ARRAY[]::varchar[]) AS taxonomy_codes,
               COALESCE(tax.specialties, ARRAY[]::varchar[]) AS specialties,
               COALESCE(tax.classifications, ARRAY[]::varchar[]) AS classifications,
               COALESCE(tax.specializations, ARRAY[]::varchar[]) AS specializations,
               tax.primary_specialty,
               tax.primary_specialization,
               addr.city_name AS city,
               addr.projected_state AS state,
               addr.projected_zip5 AS zip5,
               CONCAT('entity_address_unified:', addr.location_key)
                   AS location_hash,
               'entity_address_unified'::varchar AS location_source,
               'entity_address_unified'::varchar AS location_confidence_code,
               addr.state_address_rank,
               addr.geo_evidence_level AS _geo_evidence_level,
               {serving._geo_evidence_source_id_sql('addr.geo_evidence_level')}
                   AS _geo_evidence_source_id,
               {address_payload_sql} AS address_payload
         FROM source_npis
          LEFT JOIN {table('npi')} n ON n.npi = source_npis.npi
          JOIN ranked_addresses addr
            ON addr.npi = source_npis.npi
           AND addr.zip_address_rank = 1
         {taxonomy_sql}
         ORDER BY source_npis.npi, addr.projected_state, addr.projected_zip5
         LIMIT :provider_row_limit
    """


def _provider_rows_sql() -> str:
    from api import ptg2_serving as serving

    return "".join(
        (
            _assured_addresses_sql(serving),
            _ranked_addresses_sql(serving),
            _provider_selection_sql(serving),
        )
    )


def _append_provider_rows(
    provider_rows_by_npi: dict[int, list[dict[str, Any]]],
    provider_result: Any,
) -> list[dict[str, Any]]:
    appended_rows: list[dict[str, Any]] = []
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
        state_address_rank = provider_by_field.get("state_address_rank")
        provider_by_field["state_address_rank"] = (
            int(state_address_rank) if state_address_rank is not None else None
        )
        provider_rows_by_npi[npi].append(provider_by_field)
        appended_rows.append(provider_by_field)
    return appended_rows


async def _hydrate_state_address_provenance(
    session: Any,
    provider_rows: list[dict[str, Any]],
) -> None:
    from api import ptg2_serving as serving

    witness_rows = [
        provider_by_field
        for provider_by_field in provider_rows
        if provider_by_field.get("state_address_rank") == 1
    ]
    if not witness_rows:
        return
    expected_count = len(witness_rows)
    status = await serving._hydrate_address_provenance(
        session,
        witness_rows,
        include_response_evidence=True,
        use_stored_only=False,
        strict_stored_identity=True,
        backfill_admitted_source_record_ids=True,
    )
    if status != "available" or len(witness_rows) != expected_count:
        raise ValueError("pricing projection provider-state provenance is incomplete")


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
            {
                "npis": npi_batch,
                "provider_row_limit": MAX_PROVIDER_ROWS_PER_BATCH + 1,
            },
        )
        provider_rows = list(provider_result)
        if len(provider_rows) > MAX_PROVIDER_ROWS_PER_BATCH:
            raise ValueError("pricing projection provider-row bound exceeded")
        appended_rows = _append_provider_rows(provider_rows_by_npi, provider_rows)
        await _hydrate_state_address_provenance(session, appended_rows)
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
