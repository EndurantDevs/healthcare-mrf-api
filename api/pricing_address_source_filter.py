# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stored-count eligibility for compact pricing cells and exact office rows."""

from sqlalchemy import text

from api.address_source_filter import address_source_filter_sql, parse_min_address_sources
from api.network_address_scope import scoped_address_parameters, scoped_address_relation_sql
from api.plan_pricing_projection_contract import SCHEMA, PlanPricingProjectionUnavailable
from api.ptg2_serving_utils import has_location_filter


def qualifying_cell_sql(provider_alias, minimum):
    """A compact ZIP card qualifies when any exact office in that cell qualifies."""
    minimum = parse_min_address_sources({"min_address_sources": minimum})
    if not minimum:
        return "TRUE"
    relation = scoped_address_relation_sql(f"{SCHEMA}.entity_address_unified", location_lookup=True)
    return (
        f"EXISTS (SELECT 1 FROM {relation} source_address "
        f"WHERE source_address.npi = {provider_alias}.npi "
        f"AND COALESCE(source_address.zip5, LEFT(source_address.postal_code, 5)) = {provider_alias}.geo_cell "
        "AND source_address.type IN ('primary','secondary','practice','site') "
        f"AND {address_source_filter_sql('source_address', minimum)})"
    )


def qualifying_location_sql(location_alias, minimum):
    """A frozen office may only borrow the count of that exact provider/office."""
    minimum = parse_min_address_sources({"min_address_sources": minimum})
    if not minimum:
        return "TRUE"
    relation = scoped_address_relation_sql(f"{SCHEMA}.entity_address_unified", location_lookup=True)
    return (
        f"EXISTS (SELECT 1 FROM {relation} source_address "
        f"WHERE source_address.npi = {location_alias}.npi "
        f"AND source_address.location_key = {location_alias}.location_key "
        f"AND {address_source_filter_sql('source_address', minimum)})"
    )


def normalize_address_source_order(args):
    """Use one cost comparator throughout a count-only request and its network merge."""
    if (
        parse_min_address_sources(args)
        and not has_location_filter(args, include_npi=False)
        and not str(args.get("order_by") or "").strip()
    ):
        return {**args, "order_by": "total_allowed_amount"}
    return args


async def qualifying_compact_offices(session, cards, minimum):
    """Replace only postal fields with one qualifying current office per selected cell."""
    minimum = parse_min_address_sources({"min_address_sources": minimum})
    if not minimum or not cards:
        return cards
    identities = tuple(dict.fromkeys((int(card["npi"]), str(card["zip5"])) for card in cards))
    relation = scoped_address_relation_sql(f"{SCHEMA}.entity_address_unified", location_lookup=True)
    office_result = await session.execute(
        text(f"""SELECT requested.npi, requested.geo_cell, office.city, office.state
            FROM unnest(CAST(:office_npis AS bigint[]), CAST(:office_cells AS varchar[])) requested(npi, geo_cell)
            CROSS JOIN LATERAL (
                SELECT NULLIF(BTRIM(source_address.city_name), '') AS city,
                       CASE WHEN UPPER(BTRIM(COALESCE(source_address.state_name, ''))) ~ '^[A-Z]{{2}}$'
                            THEN UPPER(BTRIM(source_address.state_name))
                            ELSE NULLIF(UPPER(BTRIM(source_address.state_code)), '') END AS state
                  FROM {relation} source_address
                 WHERE source_address.npi = requested.npi
                   AND COALESCE(source_address.zip5, LEFT(source_address.postal_code, 5)) = requested.geo_cell
                   AND source_address.type IN ('primary','secondary','practice','site')
                   AND {address_source_filter_sql("source_address", minimum)}
                 ORDER BY CASE source_address.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,
                          source_address.location_key
                 LIMIT 1
            ) office"""),
        scoped_address_parameters(
            {
                "office_npis": [identity[0] for identity in identities],
                "office_cells": [identity[1] for identity in identities],
                "min_address_sources": minimum,
            }
        ),
    )
    offices_by_identity = {
        (int(office_row["npi"]), str(office_row["geo_cell"])): {
            "city": office_row["city"],
            "state": office_row["state"],
        }
        for office_row in office_result.mappings()
    }
    if set(offices_by_identity) != set(identities):
        raise PlanPricingProjectionUnavailable("qualifying compact-card office is unavailable")
    return [dict(card, **offices_by_identity[(int(card["npi"]), str(card["zip5"]))]) for card in cards]
