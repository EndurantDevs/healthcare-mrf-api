# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Choose a qualifying office before a statewide pricing identity page."""

import orjson

from api.address_source_filter import address_source_filter_sql
from api.network_address_scope import scoped_address_relation_sql
from api.plan_pricing_projection_contract import SCHEMA, PlanPricingProjectionUnavailable
from api.plan_pricing_projection_source import projection_provider_rows_for_npis


def state_source_count_sql(minimum):
    """Use the same assured office population as the bounded address hydration."""
    from api import ptg2_serving as serving

    relation = scoped_address_relation_sql(f"{SCHEMA}.entity_address_unified", location_lookup=True)
    return f"""AND EXISTS (
        SELECT 1 FROM {relation} source_address
         WHERE source_address.npi = provider_state.npi
           AND {address_source_filter_sql("source_address", minimum)}
           AND source_address.type IN ('primary','secondary','practice','site')
           AND COALESCE(source_address.zip5, LEFT(source_address.postal_code, 5)) ~ '^[0-9]{{5}}$'
           AND (CASE
                WHEN UPPER(BTRIM(COALESCE(source_address.state_name, ''))) ~ '^[A-Z]{{2}}$'
                THEN UPPER(BTRIM(source_address.state_name))
                ELSE UPPER(BTRIM(source_address.state_code)) END) = :state
           AND ({serving._ptg2_geo_evidence_level_sql("source_address")}) IS NOT NULL
    )"""


async def qualifying_state_fragments(session, selected_rows, state, minimum_sources):
    """Retain frozen provider facts while replacing its office with the qualified choice."""
    npis = tuple(int(row["npi"]) for row in selected_rows)
    rows_by_npi = await projection_provider_rows_for_npis(session, npis, min_address_sources=minimum_sources)
    fragments_by_npi = {}
    for row in selected_rows:
        npi = int(row["npi"])
        choices = [
            address
            for address in rows_by_npi.get(npi, ())
            if address.get("state") == state and address.get("state_address_rank") == 1
        ]
        if len(choices) != 1:
            raise PlanPricingProjectionUnavailable("qualifying state-scan office is unavailable")
        fragment = orjson.loads(row["provider_fragment"])
        provider = fragment["provider"]
        for field in (
            "state",
            "city",
            "zip5",
            "location_hash",
            "location_source",
            "location_confidence_code",
            "address_payload",
        ):
            provider[field] = choices[0][field]
        fragments_by_npi[npi] = orjson.dumps(fragment)
    return fragments_by_npi
