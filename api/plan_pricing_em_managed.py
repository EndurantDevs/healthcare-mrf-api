# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""E&M cards from admitted prices and one exact retained provider office."""

from api import ptg2_geo_policy as geo_policy
from api import ptg2_geo_projection as geo_projection
from api.network_address_scope import (
    current_network_address_scope,
    scoped_address_parameters,
    scoped_address_relation_sql,
)
from api.plan_pricing_em_distance import _card_item, _progressive_page_rows, _projection_response
from api.plan_pricing_projection_contract import SCHEMA, PlanPricingProjectionUnavailable
from api.plan_release_serving import annotate_plan_release_response
from api.provider_service_source_policy import _bounded_rows, admitted_plan_release_bindings

_PAGE_SQL = """
WITH center AS MATERIALIZED (
 SELECT public.Geography(public.ST_MakePoint(
   coalesce(CAST(:longitude AS double precision),zip.longitude),
   coalesce(CAST(:latitude AS double precision),zip.latitude))) AS point
 FROM (SELECT 1) singleton LEFT JOIN {zip_relation} zip ON zip.zip_code=:zip5
 WHERE (CAST(:latitude AS double precision) IS NOT NULL AND CAST(:longitude AS double precision) IS NOT NULL)
    OR zip.zip_code IS NOT NULL
), nearest AS MATERIALIZED (
 SELECT candidate.* FROM center CROSS JOIN LATERAL (
  SELECT addr.npi,addr.location_key,addr.checksum AS address_checksum,
   CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END::smallint AS address_type_rank,
   addr.city_name AS city,
   CASE WHEN upper(btrim(coalesce(addr.state_name,''))) ~ '^[A-Z]{{2}}$' THEN upper(btrim(addr.state_name)) END AS state,
   CASE WHEN left(coalesce(addr.zip5,addr.postal_code,''),5) ~ '^[0-9]{{5}}$'
     THEN left(coalesce(addr.zip5,addr.postal_code,''),5) END AS zip5,
   ({geography} <-> center.point)/1609.344 AS distance_miles
  FROM {address} addr
  WHERE addr.npi IS NOT NULL AND addr.type IN ('primary','secondary','practice','site')
   AND coalesce(addr.address_precision,'')<>'city_zip'
   AND btrim(coalesce(addr.address_precision,'')) NOT IN ('','city_zip')
   AND addr.lat IS NOT NULL AND addr.long IS NOT NULL
   AND addr.lat BETWEEN -90 AND 90 AND addr.long BETWEEN -180 AND 180
   AND public.ST_DWithin({geography},center.point,:radius_meters)
   AND EXISTS (SELECT 1 FROM {rates_relation} rate WHERE rate.npi=addr.npi
     AND rate.code_index=:code_index AND rate.binding_ordinal=ANY(CAST(:allowed_binding_ordinals AS integer[])))
   AND EXISTS (SELECT 1 FROM (SELECT 1) reference {reference_joins} WHERE {identity})
   AND EXISTS (SELECT 1 FROM {zcta_relation} held_zcta
     WHERE held_zcta.zcta5ce=btrim(addr.zip5) AND {point})
   AND ({evidence})<>0
  ORDER BY {geography} <-> center.point,addr.npi,
   CASE addr.type WHEN 'practice' THEN 0 WHEN 'primary' THEN 1 ELSE 2 END,addr.checksum,addr.location_key
  LIMIT :candidate_limit
 ) candidate
), ranked AS MATERIALIZED (
 SELECT nearest.*,row_number() OVER(PARTITION BY npi ORDER BY distance_miles,address_type_rank,address_checksum,location_key)
   AS address_rank FROM nearest
), projected AS MATERIALIZED (SELECT * FROM ranked WHERE address_rank=1),
page AS MATERIALIZED (SELECT * FROM projected ORDER BY distance_miles,npi LIMIT :page_limit OFFSET :offset),
counts AS MATERIALIZED (SELECT (SELECT count(*) FROM nearest)::bigint AS candidate_count,
        (SELECT count(*) FROM projected)::bigint AS unique_count)
SELECT page.*,counts.candidate_count,counts.unique_count,true AS projection_ready
 FROM counts LEFT JOIN page ON true ORDER BY page.distance_miles NULLS LAST,page.npi NULLS LAST
"""


def _page_sql(inputs, rates_relation):
    """Filter admitted source ordinals and held office assurance before KNN limiting."""
    scope = current_network_address_scope()
    if scope is None:
        raise PlanPricingProjectionUnavailable("managed E&M requires a retained network scope")
    bindings = inputs.geo_bindings(SCHEMA)
    reference_joins = geo_policy.provider_address_identity_reference_joins_sql(
        "addr",
        schema_name=SCHEMA,
        geo_zip_alias="held_zip",
        zip_state_alias="held_state",
        dependency_bindings=bindings,
    )
    identity = geo_policy.provider_address_identity_coherence_sql(
        "addr", schema_name=SCHEMA, use_projection=False, reference_aliases=("held_zip", "held_state")
    )
    point = geo_policy.provider_address_point_coherence_sql("addr", use_projection=False, zcta_alias="held_zcta")
    evidence = geo_projection.legacy_evidence_source_id_sql(
        "addr", schema_name=SCHEMA, dependency_bindings=bindings, unified_schema_name=scope.manifest.schema_name
    )
    address = scoped_address_relation_sql("mrf.entity_address_unified")
    # Match the retained serving heap's GiST expression exactly.
    geography = "public.Geography(public.ST_MakePoint((addr.long)::double precision,(addr.lat)::double precision))"
    return _PAGE_SQL.format(
        zip_relation=inputs.relation("geo_zip_lookup"),
        zcta_relation=inputs.relation("tiger.zcta5"),
        address=address,
        rates_relation=rates_relation,
        geography=geography,
        reference_joins=reference_joins,
        identity=identity,
        point=point,
        evidence=evidence,
    )


def _completion_sql(inputs, rates_relation):
    from api.plan_pricing_em_distance_build import _rate_array_sql
    from api.ptg2_serving import _ptg2_provider_name_sql

    taxonomy = inputs.relation("npi_taxonomy")
    vocabulary = inputs.relation("nucc_taxonomy")
    order = "(upper(coalesce(nt.healthcare_provider_primary_taxonomy_switch,''))='Y') DESC,nt.checksum"
    return f"""SELECT requested.npi,{_ptg2_provider_name_sql("provider")} AS provider_name,
 provider.entity_type_code,provider.provider_credential_text AS credential,
 primary_tax.taxonomy_code,primary_tax.primary_specialty,classification.classification,
 prices.minimum_rates,prices.maximum_rates,prices.rate_counts
 FROM unnest($1::bigint[]) requested(npi) LEFT JOIN {inputs.relation("npi")} provider USING(npi)
 LEFT JOIN LATERAL (
  SELECT nt.healthcare_provider_taxonomy_code AS taxonomy_code,
   coalesce(nucc.display_name,nucc.classification) AS primary_specialty
  FROM {taxonomy} nt LEFT JOIN {vocabulary} nucc ON nucc.code=nt.healthcare_provider_taxonomy_code
  WHERE nt.npi=requested.npi ORDER BY {order} LIMIT 1
 ) primary_tax ON true
 LEFT JOIN LATERAL (
  SELECT nullif(nucc.classification,'') AS classification
  FROM {taxonomy} nt JOIN {vocabulary} nucc ON nucc.code=nt.healthcare_provider_taxonomy_code
  WHERE nt.npi=requested.npi AND nullif(nucc.classification,'') IS NOT NULL ORDER BY {order} LIMIT 1
 ) classification ON true
 LEFT JOIN LATERAL (
  SELECT ({_rate_array_sql("MIN", "minimum_rate", "numeric")})::text[] AS minimum_rates,
   ({_rate_array_sql("MAX", "maximum_rate", "numeric")})::text[] AS maximum_rates,
   {_rate_array_sql("SUM", "rate_count", "bigint")} AS rate_counts
  FROM {rates_relation} rate WHERE rate.npi=requested.npi
   AND rate.code_index=$3::smallint AND rate.binding_ordinal=ANY($2::integer[])
 ) prices ON true"""


async def _complete_page(driver, inputs, rates_relation, page_rows, ordinals, code_index):
    npis = sorted({int(row["npi"]) for row in page_rows if row.get("npi") is not None})
    if not npis:
        return []
    if len(npis) > 101:
        raise PlanPricingProjectionUnavailable("managed E&M completion exceeds its page bound")
    try:
        rows = await _bounded_rows(driver, _completion_sql(inputs, rates_relation), npis, list(ordinals), code_index)
        by_npi = {int(row["npi"]): row for row in rows}
        if len(by_npi) != len(rows) or set(by_npi) != set(npis):
            raise ValueError("managed E&M completion identity differs")
        return [
            _card_item({**row, **by_npi[int(row["npi"])]}, code_index)
            for row in page_rows
            if row.get("npi") is not None
        ]
    except ValueError, TypeError, KeyError:
        raise PlanPricingProjectionUnavailable("managed E&M completion is unavailable") from None


async def search_managed_em_distance(session, selection, args, pagination, zip5, latitude, longitude, code_index):
    """Complete a bounded admitted page while retaining full immutable release identity."""
    from api.plan_pricing_em_attribution import ATTRIBUTION_CONTRACT, resolve_em_attribution
    from process.network_em_provider_inputs import resolve_em_provider_inputs

    scope = current_network_address_scope()
    if scope is None:
        raise PlanPricingProjectionUnavailable("managed E&M requires a retained network scope")
    admitted = await admitted_plan_release_bindings(session, selection)
    artifact = await resolve_em_attribution(session, selection)
    connection = await session.connection()
    driver = (await connection.get_raw_connection()).driver_connection
    inputs = await resolve_em_provider_inputs(driver, scope.manifest)
    ordinals = tuple(binding.binding_ordinal for binding in admitted)
    if not ordinals:
        page_rows, exact, boundary, unique_count = [], True, False, 0
    else:
        page_window = await _progressive_page_rows(
            session,
            selection,
            args,
            pagination,
            (zip5, latitude, longitude),
            code_index,
            page_sql=_page_sql(inputs, artifact.rates_relation),
            extra_parameters=scoped_address_parameters(
                {"allowed_binding_ordinals": list(ordinals), "code_index": code_index}
            ),
        )
        if page_window is None:
            raise PlanPricingProjectionUnavailable("managed E&M page is unavailable")
        page_rows, exact, boundary, unique_count = page_window
    cards = await _complete_page(driver, inputs, artifact.rates_relation, page_rows, ordinals, code_index)
    has_more = len(cards) > int(pagination.limit) or (not exact and not boundary)
    cards = cards[: int(pagination.limit)]
    total = unique_count if exact else max(unique_count, int(pagination.offset) + len(cards) + int(has_more))
    response = _projection_response(
        args, pagination, selection, zip5, total, cards, is_total_exact=exact, has_more=has_more
    )
    response["query"].update(
        projection_contract=ATTRIBUTION_CONTRACT,
        source="plan_pricing_em_attributed_projection",
        snapshots=[
            {
                "source_key": binding.source_key,
                "snapshot_id": binding.snapshot_id,
                "plan_id": binding.plan_id,
                "plan_market_type": binding.plan_market_type,
            }
            for binding in admitted
        ],
    )
    return annotate_plan_release_response(response, selection, contributing_bindings=admitted) or response
