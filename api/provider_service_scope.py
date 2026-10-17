# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Provider-service selector boundaries and canonical membership predicates."""

from __future__ import annotations

import math
from urllib.parse import parse_qs

from sanic.exceptions import InvalidUsage, ServiceUnavailable
from sqlalchemy import Float, String, and_, case, cast, column, func, literal, or_, select, table, text

from api.custom_import_provider_service_sql import ProviderServiceImportQuery
from api.network_address_scope import current_network_address_scope, scoped_address_parameters


def _canonical_network_procedure_clause(
    args, import_context, provider_npi, *, query_string=None, location_filter=None, managed_native=False
):
    """Restrict claims providers to the pinned network before count and pagination."""
    network_scope = current_network_address_scope()
    if network_scope is None or network_scope.network_ids is None:
        return None
    raw_args = parse_qs(query_string or "", keep_blank_values=True)
    unsupported_fields = (
        "billing_entity_ref",
        "plan_id",
        "plan_external_id",
        "plan_release_id",
        "source_key",
        "snapshot_id",
    )
    if (
        managed_native
        and (import_context is None or type(import_context) is ProviderServiceImportQuery)
        and "billing_entity_ref" not in args
        and "billing_entity_ref" not in raw_args
        and any(args.get(name) for name in unsupported_fields[1:])
    ):
        # The managed plan readers apply reviewed source and retained-office predicates.
        return None
    if (import_context is not None and type(import_context) is not ProviderServiceImportQuery) or any(
        name in args or name in raw_args for name in unsupported_fields
    ):
        raise InvalidUsage("Canonical network procedure queries support the claims lane only.")
    addresses = network_scope.address_relation
    return (
        select(literal(1))
        .select_from(addresses)
        .where(
            func.coalesce(addresses.c.npi, addresses.c.inferred_npi) == provider_npi,
            location_filter if location_filter is not None else literal(True),
        )
        .exists()
        .params(**scoped_address_parameters({}))
    )


def _reject_resolver_only_procedure_search_params(args) -> None:
    """Direct callers must resolve clinical intent before pricing search."""

    resolver_only_param = next(
        (param_name for param_name in ("clinical_intent", "intent") if args.get(param_name) is not None),
        None,
    )
    if resolver_only_param is None:
        return
    raise InvalidUsage(
        f"Parameter '{resolver_only_param}' is resolver-only. Call "
        "/pricing/procedure-taxonomy/resolve first. When "
        "recommended_mode=hard_filter, copy provider_filter.taxonomy_codes, "
        "provider_filter.primary_only, and "
        "provider_filter.include_subspecialties into the pricing search."
    )


async def _canonical_procedure_zip_anchor(session, zip5):
    """Use the same first-ZIP ZCTA point as the nearby selected-office query."""
    result = await session.execute(
        text("select intptlat, intptlon from zcta5 where zcta5ce=:zip_code limit 1;"),
        {"zip_code": zip5},
    )
    row = result.mappings().first()
    if row is None:
        return None
    try:
        latitude, longitude = float(row["intptlat"]), float(row["intptlon"])
    except TypeError, ValueError:
        return None
    if not (
        math.isfinite(latitude) and math.isfinite(longitude) and -90 <= latitude <= 90 and -180 <= longitude <= 180
    ):
        return None
    return latitude, longitude


def _canonical_procedure_location_filter(*, state, city, zip5, zip_radius, coordinates, zip_anchor=None):
    """Apply every locator to one actual selected office, never claims geography."""
    addresses = current_network_address_scope().address_relation
    terms = []
    if state:
        terms.append(
            func.upper(func.coalesce(func.nullif(addresses.c.state_code, ""), addresses.c.state_name)) == state
        )
    if city:
        terms.append(
            func.lower(func.coalesce(func.nullif(addresses.c.city_norm, ""), addresses.c.city_name)).like(f"%{city}%")
        )
    if zip5 and zip_radius == 0:
        terms.append(func.coalesce(addresses.c.zip5, func.left(addresses.c.postal_code, 5)) == zip5)
    anchors = []
    if zip5 and zip_radius > 0:
        if zip_anchor is None or any(coordinate is None for coordinate in zip_anchor):
            raise ServiceUnavailable("Selected-office ZIP coordinates are unavailable.")
        anchors.append((*zip_anchor, zip_radius))
    if coordinates[0] is not None:
        anchors.append(coordinates)
    for latitude, longitude, radius in anchors:
        point = func.Geography(func.ST_MakePoint(cast(addresses.c.long, Float), cast(addresses.c.lat, Float)))
        anchor = func.Geography(func.ST_MakePoint(longitude, latitude))
        valid = and_(addresses.c.lat.between(-90, 90), addresses.c.long.between(-180, 180))
        terms.extend(
            (
                addresses.c.type.in_(("primary", "secondary", "practice", "site")),
                func.coalesce(addresses.c.address_precision, "") != "city_zip",
                case((valid, func.ST_DWithin(point, anchor, radius * 1609.34)), else_=False),
            )
        )
    return and_(*terms) if terms else literal(True)


def _procedure_office_document(addresses, bindings):
    """Build one bounded original location document or refuse it in SQL."""
    from api.network_provider_read import _DISPLAY_LIMITS

    display_column_by_name = {
        name: addresses.c[name]
        for name in (
            "location_key",
            "first_line",
            "second_line",
            "city_name",
            "state_name",
            "postal_code",
            "country_code",
            "lat",
            "long",
        )
    }
    display_column_by_name["location_id"] = cast(bindings.c.location_id, String)
    valid_fields = and_(
        bindings.c.location_id.isnot(None),
        addresses.c.location_key.op("~")("^[0-9a-f]{64}$"),
        cast(bindings.c.location_id, String) != "00000000-0000-0000-0000-000000000000",
        *(
            or_(addresses.c[name].is_(None), func.length(addresses.c[name]) <= bound)
            for name, bound in _DISPLAY_LIMITS.items()
        ),
    )
    document = func.jsonb_build_object(
        *(part for name, expression in display_column_by_name.items() for part in (name, expression))
    )
    return case((valid_fields, document), else_=None)


def _procedure_office_rows(scope, provider_npis, location_filter):
    """Select a complete bounded office prefix from the same pinned relation."""
    from api.network_provider_read import MAX_LOCATIONS

    addresses = scope.address_relation
    bindings = table(
        "provider_location_binding",
        *(column(name) for name in ("location_key", "entity_type", "entity_id", "location_id")),
        schema=scope.manifest.schema_name,
    )
    provider = func.coalesce(addresses.c.npi, addresses.c.inferred_npi)
    locations = (
        select(
            provider.label("npi"),
            addresses.c.location_key.label("location_key"),
            _procedure_office_document(addresses, bindings).label("location"),
        )
        .select_from(
            addresses.outerjoin(
                bindings,
                and_(
                    bindings.c.location_key == addresses.c.location_key,
                    bindings.c.entity_type == addresses.c.entity_type,
                    bindings.c.entity_id == addresses.c.entity_id,
                ),
            )
        )
        .where(provider.in_(provider_npis), location_filter)
        .order_by(provider, addresses.c.location_key)
        .limit(MAX_LOCATIONS + 1)
        .cte("selected_procedure_offices")
    )
    return locations


def _procedure_office_query(scope, locations):
    """Return data only when every office is unique, valid and within byte bounds."""
    from api.network_provider_read import MAX_LOCATIONS, MAX_RESPONSE_BYTES

    bounded = and_(
        func.count() <= MAX_LOCATIONS,
        func.count() == func.count(func.distinct(locations.c.location_key)),
        func.bool_and(locations.c.location.isnot(None)),
        func.coalesce(func.sum(func.octet_length(cast(locations.c.location, String))), 0) <= MAX_RESPONSE_BYTES - 1024,
    )
    query = (
        select(
            case(
                (
                    bounded,
                    func.jsonb_agg(func.jsonb_build_object("npi", locations.c.npi, "location", locations.c.location)),
                ),
                else_=None,
            )
        )
        .select_from(locations)
        .params(**scoped_address_parameters({}))
    )
    return query


async def _canonical_procedure_locations(session, provider_items, location_filter):
    """One bounded set read returns only original offices used by claims eligibility."""
    if not provider_items:
        return
    scope = current_network_address_scope()
    locations = _procedure_office_rows(
        scope, [provider_item["npi"] for provider_item in provider_items], location_filter
    )
    office_records = (await session.execute(_procedure_office_query(scope, locations))).scalar()
    if not isinstance(office_records, list):
        raise ServiceUnavailable("Complete selected-office evidence is unavailable.")
    by_npi = {}
    for office_record in office_records:
        by_npi.setdefault(int(office_record["npi"]), []).append(office_record["location"])
    if set(by_npi) != {int(provider_item["npi"]) for provider_item in provider_items}:
        raise ServiceUnavailable("Selected-office evidence does not match the provider page.")
    for provider_item in provider_items:
        provider_item["locations"] = sorted(
            by_npi[int(provider_item["npi"])], key=lambda location: location["location_key"]
        )
        provider_item["claims_geography"] = {name: provider_item.pop(name, None) for name in ("city", "state", "zip5")}
        provider_item["claims_evidence_grain"] = "provider_npi/year/procedure aggregate"


def _provider_service_evidence(response_by_field, code_context, *, canonical=False):
    """Keep original claims provenance and distinguish provider count from offices."""
    query_by_field = response_by_field["query"]
    if query_by_field["include_sources"]:
        response_by_field["sources"] = [
            {
                "source_key": "cms_medicare_provider_services_claims",
                "source_importer": "claims-pricing",
                "source_system": "cms_medicare_part_b",
                "dataset": "Medicare Physician & Other Practitioners by Provider and Service",
                "serving_tables": ["pricing_provider", "pricing_provider_procedure"],
                "grain": "provider_npi/year/procedure aggregate",
                "derived": True,
            }
        ]
    if query_by_field["include_evidence"] or query_by_field["include_debug"]:
        response_by_field["evidence"] = {
            "matched_provider_count" if canonical else "matched_provider_location_count": response_by_field[
                "pagination"
            ]["total"],
            "filters": {
                name: query_by_field[name]
                for name in (
                    "year",
                    "year_source",
                    "code",
                    "state",
                    "city",
                    "zip5",
                    "specialty",
                    "provider_sex_code",
                    "q",
                    "min_claims",
                    "min_total_cost",
                    "provider_type_resolution",
                    "procedure_term_resolution",
                )
            },
            "zip_scope": {
                "anchor_zip5": query_by_field["zip5"] or "",
                "zip_radius_miles": query_by_field["zip_radius_miles"],
                "candidate_count": query_by_field["zip_candidate_count"],
            },
            "code_resolution": code_context,
        }
