# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read provider identities and exact offices from a pinned canonical network."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from api.address_source_filter import address_source_filter_sql
from api.network_address_scope import NetworkAddressReadScope
from process.network_address_projection import _identifier
from process.network_serving_read import (
    NetworkServingReadUnavailable,
    PinnedNetworkServingManifest,
    parse_canonical_network_ids,
    resolve_network_serving_manifest,
)
from process.provider_directory_projection_fhir_values import is_valid_npi

MAX_RESPONSE_BYTES = 1024 * 1024
MAX_LOCATIONS = 100
_PAGE_ADDRESS_COLUMNS = (
    "location_key",
    "entity_type",
    "entity_id",
    "source_count",
    "entity_name",
    "npi",
    "first_line",
    "second_line",
    "city_name",
    "state_name",
    "postal_code",
    "country_code",
    "lat",
    "long",
)
_QUALIFYING_ADDRESS_COLUMNS = ("location_key", "entity_type", "entity_id", "source_count")
_DISPLAY_LIMITS = {
    "first_line": 512,
    "second_line": 512,
    "city_name": 128,
    "state_name": 64,
    "postal_code": 32,
    "country_code": 64,
}


class NetworkProviderReadError(ValueError):
    """The provider selector or caller transaction is invalid."""


class NetworkProviderReadUnavailable(RuntimeError):
    """The complete pinned provider result could not be verified."""


class NetworkProviderNotFound(LookupError):
    """This identity has no selected office in the pinned network."""


_PROVIDER_RESULT_SQL = """locations AS MATERIALIZED (
  SELECT selected.provider_system,selected.provider_id,selected.location_id,
    jsonb_build_object('location_id',selected.location_id::text,'location_key',selected.location_key,
      'first_line',first_line,'second_line',second_line,'city_name',city_name,'state_name',state_name,
      'postal_code',postal_code,'country_code',country_code,'lat',lat,'long',long) AS document
  FROM offices selected JOIN page USING(provider_system,provider_id) WHERE page.consistent AND page.location_count<=$6
), bounds AS (
  SELECT coalesce(bool_and(consistent AND location_count<=$6),true) AS valid,
    coalesce(sum(octet_length(provider_id)+octet_length(coalesce(display_name,''))+256),0)
      +coalesce((SELECT sum(octet_length(document::text)+2) FROM locations),0)<=$7-1024 AS bounded
  FROM page
), providers AS (
  SELECT page.provider_system,page.provider_id,jsonb_build_object(
    'provider_system',page.provider_system,'provider_id',page.provider_id,'display_name',page.display_name,
    'npi',page.npi,'locations',coalesce((SELECT jsonb_agg(document ORDER BY location_id)
      FROM locations WHERE locations.provider_system=page.provider_system
        AND locations.provider_id=page.provider_id),'[]'::jsonb)) AS document
  FROM page,bounds WHERE bounds.valid AND bounds.bounded
), totals AS MATERIALIZED (
  SELECT count(*)::bigint AS total_count FROM identities
), result AS (
  SELECT jsonb_build_object('generation_id',$8::bigint,'total_count',totals.total_count,
    'limit',$4::integer,'offset',$5::integer,'has_more',totals.total_count>$5+$4,
    'providers',coalesce((SELECT jsonb_agg(document ORDER BY provider_system COLLATE "C",provider_id COLLATE "C")
      FROM providers),'[]'::jsonb))::text AS document
  FROM totals
)
SELECT integrity.valid AND bounds.valid AS valid,bounds.bounded AND octet_length(result.document)<=$7 AS bounded,
  CASE WHEN integrity.valid AND bounds.valid AND bounds.bounded AND octet_length(result.document)<=$7
    THEN result.document END AS result FROM integrity,bounds,result"""

_SELECTED_ADDRESS_JOIN = """  LEFT JOIN selected_addresses address ON (address.location_key,address.entity_type,address.entity_id)
    =(binding.location_key,binding.entity_type,binding.entity_id)
"""
_SELECTED_BINDINGS = """  SELECT binding.* FROM selected_addresses address JOIN ONLY {namespace}.provider_location_binding binding
    ON address.location_key=binding.location_key
  WHERE $2::text IS NULL OR (binding.provider_system,binding.provider_id)=($2,$3)
    OR EXISTS(SELECT 1 FROM selected WHERE selected.location_key=address.location_key)
"""

_PROVIDERS_SQL = (
    """WITH selected_addresses AS NOT MATERIALIZED (
  SELECT location_key,entity_type,entity_id,entity_name,npi,first_line,second_line,city_name,state_name,
    postal_code,country_code,lat,long,source_count FROM {addresses}
  WHERE {address_membership}
), selected_memberships AS MATERIALIZED (
  SELECT DISTINCT provider_system,provider_id,location_id FROM ONLY {namespace}.network_membership
  WHERE network_id=ANY($1::text::integer[])
    AND ($2::text IS NULL OR (provider_system,provider_id)=($2,$3))
), selected AS MATERIALIZED (
  SELECT member.*,binding.location_key,address.location_key AS matched_key,address.entity_name,address.npi,
    address.first_line,address.second_line,address.city_name,address.state_name,address.postal_code,
    address.country_code,address.lat,address.long,address.source_count
  FROM selected_memberships member LEFT JOIN ONLY {namespace}.provider_location_binding binding
    ON (binding.provider_system,binding.provider_id,binding.location_id)
      =(member.provider_system,member.provider_id,member.location_id)
"""
    + _SELECTED_ADDRESS_JOIN
    + """), selected_bindings AS MATERIALIZED (
"""
    + _SELECTED_BINDINGS
    + """), integrity AS (
  SELECT count(*)=count(DISTINCT (provider_system,provider_id,location_id))
    AND coalesce(bool_and(matched_key IS NOT NULL AND provider_system IN ('npi','manual','provider_directory')
      AND location_id<>'00000000-0000-0000-0000-000000000000'::uuid
      AND location_key ~ '^[0-9a-f]{{64}}$'),true)
    AND (SELECT count(*)=count(DISTINCT (provider_system,provider_id,location_id)) FROM selected_bindings)
    AND NOT EXISTS(SELECT FROM selected_bindings GROUP BY location_key
      HAVING count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1
        OR (bool_or(provider_system='manual') AND count(*)>1))
    AND ($2::text IS NOT NULL OR NOT EXISTS(
      SELECT location_key,entity_type,entity_id FROM selected_addresses
      EXCEPT SELECT location_key,provider_system,provider_id FROM selected
      WHERE matched_key IS NOT NULL)) AS valid
  FROM selected
), eligible AS MATERIALIZED (
  SELECT * FROM selected
  WHERE ($9::uuid IS NULL OR location_id=$9)
    AND ($10::double precision IS NULL OR CASE
      WHEN lat BETWEEN -90 AND 90 AND long BETWEEN -180 AND 180 THEN
        ST_DWithin(Geography(ST_MakePoint(long::double precision,lat::double precision)),
          Geography(ST_MakePoint($11::double precision,$10::double precision)),$12::double precision*1609.34)
      ELSE false END)
), offices AS MATERIALIZED (
  SELECT DISTINCT ON(provider_system,provider_id,location_key) * FROM eligible
  ORDER BY provider_system,provider_id,location_key,location_id
), identities AS MATERIALIZED (
  SELECT provider_system,provider_id,min(entity_name) AS display_name,min(npi) AS npi,
    count(*) AS location_count,count(DISTINCT entity_name)<=1 AND count(DISTINCT npi)<=1 AS consistent
  FROM offices GROUP BY provider_system,provider_id
), page AS MATERIALIZED (
  SELECT * FROM identities ORDER BY provider_system COLLATE "C",provider_id COLLATE "C" LIMIT $4 OFFSET $5
), """
    + _PROVIDER_RESULT_SQL
)

# Only this provider's bindings and bindings sharing its selected locations affect detail integrity.
# OFFSET 0 keeps each lateral address lookup driven by its outer location key.
_PROVIDER_DETAIL_SQL = _PROVIDERS_SQL.replace(
    _SELECTED_ADDRESS_JOIN,
    """  LEFT JOIN LATERAL (SELECT * FROM selected_addresses address
    WHERE (address.location_key,address.entity_type,address.entity_id)
      =(binding.location_key,binding.entity_type,binding.entity_id) OFFSET 0) address ON true
""",
    1,
).replace(
    _SELECTED_BINDINGS,
    """  SELECT binding.* FROM (
    SELECT location_key FROM ONLY {namespace}.provider_location_binding
    WHERE (provider_system,provider_id)=($2,$3)
    UNION SELECT location_key FROM selected
  ) candidate JOIN ONLY {namespace}.provider_location_binding binding
    ON binding.location_key=candidate.location_key
  JOIN LATERAL (SELECT location_key FROM selected_addresses address
    WHERE address.location_key=candidate.location_key OFFSET 0) address ON true
  WHERE (binding.provider_system,binding.provider_id)=($2,$3)
    OR EXISTS(SELECT 1 FROM selected WHERE selected.location_key=address.location_key)
""",
    1,
)

_MEMBERSHIP_IDENTITIES_SQL = """WITH identities AS MATERIALIZED (
  SELECT DISTINCT provider_system,provider_id FROM ONLY {namespace}.network_membership
  WHERE network_id=ANY($1::text::integer[])
), """

_MEMBERSHIP_PROVIDERS_SQL = (
    _MEMBERSHIP_IDENTITIES_SQL
    + """page_identities AS MATERIALIZED (
  SELECT * FROM identities ORDER BY provider_system COLLATE "C",provider_id COLLATE "C" LIMIT $4 OFFSET $5
), selected_memberships AS MATERIALIZED (
  SELECT DISTINCT member.provider_system,member.provider_id,member.location_id
  FROM page_identities page JOIN ONLY {namespace}.network_membership member USING(provider_system,provider_id)
  WHERE member.network_id=ANY($1::text::integer[])
), selected AS MATERIALIZED (
  SELECT member.*,binding.location_key,address.location_key AS matched_key,address.entity_name,address.npi,
    address.first_line,address.second_line,address.city_name,address.state_name,address.postal_code,
    address.country_code,address.lat,address.long
  FROM selected_memberships member LEFT JOIN ONLY {namespace}.provider_location_binding binding
    ON (binding.provider_system,binding.provider_id,binding.location_id)
      =(member.provider_system,member.provider_id,member.location_id)
  LEFT JOIN LATERAL (SELECT * FROM {addresses} address
    WHERE (address.location_key,address.entity_type,address.entity_id)
      =(binding.location_key,binding.entity_type,binding.entity_id) OFFSET 0) address ON true
), integrity AS (
  SELECT count(*)=count(DISTINCT (provider_system,provider_id,location_id))
    AND coalesce(bool_and(matched_key IS NOT NULL AND provider_system IN ('npi','manual','provider_directory')
      AND location_id<>'00000000-0000-0000-0000-000000000000'::uuid
      AND location_key ~ '^[0-9a-f]{{64}}$'),true)
    AND ($2::text,$3::text,$9::uuid,$10::double precision,$11::double precision,$12::double precision) IS NULL
    AS valid FROM selected
), offices AS MATERIALIZED (
  SELECT DISTINCT ON(provider_system,provider_id,location_key) * FROM selected
  ORDER BY provider_system,provider_id,location_key,location_id
), page AS MATERIALIZED (
  SELECT provider_system,provider_id,min(entity_name) AS display_name,min(npi) AS npi,
    count(*) AS location_count,count(DISTINCT entity_name)<=1 AND count(DISTINCT npi)<=1 AS consistent
  FROM offices GROUP BY provider_system,provider_id
), """
    + _PROVIDER_RESULT_SQL
)

# Keep the entity match residual so join planning can use location-key cardinality.
_SOURCE_MEMBERSHIP_IDENTITIES_SQL = """WITH page_addresses AS NOT MATERIALIZED (
  SELECT * FROM {addresses}
), qualifying_addresses AS MATERIALIZED (
  SELECT address.location_key,address.entity_type,address.entity_id
  FROM {qualifying_addresses} address WHERE {source_predicate}
), identities AS MATERIALIZED (
  SELECT DISTINCT binding.provider_system,binding.provider_id
  FROM qualifying_addresses address JOIN ONLY {namespace}.provider_location_binding binding
    ON binding.location_key=address.location_key
      AND CASE WHEN binding.entity_type=address.entity_type THEN binding.entity_id=address.entity_id ELSE FALSE END
  WHERE EXISTS(SELECT 1 FROM ONLY {namespace}.network_membership member
    WHERE (member.provider_system,member.provider_id,member.location_id)
      =(binding.provider_system,binding.provider_id,binding.location_id)
      AND member.network_id=ANY({selected_network_ids}))
), """

_SOURCE_MEMBERSHIP_PROVIDERS_SQL = (
    _MEMBERSHIP_PROVIDERS_SQL.replace(_MEMBERSHIP_IDENTITIES_SQL, _SOURCE_MEMBERSHIP_IDENTITIES_SQL, 1)
    .replace(
        "FROM page_identities page JOIN ONLY {namespace}.network_membership member USING(provider_system,provider_id)\n"
        "  WHERE member.network_id=ANY($1::text::integer[])",
        """FROM page_identities page JOIN ONLY {namespace}.network_membership member USING(provider_system,provider_id)
  JOIN ONLY {namespace}.provider_location_binding qualifying_binding
    ON (qualifying_binding.provider_system,qualifying_binding.provider_id,qualifying_binding.location_id)
      =(member.provider_system,member.provider_id,member.location_id)
  JOIN qualifying_addresses qualified_address
    ON (qualified_address.location_key,qualified_address.entity_type,qualified_address.entity_id)
      =(qualifying_binding.location_key,qualifying_binding.entity_type,qualifying_binding.entity_id)
  WHERE member.network_id=ANY($1::text::integer[])""",
        1,
    )
    .replace("FROM {addresses} address", "FROM page_addresses address", 1)
)


def _selector(provider_system, provider_id):
    if (
        type(provider_system) is not str
        or provider_system not in ("npi", "manual", "provider_directory")
        or type(provider_id) is not str
        or not 1 <= len(provider_id) <= 128
        or provider_id.strip() != provider_id
        or not provider_id.isprintable()
        or (provider_system == "npi" and not is_valid_npi(provider_id))
    ):
        raise NetworkProviderReadError("Provider identity is invalid")
    if provider_system == "manual":
        try:
            if str(UUID(provider_id)) != provider_id or UUID(provider_id).int == 0:
                raise ValueError
        except ValueError:
            raise NetworkProviderReadError("Provider identity is invalid") from None


def _arguments(scope, limit, offset):
    if (
        type(scope) is not NetworkAddressReadScope
        or type(scope.manifest) is not PinnedNetworkServingManifest
        or type(scope.network_ids) is not tuple
        or not 1 <= len(scope.network_ids) <= 100
        or any(type(value) is not int or not 1 <= value <= 2147483647 for value in scope.network_ids)
        or scope.network_ids != tuple(sorted(set(scope.network_ids)))
        or type(limit) is not int
        or not 1 <= limit <= 100
        or type(offset) is not int
        or not 0 <= offset <= 1_000_000
    ):
        raise NetworkProviderReadError("Canonical provider selection is invalid")


def _location(location):
    if (
        set(location) != {"location_id", "location_key", "lat", "long", *_DISPLAY_LIMITS}
        or type(location["location_id"]) is not str
        or str(UUID(location["location_id"])) != location["location_id"]
        or UUID(location["location_id"]).int == 0
        or type(location["location_key"]) is not str
        or len(location["location_key"]) != 64
        or any(value not in "0123456789abcdef" for value in location["location_key"])
        or any(
            value is not None and (type(value) is not str or len(value) > _DISPLAY_LIMITS[field] or "\x00" in value)
            for field, value in location.items()
            if field in _DISPLAY_LIMITS
        )
    ):
        raise NetworkProviderReadUnavailable("Pinned office display is unavailable")
    latitude, longitude = location["lat"], location["long"]
    if (latitude is None) != (longitude is None) or any(
        value is not None and (type(value) not in (int, float) or not math.isfinite(value) or abs(value) > bound)
        for value, bound in ((latitude, 90), (longitude, 180))
    ):
        raise NetworkProviderReadUnavailable("Pinned office coordinates are unavailable")


def _result(receipt):
    if receipt["valid"] is not True or receipt["bounded"] is not True:
        raise NetworkProviderReadUnavailable("Pinned providers are inconsistent or exceed the response bound")
    result = json.loads(receipt["result"])
    for provider in result["providers"]:
        try:
            _selector(provider["provider_system"], provider["provider_id"])
        except NetworkProviderReadError:
            raise NetworkProviderReadUnavailable("Pinned provider identity is unavailable") from None
        name, npi = provider["display_name"], provider["npi"]
        if name is not None and (type(name) is not str or len(name) > 512 or "\x00" in name):
            raise NetworkProviderReadUnavailable("Pinned provider display is unavailable")
        if npi is not None and (type(npi) is not int or not is_valid_npi(str(npi))):
            raise NetworkProviderReadUnavailable("Pinned provider NPI is unavailable")
        if provider["provider_system"] == "npi" and str(npi) != provider["provider_id"]:
            raise NetworkProviderReadUnavailable("Pinned provider NPI differs from its identity")
        for location in provider["locations"]:
            _location(location)
    return result


@dataclass(frozen=True)
class NetworkOfficeSelection:
    """Exact office and complete bounded spatial selection on the retained heap."""

    location_id: str | None = None
    lat: float | None = None
    long: float | None = None
    radius_miles: float | None = None
    min_address_sources: int = 0

    def arguments(self):
        """Validate typed callers before converting UUID and spatial SQL parameters."""
        return office_selection(self.location_id, self.lat, self.long, self.radius_miles, self.min_address_sources)


def office_selection(location_id=None, lat=None, long=None, radius_miles=None, min_address_sources=0):
    """Validate exact office and bounded spatial selectors before any database read."""
    if type(min_address_sources) is not int or not 0 <= min_address_sources <= 2147483647:
        raise NetworkProviderReadError("Office source threshold is invalid")
    if location_id is not None:
        try:
            if type(location_id) is not str or str(UUID(location_id)) != location_id or UUID(location_id).int == 0:
                raise ValueError
        except ValueError, TypeError, AttributeError:
            raise NetworkProviderReadError("Office identity is invalid") from None
    coordinates = (lat, long, radius_miles)
    if any(value is not None for value in coordinates) and (
        not all(type(value) in (int, float) and math.isfinite(value) for value in coordinates)
        or not -90 <= lat <= 90
        or not -180 <= long <= 180
        or not 0.01 <= radius_miles <= 100
    ):
        raise NetworkProviderReadError("Office spatial selection is invalid")
    return (UUID(location_id) if location_id is not None else None, *coordinates)


def _office_arguments(office_filters):
    if office_filters is None:
        office_filters = NetworkOfficeSelection()
    if type(office_filters) is not NetworkOfficeSelection:
        raise NetworkProviderReadError("Office selection is invalid")
    return office_filters.arguments()


async def _require_provider_read_transaction(connection):
    if not connection.is_in_transaction() or await connection.fetchval(
        "SELECT current_setting('transaction_isolation')"
    ) not in ("repeatable read", "serializable"):
        raise NetworkProviderReadError("Canonical providers require a caller-owned repeatable transaction")


async def _read(connection, scope, selector, limit, offset, control_schema, office_filters, *, min_address_sources=0):
    """Read one exact provider page before shaping its bounded public response."""
    _arguments(scope, limit, offset)
    try:
        await _require_provider_read_transaction(connection)
        from process.network_published_api_pin import active_published_api_pin

        current = active_published_api_pin(connection, scope.manifest, control_schema=control_schema)
        if current is None:
            current = await resolve_network_serving_manifest(
                connection, generation_id=scope.manifest.generation_id, control_schema=control_schema, api_read=True
            )
        if current != scope.manifest:
            raise NetworkProviderReadUnavailable("Canonical provider generation differs from the pinned scope")
        query, address_arguments = _provider_query(
            current, scope.network_ids, selector, office_filters, min_address_sources
        )
        receipt = await connection.fetchrow(
            query,
            "{" + ",".join(str(network_id) for network_id in scope.network_ids) + "}",
            *selector,
            limit,
            offset,
            MAX_LOCATIONS,
            MAX_RESPONSE_BYTES,
            current.generation_id,
            *office_filters,
            *address_arguments,
        )
        return _result(receipt)
    except NetworkProviderReadError:
        raise
    except (
        NetworkServingReadUnavailable,
        asyncpg.PostgresError,
        ValueError,
        TypeError,
        KeyError,
    ):
        raise NetworkProviderReadUnavailable("Canonical providers are unavailable") from None


def _provider_query(manifest, network_ids, selector, office_filters, minimum):
    """Compile independently bound discovery and page projections under the published capability."""
    membership_page = (
        manifest.supports_membership_pages
        and selector == (None, None)
        and all(filter_value is None for filter_value in office_filters)
    )
    detail_lookup = (
        manifest.compact_layout is not None and manifest.supports_location_lookup and selector != (None, None)
    )
    if minimum and membership_page:
        if type(network_ids) is not tuple or any(type(network_id) is not int for network_id in network_ids):
            raise NetworkProviderReadError("Canonical provider selection is invalid")
        selected_network_ids = parse_canonical_network_ids(",".join(str(network_id) for network_id in network_ids))
        if selected_network_ids != network_ids:
            raise NetworkProviderReadError("Canonical provider selection is invalid")
    addresses, arguments = _provider_addresses(
        manifest,
        network_ids,
        location_lookup=membership_page or detail_lookup,
        columns=_PAGE_ADDRESS_COLUMNS if detail_lookup or (minimum and membership_page) else None,
    )
    query = _PROVIDERS_SQL
    if membership_page:
        query = _MEMBERSHIP_PROVIDERS_SQL
    elif detail_lookup:
        query = _PROVIDER_DETAIL_SQL
    sql_fields_by_name = {"namespace": _identifier(manifest.schema_name), "addresses": addresses}
    if minimum and membership_page:
        qualifying, qualification_arguments = _provider_addresses(
            manifest,
            None,
            first_parameter=13 + len(arguments),
            visible_membership_sql="TRUE",
            columns=_QUALIFYING_ADDRESS_COLUMNS,
        )
        sql_fields_by_name.update(
            qualifying_addresses=qualifying,
            source_predicate=address_source_filter_sql("address", minimum),
            selected_network_ids="ARRAY[" + ",".join(map(str, selected_network_ids)) + "]::integer[]",
        )
        arguments += qualification_arguments
        query = _SOURCE_MEMBERSHIP_PROVIDERS_SQL
    elif minimum:
        query = _source_filtered_provider_sql(query, minimum, selector != (None, None))
    sql_fields_by_name["address_membership"] = (
        "TRUE" if manifest.compact_layout is not None else "canonical_network_ids && $1::text::integer[]"
    )
    return query.format(**sql_fields_by_name), arguments


def _source_filtered_provider_sql(query, minimum, detail):
    """Keep integrity and authorized detail identity independent of address eligibility."""
    if detail:
        predicate = address_source_filter_sql("eligible", minimum)
        query = query.replace("* FROM eligible\n", f"* FROM eligible WHERE {predicate}\n", 1)
        query = query.replace(
            "count(*) AS location_count,count(DISTINCT entity_name)<=1 AND count(DISTINCT npi)<=1 AS consistent\n"
            "  FROM offices GROUP BY provider_system,provider_id",
            "(SELECT count(*) FROM offices office WHERE office.provider_system=eligible.provider_system "
            "AND office.provider_id=eligible.provider_id) AS location_count,"
            "count(DISTINCT entity_name)<=1 AND count(DISTINCT npi)<=1 AS consistent\n"
            "  FROM eligible GROUP BY provider_system,provider_id",
            1,
        )
    else:
        predicate = address_source_filter_sql("selected", minimum)
        query = query.replace("WHERE ($9::uuid", f"WHERE {predicate} AND ($9::uuid", 1)
    return query


def _provider_addresses(
    manifest, network_ids, *, location_lookup=False, first_parameter=13, visible_membership_sql=None, columns=None
):
    if manifest.compact_layout is None:
        return "ONLY " + _identifier(manifest.schema_name) + ".entity_address_unified", ()
    from api.network_compact_address import compact_address_native_sql

    return compact_address_native_sql(
        manifest.compact_layout,
        first_parameter=first_parameter,
        network_ids=network_ids,
        visible_membership_sql=visible_membership_sql,
        location_lookup=location_lookup,
        columns=columns,
    )


async def read_network_provider_page(
    connection,
    scope,
    *,
    limit=50,
    offset=0,
    control_schema=None,
    office_filters=None,
) -> dict:
    """Count and page exact provider identities with bounded bulk reads and no live-head fallback.

    The caller owns a repeatable-read or serializable transaction. Every returned
    office belongs to the selected network in the exact eligible closed manifest.
    More than 100 selected offices for one returned provider fails the whole page;
    offices are never silently truncated, inferred, enriched or merged by NPI.
    """
    filters = _office_arguments(office_filters)
    return await _read(
        connection,
        scope,
        (None, None),
        limit,
        offset,
        control_schema,
        filters,
        min_address_sources=office_filters.min_address_sources if office_filters is not None else 0,
    )


async def read_network_provider_detail(
    connection,
    scope,
    *,
    provider_system,
    provider_id,
    control_schema=None,
    office_filters=None,
) -> dict:
    """Retain an authorized identity when its offices fail the optional source minimum."""
    _selector(provider_system, provider_id)
    filters = _office_arguments(office_filters)
    result = await _read(
        connection,
        scope,
        (provider_system, provider_id),
        1,
        0,
        control_schema,
        filters,
        min_address_sources=office_filters.min_address_sources if office_filters is not None else 0,
    )
    if not result["providers"]:
        raise NetworkProviderNotFound("Provider has no office in this canonical network")
    return {"generation_id": result["generation_id"], "provider": result["providers"][0]}
