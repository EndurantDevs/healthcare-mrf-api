# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read provider identities and exact offices from a pinned canonical network."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from uuid import UUID

import asyncpg

from api.network_address_scope import NetworkAddressReadScope
from process.network_address_projection import _identifier
from process.network_serving_read import (
    NetworkServingReadUnavailable,
    PinnedNetworkServingManifest,
    resolve_network_serving_manifest,
)
from process.provider_directory_projection_fhir_values import is_valid_npi

MAX_RESPONSE_BYTES = 1024 * 1024
MAX_LOCATIONS = 100
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


_PROVIDERS_SQL = """WITH selected_addresses AS MATERIALIZED (
  SELECT location_key,entity_type,entity_id,entity_name,npi,first_line,second_line,city_name,state_name,
    postal_code,country_code,lat,long FROM {namespace}.entity_address_unified
  WHERE canonical_network_ids && $1::text::integer[]
), selected_memberships AS MATERIALIZED (
  SELECT DISTINCT provider_system,provider_id,location_id FROM {namespace}.network_membership
  WHERE network_id=ANY($1::text::integer[])
    AND ($2::text IS NULL OR (provider_system,provider_id)=($2,$3))
), selected AS MATERIALIZED (
  SELECT member.*,binding.location_key,address.location_key AS matched_key,address.entity_name,address.npi,
    address.first_line,address.second_line,address.city_name,address.state_name,address.postal_code,
    address.country_code,address.lat,address.long
  FROM selected_memberships member LEFT JOIN {namespace}.provider_location_binding binding
    ON (binding.provider_system,binding.provider_id,binding.location_id)
      =(member.provider_system,member.provider_id,member.location_id)
  LEFT JOIN selected_addresses address ON (address.location_key,address.entity_type,address.entity_id)
    =(binding.location_key,binding.entity_type,binding.entity_id)
), selected_bindings AS MATERIALIZED (
  SELECT binding.* FROM selected_addresses address JOIN {namespace}.provider_location_binding binding
    ON address.location_key=binding.location_key
  WHERE $2::text IS NULL OR (binding.provider_system,binding.provider_id)=($2,$3)
    OR EXISTS(SELECT 1 FROM selected WHERE selected.location_key=address.location_key)
), integrity AS (
  SELECT count(*)=count(DISTINCT (provider_system,provider_id,location_id))
    AND count(*)=count(DISTINCT location_key)
    AND coalesce(bool_and(matched_key IS NOT NULL AND provider_system IN ('npi','manual','provider_directory')
      AND location_id<>'00000000-0000-0000-0000-000000000000'::uuid
      AND location_key ~ '^[0-9a-f]{{64}}$'),true)
    AND (SELECT count(*)=count(DISTINCT location_key) FROM selected_bindings)
    AND NOT EXISTS(SELECT 1 FROM selected_bindings binding LEFT JOIN selected_memberships member
      USING(provider_system,provider_id,location_id) WHERE member.location_id IS NULL)
    AND ($2::text IS NOT NULL OR NOT EXISTS(SELECT 1 FROM selected_addresses address
      LEFT JOIN selected_bindings binding ON (binding.location_key,binding.entity_type,binding.entity_id)
        =(address.location_key,address.entity_type,address.entity_id) WHERE binding.location_key IS NULL)) AS valid
  FROM selected
), eligible AS MATERIALIZED (
  SELECT * FROM selected
  WHERE ($9::uuid IS NULL OR location_id=$9)
    AND ($10::double precision IS NULL OR CASE
      WHEN lat BETWEEN -90 AND 90 AND long BETWEEN -180 AND 180 THEN
        ST_DWithin(Geography(ST_MakePoint(long::double precision,lat::double precision)),
          Geography(ST_MakePoint($11::double precision,$10::double precision)),$12::double precision*1609.34)
      ELSE false END)
), identities AS MATERIALIZED (
  SELECT provider_system,provider_id,min(entity_name) AS display_name,min(npi) AS npi,
    count(*) AS location_count,count(DISTINCT entity_name)<=1 AND count(DISTINCT npi)<=1 AS consistent
  FROM eligible GROUP BY provider_system,provider_id
), page AS MATERIALIZED (
  SELECT * FROM identities ORDER BY provider_system COLLATE "C",provider_id COLLATE "C" LIMIT $4 OFFSET $5
), locations AS MATERIALIZED (
  SELECT selected.provider_system,selected.provider_id,selected.location_id,
    jsonb_build_object('location_id',selected.location_id::text,'location_key',selected.location_key,
      'first_line',first_line,'second_line',second_line,'city_name',city_name,'state_name',state_name,
      'postal_code',postal_code,'country_code',country_code,'lat',lat,'long',long) AS document
  FROM eligible selected JOIN page USING(provider_system,provider_id) WHERE page.consistent AND page.location_count<=$6
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
), result AS (
  SELECT jsonb_build_object('generation_id',$8::bigint,'total_count',(SELECT count(*) FROM identities),
    'limit',$4::integer,'offset',$5::integer,'has_more',(SELECT count(*) FROM identities)>$5+$4,
    'providers',coalesce((SELECT jsonb_agg(document ORDER BY provider_system COLLATE "C",provider_id COLLATE "C")
      FROM providers),'[]'::jsonb))::text AS document
)
SELECT integrity.valid AND bounds.valid AS valid,bounds.bounded AND octet_length(result.document)<=$7 AS bounded,
  CASE WHEN integrity.valid AND bounds.valid AND bounds.bounded AND octet_length(result.document)<=$7
    THEN result.document END AS result FROM integrity,bounds,result"""


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

    def arguments(self):
        """Validate typed callers before converting UUID and spatial SQL parameters."""
        return office_selection(self.location_id, self.lat, self.long, self.radius_miles)


def office_selection(location_id=None, lat=None, long=None, radius_miles=None):
    """Validate exact office and bounded spatial selectors before any database read."""
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


async def _read(connection, scope, selector, limit, offset, control_schema, office_filters):
    _arguments(scope, limit, offset)
    if not connection.is_in_transaction():
        raise NetworkProviderReadError("Canonical providers require a caller-owned repeatable transaction")
    try:
        if await connection.fetchval("SELECT current_setting('transaction_isolation')") not in (
            "repeatable read",
            "serializable",
        ):
            raise NetworkProviderReadError("Canonical providers require a caller-owned repeatable transaction")
        current = await resolve_network_serving_manifest(
            connection, generation_id=scope.manifest.generation_id, control_schema=control_schema
        )
        if current != scope.manifest:
            raise NetworkProviderReadUnavailable("Canonical provider generation differs from the pinned scope")
        receipt = await connection.fetchrow(
            _PROVIDERS_SQL.format(namespace=_identifier(current.schema_name)),
            "{" + ",".join(str(network_id) for network_id in scope.network_ids) + "}",
            *selector,
            limit,
            offset,
            MAX_LOCATIONS,
            MAX_RESPONSE_BYTES,
            current.generation_id,
            *office_filters,
        )
        return _result(receipt)
    except NetworkProviderReadError:
        raise
    except NetworkServingReadUnavailable, asyncpg.PostgresError, ValueError, TypeError, KeyError:
        raise NetworkProviderReadUnavailable("Canonical providers are unavailable") from None


async def read_network_provider_page(
    connection,
    scope,
    *,
    limit=50,
    offset=0,
    control_schema=None,
    office_filters=None,
) -> dict:
    """Count and page exact provider identities with three reads and no live-head fallback.

    The caller owns a repeatable-read or serializable transaction. Every returned
    office belongs to the selected network in the exact eligible closed manifest.
    More than 100 selected offices for one returned provider fails the whole page;
    offices are never silently truncated, inferred, enriched or merged by NPI.
    """
    filters = _office_arguments(office_filters)
    return await _read(connection, scope, (None, None), limit, offset, control_schema, filters)


async def read_network_provider_detail(
    connection,
    scope,
    *,
    provider_system,
    provider_id,
    control_schema=None,
    office_filters=None,
) -> dict:
    """Read one exact identity, rejecting identities without a selected office."""
    _selector(provider_system, provider_id)
    filters = _office_arguments(office_filters)
    result = await _read(connection, scope, (provider_system, provider_id), 1, 0, control_schema, filters)
    if not result["providers"]:
        raise NetworkProviderNotFound("Provider has no office in this canonical network")
    return {"generation_id": result["generation_id"], "provider": result["providers"][0]}
