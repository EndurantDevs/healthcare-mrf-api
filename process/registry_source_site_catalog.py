# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read exact office choices from one eligible, physically closed generation."""

from __future__ import annotations

import json

import asyncpg

from process.network_address_projection import _identifier
from process.network_serving_read import NetworkServingReadUnavailable, resolve_network_serving_manifest
from process.provider_directory_projection_fhir_values import is_valid_npi
from process.registry_site_binding_store import validated_site_binding_fields

MAX_RESPONSE_BYTES = 1024 * 1024
_DISPLAY_LIMITS = {
    "entity_name": 512,
    "first_line": 512,
    "second_line": 512,
    "city_name": 128,
    "state_name": 64,
    "postal_code": 32,
    "country_code": 64,
}


class RegistrySourceSiteCatalogError(ValueError):
    """The exact source-office selector or caller transaction is invalid."""


class RegistrySourceSiteCatalogUnavailable(RuntimeError):
    """The whole retained source-office result could not be verified."""


_SITES_SQL = """WITH bindings AS MATERIALIZED (
  SELECT binding.*,address.location_key AS matched_key
  FROM {namespace}.provider_location_binding binding
  LEFT JOIN {namespace}.entity_address_unified address
    ON (address.location_key,address.entity_type,address.entity_id)
      =(binding.location_key,binding.entity_type,binding.entity_id)
  WHERE binding.provider_system=$1 AND binding.provider_id=$2
), integrity AS (
  SELECT count(*)=count(DISTINCT location_id)
    AND count(*)=count(DISTINCT location_key)
    AND coalesce(bool_and(matched_key IS NOT NULL AND location_id<>'00000000-0000-0000-0000-000000000000'::uuid
      AND location_key ~ '^[0-9a-f]{{64}}$'),true) AS valid FROM bindings
), page AS MATERIALIZED (
  SELECT * FROM bindings ORDER BY location_id,location_key LIMIT $3 OFFSET $4
), sites AS (
  SELECT page.location_id,jsonb_build_object('fields',jsonb_build_object(
      'source_generation',$5::bigint,'provider_system',page.provider_system,'provider_id',page.provider_id,
      'location_id',page.location_id::text,'location_key',page.location_key,
      'address_row_sha256',encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex')),
    'display',jsonb_build_object('entity_name',address.entity_name,'first_line',address.first_line,
      'second_line',address.second_line,'city_name',address.city_name,'state_name',address.state_name,
      'postal_code',address.postal_code,'country_code',address.country_code)) AS document
  FROM page JOIN {namespace}.entity_address_unified address
    ON (address.location_key,address.entity_type,address.entity_id)
      =(page.location_key,page.entity_type,page.entity_id)
  ORDER BY page.location_id,page.location_key LIMIT ($3-1)
), response AS MATERIALIZED (
  SELECT jsonb_build_object('source_generation',$5::bigint,'provider_system',$1::text,'provider_id',$2::text,
    'limit',($3-1),'offset',$4::integer,'has_more',(SELECT count(*) FROM page)>($3-1),
    'sites',coalesce((SELECT jsonb_agg(document ORDER BY location_id) FROM sites),'[]'::jsonb))::text AS result
)
SELECT integrity.valid,octet_length(response.result)<=$6 AS bounded,
  CASE WHEN integrity.valid AND octet_length(response.result)<=$6 THEN response.result END AS result
FROM integrity,response"""


def _validate_selector(generation_id, provider_system, provider_id, limit, offset):
    if (
        type(generation_id) is not int
        or not 0 < generation_id <= 9223372036854775807
        or type(provider_system) is not str
        or provider_system not in ("npi", "provider_directory")
        or type(provider_id) is not str
        or not 1 <= len(provider_id) <= 128
        or provider_id.strip() != provider_id
        or not provider_id.isprintable()
        or (provider_system == "npi" and not is_valid_npi(provider_id))
        or type(limit) is not int
        or not 1 <= limit <= 100
        or type(offset) is not int
        or not 0 <= offset <= 1_000_000
    ):
        raise RegistrySourceSiteCatalogError("Source office selection is invalid")


def _verified_result(receipt):
    if receipt["valid"] is not True or receipt["bounded"] is not True:
        raise RegistrySourceSiteCatalogUnavailable("Source offices are inconsistent or exceed the response bound")
    result_by_field = json.loads(receipt["result"])
    for site in result_by_field["sites"]:
        validated_site_binding_fields(site["fields"])
        display_by_field = site["display"]
        if set(display_by_field) != set(_DISPLAY_LIMITS) or any(
            value is not None and (type(value) is not str or len(value) > _DISPLAY_LIMITS[field] or "\x00" in value)
            for field, value in display_by_field.items()
        ):
            raise RegistrySourceSiteCatalogUnavailable("Source office display is unavailable")
    return result_by_field


async def read_registry_source_sites(
    connection,
    *,
    generation_id,
    provider_system,
    provider_id,
    limit=50,
    offset=0,
    control_schema=None,
) -> dict:
    """Read exact bindings with three SELECTs in a caller-owned pinned transaction.

    Resolve the explicit eligible generation once, then validate all bindings
    for this provider before returning a bounded page. Hashes cover complete
    native address rows; display never includes network arrays or physical
    coordinates. A prior field hash is rechecked by the site-adoption service.
    Protected ownership and native ACL closure supply the immutability boundary;
    the retained manifest does not contain an independent address-content hash.
    No live-head fallback, writes, locks, transaction creation or commit occurs.
    """
    _validate_selector(generation_id, provider_system, provider_id, limit, offset)
    if not connection.is_in_transaction():
        raise RegistrySourceSiteCatalogError("Source offices require a caller-owned repeatable transaction")
    try:
        if await connection.fetchval("SELECT current_setting('transaction_isolation')") not in (
            "repeatable read",
            "serializable",
        ):
            raise RegistrySourceSiteCatalogError("Source offices require a caller-owned repeatable transaction")
        pinned_manifest = await resolve_network_serving_manifest(
            connection, generation_id=generation_id, control_schema=control_schema
        )
        receipt = await connection.fetchrow(
            _SITES_SQL.format(namespace=_identifier(pinned_manifest.schema_name)),
            provider_system,
            provider_id,
            limit + 1,
            offset,
            pinned_manifest.generation_id,
            MAX_RESPONSE_BYTES,
        )
        return _verified_result(receipt)
    except RegistrySourceSiteCatalogError:
        raise
    except NetworkServingReadUnavailable, asyncpg.PostgresError, ValueError, TypeError, KeyError:
        raise RegistrySourceSiteCatalogUnavailable("Retained source offices are unavailable") from None
