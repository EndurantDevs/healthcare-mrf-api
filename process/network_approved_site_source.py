# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Revalidate approved exact source sites in bounded native sets."""

from __future__ import annotations

import json

from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_retained_site_adoption import RetainedSiteAdoptionError, resolve_retained_site_adoptions

_SITE_CTE = """WITH active AS MATERIALIZED (
  SELECT * FROM {namespace}.registry_approved_record WHERE approved_revision=$1
    AND record_json->'archived'='false'::jsonb
), pairs AS MATERIALIZED (
  SELECT DISTINCT member.provider_system,member.provider_id,member.location_id
  FROM active head CROSS JOIN LATERAL jsonb_to_recordset(
    CASE WHEN head.record_kind='membership' THEN head.record_json->'memberships_json' ELSE '[]'::jsonb END)
    AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), sites AS MATERIALIZED (
  SELECT binding.record_key,binding.record_json FROM active binding JOIN pairs pair
    ON (binding.record_json->>'provider_system',binding.record_json->>'provider_id',binding.record_json->>'location_id')
      =(pair.provider_system,pair.provider_id,pair.location_id::text)
  WHERE binding.record_kind='site_binding'
)
"""


async def _page(connection, namespace, revision, generation, offset):
    page = await connection.fetchrow(
        _SITE_CTE.format(namespace=namespace)
        + """, page AS MATERIALIZED (
          SELECT * FROM sites WHERE (record_json->>'source_generation')::bigint=$2
            ORDER BY record_key OFFSET $3 LIMIT $4
        ), bounds AS (
          SELECT count(*) AS count,coalesce(sum(octet_length(record_json::text)+2),0)+2<=$5 AS bounded FROM page
        ) SELECT bounds.*,CASE WHEN bounded THEN
          coalesce((SELECT jsonb_agg(record_json ORDER BY record_key) FROM page),'[]'::jsonb)::text END AS records
        FROM bounds""",
        revision,
        generation,
        offset,
        MAX_ROWS,
        MAX_INPUT_BYTES,
    )
    if not page["bounded"]:
        raise RetainedSiteAdoptionError("Approved source-site page exceeds its bound")
    return json.loads(page["records"])


def _verify_records(binding_records, receipt):
    document = receipt.as_dict()
    proven_by_key = {
        (row["provider_system"], row["provider_id"], row["location_id"]): row for row in document["records"]
    }
    for binding_record in binding_records:
        identity_values = tuple(binding_record[field] for field in ("provider_system", "provider_id", "location_id"))
        expected_by_name = {**document, "records": [proven_by_key[identity_values]]}
        if binding_record["source_receipt_json"] != expected_by_name:
            raise RetainedSiteAdoptionError("Approved source-site proof differs from retained evidence")


async def verify_approved_site_sources(connection, *, approved_revision, control_schema):
    """Return verified sets; never query once per provider or location.

    The approved map supplies exact six-field selections and sealed receipts.
    Each retained generation is resolved again, and each bounded page uses the
    native identity validator before returning its physical retained_source coordinates.
    """
    from process.network_address_projection import _identifier

    namespace = _identifier(control_schema)
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        raise RetainedSiteAdoptionError("Approved retained_source sites require repeatable read")
    scopes = await connection.fetch(
        _SITE_CTE.format(namespace=namespace)
        + """
        SELECT (record_json->>'source_generation')::bigint AS generation,count(*) AS count
        FROM sites GROUP BY generation ORDER BY generation""",
        approved_revision,
    )
    receipts = []
    for scope in scopes:
        retained_source = await resolve_network_serving_manifest(
            connection, generation_id=scope["generation"], control_schema=control_schema
        )
        for offset in range(0, scope["count"], MAX_ROWS):
            binding_records = await _page(connection, namespace, approved_revision, scope["generation"], offset)
            selection_rows = [
                {
                    field: binding_record[field]
                    for field in ("provider_system", "provider_id", "location_id", "location_key", "address_row_sha256")
                }
                for binding_record in binding_records
            ]
            receipt = await resolve_retained_site_adoptions(
                connection,
                retained_source,
                json.dumps(selection_rows, separators=(",", ":")).encode(),
                control_schema=control_schema,
            )
            _verify_records(binding_records, receipt)
            receipts.append(receipt)
    return tuple(receipts)


async def copy_approved_site_rows(connection, receipts, target_schema):
    """Reuse a pinned address with the same entity/site key, or copy its retained row."""
    from process.network_address_projection import _identifier

    target = _identifier(target_schema)
    for receipt in receipts:
        source = _identifier(receipt.schema_name)
        records = json.dumps(receipt.as_dict()["records"], separators=(",", ":"))
        selected = """SELECT * FROM jsonb_to_recordset($1::jsonb) AS site(
          provider_system text,provider_id text,location_id uuid,location_key text,
          entity_type text,entity_id text,address_row_sha256 text)"""
        await connection.execute(
            f"INSERT INTO {target}.entity_address_unified SELECT address.* FROM {source}.entity_address_unified address "
            f"JOIN ({selected}) site USING(location_key,entity_type,entity_id) ON CONFLICT(location_key) DO NOTHING",
            records,
        )
        await connection.execute(
            f"INSERT INTO {target}.provider_location_binding "
            f"SELECT provider_system,provider_id,location_id,location_key,entity_type,entity_id FROM ({selected}) site",
            records,
        )
