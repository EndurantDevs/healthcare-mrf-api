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


def _address_overlap_sql(source_namespace, target_namespace, reproject_network_ids):
    """Keep source rows exact while a PTG artifact awaits mandatory array projection."""
    address_rows_sql = f"JOIN {target_namespace}.entity_address_unified address USING(location_key)"
    address_conflict_sql = """(address.entity_type,address.entity_id) IS DISTINCT FROM (site.entity_type,site.entity_id)
      OR encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex') IS DISTINCT FROM site.address_row_sha256"""
    if reproject_network_ids:
        # This PTG path retains the original complete row proof. The pipeline
        # rebuilds canonical_network_ids and validates its exact membership set.
        address_rows_sql = f"""LEFT JOIN {source_namespace}.entity_address_unified retained USING(location_key)
          LEFT JOIN {target_namespace}.entity_address_unified address USING(location_key)"""
        address_conflict_sql = """retained.location_key IS NULL
          OR (retained.entity_type,retained.entity_id) IS DISTINCT FROM (site.entity_type,site.entity_id)
          OR encode(sha256(convert_to(to_jsonb(retained)::text,'UTF8')),'hex') IS DISTINCT FROM site.address_row_sha256
          OR (address.location_key IS NOT NULL AND
            (to_jsonb(address)-'canonical_network_ids') IS DISTINCT FROM
            (to_jsonb(retained)-'canonical_network_ids'))"""
    return address_rows_sql, address_conflict_sql


async def _address_copy_columns(connection, source_namespace, target_namespace):
    """Match every logical field once; native column positions may differ."""
    from process.network_address_projection import _identifier

    columns = await connection.fetch(
        """SELECT relation.position,attribute.attname,attribute.atttypid,attribute.atttypmod,
          attribute.attcollation,attribute.attnotnull,
          attribute.attgenerated::text AS attgenerated,attribute.attidentity::text AS attidentity
        FROM unnest(ARRAY[to_regclass($1),to_regclass($2)]) WITH ORDINALITY relation(oid,position)
        JOIN pg_attribute attribute ON attribute.attrelid=relation.oid
        WHERE attribute.attnum>0 AND NOT attribute.attisdropped ORDER BY relation.position,attribute.attnum""",
        source_namespace + ".entity_address_unified",
        target_namespace + ".entity_address_unified",
    )
    # LIKE omits advisory array dimensions; native array types and typmods must match.
    shapes = [{}, {}]
    for column in columns:
        shapes[column["position"] - 1][column["attname"]] = tuple(
            column[field]
            for field in ("atttypid", "atttypmod", "attcollation", "attnotnull", "attgenerated", "attidentity")
        )
        if column["attgenerated"] or column["attidentity"]:
            raise RetainedSiteAdoptionError("Retained address copy requires ordinary columns")
    if not shapes[0] or shapes[0] != shapes[1]:
        raise RetainedSiteAdoptionError("Retained address copy columns or types differ")
    return tuple(_identifier(name) for name in shapes[1])


async def copy_approved_site_rows(
    connection, receipts, target_schema, *, exact_rows=False, reproject_network_ids=False
):
    """Reuse or copy a pinned address; PTG may reproject only its derived network array."""
    if reproject_network_ids and not exact_rows:
        raise RetainedSiteAdoptionError("Network array reprojection requires exact retained rows")
    from process.network_address_projection import _identifier

    target_namespace = _identifier(target_schema)
    for receipt in receipts:
        source_namespace = _identifier(receipt.schema_name)
        selection_json = json.dumps(receipt.as_dict()["records"], separators=(",", ":"))
        selected = """SELECT * FROM jsonb_to_recordset($1::jsonb) AS site(
          provider_system text,provider_id text,location_id uuid,location_key text,
          entity_type text,entity_id text,address_row_sha256 text)"""
        if exact_rows:
            address_rows_sql, address_conflict_sql = _address_overlap_sql(
                source_namespace, target_namespace, reproject_network_ids
            )
            invalid = await connection.fetchval(
                f"""WITH selected AS MATERIALIZED ({selected}) SELECT
                EXISTS(SELECT 1 FROM selected site {address_rows_sql} WHERE {address_conflict_sql})
                OR EXISTS(SELECT 1 FROM selected site JOIN {target_namespace}.provider_location_binding binding
                  ON (binding.provider_system,binding.provider_id,binding.location_id)=
                     (site.provider_system,site.provider_id,site.location_id) OR binding.location_key=site.location_key
                  WHERE (binding.provider_system,binding.provider_id,binding.location_id,binding.location_key,binding.entity_type,binding.entity_id)
                    IS DISTINCT FROM (site.provider_system,site.provider_id,site.location_id,site.location_key,site.entity_type,site.entity_id))""",
                selection_json,
            )
            if invalid is not False:
                raise RetainedSiteAdoptionError("PTG retained address or binding conflicts with replacement")
        columns = await _address_copy_columns(connection, source_namespace, target_namespace)
        target_columns = ",".join(columns)
        source_columns = ",".join("address." + name for name in columns)
        await connection.execute(
            f"INSERT INTO {target_namespace}.entity_address_unified ({target_columns}) "
            f"SELECT {source_columns} FROM {source_namespace}.entity_address_unified address "
            f"JOIN ({selected}) site USING(location_key,entity_type,entity_id) ON CONFLICT(location_key) DO NOTHING",
            selection_json,
        )
        await connection.execute(
            f"INSERT INTO {target_namespace}.provider_location_binding "
            "(provider_system,provider_id,location_id,location_key,entity_type,entity_id) "
            f"SELECT provider_system,provider_id,location_id,location_key,entity_type,entity_id FROM ({selected}) site"
            + (" ON CONFLICT DO NOTHING" if exact_rows else ""),
            selection_json,
        )
