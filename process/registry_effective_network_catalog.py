# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read native imported catalogue data alongside retained manual overrides."""

import json
import re
from contextlib import contextmanager
from contextvars import ContextVar

from db.plan_release_storage import storage_identifier
from process.registry_binding_override_sql import binding_override_identity_sql

CATALOG_GENERATION = "fhir_catalog"
_READ_RECORDS = ContextVar("effective_network_catalog_records", default=None)
_READ_NETWORKS = ContextVar("effective_network_catalog_networks", default=None)


def _generation(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("registry_effective_catalog_generation_invalid")
    return value


def current_catalog_records_sql():
    """Return only the read projection resolved in the caller's pinned snapshot."""
    return _READ_RECORDS.get()


def current_catalog_networks_sql():
    """Return the same snapshot's indexable network identity relation."""
    return _READ_NETWORKS.get()


@contextmanager
def effective_catalog_read_scope(records, networks=None):
    """Share one named catalogue projection with synchronous address SQL builders."""
    token = _READ_RECORDS.set(records)
    networks_token = _READ_NETWORKS.set(networks)
    try:
        yield records
    finally:
        _READ_NETWORKS.reset(networks_token)
        _READ_RECORDS.reset(token)


def apply_effective_catalog_sql(statement, records, control_schema):
    """Replace only the fixed manual read relation, leaving every policy predicate intact."""
    if records is None:
        return statement
    relation = storage_identifier(control_schema) + ".registry_approved_record"
    return re.sub(r"(?:ONLY )?" + re.escape(relation), lambda _match: records, statement)


async def _catalog_coordinates(connection, manifest, control_schema):
    """Resolve an immutable named snapshot at the manifest's exact manual revision."""
    generation = manifest.source_generations.get(CATALOG_GENERATION)
    if generation is None:
        return None
    _generation(generation)
    if not connection.is_in_transaction():
        raise ValueError("registry_effective_catalog_transaction_required")
    control = storage_identifier(control_schema)
    snapshot = await connection.fetchrow(
        f"SELECT schema_name,metadata_approved_revision FROM ONLY {control}.registry_fhir_catalog_snapshot "
        "WHERE metadata_generation=$1 AND published_at IS NOT NULL",
        generation,
    )
    if snapshot is None or not 0 <= snapshot["metadata_approved_revision"] <= manifest.approved_custom_revision:
        raise ValueError("registry_effective_catalog_snapshot_changed")
    if (
        type(snapshot["schema_name"]) is not str
        or re.fullmatch(r"fhir_catalog_[0-9a-f]{32}", snapshot["schema_name"]) is None
    ):
        raise ValueError("registry_effective_catalog_snapshot_invalid")
    catalog = storage_identifier(snapshot["schema_name"])
    return control, catalog, generation, manifest.approved_custom_revision


async def effective_catalog_sql(connection, manifest, control_schema):
    """Read imported metadata with exact retained manual overrides and source bindings."""
    coordinates = await _catalog_coordinates(connection, manifest, control_schema)
    if coordinates is None:
        return None
    control, catalog, generation, revision = coordinates
    ctes = _catalog_ctes(control, catalog, generation, manifest.approved_custom_revision)
    if (
        await connection.fetchval(
            "WITH "
            + ctes
            + """ SELECT EXISTS(SELECT FROM overrides o WHERE conflicting
          OR NOT archived AND NOT EXISTS(SELECT FROM manual n
            WHERE n.record_kind='network' AND n.record_key=o.network_id::text))"""
        )
        is not False
    ):
        raise ValueError("registry_effective_catalog_binding_conflict")
    return (
        "(WITH "
        + ctes
        + ","
        + _record_ctes(catalog, generation, manifest.approved_custom_revision)
        + " SELECT * FROM effective_records)"
    )


async def effective_catalog_networks_sql(connection, manifest, control_schema):
    """Keep correlated provider and geo eligibility on the native network primary key."""
    coordinates = await _catalog_coordinates(connection, manifest, control_schema)
    if coordinates is None:
        return None
    control, catalog, generation, revision = coordinates
    return f"""(SELECT m.record_key::integer AS network_id,(m.record_json->>'archived')::boolean AS archived,
      m.approved_revision FROM ONLY {control}.registry_approved_record m
      WHERE m.approved_revision={revision} AND m.record_kind='network'
        AND jsonb_typeof(m.record_json->'network_id')='number' AND m.record_json->>'network_id'=m.record_key
        AND jsonb_typeof(m.record_json->'archived')='boolean'
      UNION ALL SELECT n.network_id,n.archived,{revision}::bigint FROM ONLY {catalog}.registry_fhir_catalog_network n
      WHERE n.metadata_generation='{generation}' AND NOT EXISTS(
        SELECT FROM ONLY {control}.registry_approved_record m WHERE m.approved_revision={revision}
          AND m.record_kind='network' AND m.record_key=n.network_id::text))"""


def _catalog_ctes(control, catalog, generation, revision):
    if type(revision) is not int or not 0 <= revision < 2**63:
        raise ValueError("registry_effective_catalog_revision_invalid")
    override_identity = binding_override_identity_sql("m.record_json", "b.document_json", "b.source_aliases")
    return f"""manual AS MATERIALIZED (
      SELECT approved_revision,record_kind,record_key,record_revision,custom_revision,record_json
      FROM ONLY {control}.registry_approved_record WHERE approved_revision={revision}
    ), imported AS MATERIALIZED (
      SELECT b.*,s.descriptor_json->'source_aliases' AS source_aliases
      FROM ONLY {catalog}.registry_fhir_catalog_binding b
      LEFT JOIN ONLY {catalog}.registry_fhir_catalog_source s
        ON s.source_generation=split_part(b.document_json->>'evidence_id',':',1)
      WHERE b.metadata_generation='{generation}'
    ), manual_matches AS MATERIALIZED (
      SELECT b.binding_id,m.record_json,m.custom_revision,
        max(m.custom_revision) OVER(PARTITION BY b.binding_id) AS latest
      FROM imported b JOIN manual m ON m.record_kind='network_binding' AND {override_identity}
    ), overrides AS MATERIALIZED (
      SELECT binding_id,min((record_json->>'network_id')::integer) AS network_id,
        bool_or((record_json->>'archived')::boolean) AS archived,
        count(DISTINCT (record_json->>'network_id',record_json->>'archived'))>1 AS conflicting
      FROM manual_matches WHERE custom_revision=latest GROUP BY binding_id
    ), effective_bindings AS MATERIALIZED (
      SELECT b.*,coalesce(o.network_id,b.network_id) AS effective_network_id
      FROM imported b LEFT JOIN overrides o USING(binding_id)
      WHERE NOT coalesce(o.archived,false) AND NOT coalesce(o.conflicting,false)
        AND NOT EXISTS(SELECT FROM manual n WHERE n.record_kind='network'
          AND n.record_key=coalesce(o.network_id,b.network_id)::text AND n.record_json->'archived'='true'::jsonb)
    ), catalogue_networks AS MATERIALIZED (
      SELECT n.* FROM ONLY {catalog}.registry_fhir_catalog_network n WHERE metadata_generation='{generation}'
    )"""


def _source_organization_ctes(catalog, revision):
    """Read pinned source-period organizations without creating manual approval records."""
    return f"""aca_metadata_sources AS MATERIALIZED (
      SELECT source_generation,descriptor_json FROM ONLY {catalog}.registry_fhir_catalog_source
      WHERE source_id LIKE 'aca:%'
    ), native_organization_inputs AS MATERIALIZED (
      SELECT company,metadata->'group_record' AS company_group,company->>'company_id' AS company_id,
        (metadata->'resolution'->>'reporting_year')::integer AS reporting_year,source.source_generation
      FROM aca_metadata_sources source
      CROSS JOIN LATERAL (SELECT source.descriptor_json->'input'->'company_catalog' AS metadata) selected
      CROSS JOIN LATERAL (SELECT metadata->'company_record' AS company) named
      WHERE metadata->>'contract'='aca-issuer-company-catalog-v1'
        AND jsonb_typeof(company)='object'
        AND EXISTS(SELECT FROM native_company_networks n WHERE n.company_id=company->>'company_id')
    ), native_company_group_choices AS MATERIALIZED (
      SELECT company_id,CASE WHEN count(DISTINCT coalesce(company_group->>'group_id',''))=1
        THEN min(company_group->>'group_id') END AS group_id FROM native_organization_inputs GROUP BY company_id
    ), native_company_metadata AS MATERIALIZED (
      SELECT DISTINCT ON (n.company_id) n.company,n.company_id,
        CASE WHEN n.company_group->>'group_id'=choice.group_id THEN n.company_group END AS company_group
      FROM native_organization_inputs n JOIN native_company_group_choices choice USING(company_id)
      ORDER BY n.company_id,n.reporting_year DESC NULLS LAST,n.source_generation
    ), native_group_metadata AS MATERIALIZED (
      SELECT DISTINCT ON (company_group->>'group_id') company_group
      FROM native_company_metadata WHERE jsonb_typeof(company_group)='object'
      ORDER BY company_group->>'group_id',company_id
    ), organization_records AS MATERIALIZED (
      SELECT * FROM manual WHERE record_kind IN ('company','group')
      UNION ALL
      SELECT {revision}::bigint,'company',n.company_id,1::bigint,0::bigint,n.company||jsonb_build_object('revision',1)
      FROM native_company_metadata n WHERE NOT EXISTS(SELECT FROM manual m
        WHERE m.record_kind='company' AND m.record_key=n.company_id)
      UNION ALL
      SELECT {revision}::bigint,'group',n.company_group->>'group_id',1::bigint,0::bigint,
        n.company_group||jsonb_build_object('revision',1)
      FROM native_group_metadata n WHERE jsonb_typeof(n.company_group)='object'
        AND NOT EXISTS(SELECT FROM manual m WHERE m.record_kind='group'
          AND m.record_key=n.company_group->>'group_id')
    )"""


def _record_ctes(catalog, generation, revision):
    return f"""native_company_networks AS MATERIALIZED (
      SELECT DISTINCT r.company_id::text AS company_id,b.effective_network_id AS network_id
      FROM ONLY {catalog}.registry_fhir_catalog_relationship r
      JOIN effective_bindings b USING(binding_id)
      WHERE r.metadata_generation='{generation}' AND r.company_id IS NOT NULL
    ), {_source_organization_ctes(catalog, revision)}, company_links AS MATERIALIZED (
      SELECT m.approved_revision,m.record_kind,m.record_key,m.record_revision,m.custom_revision,
        CASE WHEN m.record_json->'archived'='false'::jsonb THEN m.record_json||jsonb_build_object('network_ids',
          coalesce((SELECT jsonb_agg(network_id ORDER BY network_id) FROM (
            SELECT jsonb_array_elements_text(m.record_json->'network_ids')::integer AS network_id
            UNION SELECT n.network_id FROM native_company_networks n WHERE n.company_id=m.record_key) ids),'[]'::jsonb))
        ELSE m.record_json END AS record_json
      FROM manual m WHERE m.record_kind='company_links'
      UNION ALL
      SELECT m.approved_revision,'company_links',m.record_key,m.record_revision,m.custom_revision,
        jsonb_build_object('archived',false,'network_ids',jsonb_agg(n.network_id ORDER BY n.network_id),
          'group_id',(SELECT company_group->>'group_id' FROM native_company_metadata WHERE company_id=m.record_key))
      FROM organization_records m JOIN native_company_networks n ON n.company_id=m.record_key
      WHERE m.record_kind='company' AND NOT EXISTS(SELECT FROM manual link
        WHERE link.record_kind='company_links' AND link.record_key=m.record_key)
      GROUP BY m.approved_revision,m.record_key,m.record_revision,m.custom_revision
    ), effective_records AS (
      SELECT * FROM manual WHERE record_kind NOT IN ('company_links','company','group')
      UNION ALL SELECT * FROM organization_records
      UNION ALL SELECT * FROM company_links
      UNION ALL
      SELECT {revision}::bigint,'network',n.network_id::text,n.revision,0,
        jsonb_build_object('network_id',n.network_id,'display_name',n.display_name,'aliases',n.aliases,
          'archived',n.archived,'benefit_codes',n.benefit_codes)
      FROM catalogue_networks n WHERE NOT EXISTS(SELECT FROM manual m
        WHERE m.record_kind='network' AND m.record_key=n.network_id::text)
      UNION ALL
      SELECT {revision}::bigint,'network_binding',b.binding_id::text,
        (b.document_json->>'revision')::bigint,0,
        (b.document_json-'source_scope')||jsonb_build_object('network_id',b.effective_network_id,
          'source_scope_json',b.document_json->'source_scope','archived',false)
      FROM effective_bindings b WHERE NOT EXISTS(SELECT FROM manual m
        WHERE m.record_kind='network_binding' AND m.record_key=b.binding_id::text)
    )"""


async def pair_network_catalog_generation(
    connection, *, metadata_generation, generation_id, manifest_sha256, approved_revision, control_schema
):
    """Pair catalogue and membership pointers inside the publisher's final transaction."""
    if metadata_generation is None:
        return
    _generation(metadata_generation)
    _generation(manifest_sha256)
    if type(approved_revision) is not int or not 0 <= approved_revision < 2**63:
        raise ValueError("registry_effective_catalog_revision_invalid")
    if not connection.is_in_transaction() or type(generation_id) is not int or generation_id <= 0:
        raise ValueError("registry_effective_catalog_transaction_required")
    control = storage_identifier(control_schema)
    manifest = await connection.fetchrow(
        f"SELECT approved_custom_revision,source_generations,manifest_sha256 FROM ONLY {control}.network_serving_manifest "
        "WHERE generation_id=$1",
        generation_id,
    )
    source_generations = None if manifest is None else manifest["source_generations"]
    source_generations = json.loads(source_generations) if type(source_generations) is str else source_generations
    if (
        manifest is None
        or manifest["approved_custom_revision"] != approved_revision
        or manifest["manifest_sha256"] != manifest_sha256
        or type(source_generations) is not dict
        or source_generations.get(CATALOG_GENERATION) != metadata_generation
    ):
        raise ValueError("registry_effective_catalog_manifest_changed")
    status = await connection.execute(
        f"UPDATE {control}.registry_fhir_catalog_snapshot SET network_generation=$2,manifest_sha256=$3,"
        "published_at=coalesce(published_at,clock_timestamp()) WHERE metadata_generation=$1 AND metadata_approved_revision<=$4",
        metadata_generation,
        generation_id,
        manifest_sha256,
        approved_revision,
    )
    if status != "UPDATE 1":
        raise ValueError("registry_effective_catalog_snapshot_changed")
    status = await connection.execute(
        f"UPDATE {control}.network_serving_control SET metadata_generation=$1 WHERE id=1 AND generation_id=$2",
        metadata_generation,
        generation_id,
    )
    if status != "UPDATE 1":
        raise ValueError("registry_effective_catalog_head_changed")
